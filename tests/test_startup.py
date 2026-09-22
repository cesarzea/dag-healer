"""Service startup must require consent, verify readiness and precede data changes."""

from __future__ import annotations

import subprocess
from types import SimpleNamespace

import httpx
import pytest

from dag_healer import demo, startup
from dag_healer.backends.mock import MockBackend
from dag_healer.config import Settings


@pytest.fixture()
def runtime(tmp_path, monkeypatch):
    settings = Settings(root=tmp_path, base_url="http://127.0.0.1:8099")
    (tmp_path / "docker-compose.yml").write_text("services: {}\n")
    state = SimpleNamespace(
        settings=settings, docker=False, api=False, starts=[], prompts=[],
        context="unix:///var/run/docker.sock", compose=True,
    )
    monkeypatch.delenv("DOCKER_HOST", raising=False)
    monkeypatch.setattr(startup.shutil, "which", lambda name: f"/bin/{name}")
    monkeypatch.setattr(startup, "docker_status", lambda: startup.Probe(state.docker, "Docker status", unavailable=not state.docker))
    monkeypatch.setattr(startup, "api_status", lambda url: startup.Probe(state.api, "API status", unavailable=not state.api))

    def query(command):
        if command[1:3] == ["compose", "version"]:
            return subprocess.CompletedProcess(command, 0 if state.compose else 1, "Compose test", "")
        assert command[1:3] == ["context", "inspect"]
        return subprocess.CompletedProcess(command, 0, state.context, "")

    def start(command, root, out, **kwargs):
        assert root == settings.root
        state.starts.append(command)
        if command[0] == "open":
            state.docker = True
        else:
            assert command == ["docker", "compose", "-f", str(root / "docker-compose.yml"), "up", "-d", "--build", "api"]
            assert state.docker, "the daemon must be ready before Compose starts"
            state.api = True

    def yes(prompt):
        state.prompts.append(prompt)
        return "yes"

    monkeypatch.setattr(startup, "_query", query)
    monkeypatch.setattr(startup, "_start", start)
    monkeypatch.setattr(startup, "_docker_start_command", lambda endpoint: ["open", "-a", "Docker"])
    monkeypatch.setattr("builtins.input", yes)
    return state


def test_missing_services_are_started_after_two_confirmations(runtime):
    startup.ensure_services(runtime.settings, demo.Console())
    assert len(runtime.prompts) == 2
    assert "Docker is stopped" in runtime.prompts[0]
    assert "fake API" in runtime.prompts[1]
    assert len(runtime.starts) == 2
    assert runtime.starts[0] == ["open", "-a", "Docker"]
    assert runtime.starts[1][-4:] == ["up", "-d", "--build", "api"]
    assert runtime.docker and runtime.api


@pytest.mark.parametrize("docker_ready", [False, True])
def test_working_api_is_reused_without_startup_or_questions(runtime, docker_ready):
    runtime.docker, runtime.api = docker_ready, True
    startup.ensure_services(runtime.settings, demo.Console())
    assert runtime.starts == [] and runtime.prompts == []


def test_running_docker_is_reused_and_only_api_start_is_approved(runtime):
    runtime.docker = True
    startup.ensure_services(runtime.settings, demo.Console())
    assert len(runtime.prompts) == len(runtime.starts) == 1
    assert runtime.starts[0][-1] == "api"


@pytest.mark.parametrize("answer", ["", "n", "no"])
def test_declining_startup_stops_before_reset(runtime, monkeypatch, answer, capsys):
    runtime.settings.mapping_path.parent.mkdir()
    runtime.settings.mapping_path.write_text("existing mapping\n")
    monkeypatch.setattr("builtins.input", lambda prompt: answer)
    monkeypatch.setattr(demo, "run_demo", lambda *args: pytest.fail("declined startup must not reach the data reset"))
    assert demo._run(runtime.settings, MockBackend(), demo.Console(), debug=False) == 1
    assert runtime.starts == []
    assert runtime.settings.mapping_path.read_text() == "existing mapping\n"
    assert "DEMO NOT STARTED" in capsys.readouterr().out


def test_no_pause_does_not_authorize_startup(runtime):
    with pytest.raises(startup.StartupError, match="--start-services"):
        startup.ensure_services(runtime.settings, demo.Console(), no_pause=True)
    assert runtime.starts == [] and runtime.prompts == []


def test_explicit_startup_flag_allows_unattended_start(runtime):
    startup.ensure_services(runtime.settings, demo.Console(), no_pause=True, start_services=True)
    assert len(runtime.starts) == 2 and runtime.prompts == []
    assert runtime.docker and runtime.api


def test_eof_is_not_startup_consent(runtime, monkeypatch):
    def eof(prompt):
        raise EOFError
    monkeypatch.setattr("builtins.input", eof)
    with pytest.raises(startup.StartupError, match="No input"):
        startup.ensure_services(runtime.settings, demo.Console())
    assert runtime.starts == []


def test_unreachable_custom_endpoint_does_not_launch_local_compose(runtime):
    runtime.settings.base_url = "http://127.0.0.1:9001"
    with pytest.raises(startup.StartupError, match="SHOP_API_URL"):
        startup.ensure_services(runtime.settings, demo.Console(), start_services=True)
    assert runtime.starts == []


@pytest.mark.parametrize("via_environment", [False, True])
def test_remote_docker_endpoint_is_not_started_for_local_api(runtime, monkeypatch, via_environment):
    runtime.docker = True
    if via_environment:
        monkeypatch.setenv("DOCKER_HOST", "tcp://remote.example.test:2376")
    else:
        runtime.context = "ssh://remote.example.test"
    with pytest.raises(startup.StartupError, match="remote engine"):
        startup.ensure_services(runtime.settings, demo.Console(), start_services=True)
    assert runtime.starts == []


def test_docker_access_error_does_not_trigger_a_restart(runtime, monkeypatch):
    monkeypatch.setattr(startup, "docker_status", lambda: startup.Probe(False, "permission denied"))
    with pytest.raises(startup.StartupError, match="inaccessible"):
        startup.ensure_services(runtime.settings, demo.Console(), start_services=True)
    assert runtime.starts == []


def test_missing_compose_stops_before_starting_docker(runtime):
    runtime.compose = False
    with pytest.raises(startup.StartupError, match="Compose is unavailable"):
        startup.ensure_services(runtime.settings, demo.Console(), start_services=True)
    assert runtime.starts == []


def test_responding_incompatible_api_is_not_replaced(runtime, monkeypatch):
    monkeypatch.setattr(startup, "api_status", lambda url: startup.Probe(False, "API is outdated"))
    with pytest.raises(startup.StartupError, match="will not be replaced"):
        startup.ensure_services(runtime.settings, demo.Console(), start_services=True)
    assert runtime.starts == []


@pytest.mark.parametrize("mode,ready,unavailable", [
    ("healthy", True, False), ("outdated", False, False),
    ("wrong_service", False, False), ("offline", False, True),
])
def test_api_probe_checks_health_and_fault_controls(monkeypatch, mode, ready, unavailable):
    def response(request):
        if mode == "offline":
            raise httpx.ConnectError("connection refused", request=request)
        if request.url.path == "/health":
            return httpx.Response(200, json={"status": "ok"} if mode != "wrong_service" else {"message": "hello"})
        assert request.url.path == "/admin/state"
        state = {"rename_total_price": False, "rate_limit": False, "fail_next": 0}
        if mode != "outdated":
            state["rename_to_subtotal"] = False
        return httpx.Response(200, json=state)

    client = httpx.Client
    monkeypatch.setattr(startup.httpx, "Client", lambda **kwargs: client(transport=httpx.MockTransport(response), **kwargs))
    result = startup.api_status("http://127.0.0.1:8099")
    assert result.ready is ready and result.unavailable is unavailable


@pytest.mark.parametrize("detail,stopped", [
    ("Cannot connect to the Docker daemon. Is the docker daemon running?", True),
    ("permission denied while trying to connect to the docker API", False),
])
def test_docker_probe_distinguishes_stopped_from_access_denied(monkeypatch, detail, stopped):
    monkeypatch.setattr(startup.shutil, "which", lambda name: "/bin/docker")
    monkeypatch.setattr(startup, "_query", lambda command: subprocess.CompletedProcess(command, 1, "", detail))
    result = startup.docker_status()
    assert not result.ready and result.unavailable is stopped


def test_readiness_wait_times_out_instead_of_hanging(monkeypatch, capsys):
    clock = [0.0]
    monkeypatch.setattr(startup.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(startup.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    with pytest.raises(startup.StartupError, match="Timed out after 5s"):
        startup._wait_for("fake API", lambda: startup.Probe(False, "still unavailable"), demo.Console(), timeout=5)
    assert clock[0] == 5
    assert "Waiting" in capsys.readouterr().out


@pytest.mark.parametrize("failure", ["exit", "timeout"])
def test_startup_command_failures_are_reported(monkeypatch, tmp_path, failure):
    def run(command, **kwargs):
        assert kwargs["timeout"] == 10
        if failure == "timeout":
            raise subprocess.TimeoutExpired(command, 10)
        return subprocess.CompletedProcess(command, 1)
    monkeypatch.setattr(startup.subprocess, "run", run)
    with pytest.raises(startup.StartupError, match="exceeded 10s|failed with exit 1"):
        startup._start(["docker", "compose", "up", "-d", "api"], tmp_path, demo.Console(), timeout=10)
