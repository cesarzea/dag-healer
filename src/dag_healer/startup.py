# DAG-Healer (https://github.com/cesarzea/dag-healer)
# Copyright (c) 2026 César Pedro Zea Gómez (https://www.cesarzea.com)
# SPDX-License-Identifier: MIT

"""Check the demo's services and start missing dependencies only with consent."""

from __future__ import annotations

import logging
import os
import platform
import shlex
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol
from urllib.parse import urlsplit

import httpx

from .config import Settings

log = logging.getLogger(__name__)


class StartupError(Exception):
    """An unmet prerequisite, reported before the demonstration changes data."""


class Output(Protocol):
    def say(self, text: str = "") -> None: ...
    def fact(self, label: str, value: object) -> None: ...


@dataclass
class Probe:
    ready: bool
    detail: str
    unavailable: bool = False


def _query(command: list[str]) -> subprocess.CompletedProcess:
    log.info("CHECK %s", shlex.join(command))
    return subprocess.run(command, capture_output=True, text=True, timeout=5, check=False)


def docker_status() -> Probe:
    if not shutil.which("docker"):
        return Probe(False, "Docker CLI is not installed or is not on PATH.")
    try:
        result = _query(["docker", "info", "--format", "{{.ServerVersion}}"])
    except (OSError, subprocess.TimeoutExpired) as exc:
        return Probe(False, f"Docker could not be queried: {exc}")
    if result.returncode == 0:
        return Probe(True, f"Docker Engine {result.stdout.strip()} is responding.")
    detail = (result.stderr or result.stdout).strip()[-500:]
    stopped = any(word in detail.lower() for word in (
        "cannot connect", "is the docker daemon running", "connection refused",
        "no such file or directory", "the system cannot find the file",
    ))
    # Access failures must not be mistaken for a stopped daemon.
    denied = any(word in detail.lower() for word in ("permission denied", "access is denied"))
    return Probe(False, detail or "Docker Engine is not responding.", unavailable=stopped and not denied)


def api_status(base_url: str) -> Probe:
    log.info("CHECK fake API: GET %s/health and /admin/state", base_url.rstrip("/"))
    try:
        with httpx.Client(timeout=2) as client:
            health = client.get(f"{base_url.rstrip('/')}/health")
            health.raise_for_status()
            state = client.get(f"{base_url.rstrip('/')}/admin/state")
            state.raise_for_status()
        if health.json() != {"status": "ok"}:
            return Probe(False, "The health response does not identify a healthy fake API.")
        values = state.json()
        switches = ("rename_total_price", "rename_to_subtotal", "rate_limit")
        if not isinstance(values, dict) or not all(type(values.get(key)) is bool for key in switches) or "fail_next" not in values:
            return Probe(False, "The running API is incompatible with this demo: its state must include rename_to_subtotal and the other fault switches. Rebuild or restart it from this checkout.")
    except httpx.RequestError as exc:
        return Probe(False, f"Cannot reach the fake API at {base_url}: {exc}", unavailable=True)
    except (httpx.HTTPStatusError, ValueError) as exc:
        return Probe(False, f"The service at {base_url} is responding but is not ready for this demo: {exc}")
    return Probe(True, f"{base_url}: health and demo fault controls are ready.")


def _confirm(out: Output, question: str, *, start_services: bool, no_pause: bool) -> None:
    if start_services:
        out.fact("Startup consent", "--start-services authorizes this startup operation.")
        return
    if no_pause:
        raise StartupError("A service needs to be started. Run interactively to approve startup, start it manually, or use --start-services with --no-pause.")
    try:
        answer = input(f"\n  {question} [y/N] ").strip().lower()
    except EOFError as exc:
        raise StartupError("No input is available to approve service startup. Start services manually, or use --start-services to authorize startup explicitly.") from exc
    if answer not in ("y", "yes"):
        raise StartupError("Startup declined. The demo has not reset its mapping, incidents or warehouse.")


def _start(command: list[str], root: Path, out: Output, *, timeout: int = 300) -> None:
    out.fact("Starting", shlex.join(command))
    log.info("START %s", shlex.join(command))
    try:
        # Inherit the terminal so Docker build progress and errors remain visible.
        result = subprocess.run(command, cwd=root, timeout=timeout, check=False)
    except subprocess.TimeoutExpired as exc:
        raise StartupError(f"Startup command exceeded {timeout}s: {shlex.join(command)}. Inspect Docker before retrying; services may have partially started.") from exc
    except OSError as exc:
        raise StartupError(f"Could not run {shlex.join(command)}: {exc}") from exc
    log.info("END %s -> exit %d", shlex.join(command), result.returncode)
    if result.returncode:
        raise StartupError(f"Startup command failed with exit {result.returncode}. See its output above; the walkthrough has not started.")


def _wait_for(name: str, probe: Callable[[], Probe], out: Output, *, timeout: int = 120) -> None:
    started = time.monotonic()
    next_update = started
    while True:
        result = probe()
        if result.ready:
            out.fact("Ready", result.detail)
            return
        now = time.monotonic()
        if now - started >= timeout:
            raise StartupError(f"Timed out after {timeout}s waiting for {name}. Last check: {result.detail}")
        if now >= next_update:
            out.fact("Waiting", f"{name} is starting ({int(now - started)}s elapsed; limit {timeout}s).")
            next_update = now + 10
        time.sleep(min(2, timeout - (now - started)))


def _docker_start_command(endpoint: str) -> list[str]:
    system = platform.system()
    if system == "Darwin":
        if ".orbstack/" in endpoint:
            raise StartupError("The selected Docker socket belongs to OrbStack. Start that runtime manually, then rerun the demo.")
        for app in (Path("/Applications/Docker.app"), Path.home() / "Applications/Docker.app"):
            if app.exists():
                return ["open", "-a", str(app)]
        raise StartupError("Docker Desktop was not found in Applications. Start your Docker runtime manually, then rerun the demo.")
    try:
        if _query(["docker", "desktop", "version"]).returncode == 0:
            return ["docker", "desktop", "start", "--detach"]
    except (OSError, subprocess.TimeoutExpired):
        pass
    if system == "Linux" and shutil.which("systemctl"):
        if "/run/user/" in endpoint:
            return ["systemctl", "--user", "start", "docker"]
        # Let sudo request credentials in the terminal; never change permissions.
        return ["sudo", "systemctl", "start", "docker"]
    raise StartupError("Automatic Docker startup is not available on this system. Start your Docker runtime manually, then rerun the demo.")


def ensure_services(settings: Settings, out: Output, *, start_services: bool = False, no_pause: bool = False) -> None:
    out.say("Before the walkthrough: check Docker and the fake orders API.")
    engine = docker_status()
    out.fact("Docker", engine.detail)
    api = api_status(settings.base_url)
    out.fact("Fake API", api.detail)
    if api.ready:
        if not engine.ready:
            out.say("The existing API is ready, so this console walkthrough can use it without starting Docker. Docker is needed here only to launch the API container.")
        out.say("Services are ready. The walkthrough calls the DAGs' Python functions directly; it does not require an Airflow scheduler.")
        return
    if not api.unavailable:
        raise StartupError(api.detail + " A responding service will not be replaced automatically.")
    url = urlsplit(settings.base_url)
    if url.scheme != "http" or url.hostname not in ("localhost", "127.0.0.1") or url.port != 8099 or url.path not in ("", "/") or url.query or url.fragment:
        raise StartupError(f"SHOP_API_URL points to {settings.base_url}. Compose starts this project's API at http://127.0.0.1:8099; start the configured endpoint yourself or unset SHOP_API_URL.")
    compose_file = settings.root / "docker-compose.yml"
    if not compose_file.is_file():
        raise StartupError(f"Missing Compose definition: {compose_file}")
    if not shutil.which("docker"):
        raise StartupError("Install Docker with Compose to start the API container, or run 'make api' in another terminal and retry.")
    try:
        compose = _query(["docker", "compose", "version"])
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise StartupError(f"Docker Compose could not be queried: {exc}") from exc
    if compose.returncode:
        raise StartupError("Docker Compose is unavailable. Install the Compose plugin, or start the API manually with 'make api'.")
    out.fact("Compose", compose.stdout.strip())
    # Compose follows DOCKER_HOST when set, even if the saved context is local.
    endpoint = os.environ.get("DOCKER_HOST")
    if not endpoint:
        try:
            context = _query(["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"])
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise StartupError(f"Could not inspect the Docker context: {exc}") from exc
        if context.returncode:
            raise StartupError("Could not inspect the selected Docker context. Resolve its configuration before starting containers.")
        endpoint = context.stdout.strip()
    if not endpoint.startswith(("unix://", "npipe://")):
        raise StartupError("The selected Docker endpoint is not a local socket. This demo will not start containers on a remote engine for a localhost API URL.")
    if not engine.ready:
        if not engine.unavailable:
            raise StartupError("Docker is inaccessible, rather than known to be stopped. Resolve the reported access or context error before retrying: " + engine.detail)
        command = _docker_start_command(endpoint)
        _confirm(out, f"Docker is stopped. Start it with {shlex.join(command)}?", start_services=start_services, no_pause=no_pause)
        _start(command, settings.root, out, timeout=30)
        _wait_for("Docker Engine", docker_status, out)
    command = ["docker", "compose", "-f", str(compose_file), "up", "-d", "--build", "api"]
    out.say("The fake API is not running. Compose will build its image from this checkout and start the api service on port 8099. Docker build output will be shown; Airflow is not started by this command.")
    _confirm(out, "Build and start the fake API container?", start_services=start_services, no_pause=no_pause)
    _start(command, settings.root, out)
    try:
        _wait_for("the fake API", lambda: api_status(settings.base_url), out)
    except StartupError as exc:
        raise StartupError(f"{exc} Inspect 'docker compose logs --tail=50 api' for startup errors.") from exc
    out.say("Services are ready. The API container will remain running after the demo; stop it with 'docker compose stop api' when finished.")
