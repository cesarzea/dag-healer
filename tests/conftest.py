# DAG-Healer (https://github.com/cesarzea/dag-healer)
# Copyright (c) 2026 César Pedro Zea Gómez (https://www.cesarzea.com)
# SPDX-License-Identifier: MIT

"""Fixtures that give every test its own project directory and its own API.

The API is a real server on a real socket rather than a mocked transport,
because the extraction path and its retry behaviour are part of what is being
tested, and a mock would quietly excuse them.
"""

from __future__ import annotations

import json
import shutil
import socket
import sys
import threading
import time
from pathlib import Path

import httpx
import pytest
import uvicorn

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from dag_healer.config import Settings  # noqa: E402


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture()
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A throwaway copy of the repo's declarative files.

    The mapping comes from a fixture rather than from the working tree,
    because it is the one file the layer rewrites when it runs. Copying the
    live one made the suite depend on whether anyone had run the demo since
    the last reset, which is a bad property for the tests that are supposed
    to say what the layer will and will not do.
    """
    shutil.copytree(ROOT / "contracts", tmp_path / "contracts")
    (tmp_path / "mappings").mkdir()
    shutil.copy(
        Path(__file__).parent / "fixtures" / "orders.mapping.yml",
        tmp_path / "mappings" / "orders.mapping.yml",
    )
    shutil.copy(ROOT / "policy.yml", tmp_path / "policy.yml")
    (tmp_path / "baselines").mkdir()
    (tmp_path / "incidents").mkdir()
    (tmp_path / "data").mkdir()
    monkeypatch.setenv("FAKE_SHOP_STATE", str(tmp_path / "data" / "api_state.json"))
    return tmp_path


@pytest.fixture()
def api(project: Path):
    """A fake upstream whose schema the test can change."""
    import importlib

    from fake_shop_api import main as api_main

    importlib.reload(api_main)

    port = _free_port()
    config = uvicorn.Config(api_main.app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            if httpx.get(f"{base}/health", timeout=0.5).status_code == 200:
                break
        except httpx.RequestError:
            time.sleep(0.05)
    else:  # pragma: no cover
        server.should_exit = True
        pytest.fail("fake shop API did not start")

    class Api:
        url = base

        @staticmethod
        def set(**patch) -> None:
            httpx.post(f"{base}/admin/state", json=patch, timeout=5).raise_for_status()

    yield Api()

    server.should_exit = True
    thread.join(timeout=10)


@pytest.fixture()
def settings(project: Path, api) -> Settings:
    return Settings(root=project, entity="orders", base_url=api.url)


@pytest.fixture()
def clean_baseline(settings: Settings):
    """Run the pipeline once while upstream is healthy, to have something to compare to."""
    from dag_healer.pipeline import run_ingest

    return run_ingest(settings)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))
