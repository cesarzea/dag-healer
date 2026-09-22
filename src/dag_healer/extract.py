# DAG-Healer (https://github.com/cesarzea/dag-healer)
# Copyright (c) 2026 César Pedro Zea Gómez (https://www.cesarzea.com)
# SPDX-License-Identifier: MIT

"""Talking to the upstream API, and classifying the ways it says no."""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from .errors import RateLimited, SourceUnavailable
from .mapping import Mapping

# Logged rather than printed, so the retries show up in a terminal and in an
# Airflow task log without this module deciding where output goes. A retry that
# leaves no trace looks exactly like a call that never had a problem.
log = logging.getLogger(__name__)


def fetch_raw(
    mapping: Mapping,
    base_url: str,
    *,
    timeout: float = 10.0,
    retries: int = 2,
    backoff: float = 0.5,
) -> list[dict[str, Any]]:
    """Fetch upstream records, retrying only what is worth retrying.

    Transient errors are handled here rather than escalated, because a retry is
    verifiable by definition: either the next call succeeds or it does not.
    """
    url = base_url.rstrip("/") + mapping.endpoint
    log.debug("GET %s (up to %d attempts; a retry verifies itself)", url, retries + 1)
    last_error: Exception | None = None

    for attempt in range(retries + 1):
        try:
            response = httpx.get(url, timeout=timeout)
        except httpx.RequestError as exc:
            last_error = SourceUnavailable(f"could not reach {url}: {exc}")
        else:
            if response.status_code == 429:
                wait = float(response.headers.get("Retry-After", backoff))
                last_error = RateLimited(f"{url} returned 429; Retry-After={wait}s")
                if attempt < retries:
                    log.info(
                        "upstream said no (%s); waiting %.1fs, attempt %d of %d",
                        last_error,
                        min(wait, 5.0),
                        attempt + 2,
                        retries + 1,
                    )
                    time.sleep(min(wait, 5.0))
                    continue
            elif response.status_code >= 500:
                last_error = SourceUnavailable(
                    f"{url} returned {response.status_code}: {response.text[:200]}"
                )
            elif response.status_code >= 400:
                raise SourceUnavailable(
                    f"{url} returned {response.status_code}: {response.text[:200]}"
                )
            else:
                payload = response.json()
                records = payload.get(mapping.records_path, [])
                log.debug("upstream returned %d records", len(records) if isinstance(records, list) else 0)
                if not isinstance(records, list):
                    raise SourceUnavailable(
                        f"expected a list at '{mapping.records_path}', got {type(records).__name__}"
                    )
                return records

        if attempt < retries:
            log.info(
                "upstream said no (%s); retrying, attempt %d of %d",
                last_error,
                attempt + 2,
                retries + 1,
            )
            time.sleep(backoff * (2**attempt))

    assert last_error is not None
    raise last_error
