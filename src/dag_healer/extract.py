"""Talking to the upstream API, and classifying the ways it says no."""

from __future__ import annotations

import time
from typing import Any

import httpx

from .errors import RateLimited, SourceUnavailable
from .mapping import Mapping


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
                if not isinstance(records, list):
                    raise SourceUnavailable(
                        f"expected a list at '{mapping.records_path}', got {type(records).__name__}"
                    )
                return records

        if attempt < retries:
            time.sleep(backoff * (2**attempt))

    assert last_error is not None
    raise last_error
