"""A deliberately unhelpful stand-in for a merchant's order API.

It behaves like the real thing in the way that matters here: it can change its
own schema without telling anyone, because that is what upstream systems do.

Run it with:  uvicorn fake_shop_api.main:app --port 8099
"""

from __future__ import annotations

import json
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Response

STATE_PATH = Path(
    os.environ.get(
        "FAKE_SHOP_STATE",
        Path(__file__).resolve().parent.parent / "data" / "api_state.json",
    )
)
DEFAULT_STATE: dict[str, Any] = {
    # Renames total_price -> order_total, the way a merchant might during a
    # platform upgrade, without a deprecation window.
    "rename_total_price": False,
    # Returns 503 on the next N calls, to exercise the transient path.
    "fail_next": 0,
    # Returns 429 with Retry-After.
    "rate_limit": False,
    "seed": 20260922,
    "order_count": 120,
}

app = FastAPI(title="fake-shop-api", version="1.0.0")


def _read_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return dict(DEFAULT_STATE)
    try:
        return {**DEFAULT_STATE, **json.loads(STATE_PATH.read_text(encoding="utf-8"))}
    except json.JSONDecodeError:
        return dict(DEFAULT_STATE)


def _write_state(state: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")


def generate_orders(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Deterministic orders, so a baseline computed yesterday still means something."""
    rng = random.Random(state["seed"])
    statuses = ["paid", "paid", "paid", "pending", "authorized", "refunded"]
    base = datetime(2026, 9, 1, tzinfo=timezone.utc)
    orders: list[dict[str, Any]] = []

    for i in range(int(state["order_count"])):
        total = round(rng.uniform(18.0, 420.0), 2)
        order: dict[str, Any] = {
            "id": f"ord_{10_000 + i}",
            "created_at": (base + timedelta(minutes=17 * i)).isoformat(),
            "currency": "EUR",
            "total_price": total,
            # The decoy. Same type, plausible range, completely different meaning.
            "shipping_price": round(rng.uniform(0.0, 12.0), 2),
            "customer_id": f"cus_{rng.randint(1, 60):04d}" if rng.random() > 0.08 else None,
            "line_items_count": rng.randint(1, 9),
            "financial_status": rng.choice(statuses),
        }
        if state["rename_total_price"]:
            order["order_total"] = order.pop("total_price")
        orders.append(order)

    return orders


@app.get("/admin/api/orders.json")
def list_orders() -> Response:
    state = _read_state()

    if state["fail_next"] > 0:
        state["fail_next"] -= 1
        _write_state(state)
        raise HTTPException(status_code=503, detail="upstream temporarily unavailable")

    if state["rate_limit"]:
        return Response(
            content=json.dumps({"errors": "Too Many Requests"}),
            status_code=429,
            media_type="application/json",
            headers={"Retry-After": "2"},
        )

    return Response(
        content=json.dumps({"orders": generate_orders(state)}),
        media_type="application/json",
    )


@app.get("/admin/state")
def get_state() -> dict[str, Any]:
    return _read_state()


@app.post("/admin/state")
def set_state(patch: dict[str, Any]) -> dict[str, Any]:
    """Used by scripts/inject_failure.sh. Not something a real API would expose."""
    state = {**_read_state(), **patch}
    _write_state(state)
    return state


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
