# DAG-Healer (https://github.com/cesarzea/dag-healer)
# Copyright (c) 2026 César Pedro Zea Gómez (https://www.cesarzea.com)
# SPDX-License-Identifier: MIT

from __future__ import annotations

from dag_healer.contracts import load_contract, validate

import pytest


@pytest.fixture()
def contract(project):
    return load_contract(project / "contracts" / "orders.contract.yml")


def _row(**overrides):
    row = {
        "order_id": "ord_1",
        "created_at": "2026-09-01T00:00:00+00:00",
        "currency": "EUR",
        "total_amount": 42.5,
        "customer_id": "cus_0001",
        "line_items": 3,
        "status": "paid",
    }
    row.update(overrides)
    return row


def test_clean_batch_has_no_violations(contract):
    assert validate([_row()], contract) == []


def test_missing_required_field_is_reported(contract):
    row = _row()
    del row["total_amount"]
    kinds = {v.kind for v in validate([row], contract)}
    assert "missing_field" in kinds


def test_null_in_required_field_is_reported(contract):
    kinds = {v.kind for v in validate([_row(total_amount=None)], contract)}
    assert kinds == {"null_in_required_field"}


def test_optional_field_may_be_null(contract):
    assert validate([_row(customer_id=None)], contract) == []


def test_wrong_type_is_reported(contract):
    violation = validate([_row(line_items="three")], contract)[0]
    assert violation.kind == "wrong_type"
    assert violation.field == "line_items"


def test_value_outside_declared_range_is_reported(contract):
    kinds = {v.kind for v in validate([_row(total_amount=-1)], contract)}
    assert "out_of_range" in kinds


def test_value_outside_enum_is_reported(contract):
    kinds = {v.kind for v in validate([_row(status="abducted")], contract)}
    assert "value_not_allowed" in kinds


def test_pattern_is_enforced(contract):
    kinds = {v.kind for v in validate([_row(currency="euro")], contract)}
    assert "pattern_mismatch" in kinds


def test_duplicate_primary_key_is_reported(contract):
    kinds = {v.kind for v in validate([_row(), _row()], contract)}
    assert "duplicate_key" in kinds


def test_empty_batch_violates_min_rows(contract):
    assert [v.kind for v in validate([], contract)] == ["too_few_rows"]
