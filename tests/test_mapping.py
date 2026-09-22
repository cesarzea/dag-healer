# DAG-Healer (https://github.com/cesarzea/dag-healer)
# Copyright (c) 2026 César Pedro Zea Gómez (https://www.cesarzea.com)
# SPDX-License-Identifier: MIT

from __future__ import annotations

import pytest

from dag_healer.mapping import load_mapping


@pytest.fixture()
def mapping(project):
    return load_mapping(project / "mappings" / "orders.mapping.yml")


def test_the_suite_does_not_inherit_a_repaired_mapping_from_the_working_tree(mapping):
    # Running the demo leaves the real mapping at v2. If the fixtures copied it,
    # every expectation below would depend on who ran what last.
    assert mapping.version == 1
    assert mapping.fields["total_amount"] == "total_price"
    assert mapping.history == []


def test_apply_projects_onto_canonical_names(mapping):
    out = mapping.apply([{"id": "ord_1", "total_price": 10.0, "noise": "ignored"}])
    assert out[0]["order_id"] == "ord_1"
    assert out[0]["total_amount"] == 10.0
    assert "noise" not in out[0]


def test_absent_source_yields_none_not_a_missing_key(mapping):
    out = mapping.apply([{"id": "ord_1"}])
    assert out[0]["total_amount"] is None
    assert "total_amount" in out[0]


def test_missing_sources_detects_a_vanished_field(mapping):
    missing = mapping.missing_sources([{"id": "ord_1", "order_total": 10.0}])
    assert missing["total_amount"] == "total_price"
    assert "order_id" not in missing


def test_missing_sources_is_empty_when_upstream_is_intact(mapping):
    raw = [{src: 1 for src in mapping.fields.values()}]
    assert mapping.missing_sources(raw) == {}


def test_remap_bumps_version_and_records_history(mapping):
    updated = mapping.with_remap("total_amount", "order_total", reason="upstream rename")
    assert updated.version == mapping.version + 1
    assert updated.fields["total_amount"] == "order_total"
    assert updated.history[-1]["review"] == "pending"
    assert updated.history[-1]["from"] == "total_price"
    # The original is untouched, so a rejected candidate leaves no trace.
    assert mapping.fields["total_amount"] == "total_price"


def test_remap_refuses_to_invent_a_canonical_field(mapping):
    with pytest.raises(KeyError):
        mapping.with_remap("profit_margin", "order_total", reason="nope")


def test_save_and_reload_roundtrips(mapping, project):
    updated = mapping.with_remap("total_amount", "order_total", reason="r")
    path = updated.save(project / "mappings" / "orders.mapping.yml")
    assert load_mapping(path).fields["total_amount"] == "order_total"


def test_an_automated_repair_does_not_rewrite_the_header_stating_its_own_limits(
    mapping, project
):
    path = project / "mappings" / "orders.mapping.yml"
    header_before = path.read_text().split("entity:")[0]
    mapping.with_remap("total_amount", "order_total", reason="r").save(path)
    assert path.read_text().startswith(header_before)
