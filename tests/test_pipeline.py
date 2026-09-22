# DAG-Healer (https://github.com/cesarzea/dag-healer)
# Copyright (c) 2026 César Pedro Zea Gómez (https://www.cesarzea.com)
# SPDX-License-Identifier: MIT

from __future__ import annotations

import pytest

from dag_healer.baseline import load_baseline
from dag_healer.errors import SourceUnavailable
from dag_healer.pipeline import ContractViolationWithIncident, run_ingest


def test_clean_run_loads_rows_and_writes_a_baseline(settings):
    result = run_ingest(settings)
    assert result.ok and result.rows == 120
    baseline = load_baseline(settings.baseline_path)
    assert baseline["total_amount"].kind == "numeric"
    assert settings.warehouse_path.exists()


def test_renamed_upstream_field_fails_the_run_and_files_an_incident(settings, clean_baseline, api):
    api.set(rename_total_price=True)

    with pytest.raises(ContractViolationWithIncident) as excinfo:
        run_ingest(settings)

    incident_path = excinfo.value.incident_path
    assert incident_path.exists()
    kinds = {v.kind for v in excinfo.value.violations}
    assert "missing_source_field" in kinds


def test_incident_carries_the_evidence_a_diagnosis_needs(settings, clean_baseline, api):
    from dag_healer.incident import Incident

    api.set(rename_total_price=True)
    with pytest.raises(ContractViolationWithIncident) as excinfo:
        run_ingest(settings)

    incident = Incident.load(excinfo.value.incident_path)
    assert "order_total" in incident.upstream_fields_present
    assert incident.upstream_samples, "samples are what make the failure diagnosable"
    assert incident.baseline["total_amount"]["kind"] == "numeric"
    assert incident.mapping_fields["total_amount"] == "total_price"


def test_transient_upstream_errors_are_retried_not_escalated(settings, api):
    api.set(fail_next=2)
    result = run_ingest(settings)
    assert result.ok, "two 503s are inside the retry budget"


def test_upstream_that_never_recovers_raises(settings, api):
    api.set(fail_next=99)
    with pytest.raises(SourceUnavailable):
        run_ingest(settings)


def test_rate_limiting_is_surfaced_as_its_own_error(settings, api):
    from dag_healer.errors import RateLimited

    api.set(rate_limit=True)
    with pytest.raises(RateLimited):
        run_ingest(settings)
