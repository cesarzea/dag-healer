<!--
DAG-Healer (https://github.com/cesarzea/dag-healer)
Copyright (c) 2026 César Pedro Zea Gómez (https://www.cesarzea.com)
SPDX-License-Identifier: MIT
-->

# Engineering rationale: reliable merchant integrations

This proof of concept explores recurring failures in data pipelines. Its
question is concrete: can an Airflow failure be diagnosed, checked and repaired
with enough evidence that an engineer can review the result afterwards?

## Why use an orders feed

An orders API provides a small example with a clear correctness requirement:
the reporting field `total_amount` must contain the order total. A mapping
connects that stable field to the API's `total_price` key. When the provider
renames it to `order_total`, the old mapping yields missing values and the
data contract rejects the batch before it reaches the warehouse.

The response also contains `shipping_price`. Delivery charges are numeric,
non-null and within the contract's allowed range, so a repair that selects
that field could pass validation while corrupting revenue reports. The demo
checks this deliberately wrong proposal against statistics from a healthy
import. This illustrates why task completion and basic type checks alone
cannot establish that an import preserved the meaning of the data.

## The Airflow design being demonstrated

`orders_ingest` has one TaskFlow task, `validate_and_load`, which calls
`run_ingest`. Extraction, mapping, validation and loading happen inside that
task. They are not four separate Airflow tasks. Its schedule is every fifteen
minutes, with one task retry after five minutes and `max_active_runs=1`.

`reliability_layer` is a separate DAG:

```text
wait_for_an_incident -> drain_incident_queue -> anything_repaired
                                                   |
                                                   v
                                          rerun_orders_ingest
                                          (TriggerDagRunOperator)
```

The producer writes an incident and raises an exception. `IncidentSensor`
checks for unresolved files; when none are present, it defers to the Airflow
triggerer. An asynchronous loop polls the directory every five seconds. This
releases the worker slot while waiting. It remains filesystem polling, not a
distributed event bus.

The guided script executes the shared Python functions directly and pauses
between phases. Its timestamps, call durations, HTTP activity and file-write
traces come from that execution. Running Docker Compose exercises the actual
scheduler and DAG task dependencies, with Claude Code diagnosing inside the
Airflow container.

## What to inspect in each phase

| Phase | Implementation | Engineering question |
|---|---|---|
| Healthy import | `fetch_raw`, `Mapping.apply`, `contracts.validate`, SQLite load and `baseline.profile` | Can shared reports depend on stable canonical fields? |
| Source change | The fake API's state endpoint renames one response key | Can a provider change be reproduced without changing the consumer? |
| Failed task | `incident.build` and `Incident.save`, then `ContractViolationWithIncident` | Is evidence captured before the failing task loses its context? |
| Diagnosis | `LLMBackend.diagnose` returns a structured `Diagnosis` | Is proposing a repair separate from permission to perform it? |
| Verification | `heal` enforces all five gates, including the required content assessment; `dry_run` and `baseline.compare` test the candidate before `Mapping.save` | Can checks reject a plausible answer, and where do they still trust supplied claims? |
| Recovery | Another `run_ingest`, followed by profile refresh | Does the repaired import actually load data, and is the change reviewable? |
| Wrong proposal | The same `heal` function, followed by an escalation record | Can the system preserve reporting correctness when the diagnosis is wrong? |
| Accepted semantic mistake | An isolated copy, two supplied classifications, the same `heal` and `run_ingest` functions | Can a wrong mapping pass every check and load incorrect values before review? |

The confidence threshold is only an admission check. It cannot establish
correctness. The evidence checks run on a fresh extraction and compare the
candidate with saved statistics. They detect this particular wrong proposal;
they do not prove that two fields have the same business meaning. Phase 8
demonstrates the gap: the `subtotal` fixture is 10% lower than the total and
passes the default 25% mean tolerance when misclassified as a rename. Labeling
the same evidence `semantic_change` forces escalation. Both classifications
are supplied test answers, not a benchmark of live model behavior. They also
carry a deliberately false content-equivalence assessment. The healer requires
a complete `content_check` for every remap, regardless of its source, and
records that decision between the structure and evidence gates. The assessment
compares old examples in the baseline with current samples and the contract's
description. The healer checks its completeness and evidence availability;
this is not an independent proof of meaning. A confident but incorrect claim
can pass even though a verdict admitting uncertainty is refused. Historical
examples are bounded and require `sample_for_diagnosis: true` on the field;
missing examples make any remap ineligible.

A [recorded live check](observations/subtotal-claude-code.json) returned
`semantic_change` and `escalate` for this fixture. It also suggested the
unsupported reconstruction `subtotal + shipping_price`: in this API,
shipping is independent of the 10% difference. This is one correct refusal
with a partly incorrect explanation, not an accuracy benchmark. Controlled
diagnoses keep the demonstration of the gates reproducible.

A [repeat with Opus 5.5/max](observations/subtotal-opus-5-5-max.json) also
escalated. It recognized that the proposed sum is insufficient and the samples
are unpaired, but still inferred excluded shipping and speculated about taxes
or other charges without supporting fixture evidence. The current healer
refused the change and preserved the mapping, baseline and warehouse. These
are separate observations with changes in CLI and healer as well as model
selection, not a controlled model benchmark.

## Extending the idea to a production platform

The reliability layer could complement the chosen ingestion connector and
transformation framework. Connector selection still needs evaluation of
authentication, pagination, incremental sync, rate limits, schema handling,
operational visibility and cost. Reusable SQL and transformation tests remain
necessary downstream. Neither tool choice is implemented or evaluated here.

A deployment serving multiple pipelines would also need durable incident
delivery, access controls, concurrent-update protection and a review interface.
SQLite and a shared directory make the local example easy to inspect; they do
not demonstrate distributed operation or production throughput.

Before making operational claims, measure failure frequency, investigation
time, recovery time, rejected repairs, false acceptances, model cost and
review backlog. Harden and test the scheduler's negative paths, retries and
deferral lifecycle. Use rolling, seasonally appropriate references instead of
this single-run baseline. The intended benefit is less repeated manual work;
this POC has not measured that benefit at fleet scale.
