# Walkthrough

`./scripts/demo.sh` runs the main loop and narrates itself. This is for poking
at it afterwards: manual experiments, including a closer look at the rejected
proposal from phase 7, and a suggested order for reading the code.

The output excerpts are abbreviated to highlight the relevant checks. The
default upstream fixture is seeded, so its numeric examples are reproducible;
timestamps, paths and full output formatting can vary.

## Before you start

```bash
make install
make api          # leave this running in one terminal
```

Everything else runs in a second terminal, from the repository root. Each
experiment begins from the same starting point:

```bash
export PYTHONPATH=src
./scripts/reset.sh && ./scripts/inject_failure.sh reset
```

That puts the mapping back to version 1 and removes the local warehouse,
baselines and incidents, so each experiment starts with a healthy source and
no saved reference data.

The commands below print a running `DEBUG` commentary of what they are doing,
which is most of the point of running them by hand. The outputs quoted here
leave it out for brevity; `--no-debug` does the same on the command line.

To get to a filed incident, which most of these need:

```bash
.venv/bin/python -m dag_healer.cli ingest     # healthy run; writes the baseline
./scripts/inject_failure.sh rename            # the merchant renames the field
.venv/bin/python -m dag_healer.cli ingest     # fails; files an incident
```

## 1. The repair it refuses

This negative case illustrates why a plausible diagnosis still needs checking.

The renamed payload also contains `shipping_price`. It is a number, it is in
range, it is never null. Point `total_amount` at it and the data contract is
satisfied — no violation, no error, no failed DAG. Revenue reporting is just
wrong from that day forward.

`--diagnosis` runs the gates against a diagnosis you wrote yourself, with no
model in the loop, making this negative case reproducible. The
content assessment below is deliberately false so the proposal reaches the
numeric evidence check; omitting it would stop at the content gate instead:

```bash
.venv/bin/python -m dag_healer.cli heal --latest --diagnosis '{
  "cause_class": "schema_drift_renamed_field",
  "ownership": "customer",
  "confidence": 0.95,
  "summary": "the total appears to have moved to shipping_price",
  "content_check": {
    "canonical_field": "total_amount",
    "new_source_field": "shipping_price",
    "verdict": "equivalent",
    "reference_summary": "Historical numeric order totals",
    "candidate_summary": "Current numeric delivery charges",
    "reason": "Deliberately false equivalence claim to exercise the numeric checks"
  },
  "proposed_action": {
    "type": "remap_field",
    "canonical_field": "total_amount",
    "new_source_field": "shipping_price",
    "rationale": "same type, same range, never null"
  }
}'
```

```
  checks     five gates, all in the healer:
      ok   policy    'schema_drift_renamed_field' is 'auto_then_review' in policy.yml
      ok   policy    confidence 0.95 clears the 0.60 threshold
      ok   allowlist 'remap_field' is one of ['remap_field', 'retry', 'escalate']
      ok   structure 'total_amount' is an existing canonical field
      ok   structure 'shipping_price' really was in the upstream payload
      ok   structure 'shipping_price' is not already the source for another field
      ok   content   complete equivalent-content assessment matches the proposed fields and has old and new examples; its semantic truth is not independently verified
      ok   evidence  a run with the candidate mapping satisfies the contract (120 rows, 0 violations)
      STOP evidence  'total_amount' average moved from 232.46 to 6.94 (97% away, tolerance 25%)
  outcome    escalated: 'total_amount' average moved from 232.46 to 6.94 (97% away, tolerance 25%)
```

Read the passing lines before the failing one. The failure class is automatable,
the action is on the allowlist, the field exists, the upstream really did send
it, and a full run with the candidate mapping satisfies the contract. This was
not a bad proposal caught by an obvious check. It was one measurement away from
being applied.

The mapping is still at version 1. Nothing was applied.

The report it leaves behind is the other half of the point:

```bash
cat incidents/*.escalation.md
```

It records the diagnosis that was rejected, its confidence, the action it
proposed, every gate in order, and the number that killed it. A human arriving
in the morning does not have to reconstruct any of that.

## 2. No baseline, no repair

```bash
./scripts/reset.sh && ./scripts/inject_failure.sh reset
.venv/bin/python -m dag_healer.cli ingest
./scripts/inject_failure.sh rename
.venv/bin/python -m dag_healer.cli ingest
rm baselines/orders.baseline.json
.venv/bin/python -m dag_healer.cli heal --latest --backend mock
```

```
      ok   evidence  a run with the candidate mapping satisfies the contract (120 rows, 0 violations)
      STOP evidence  no known-good baseline for 'total_amount', so the repair cannot be verified
```

The incident retains historical examples captured before the baseline file
was deleted, so the content gate can pass. The evidence gate reads the current
baseline file and refuses the repair when that independent reference is gone.

The diagnosis here is the correct one. `order_total` really is where the total
went. It is refused anyway, because the evidence needed to check it is missing.
A layer that repairs when it cannot verify is a layer that repairs wrongly and
does not find out.

## 3. Transient failures resolve themselves

```bash
./scripts/reset.sh && ./scripts/inject_failure.sh reset
.venv/bin/python -m dag_healer.cli ingest
./scripts/inject_failure.sh transient
.venv/bin/python -m dag_healer.cli ingest
```

```
  upstream said no (... returned 503 ...); retrying, attempt 2 of 3
  upstream said no (... returned 503 ...); retrying, attempt 3 of 3
  extract   120 records from http://127.0.0.1:8099/admin/api/orders.json
  map       onto 7 canonical fields of 'orders' (mapping v1)
  validate  contract satisfied
```

Two 503s and the run still succeeds. No incident, no diagnosis, no model. This
is autonomy level 0: retrying tests source availability without a model call.
The next request either works or it does not; successful data still has to
pass the normal contract checks before loading.

## 4. The structural refusals

More refusal cases live in the tests. Run them with:

```bash
.venv/bin/python -m pytest tests/test_healer.py -v
```

Selected test names:

```text
test_correct_repair_is_applied_and_the_pipeline_recovers
test_plausible_but_wrong_field_is_rejected_by_the_baseline
test_the_record_shows_the_wrong_repair_cleared_every_gate_but_the_evidence
test_a_duplicate_incident_does_not_repair_the_same_thing_twice
test_action_outside_the_allowlist_is_refused
test_low_confidence_is_refused_however_sensible_the_action
test_cause_class_marked_escalate_in_policy_is_never_automated
test_unknown_cause_class_defaults_to_escalation
test_layer_cannot_invent_a_canonical_field
test_source_field_that_upstream_never_sent_is_refused
test_layer_will_not_steal_a_source_field_from_another_canonical_field
test_repair_is_refused_when_there_is_no_known_good_baseline
test_transient_class_resolves_as_a_retry_without_touching_the_mapping
test_escalation_report_contains_the_evidence_a_human_needs
test_default_mock_picks_the_only_sensible_candidate
```

Three of those are worth dwelling on. The layer will not invent a canonical
field, because that is a contract change wearing a repair's clothes. It will
not point a field at a source the upstream never sent, however plausible the
name. And it will not take a source field that another canonical field is
already using, which is the failure mode where fixing one column quietly breaks
a second.

You can reproduce any of them from the command line by changing the
`--diagnosis` payload from experiment 1. An action of `"rewrite_dag"` is refused
at the allowlist; a `canonical_field` of `"profit_margin"` is refused at the
structure gate; a `confidence` of `0.4` never gets past policy.

## Reading order

Do not start with `healer.py`. Start with the declarative files, because that is
where the design decision actually lives.

1. `contracts/orders.contract.yml` — what downstream is allowed to assume. The
   layer never touches it. A system that can relax its own contract to make a
   failure go away does not have a contract.
2. `mappings/orders.mapping.yml` — the only configuration repair may edit, by
   repointing the `source` of a field that already exists. That limit is what
   turns a schema change into a verifiable configuration change instead of a
   code change.
3. `policy.yml` — which failure classes are automated, and with what tolerances.
   Autonomy is granted for verifiability, not for model confidence. The two
   lines worth pausing on are `schema_drift_renamed_field: auto_then_review` and
   `schema_drift_removed_field: escalate`: a renamed field can be checked
   against history, a removed one has nothing to point at.
4. `src/dag_healer/pipeline.py` — deliberately short. The only unusual thing it
   does is build a case file on failure instead of writing a log line.
5. `src/dag_healer/healer.py` — the five gates: policy, allowlist, structure,
   content, evidence. Cheapest and most categorical first, so a refusal is always
   explained by the simplest rule that caught it. The docstring says why.
6. `src/dag_healer/baseline.py`, function `compare` — this is what
   catches experiment 1.
7. `tests/test_healer.py` — what the layer declines to do.

The two DAGs keep import execution separate from diagnosis and repair.
`tests/test_dag_integrity.py` checks that the producer does not import or call
the reliability layer. The consumer still names `orders_ingest` when requesting
another run; supporting multiple pipelines requires routing and coordination.
