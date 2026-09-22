# dag-healer: context for Claude Code

## What this is

A working experiment in LLM-assisted diagnosis and bounded pipeline repair.
The end-to-end example covers a renamed source field: the pipeline fails its
contract, and the reliability layer evaluates a mapping proposal against
policy, the action allowlist, structure, a required content assessment, the
contract and a statistical baseline.

Passing those checks does not prove semantic equivalence. The eight-phase demo
includes a rejected shipping-price remap and an accepted wrong subtotal remap.
Phases 7 and 8 supply controlled diagnoses; they test the checks, not the live
model's accuracy. Keep this distinction explicit in explanations and changes.

Claude Code must compare historical and current field contents, not just names
or statistics. The healer requires a complete `content_check` with an
`equivalent` verdict and available samples before applying a remap, regardless
of its source (Claude Code, mock, supplied JSON or another backend). Historical
examples are opt-in through `sample_for_diagnosis` in the contract and bounded
to three scalar values, with text capped at 240 characters. Known sensitive
canonical and source names are excluded. This response check does not prove
that the model's semantic judgment is correct.

## Invariants: do not break these without a reason stated in the commit

1. **The contract is never machine-edited.** `contracts/*.contract.yml` is what
   downstream depends on. A layer that can relax its own contract to make a
   failure disappear has no contract.
2. **The mapping is the only configuration the repair may edit**, by repointing
   the `source` of a canonical field that already exists. No adding or removing
   canonical fields, ever. That would be a contract change.
3. **The layer never edits code.** Its configuration edits are limited to
   `mappings/*.mapping.yml`; it also writes incidents, resolutions and reports.
   Successful imports write the warehouse and baseline.
4. **Actions come from an allowlist** in `policy.yml` (`remap_field`, `retry`,
   `escalate`). Anything else is refused regardless of model confidence.
5. **Passing the contract is necessary and not sufficient.** A repair must also
   resemble the column it replaces, compared against the last known-good
   profile in `baselines/`. These limited checks can still accept a wrong repair.
6. **The pipeline DAG knows nothing about the reliability layer.** No import,
   no callback, no shared dag_id. The incident directory is the interface.
   `tests/test_dag_integrity.py` fails if they grow a coupling.
7. **Model output is untrusted.** Incidents embed third-party API samples. The
   prompt says to treat evidence as data, but the prompt is not the boundary.
   The allowlist and the verification are. The healer owns the content gate;
   backends return diagnoses without deciding whether their own answer is safe
   to apply. Content refusals must appear in the recorded PASS/STOP checks and
   preserve the original diagnosis. A confident but false equivalence claim
   remains possible; never describe this gate as proof of meaning.

## One illustrative failure case

When the merchant renames `total_price` to `order_total`, the payload also
contains `shipping_price`. It is a number, in range, never null, and a repair
pointing `total_amount` at it **passes the data contract cleanly** while
putting delivery charges in the order-total column. The task succeeds while
revenue reports contain the wrong quantity.

That case is `tests/test_healer.py::test_plausible_but_wrong_field_is_rejected_by_the_baseline`.
Keep it exercising the numeric evidence gate rather than an earlier refusal.

## Layout

```
contracts/      canonical shape; never machine-edited
mappings/       upstream -> canonical; the only configuration repair may edit
policy.yml      autonomy per failure class, tolerances, allowlist
baselines/      profile of the last known-good run
src/dag_healer/
  pipeline.py   extract, map, validate, load
  incident.py   structured evidence, built at the moment of failure
  queue.py      incidents are files in a directory; the layer drains them
  healer.py     five gates: policy, allowlist, structure, content, evidence
  baseline.py   compare(); this is what catches a plausible wrong repair
  backends/     Claude Code CLI + a deterministic mock for tests
dags/           orders_ingest, reliability_layer (decoupled, queue-mediated)
fake_shop_api/  an upstream that changes its schema without telling you
```

## Commands

```bash
make versions   # python / airflow / constraints it will use
make install
make test       # includes the repair controls and interactive demo boundaries
make api        # fake upstream on :8099, leave running
./scripts/demo.sh        # check services, ask before startup, then use Claude Code
./scripts/demo.sh mock   # same, deterministic backend, no model
./scripts/demo.sh mock --no-pause   # all phases without waiting for Return
./scripts/demo.sh mock --no-pause --start-services  # explicitly allow startup
./scripts/demo.sh --debug           # expand live traces with model instructions
./scripts/reset.sh       # back to mapping v1, no baseline, no incidents
./scripts/inject_failure.sh {rename|subtotal|transient|ratelimit|reset}
```

## Environment

Python 3.13 with Airflow 3.3.2. The DAGs also run on Airflow 2.11; imports that
moved between versions are guarded with try/except and CI runs both. The
Makefile derives the constraints URL from the running Python, so do not pin it
back to a hardcoded version.

Diagnosis runs through Claude Code 2.1.280 or later, explicitly selecting
Opus 5.5 with maximum reasoning effort (`claude -p ... --model claude-opus-5-5
--effort max --output-format json`). Keep the model and effort visible in the
demo and trace. There is no API key in this repo and none should be added.

## State and known gaps

- The healer implements `remap_field`, `retry` and `escalate`. The policy
  admits both `schema_drift_renamed_field` and `schema_drift_new_field` for
  remapping existing canonical fields; the end-to-end example covers a rename.
  This does not establish general handling of newly added source fields.
- Only contract failures create structured incidents. Exhausted HTTP retries
  currently raise without creating an incident for the reliability DAG.
- The baseline is a single-run profile. Real use wants a rolling window and
  seasonality, otherwise a quiet Sunday looks like a broken pipeline.
- The layer re-runs one pipeline after a repair. With several, that becomes a
  dynamic mapping over the origin DAGs named in the resolutions.
- The Claude Code backend's output parser is tested against the three shapes
  the CLI returns; the live call has had less exercise than everything else.
- No pagination, no partial failures, no schema changing halfway through a page.

## Style

Comments explain why, not what. If a test name can carry the argument, put it
in the test name. Honest limitations go in the README rather than being
quietly omitted.
