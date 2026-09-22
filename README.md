# dag-healer

A working proof of concept (POC) demonstrating how AI can help engineers
investigate and resolve problems in data pipelines orchestrated by Apache
Airflow: interpreting evidence, diagnosing a failure and proposing a repair
that code can evaluate. An Airflow DAG defines a workflow's tasks and the
dependencies between them.

The implementation makes that idea concrete through one limited example in
Airflow: a simulated orders API renames a field and breaks an import. An LLM
diagnoses the failure and proposes an action; a separate reliability DAG
applies policy and deterministic checks before changing a field mapping and
recording the outcome for review. This illustrates AI-assisted problem
resolution; the implementation covers only this narrow scenario.

The workflow, verification rules and infrastructure are built for this POC,
not designed or validated as a production solution. The checks reject some
incorrect proposals, but **passing them does not prove semantic equivalence**.
The demo shows a successful recovery, a rejected wrong proposal and a wrong
mapping that passes every implemented check, making both the potential and
the limits visible.

## The problem

Data movement generates incidents forever. Schemas change, sources go down,
entities grow in ways nobody forecast, credentials expire. That is normal and
it is why on-call rotations exist. It stops being manageable when the number of
integrations grows faster than the number of engineers: each new pipeline adds
another source of recurring investigation and maintenance work.

The engineering question is whether recurring, verifiable failures can be
resolved with less repeated investigation, while the rest arrive with their
evidence already gathered. The example uses order data because a field mapping
can produce valid-looking numbers that silently distort a revenue report.

[Engineering rationale](docs/design-rationale.md) explains the orders example,
the two Airflow DAGs and the production work beyond this local demonstration.

## What this does, and what it refuses to do

**It can**: request a diagnosis from structured evidence, evaluate a proposed
remap against policy, structure, a required content assessment, a data contract
and a statistical baseline,
apply a passing proposal, record it as pending human review and re-run the
pipeline. These are implemented checks, not proof of business correctness.

**It cannot**: change the data contract, change any code, add or remove a
canonical field, take an action outside a three-item allowlist, or apply any
repair that fails the configured checks. It also cannot guarantee that an
accepted source field represents the intended business quantity.

## Run the guided demonstration

Use Python 3.13, Git, Make, Bash and curl. For the live diagnosis, install and
authenticate Claude Code with access to the selected model; the `mock` backend
needs no model account. Docker with Compose can start the fake API, or you can
run `make api` in a separate terminal without Docker.

```bash
git clone https://github.com/cesarzea/dag-healer.git
cd dag-healer
make versions     # shows the Python, Airflow and constraints it will use
make install
./scripts/demo.sh # add `mock` to run without Claude Code
```

The live backend explicitly selects **Claude Opus 5.5 with maximum reasoning
effort**, using `--model claude-opus-5-5 --effort max`. It does not rely on the
CLI's default model. Model and effort appear in the demo introduction and
execution trace. Claude Code **2.1.280 or later** is required; check
`claude --version` and update with `claude update` if needed. See the
[official model configuration documentation](https://code.claude.com/docs/en/model-config).

Before phase 1, the script checks its Python dependencies, availability of the
selected diagnosis CLI, Docker Engine, and the API's `/health` and `/admin/state`
endpoints. A working API is reused, including one started separately with
`make api`. Docker is needed only when starting the API container.

If the local API is unavailable, the script checks Compose and asks before
starting Docker and before running `docker compose up -d --build api`. On
macOS it can open Docker Desktop; supported Desktop installations on other
systems use the Docker Desktop CLI, and Linux Engine installations can use
systemd (which may request a sudo password). It then waits for readiness,
showing real command output and progress. Startup commands and readiness waits
have time limits; a refusal or failure stops the demo before its data reset.
Airflow is not required or started for this console walkthrough.

Startup questions default to **No**. `--no-pause` does not authorize starting
services: for an unattended run that may start them, use
`./scripts/demo.sh mock --no-pause --start-services`. Without that explicit
startup flag, unattended runs require services to be ready already. The API
container remains running afterwards; stop it with `docker compose stop api`.

Automatic API startup targets `http://127.0.0.1:8099` (or `localhost:8099`).
A custom `SHOP_API_URL` must already be running. A responding incompatible
service, Docker access error or remote Docker endpoint is reported for manual
resolution instead of starting an unrelated container.

The demo has eight phases. Press **Return** after each phase to continue.
Each phase introduces the actor and the operation, shows the actual result,
and explains what it means. Technical labels such as `policy`, `remap_field`
and `auto_then_review` appear alongside their meaning and consequences.

The diagnosis has its own phase: you read the proposal before advancing to
verification and application. Return advances the presentation; it is not
human approval of the repair. The change is recorded for later review while
data loading resumes.

While Claude Code is running, a `WAITING` message appears every five seconds
with elapsed time and the timeout (180 seconds by default). The demo collects
the complete JSON answer, so no partial answer is displayed. These updates
report the wait, not the model's internal progress. No input is needed;
**Ctrl+C** cancels. Completion, process failure and timeout are reported
explicitly, and the wait updates stop before the result is shown.

Each phase also explains its implementation and emits live `TRACE` records:
timestamps, function calls, elapsed time, extraction results, SQL loads and
files written. Paragraphs and labeled values are separated by blank lines.
Runtime records appear in blocks labeled `Execution trace`, bounded by dashed
lines; each record shows its time and function above the operation's message.
The explanations follow the actual results; an unexpected
failure stops the walkthrough instead of printing a scripted success.

Use `--no-pause` for an unattended recording, or `--debug` to also show the
model instructions and exception tracebacks. `--no-debug` keeps the standard
explanation and execution traces. The demo resets its local mapping, data and
incident files when the first phase starts.

The DAGs run on **Airflow 2.11 and Airflow 3.x**. The imports that moved
between the two are guarded, and CI runs the whole suite against both
(Python 3.13 / Airflow 3.3.2, and Python 3.11 / Airflow 2.11.0). `make install`
picks the constraints file matching your Python.

The first six phases cover a healthy import, an API rename, the failed task,
diagnosis, repair checks and recovery. Phase 7 supplies a wrong `shipping_price`
proposal that the baseline rejects. Phase 8 compares two supplied diagnoses of
a `subtotal` replacement and demonstrates a wrong mapping being accepted and
loaded into an isolated warehouse copy. Every step identifies its actor and
shows the actual result.

Only phase 4 calls the live model when using the default backend. Phases 7 and
8 supply controlled diagnoses to exercise the checks, even in a live-model
run. They do not measure the LLM's classification accuracy. With `mock`, no
phase calls a model.

Diagnosis runs through the **Claude Code CLI on your own machine**, using your
Claude Code authentication and model access. The project does not require a
separate API key; live calls use your account's allowance or billing.

[docs/walkthrough.md](docs/walkthrough.md) goes further: four experiments you
can run by hand, and the order worth reading the code in.

### Or watch it run as Airflow

The demo above drives the same code from the command line so it can be read one
step at a time. To see it as two DAGs on a scheduler:

```bash
docker compose up
```

Both published ports bind to `127.0.0.1`. Then open <http://127.0.0.1:8080>.
No login: the demo container is configured to
let you straight in, which is not a thing to copy anywhere real. Use the IPv4
literal rather than `localhost`, which can resolve to IPv6 and reach nothing.

If you ran the console demo first, keep both DAGs paused and restore the
starting state once the API is ready:

```bash
./scripts/reset.sh
./scripts/inject_failure.sh reset
```

This removes the local warehouse, baselines and incidents. Then unpause both
DAGs, trigger `orders_ingest`, wait for the healthy run to succeed, break the
upstream with
`./scripts/inject_failure.sh rename`, and trigger it again. `reliability_layer`
waits on the incident queue with a deferred sensor and uses
`TriggerDagRunOperator` to request another ingestion run. A previous local
Docker Compose run with the mock diagnosis backend took about twelve seconds
from the incident being written to the pipeline being re-triggered. That is a
local observation, not a latency guarantee or a live-model benchmark.

Each task log carries the full gate trace, so the Airflow UI shows what was
verified, not only what was concluded.

## How it works

A run that fails its data contract builds an incident
while the context is still in memory: the contract, the current mapping, the
violations, the fields the upstream actually returned, redacted samples, and
the profile of the last known-good run.

The layer puts every mapping proposal through five gates, whether it comes
from Claude Code, the mock, `--diagnosis` or another backend. The first gate
uses the model's own `cause_class`; it does not independently establish the
failure's meaning.

| Gate | Question | Example refusal |
|---|---|---|
| Policy | Is the supplied failure class allowed to proceed? | A `semantic_change` diagnosis is never automated |
| Allowlist | Is the proposed action one of the few we can perform? | `rewrite_dag` is not on the list |
| Structure | Does the action make sense against the mapping and the payload? | the named source field was never sent |
| Content | Is there a complete `equivalent` assessment for the proposed fields, with actual old and new samples? | missing assessment, uncertain verdict or unavailable examples |
| Evidence | Does a fresh run satisfy the contract *and* still look right? | see below |

### What the baseline rejects

When the merchant renames `total_price` to `order_total`, the payload also
contains `shipping_price`. It is a number. It is in range. It is never null. A
repair that points `total_amount` at `shipping_price` **passes the data
contract cleanly** while putting delivery charges in the order-total column.
The import can complete successfully while revenue reports contain wrong
amounts.

So passing the contract is necessary and not sufficient. The candidate column
also has to resemble the column it replaces, compared against the last
known-good profile: same shape, comparable null rate, mean within tolerance.

That case is a test, not a paragraph:
`tests/test_healer.py::test_plausible_but_wrong_field_is_rejected_by_the_baseline`.

You can also watch it happen. `./scripts/demo.sh` hands the layer that exact
diagnosis: confidence 0.95, an action on the allowlist and true observations
about numeric type, range and nulls, alongside a deliberately false claim of
equivalent meaning. The demo explains every gate.
Its final evidence checks read:

```
5. evidence | Does a trial import pass BOTH data checks?
  PASS  a run with the candidate mapping passes the required-field,
        type and value rules (120 rows, 0 broken rules)
  STOP  'total_amount' mean order amount moved from 232.46 to 6.94
        (97% away, tolerance 25%)
```

Eight checks passed, including the content requirement and the contract. The
supplied diagnosis deliberately claims content equivalence incorrectly, so
this example reaches the numeric evidence check. The wrong proposal was rejected;
the accepted mapping from the earlier phase stayed at version 2. One
measurement stood between that diagnosis and silently wrong revenue figures.

### What the baseline accepts incorrectly

Phase 8 uses the fake API's `rename_to_subtotal` switch. It replaces the total
with a different quantity, defined in this fixture as 90% of the original
amount. That field is numeric, non-null and within the contract's range. Its
mean is only 10% away from the healthy baseline, inside the default 25%
tolerance.

The demo runs the real healer twice on the same evidence. Both supplied
answers propose the same deliberately wrong `total_amount -> subtotal` remap
with the same confidence. Only `cause_class` changes:

| Supplied classification | Observed behavior with the demo policy |
|---|---|
| `semantic_change` | Policy rejects the proposal; mapping and warehouse remain unchanged. |
| `schema_drift_renamed_field` | Every check passes, the mapping is applied and another import loads the wrong amounts. |

With the default batch, the loaded mean falls from 232.46 to 209.21. The demo
reads the copied database to verify the actual impact and confirms that the
main recovered mapping, baseline and warehouse remain unchanged. The source's
rename settings are restored in a `finally` block. Inspect the retained
`data/semantic-check-*/` directory for both resolutions, the applied wrong
mapping, the warehouse and the baseline refreshed from those wrong values.
These generated experiment directories are excluded from Git.

If a changed policy rejects the second proposal, the demo stops and reports
that result rather than claiming to have demonstrated a false acceptance.

These are deliberately limited POC verification rules, **not a finished
production validation standard**. The goal is to demonstrate how AI can assist
pipeline problem resolution through one concrete example. A real deployment's
acceptance criteria can be made as strict as its use case requires, through
configuration and additional implementation based on its data requirements
and acceptable risk.

Some controls are already configurable in `policy.yml`. For example, setting
`verification.baseline_tolerance.mean_relative_delta` to `0.05` would reject
this 10% difference. Setting `levels.schema_drift_renamed_field` to `escalate`
would refuse automatic repair and leave the incident for a person. The latter
does not provide an approval-and-resume workflow.

Checks against business rules, authoritative field definitions or human
approval before applying a repair would require further implementation.
Stricter thresholds can also reject valid changes, and no numeric tolerance
alone establishes equivalent meaning. The counterexample identifies a gap to
address when designing those checks; describing the project as a POC does not
make the accepted mapping correct.

### What the LLM contributes, and what remains unproven

Claude Code is instructed to compare the contents of historical and current
samples against the canonical field's description before proposing a rename.
For example, product descriptions should remain descriptions of products, not
customer reviews, shipping instructions or unrelated text. New products can
have different descriptions; literal equality is not required. Numeric fields
also need compatible quantities, units, scale and scope, such as order totals
versus subtotals or amounts versus percentages.

Its response includes a structured `content_check`: `equivalent`, `different`
or `insufficient_evidence`, the two field names, a summary of each set of
values and a concise reason. The healer's `content` gate requires this
assessment to be complete, refer to the proposed fields, report equivalent
content and have actual historical and current examples available. It applies
to **every diagnosis source**, including the mock, `--diagnosis` and future
backends, before a candidate is applied. Missing evidence or an unfavorable
assessment records `STOP` and escalates. The original diagnosis is retained;
the healer does not rewrite the model's answer to make it look like a refusal.

This requirement blocks an assessment that admits uncertainty, but a confident,
incorrect `equivalent` claim can pass. It checks the supplied assessment's
completeness and evidence availability, not its semantic truth. The default
mock supplies an explicitly scripted equivalence claim without performing
semantic analysis. Explicitly supplied answers are never filled in for the
caller: omit `content_check` and the remap is refused.

To supply historical values, explicitly enable sampling in a field's contract:

```yaml
- name: product_description
  type: string
  required: true
  description: Product features, materials and intended use for the catalog item.
  sample_for_diagnosis: true
```

A successful import saves up to three distinct non-empty scalar examples per
enabled field in the baseline; strings are capped at 240 characters. A later
incident includes them alongside the current upstream samples. Sampling is off
by default and known sensitive canonical or source field names are excluded.
This is not content-based redaction: enable it only for fields whose values
are appropriate to store and send for diagnosis. The orders demo enables it
for `total_amount`. Older baselines without examples still load, but any
remap needs a successful reference import with sampling enabled. These small samples
are not a full-dataset comparison and are not paired by record ID.

The model can classify a subtotal replacement as
`semantic_change`, which routes the incident to a person. Its diagnostic role
is broader than string similarity, but this repository has no benchmark
establishing better accuracy, cost or latency than a rule-based alternative.

One [recorded live observation](docs/observations/subtotal-claude-code.json)
on September 22, 2026 returned `semantic_change`, `different` and `escalate`
for the phase 8 incident. This observation predates the explicit Opus 5.5/max
selection; it is not an evaluation of that configuration. It records one
correct refusal, not an accuracy guarantee. Its explanation also suggested `subtotal + shipping_price` as a
possible reconstruction. The fixture does not support that formula: it defines
`subtotal` as 90% of the original total and generates shipping independently.
For example, `343.31 + 8.07 = 351.38`, while the original total is `381.46`.
The record retains the response and the calculation so the correct refusal
does not hide the flawed reasoning. Phase 8 therefore remains a comparison of
controlled answers, with no additional live call during the demo.

A [repeat with Opus 5.5 and maximum effort](docs/observations/subtotal-opus-5-5-max.json)
used the same incident and returned `semantic_change`, `different` and
`escalate` in 131.64 seconds. The healer refused the proposal; the isolated
mapping, baseline and warehouse remained unchanged. This answer explicitly
noted that the samples have no shared order IDs and that `subtotal + shipping_price`
still falls short of the historical examples if they refer to the same orders.
It nevertheless inferred that shipping is excluded and speculated about tax
or other charges, which this fixture does not establish. This is another
observed correct refusal with limits in its explanation, not proof of model
reliability. The CLI version and healer also changed between the observations;
these two runs are not a controlled benchmark of model versions.

The policy guarantee is conditional: **a diagnosis labeled `semantic_change`
is rejected**. It does not guarantee that every semantic change receives that
label. A model can misclassify a change and incorrectly assert equivalent
content. The numeric checks can then admit the wrong mapping. Phase 8 supplies
that false content assertion explicitly; it does not call the live backend.
Tests of supplied diagnoses
validate enforcement; they do not establish the live model's reliability.

Rolling windows and seasonality could improve the statistical reference, but
even identical distributions do not establish equivalent meaning. Provider
definitions, business invariants or human review can supply evidence that
these column profiles lack.

### Review happens after application

[`policy.yml`](policy.yml) assigns field renames to `auto_then_review`. The
mapping is applied and the pipeline may load data before a human reviews it.
If an accepted mapping is wrong, that permits incorrect values to enter the
warehouse and downstream reports. The next successful import also refreshes
the baseline from those values. A pending-review record is an audit obligation;
it does not prevent exposure, quarantine data, roll back a load or backfill
affected outputs. See [docs/autonomy-levels.md](docs/autonomy-levels.md).

### Untrusted input

Incidents embed samples from a third-party API. That is a natural place for
someone to put text arguing for a particular repair. The prompt says to treat
evidence as data, but the prompt is not the boundary. Two things are.

The first is that the diagnosis step cannot act. It runs as
`claude -p … --restricted --tools "" --strict-mcp-config`, which removes the
tools that run commands or code, disables the built-in tools, ignores every MCP
server configured on the machine, and bypasses the local settings files that
could hand any of it back. It can read its prompt and write an answer.

The second is that the answer is then treated as a hypothesis. A diagnosis
asking for something outside the allowlist is refused no matter how it is
phrased, and one that asks for something allowed still has to survive being
checked against the last known-good run.

Both of those are the process restricting itself. The next step, not taken
here, is to have the operating system enforce it — `sandbox-exec` with a
profile denying writes to the repository on macOS, or running the diagnosis in
a container with no repository mount. The evidence travels in the prompt, so
diagnosis does not require repository access. Such isolation would still need
implementation and validation.

## Layout

```
contracts/     the canonical shape downstream depends on; never machine-edited
mappings/      upstream-to-canonical mapping; the only configuration repair may edit
policy.yml     which failure classes may be automated, and the tolerances
baselines/     profile of the last known-good run
src/dag_healer/
  pipeline.py  extract, map, validate, load
  demo.py      interactive walkthrough, using the same pipeline and healer
  incident.py  structured evidence, built at the moment of failure
  queue.py     incidents are files in a directory; the layer drains them
  healer.py    five gates: policy, allowlist, structure, content, evidence
  backends/    Claude Code CLI, plus a deterministic stand-in for tests
dags/          orders_ingest, and the reliability_layer that drains its queue
fake_shop_api/ an upstream that changes its schema without telling you
```

The producer DAG writes an incident and raises an exception; it has no callback
into the reliability layer. The consumer uses a deferred sensor whose trigger
polls a shared directory every five seconds, releasing the worker slot while
waiting. This is filesystem polling, not a distributed event bus. The current
consumer is configured for one entity and re-triggers `orders_ingest` by name;
serving multiple pipelines requires additional routing and coordination.

## Tests

```bash
make test
```

Most of them are about what the layer declines to do. Repairing a renamed field
is the easy half.

## Honest limitations

- The end-to-end demonstration covers a renamed source field. The healer also
  accepts retry proposals and the policy admits `schema_drift_new_field`, but
  those settings do not establish complete handling of every failure class.
- The layer re-runs one pipeline after a successful repair. With several, that
  step becomes a dynamic mapping over the origin DAGs named in the resolutions.
- The baseline compares type category, null rate and numeric mean against one
  successful run. Similar quantities can pass incorrectly; legitimate seasonal
  changes can fail. Neither false-acceptance nor false-escalation rates have
  been measured. More statistical sophistication alone cannot prove meaning.
- `auto_then_review` allows loading before review. The POC has no approval
  gate, quarantine, automatic rollback or downstream backfill mechanism.
- YAML mappings and a shared file queue have no transactional coordination
  across workers. A distributed Airflow deployment would need shared durable
  state, concurrency control and consistent mapping versions; local execution
  does not validate that infrastructure.
- The upstream is a fixture, not a real merchant API, so nothing here has met
  pagination, partial failures, or a schema that changes halfway through a page.
- Structured incidents cover contract failures. Exhausted HTTP retries raise
  an error but do not currently create an incident for the reliability DAG.
- Sample redaction replaces values for a short list of sensitive top-level
  field names. It does not detect sensitive content in arbitrary or nested
  fields. The demonstration uses synthetic data.
- It has not run against production traffic. It is an experiment that works,
  not a product.

## Why it exists

Written to explore how diagnosis, bounded actions and independent checks fit
together in a pipeline recovery workflow. The accepted counterexample is part
of the result: useful checks still leave a gap between statistical resemblance
and business correctness.

MIT licensed.
