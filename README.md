# dag-healer

A small, working experiment in letting an AI agent repair a data pipeline, and
in the machinery required before that is a good idea.

The interesting claim is not that a model can read a stack trace. It is that a
narrow class of pipeline failures is both **recurring** and **mechanically
verifiable**, and that those two properties together are what make automation
safe. Everything outside that intersection is escalated to a human on purpose.

## The problem

Data movement generates incidents forever. Schemas change, sources go down,
entities grow in ways nobody forecast, credentials expire. That is normal and
it is why on-call rotations exist. It stops being manageable when the number of
integrations grows faster than the number of engineers: the same failure rate
per integration that is a minor nuisance across a dozen pipelines becomes a
full-time job across a thousand.

The usual response is to hire. The alternative worth testing is whether the
recurring, diagnosable subset can be handled without waking anyone, and whether
the rest can at least arrive with its evidence already gathered.

## What this does, and what it refuses to do

**It can**: classify a failure from structured evidence, propose repointing one
canonical field at a different upstream source field, prove that the repair
holds, apply it, record it as pending human review, and re-run the pipeline.

**It cannot**: change the data contract, change any code, add or remove a
canonical field, take an action outside a three-item allowlist, or apply any
repair it cannot verify. Those are not missing features. A layer that can relax
its own contract to make a failure disappear is not a reliability layer.

## See it in 60 seconds

```bash
make install
make api          # in one terminal
./scripts/demo.sh # in another; add `mock` to run without Claude Code
```

The demo runs a healthy pipeline, has the upstream rename a field without
warning, watches the next run fail, hands the incident to the reliability
layer, and shows what the layer did and what it is waiting for a human to
confirm.

Diagnosis runs through the **Claude Code CLI on your own machine**. There is no
API key in this repository and none is needed.

## How it works

A failing run does not write a log line and give up. It builds an incident
while the context is still in memory: the contract, the current mapping, the
violations, the fields the upstream actually returned, redacted samples, and
the profile of the last known-good run.

The layer then puts the model's diagnosis through four gates.

| Gate | Question | Example refusal |
|---|---|---|
| Policy | Is this class of failure one we agreed to automate? | `semantic_change` is never automated |
| Allowlist | Is the proposed action one of the few we can perform? | `rewrite_dag` is not on the list |
| Structure | Does the action make sense against the mapping and the payload? | the named source field was never sent |
| Evidence | Does a fresh run satisfy the contract *and* still look right? | see below |

### The gate that earns its keep

When the merchant renames `total_price` to `order_total`, the payload also
contains `shipping_price`. It is a number. It is in range. It is never null. A
repair that points `total_amount` at `shipping_price` **passes the data
contract cleanly** and silently destroys every downstream number. Nothing
breaks. Revenue reporting is simply wrong from that day forward, which is worse
than a failed DAG, because a failed DAG tells you.

So passing the contract is necessary and not sufficient. The candidate column
also has to resemble the column it replaces, compared against the last
known-good profile: same shape, comparable null rate, mean within tolerance.

That case is a test, not a paragraph:
`tests/test_healer.py::test_plausible_but_wrong_field_is_rejected_by_the_baseline`.

### Autonomy follows verifiability

Autonomy is granted per failure class in [`policy.yml`](policy.yml), and the
level depends on how mechanically checkable the outcome is, not on how
confident the model sounds. A retry verifies itself. A field remap needs a
contract pass and a profile match. A semantic change needs a human, because the
question it raises is what the data *means*, and no amount of evidence in the
payload answers that. See [docs/autonomy-levels.md](docs/autonomy-levels.md).

### Untrusted input

Incidents embed samples from a third-party API. That is a natural place for
someone to put text arguing for a particular repair. The prompt says to treat
evidence as data, but the prompt is not the boundary: the allowlist and the
verification are. A diagnosis that asks for something outside the allowlist is
refused no matter how it is phrased.

## Layout

```
contracts/     the canonical shape downstream depends on; never machine-edited
mappings/      upstream-to-canonical field mapping; the ONLY machine-editable file
policy.yml     which failure classes may be automated, and the tolerances
baselines/     profile of the last known-good run
src/dag_healer/
  pipeline.py  extract, map, validate, load
  incident.py  structured evidence, built at the moment of failure
  healer.py    the four gates
  backends/    Claude Code CLI, plus a deterministic stand-in for tests
dags/          orders_ingest, and the reliability_layer it hands failures to
fake_shop_api/ an upstream that changes its schema without telling you
```

The pipeline and the reliability layer are deliberately separate DAGs. A
pipeline that repairs itself is hard to reason about; a reliability layer that
sits beside many pipelines is something you can give a policy to.

## Tests

```bash
make test
```

Most of them are about what the layer declines to do. Repairing a renamed field
is the easy half.

## Honest limitations

- One failure class is implemented end to end (renamed source field), plus
  retries. The rest of the taxonomy in `policy.yml` routes to escalation.
- The baseline is a single-run profile. Real use wants a rolling window and
  seasonality, otherwise a quiet Sunday looks like a broken pipeline.
- The upstream is a fixture, not a real merchant API, so nothing here has met
  pagination, partial failures, or a schema that changes halfway through a page.
- It has not run against production traffic. It is an experiment that works,
  not a product.

## Why it exists

Written to test an argument I would otherwise only be able to assert: that the
useful question is not whether a model can fix a pipeline, but how much
verification you have to build before letting it try. The answer, at least for
this failure class, is: quite a lot, and it is the part worth building.

MIT licensed.
