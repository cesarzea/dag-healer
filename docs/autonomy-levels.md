# Autonomy levels

Autonomy is granted per failure class, and the deciding question is always the
same: **can the outcome be checked by something other than the thing that
proposed it?** Confidence scores are an input, never a licence.

## Level 0: automatic

*Transient source errors, rate limits, safe retries.*

Retrying the source request **is** the availability test: the next call either
succeeds or it does not. It changes no mapping and needs no model diagnosis.
If extraction succeeds, the normal import still validates and loads the data.
Exhausted HTTP retries currently raise without creating a structured incident.

## Level 1: automatic, recorded for review

*A known schema adaptation whose result can be checked mechanically.*

Implemented here for one case: an upstream field that was renamed. The repair
must pass policy, allowlist and structure checks, provide a complete equivalent
content assessment with old and new examples, and pass a fresh contract and
baseline check. The content gate checks the assessment, not its semantic truth.
The change bumps the mapping version,
is recorded in `history` with `review: pending`, and is visible in
`PYTHONPATH=src .venv/bin/python -m dag_healer.cli status`.

The pipeline can resume with the applied change before anyone reviews it. This
demo records pending review; it does not implement an approval gate or screen.

The verification checks whether the new column resembles the old one; it
cannot prove that is what the merchant intended. Phase 8 of the guided demo
shows an incorrect subtotal mapping passing all checks and loading wrong
amounts in an isolated copy. The recorded change lets a human inspect that
decision afterwards; it does not quarantine data, undo the load or backfill
affected reports. A successful load also refreshes the baseline, which can
make the wrong amounts the next reference.

## Level 2: proposed, never applied

*Anything requiring a change to connector, transformation or orchestration code.*

Out of scope for this repo, and deliberately so. The honest version needs an
isolated execution environment, regression tests, a full audit trail and a pull
request a human merges. Until that exists, code changes escalate.

## Level 3: escalate to a human, always

*Semantic change, type changes, removed fields, data-quality violations.*

These rules apply to the `cause_class` supplied by the diagnosis. The model
can infer a possible change of meaning from names and context, but Python does
not independently validate that classification. The guarantee is that a
`semantic_change` diagnosis is rejected, not that every semantic change is
recognized. An incorrect rename classification may reach and pass the data
checks. The escalation report gathers the violations, fields received, mapping
and diagnosis so a person can investigate the meaning with additional context.

## Why not just raise the confidence threshold

Because confidence and correctness come apart in exactly the case that matters.
The `shipping_price` decoy in the tests is a repair a model can propose with
high confidence and plausible numeric observations, and it is wrong in a way
that no contract check catches. The controlled diagnosis also asserts false
content equivalence. The baseline rejects that particular proposal. The subtotal
counterexample in phase 8 passes it, showing why neither confidence nor a
matching profile proves that a repair preserves meaning.
