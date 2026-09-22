# Autonomy levels

Autonomy is granted per failure class, and the deciding question is always the
same: **can the outcome be checked by something other than the thing that
proposed it?** Confidence scores are an input, never a licence.

## Level 0: automatic

*Transient source errors, rate limits, safe retries.*

Verification is free, because retrying **is** the test: the next call either
succeeds or it does not. No state changes, nothing to review.

## Level 1: automatic, held for review

*A known schema adaptation whose result can be checked mechanically.*

Implemented here for one case: an upstream field that was renamed. The repair
is applied only if a fresh run satisfies the contract and the remapped column
still resembles the column it replaces. The change bumps the mapping version,
is recorded in `history` with `review: pending`, and is visible in
`dag-healer status`.

The review is not ceremony. The verification proves the new column behaves like
the old one; it cannot prove that is what the merchant intended.

## Level 2: proposed, never applied

*Anything requiring a change to connector, transformation or orchestration code.*

Out of scope for this repo, and deliberately so. The honest version needs an
isolated execution environment, regression tests, a full audit trail and a pull
request a human merges. Until that exists, code changes escalate.

## Level 3: escalate to a human, always

*Semantic change, type changes, removed fields, data-quality violations.*

When a merchant adds a status code, or a field's meaning shifts while its type
stays put, the question is what the data means. Nothing in the payload answers
that, so no amount of verification substitutes for asking. The layer's job here
is to make the ask cheap: the escalation report arrives with the violations,
the fields actually seen, the current mapping and the diagnosis already
assembled.

## Why not just raise the confidence threshold

Because confidence and correctness come apart in exactly the case that matters.
The `shipping_price` decoy in the tests is a repair a model can propose with
high confidence and clean reasoning, and it is wrong in a way that no contract
check catches. Thresholds filter noise. Verification catches this.
