# Readiness JSON bounds before serialization

Primary arc42 block: `warehouse`. Goal: #199, under roadmap #76.

`source_bytes` now delegates to `bounded_source_bytes`. Previously it allocated
`json.dumps(value)` in full before checking the 1 MiB result. The new helper
checks plain JSON types, traversal depth, visited nodes and exact encoded size
before invoking the document encoder. Existing record and decision semantic
validators remain unchanged.

## Contract

The maximum encoded document is 1,048,576 bytes, inclusive. Keys, punctuation,
quotes and ASCII escapes count toward that limit. Non-ASCII BMP characters and
control characters use JSON escaping; supplementary characters require two
six-byte escapes. The returned sorted, compact, ASCII-escaped bytes match the
previous encoder for supported values, including finite floats and negative zero.
Consequently ordinary retained record digests and model versions do not change.

The root can be a mapping and is copied. Nested values must be built-in JSON
objects, arrays, strings, finite numbers, booleans or null. String keys are
mandatory: integer-key coercion and tuple-to-array conversion are rejected.
Depth is at most 64 edges from the root; at most 100,000 nodes are visited,
including keys and repeated references. Integers have at most 4,096 magnitude
bits. Cycles are rejected; shared acyclic values are permitted and counted for
every occurrence. Diagnostics never interpolate keys or input values.

These are explicit input and work budgets, not a promise that total process
memory is 1 MiB. The caller's input is already allocated, and copying, sorting and
encoding require additional memory. Callers must not mutate inputs concurrently.
Custom mapping code is inside the caller trust boundary, not sandboxed by this
helper. Output is an integrity encoding, not a signature or runtime permission.

## Evidence

Run `python -m pytest -q tests/unit/test_worker_readiness_json.py` and the existing
source/snapshot/reader suites. Tests compare old and new bytes and SHA-256, cover
exact byte, depth and node boundaries, and prove oversized or unsupported inputs
fail before the document encoder is called. Escaping tests include ASCII,
controls, DEL, BMP, surrogate escapes and supplementary Unicode characters.

The source verifier itself is exercised with oversized evidence to show that the
shared boundary is reached before semantic processing. Normal semantic validation
is still covered by the existing real-record tests in repository CI.

This change performs no I/O, adds no dependency, and changes no schema, delivery
configuration, workflow or activation default. No scheduler, notification,
deployment or Terraform apply is introduced. Human acceptance remains separate
from automated validation.
