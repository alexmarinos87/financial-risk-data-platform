# Bounded JSON evidence intake

Primary arc42 block: `common`. Goal #186 under roadmap #76.

## Decision

`src/common/bounded_json.py` provides `load_bounded_json_object(Path(...))` for
local retained evidence. It accepts one UTF-8 JSON object with unique fields,
finite floating-point numbers and at most 64 nested containers, counting the
root object. Callers may lower the byte limit; the ceiling is 1,048,576 bytes.

The reader checks the path without following its final component, opens a
read-only descriptor, validates the opened file type and identity, then reads
at most the byte limit plus one sentinel byte. Short reads are accumulated.
A prior reported file size never establishes the byte guarantee. A changed
path identity or symbolic-link replacement is rejected where the available
platform checks detect it. Every opened descriptor is closed in a `finally`
block; read or close errors do not produce a successful result.

A lightweight scan bounds nesting before recursive JSON decoding and ignores
brackets inside escaped strings. The ordinary decoder still validates the full
syntax and UTF-8. Duplicate fields are rejected at every object level, including
keys made equal by Unicode escapes. Non-finite tokens and floating-point
overflow are rejected. JSON arrays or scalar roots are not evidence objects.
Failures use fixed diagnostics without filenames, JSON fragments or driver text.

## Why plain JSON decoding is not enough

Python's default JSON decoder accepts repeated object names, retaining the last
value. `object_pairs_hook` preserves the pairs long enough to reject duplicates.
`parse_constant` handles non-finite tokens, while a finite-checking `parse_float`
also rejects numeric overflow such as `1e9999`.

References: Python's JSON decoder and file-descriptor interfaces:

- https://docs.python.org/3.11/library/json.html
- https://docs.python.org/3.11/library/os.html#os.read

## Limits and compatibility

Parent directories, filesystem availability and the local operating system are
trusted. No-follow and nonblocking flags are used only where available. Inode
and device checks detect ordinary replacement but are not an adversarial
filesystem sandbox. Concurrent in-place modification is not prevented, and a
regular-file read has no deadline. Callers needing stable evidence must use
immutable reviewed files. A close failure is reported, not retried against a
possibly reused descriptor.

The byte and nesting limits bound serialized input, not the exact memory used
by the decoded object. This helper does not verify signatures, source approval,
application field semantics or current database state. Those checks remain with
the existing downstream validators. It performs no writes, database reads,
network operations, scheduling or execution.

This first change is additive. The separate readiness-recorder integration will
replace its path-size-check/unbounded-read sequence and tighten duplicate-key
acceptance without changing persistence or CLI options.

## Validation

```bash
python -m pytest -q tests/unit/test_bounded_json.py
make quality-check
make security-check
make readiness-check
```

Regression cases cover real file reads, exact byte/depth boundaries, short
reads, file growth after inspection, path swaps, final-component symlinks,
directories, FIFOs/devices where supported, malformed UTF-8/JSON, duplicate
fields, non-finite values and descriptor read/stat/close failures. Platform-only
tests declare their prerequisites rather than claiming unavailable protection.
