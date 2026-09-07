# Unicode-safe JSON evidence intake

Primary arc42 block: `common`. Goal #207; prerequisite #186 / PR #192.

## Decision

The bounded JSON file reader now validates decoded string values and object
keys before returning evidence. An escaped unpaired surrogate is rejected with
a fixed diagnostic, including when nested inside arrays or objects. Correct
escaped surrogate pairs decode to supplementary characters and remain valid.

This is a deliberate interoperability tightening, not a claim that the original
input bytes were invalid UTF-8 or that all JSON parsers reject surrogate escapes.
Python's decoder can accept such escapes. The result can subsequently fail
strict UTF-8 encoding. The existing byte decoder did not cover this case because
JSON escape interpretation happens afterward.

RFC 8259 section 8.2 discusses the interoperability risk of unpaired surrogates:
https://www.rfc-editor.org/rfc/rfc8259.html#section-8.2

Python documents its permissive Unicode behavior:
https://docs.python.org/3.11/library/json.html#character-encodings

## Implementation and compatibility

`_check_unicode` walks the already-decoded built-in JSON containers. The existing
pre-decoding byte and depth limits bound its input and recursion depth. It checks
code points without encoding another copy of every string. It does not
normalize, replace, drop or repair characters. Valid multilingual text, literal
backslash-u text, supplementary characters, and distinct normalization forms
retain exactly their prior decoded values and canonical bytes/digests.

The file descriptor is already closed when this validation runs. File opening,
identity/type checks, actual read limits, duplicate-field checks, non-finite
number rejection and depth handling are unchanged. Existing consumers do not
need a second Unicode-specific parser once this change is integrated.

The policy does not reject every character that some downstream database may
restrict. For example, escaped NUL remains a Unicode scalar accepted by this
file reader. Domain and storage contracts retain their own requirements. Nor
does validation provide source authenticity, immutable file contents, a
filesystem sandbox or an I/O deadline. The original trusted-parent assumptions
in [bounded JSON evidence](bounded-json-evidence.md) still apply.

## Validation and acceptance

```bash
python -m pytest -q tests/unit/test_bounded_json.py \
  tests/unit/test_bounded_json_unicode.py
make quality-check
make security-check
make readiness-check
```

Cases cover high/low and malformed pairs in values, keys and nested arrays;
valid pair boundaries and actual UTF-8 text; literal escapes; normalization
preservation; equivalent-key duplicates; maximum supported depth; and closed
descriptors on validation failure. The baseline was reproduced with the exact
prerequisite source before applying the fix.

The candidate is a separate child of #192. It does not rewrite that PR or any
existing consumer sibling. Independent review and final-diff acceptance remain
separate from local tests and CI. No database, schema, workflow, dependency,
configuration enablement, transport or deployment changes are included.
