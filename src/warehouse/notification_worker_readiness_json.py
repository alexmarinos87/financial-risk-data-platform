"""Bound JSON work before allocating the canonical readiness document."""
from __future__ import annotations

import json
import math
from collections.abc import Mapping
from typing import Any

from src.common.exceptions import ValidationError

MAX_SOURCE_BYTES = 1_048_576
MAX_SOURCE_DEPTH = 64
MAX_SOURCE_NODES = 100_000
MAX_INTEGER_BITS = 4096


def bounded_source_bytes(value: Mapping[str, Any]) -> bytes:
    """Preserve compact sorted ASCII JSON, with a bounded pre-encoding traversal.

    Root mappings are copied; nested values must be plain JSON types. The caller
    must not mutate the input during validation. Bounds cover encoded size and
    traversal work, not the caller's existing allocation or total process memory.
    """
    if not isinstance(value, Mapping):
        raise ValidationError("readiness source must be an object")
    if len(value) > MAX_SOURCE_NODES:
        raise ValidationError("readiness source exceeds the node limit")
    size = 0
    nodes = 0
    ancestors: set[int] = set()

    def add(amount: int) -> None:
        nonlocal size
        size += amount
        if size > MAX_SOURCE_BYTES:
            raise ValidationError("readiness source exceeds 1 MiB")

    def visit(item: Any, depth: int) -> None:
        nonlocal nodes
        nodes += 1
        if nodes > MAX_SOURCE_NODES or depth > MAX_SOURCE_DEPTH:
            raise ValidationError("readiness source exceeds structural limits")
        kind = type(item)
        if item is None:
            add(4)
        elif kind is bool:
            add(4 if item else 5)
        elif kind is str:
            # Every character takes at least one byte, before ASCII escaping.
            if len(item) + 2 > MAX_SOURCE_BYTES - size:
                raise ValidationError("readiness source exceeds 1 MiB")
            add(2)
            for character in item:
                code = ord(character)
                if character in '"\\\b\f\n\r\t':
                    add(2)
                elif 32 <= code <= 126:
                    add(1)
                else:
                    add(6 if code <= 0xFFFF else 12)
        elif kind is int:
            if item.bit_length() > MAX_INTEGER_BITS:
                raise ValidationError("readiness source integer exceeds the bit limit")
            add(len(str(item)))
        elif kind is float:
            if not math.isfinite(item):
                raise ValidationError("readiness source numbers must be finite")
            add(len(repr(item)))
        elif kind is dict or kind is list:
            if len(item) > MAX_SOURCE_NODES - nodes:
                raise ValidationError("readiness source exceeds the node limit")
            identity = id(item)
            if identity in ancestors:
                raise ValidationError("readiness source must not contain cycles")
            ancestors.add(identity)
            try:
                add(2 + max(0, len(item) - 1))
                if kind is dict:
                    add(len(item))  # One colon per key/value pair.
                    for key, child in item.items():
                        if type(key) is not str:
                            raise ValidationError("readiness source keys must be strings")
                        visit(key, depth + 1)
                        visit(child, depth + 1)
                else:
                    for child in item:
                        visit(child, depth + 1)
            finally:
                ancestors.remove(identity)
        else:
            raise ValidationError("readiness source contains a non-JSON value")

    try:
        document = dict(value)
        visit(document, 0)
        raw = json.dumps(
            document, sort_keys=True, separators=(",", ":"),
            ensure_ascii=True, allow_nan=False,
        ).encode("ascii")
        if len(raw) != size or len(raw) > MAX_SOURCE_BYTES:
            raise ValidationError("readiness source changed during serialization")
        return raw
    except (ValueError, TypeError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("readiness source must be bounded canonical JSON") from None
