"""Bounded regular-file JSON intake; source authenticity is a caller concern."""
from __future__ import annotations

import json
import math
import os
import stat
from pathlib import Path
from typing import Any

from src.common.exceptions import ValidationError

MAX_JSON_BYTES = 1_048_576
MAX_JSON_DEPTH = 64
READ_CHUNK_BYTES = 65_536


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError("JSON evidence contains duplicate fields")
        result[key] = value
    return result


def _reject_constant(value: str) -> Any:
    raise ValidationError("JSON evidence contains a non-finite number")


def _finite_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValidationError("JSON evidence contains a non-finite number")
    return number


def _check_depth(raw: bytes) -> None:
    # Inspect only ASCII syntax outside strings before recursive JSON decoding.
    # UTF-8 and full syntax validation are still performed by the decoder.
    depth = 0
    quoted = False
    escaped = False
    for char in raw:
        if quoted:
            if escaped:
                escaped = False
            elif char == 92:  # backslash
                escaped = True
            elif char == 34:  # double quote
                quoted = False
        elif char == 34:
            quoted = True
        elif char in (91, 123):  # opening array or object
            depth += 1
            if depth > MAX_JSON_DEPTH:
                raise ValidationError("JSON evidence exceeds the nesting limit")
        elif char in (93, 125):
            depth -= 1


def _read_regular_file(path: Path, maximum: int) -> bytes:
    before = path.lstat()
    if stat.S_ISLNK(before.st_mode):
        raise ValidationError("JSON evidence input must not be a symbolic link")
    if not stat.S_ISREG(before.st_mode):
        raise ValidationError("JSON evidence input must be a regular file")
    flags = os.O_RDONLY
    for flag in ("O_NOFOLLOW", "O_NONBLOCK", "O_BINARY"):
        flags |= getattr(os, flag, 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise ValidationError("JSON evidence input must be a regular file")
        if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            raise ValidationError("JSON evidence input changed while opening")
        raw = bytearray()
        while len(raw) <= maximum:
            chunk = os.read(descriptor, min(READ_CHUNK_BYTES, maximum + 1 - len(raw)))
            if not chunk:
                break
            raw.extend(chunk)
        if len(raw) > maximum:
            raise ValidationError("JSON evidence exceeds the byte limit")
        return bytes(raw)
    finally:
        os.close(descriptor)


def load_bounded_json_object(path: Path, *, max_bytes: int = MAX_JSON_BYTES) -> dict[str, Any]:
    """Read one UTF-8 object with unique fields, finite numbers and bounded depth.

    Parent directories and filesystem availability must be trusted. Available
    no-follow/nonblocking flags and descriptor checks reduce path races; this is
    not a hostile-filesystem sandbox, an immutable snapshot or an I/O deadline.
    The byte limit is enforced on actual reads, not a prior reported file size.
    """
    if not isinstance(path, Path):
        raise ValidationError("JSON evidence path must be a Path")
    if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_JSON_BYTES:
        raise ValidationError("JSON evidence byte limit is invalid")
    try:
        raw = _read_regular_file(path, max_bytes)
        _check_depth(raw)
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object,
                           parse_constant=_reject_constant, parse_float=_finite_float)
        if not isinstance(value, dict):
            raise ValidationError("JSON evidence must be an object")
        return value
    except (OSError, ValueError, RecursionError, OverflowError):
        # Never retain filenames, input fragments or parser/provider diagnostics.
        raise ValidationError("unable to read valid JSON evidence") from None
