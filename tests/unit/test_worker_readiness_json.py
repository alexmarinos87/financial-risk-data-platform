from __future__ import annotations

import hashlib
import json
from types import MappingProxyType
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.warehouse import notification_worker_readiness_json as bounded
from src.warehouse.notification_worker_readiness_source import source_bytes


def original_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")


@pytest.mark.parametrize("value", [
    {}, {"items": []}, {"flags": [None, True, False]},
    {"ints": [0, -1, 10**100, -(2**4095)]},
    {"floats": [-0.0, 1.0, 1e-100, 1.23456789012345, 1e300]},
    {"z": {"b": 2, "a": 1}, "a": [{"x": "value"}]},
    {"text": ''.join(chr(code) for code in range(256))},
    {"text": '\u2028\ud800\udfff\uffff\U00010000\U0010ffff'},
])
def test_plain_json_preserves_original_bytes_and_digest(value: Any) -> None:
    expected = original_bytes(value)
    actual = source_bytes(value)
    assert actual == expected
    assert hashlib.sha256(actual).digest() == hashlib.sha256(expected).digest()


def test_root_mapping_is_copied_without_coercing_nested_values() -> None:
    assert source_bytes(MappingProxyType({"b": 2, "a": 1})) == b'{"a":1,"b":2}'
    with pytest.raises(ValidationError):
        source_bytes({"nested": MappingProxyType({"a": 1})})


@pytest.mark.parametrize("character", ['x', '\n', '\\', '"', '\x00', '\x7f', '\u00e9', '\U0001f600'])
def test_exact_encoded_size_boundary_includes_ascii_escaping(character: str) -> None:
    unit = len(original_bytes(character)) - 2
    count, remaining = divmod(bounded.MAX_SOURCE_BYTES - 8, unit)
    value = {"x": character * count + 'a' * remaining}
    raw = source_bytes(value)
    assert raw == original_bytes(value)
    assert len(raw) == bounded.MAX_SOURCE_BYTES
    value["x"] += 'a'
    with pytest.raises(ValidationError, match="1 MiB"):
        source_bytes(value)


@pytest.mark.parametrize("value", [
    {1: "coerced-key"}, {"tuple": (1, 2)}, {"number": float("nan")},
    {"number": float("inf")}, {"number": -float("inf")}, {"object": object()},
    {"bytes": b"value"}, {"set": {1, 2}}, {"int": 1 << 4096},
    {"int": -(1 << 4096)}, [], None,
])
def test_non_json_or_unbounded_values_fail_before_encoding(value: Any, monkeypatch: Any) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("encoder must not see invalid input")
    monkeypatch.setattr(bounded.json, "dumps", forbidden)
    with pytest.raises(ValidationError):
        source_bytes(value)


def test_oversized_string_fails_before_encoding(monkeypatch: Any) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("encoder must not allocate the oversized document")
    monkeypatch.setattr(bounded.json, "dumps", forbidden)
    with pytest.raises(ValidationError, match="1 MiB"):
        source_bytes({"x": "a" * bounded.MAX_SOURCE_BYTES})


@pytest.mark.parametrize("container_type", [list, dict])
def test_cycles_are_rejected_but_shared_acyclic_values_are_valid(container_type: Any) -> None:
    cyclic = container_type()
    if isinstance(cyclic, list):
        cyclic.append(cyclic)
    else:
        cyclic["self"] = cyclic
    with pytest.raises(ValidationError, match="cycles"):
        source_bytes({"x": cyclic})
    shared = [1, {"a": True}]
    assert source_bytes({"a": shared, "b": shared}) == original_bytes({"a": shared, "b": shared})


def nested(depth: int) -> dict[str, Any]:
    child: Any = 0
    for _ in range(depth - 1):
        child = [child]
    return {"x": child}


def test_depth_boundary_is_explicit() -> None:
    value = nested(bounded.MAX_SOURCE_DEPTH)
    assert source_bytes(value) == original_bytes(value)
    with pytest.raises(ValidationError, match="structural"):
        source_bytes(nested(bounded.MAX_SOURCE_DEPTH + 1))


def test_node_boundary_counts_keys_and_repeated_values() -> None:
    value = {"x": [None] * (bounded.MAX_SOURCE_NODES - 3)}
    assert source_bytes(value) == original_bytes(value)
    value["x"].append(None)
    with pytest.raises(ValidationError, match="node"):
        source_bytes(value)


def test_success_does_not_mutate_input() -> None:
    value = {"b": [3, 2, 1], "a": {"z": True}}
    before = original_bytes(value)
    source_bytes(value)
    assert original_bytes(value) == before
    assert list(value) == ["b", "a"]


def test_source_validator_inherits_guard_before_loading_semantic_dependencies(monkeypatch: Any) -> None:
    from src.warehouse.notification_worker_readiness_source import verify_worker_readiness_record
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("encoder must not see oversized readiness evidence")
    monkeypatch.setattr(bounded.json, "dumps", forbidden)
    with pytest.raises(ValidationError, match="1 MiB"):
        verify_worker_readiness_record(
            record={"x": "a" * bounded.MAX_SOURCE_BYTES}, document_sha256="0" * 64,
            expected_record_id="record", destination_id="destination", execution_kind="initial",
            observed_at="2026-06-01T12:00:00+00:00",
        )
