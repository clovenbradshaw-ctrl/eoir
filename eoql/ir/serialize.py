"""
EOQL IR Serialization

The EOQL-IR must be:
- Serializable: inspectable, diffable, auditable
- Roundtrip-safe: IR → JSON → IR retains meaning

This module provides JSON serialization for all IR types.
"""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any, Dict, Type, TypeVar

from .model import (
    AbsenceSpec,
    ConflictPolicy,
    EOQLQuery,
    ExpectationRef,
    FrameRef,
    GroundingSpec,
    Mode,
    Pattern,
    Predicate,
    ReturnSpec,
    SelectionRule,
    Target,
    TimeKind,
    TimeWindow,
    Visibility,
)

T = TypeVar("T")


class EOQLEncoder(json.JSONEncoder):
    """JSON encoder for EOQL IR types."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, Enum):
            return obj.value
        if is_dataclass(obj) and not isinstance(obj, type):
            return asdict(obj)
        if isinstance(obj, tuple):
            return list(obj)
        return super().default(obj)


def to_json(query: EOQLQuery, indent: int = 2) -> str:
    """
    Serialize an EOQL query to JSON.

    Args:
        query: The EOQL query to serialize
        indent: Indentation level for pretty-printing

    Returns:
        JSON string representation
    """
    return json.dumps(query, cls=EOQLEncoder, indent=indent)


def to_dict(query: EOQLQuery) -> Dict[str, Any]:
    """
    Convert an EOQL query to a dictionary.

    Args:
        query: The EOQL query to convert

    Returns:
        Dictionary representation
    """
    return json.loads(to_json(query, indent=None))


def from_json(json_str: str) -> EOQLQuery:
    """
    Deserialize an EOQL query from JSON.

    Args:
        json_str: JSON string to deserialize

    Returns:
        Reconstructed EOQL query

    Raises:
        ValueError: If the JSON is invalid or missing required fields
    """
    data = json.loads(json_str)
    return from_dict(data)


def from_dict(data: Dict[str, Any]) -> EOQLQuery:
    """
    Reconstruct an EOQL query from a dictionary.

    Args:
        data: Dictionary representation of the query

    Returns:
        Reconstructed EOQL query

    Raises:
        ValueError: If the dictionary is missing required fields
    """
    try:
        # Reconstruct nested objects
        frame = FrameRef(
            frame_id=data["frame"]["frame_id"],
            version=data["frame"].get("version"),
        )

        time_data = data["time"]
        time = TimeWindow(
            kind=TimeKind(time_data["kind"]),
            as_of=time_data.get("as_of"),
            start=time_data.get("start"),
            end=time_data.get("end"),
        )

        pattern_data = data.get("pattern", {})
        pattern = Pattern(
            match=pattern_data.get("match"),
            where=tuple(
                Predicate(
                    field=p["field"],
                    op=p["op"],
                    value=p["value"],
                )
                for p in pattern_data.get("where", [])
            ),
        )

        grounding_data = data.get("grounding", {})
        grounding = GroundingSpec(
            trace=grounding_data.get("trace", False),
            max_depth=grounding_data.get("max_depth", 0),
            grounded_by=tuple(
                Predicate(
                    field=p["field"],
                    op=p["op"],
                    value=p["value"],
                )
                for p in grounding_data.get("grounded_by", [])
            ),
        )

        absence = None
        if data.get("absence"):
            absence_data = data["absence"]
            expectation_data = absence_data["expectation"]
            absence = AbsenceSpec(
                expectation=ExpectationRef(
                    expectation_id=expectation_data["expectation_id"],
                    version=expectation_data.get("version"),
                ),
                scope=absence_data.get("scope"),
                deadline_hours=absence_data.get("deadline_hours"),
            )

        returns_data = data.get("returns", {})
        selection_rule = None
        if returns_data.get("selection_rule"):
            sr_data = returns_data["selection_rule"]
            selection_rule = SelectionRule(
                rule_id=sr_data["rule_id"],
                params=sr_data.get("params", {}),
            )

        returns = ReturnSpec(
            include_context=returns_data.get("include_context", True),
            include_frame=returns_data.get("include_frame", True),
            include_visibility_notes=returns_data.get("include_visibility_notes", True),
            include_conflicts=returns_data.get("include_conflicts", True),
            conflict_policy=ConflictPolicy(
                returns_data.get("conflict_policy", "EXPOSE_ALL")
            ),
            selection_rule=selection_rule,
        )

        return EOQLQuery(
            target=Target(data["target"]),
            mode=Mode(data["mode"]),
            visibility=Visibility(data["visibility"]),
            frame=frame,
            time=time,
            pattern=pattern,
            grounding=grounding,
            absence=absence,
            returns=returns,
        )

    except KeyError as e:
        raise ValueError(f"Missing required field: {e}")
    except (TypeError, ValueError) as e:
        raise ValueError(f"Invalid data format: {e}")


def diff_queries(q1: EOQLQuery, q2: EOQLQuery) -> Dict[str, Any]:
    """
    Compare two EOQL queries and return their differences.

    Args:
        q1: First query
        q2: Second query

    Returns:
        Dictionary describing the differences
    """
    d1 = to_dict(q1)
    d2 = to_dict(q2)

    def _diff_dicts(a: Dict, b: Dict, path: str = "") -> Dict[str, Any]:
        differences = {}

        all_keys = set(a.keys()) | set(b.keys())
        for key in all_keys:
            current_path = f"{path}.{key}" if path else key

            if key not in a:
                differences[current_path] = {"added": b[key]}
            elif key not in b:
                differences[current_path] = {"removed": a[key]}
            elif a[key] != b[key]:
                if isinstance(a[key], dict) and isinstance(b[key], dict):
                    nested_diff = _diff_dicts(a[key], b[key], current_path)
                    differences.update(nested_diff)
                else:
                    differences[current_path] = {"was": a[key], "now": b[key]}

        return differences

    diff = _diff_dicts(d1, d2)

    return {
        "same": len(diff) == 0,
        "differences": diff,
    }


def validate_json(json_str: str) -> tuple[bool, list[str]]:
    """
    Validate that a JSON string represents a valid EOQL query.

    Args:
        json_str: JSON string to validate

    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []

    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        return False, [f"Invalid JSON: {e}"]

    # Check required top-level fields
    required = ["target", "mode", "visibility", "frame", "time"]
    for field in required:
        if field not in data:
            errors.append(f"Missing required field: {field}")

    # Validate enums
    if "target" in data:
        try:
            Target(data["target"])
        except ValueError:
            errors.append(f"Invalid target: {data['target']}")

    if "mode" in data:
        try:
            Mode(data["mode"])
        except ValueError:
            errors.append(f"Invalid mode: {data['mode']}")

    if "visibility" in data:
        try:
            Visibility(data["visibility"])
        except ValueError:
            errors.append(f"Invalid visibility: {data['visibility']}")

    # Validate frame
    if "frame" in data:
        if not isinstance(data["frame"], dict):
            errors.append("frame must be an object")
        elif "frame_id" not in data["frame"]:
            errors.append("frame.frame_id is required")

    # Validate time
    if "time" in data:
        if not isinstance(data["time"], dict):
            errors.append("time must be an object")
        elif "kind" not in data["time"]:
            errors.append("time.kind is required")
        else:
            try:
                kind = TimeKind(data["time"]["kind"])
                if kind == TimeKind.AS_OF and not data["time"].get("as_of"):
                    errors.append("time.as_of is required for AS_OF queries")
                elif kind == TimeKind.BETWEEN:
                    if not data["time"].get("start"):
                        errors.append("time.start is required for BETWEEN queries")
                    if not data["time"].get("end"):
                        errors.append("time.end is required for BETWEEN queries")
            except ValueError:
                errors.append(f"Invalid time.kind: {data['time']['kind']}")

    return len(errors) == 0, errors
