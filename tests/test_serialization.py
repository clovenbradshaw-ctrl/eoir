"""
Tests for EOQL IR Serialization

The EOQL-IR must be:
- Serializable: inspectable, diffable, auditable
- Roundtrip-safe: IR → JSON → IR retains meaning
"""

import pytest
import json

from eoql.ir import (
    EOQLQuery,
    FrameRef,
    Mode,
    Target,
    TimeWindow,
    Visibility,
    to_json,
    to_dict,
    from_json,
    from_dict,
    diff_queries,
    validate_json,
)
from eoql.builder import query


class TestSerialization:
    """Test JSON serialization."""

    def test_to_json(self):
        """Can serialize query to JSON."""
        q = query().claims().given().visible().default_frame().now().build()
        json_str = to_json(q)

        # Should be valid JSON
        data = json.loads(json_str)
        assert data["target"] == "CLAIMS"
        assert data["mode"] == "GIVEN"
        assert data["visibility"] == "VISIBLE"

    def test_to_dict(self):
        """Can convert query to dict."""
        q = query().claims().given().visible().default_frame().now().build()
        data = to_dict(q)

        assert isinstance(data, dict)
        assert data["target"] == "CLAIMS"


class TestDeserialization:
    """Test JSON deserialization."""

    def test_from_json(self):
        """Can deserialize query from JSON."""
        q1 = query().claims().given().visible().default_frame().now().build()
        json_str = to_json(q1)
        q2 = from_json(json_str)

        assert q2.target == q1.target
        assert q2.mode == q1.mode
        assert q2.visibility == q1.visibility
        assert q2.frame.frame_id == q1.frame.frame_id

    def test_from_dict(self):
        """Can reconstruct query from dict."""
        q1 = query().claims().given().visible().default_frame().now().build()
        data = to_dict(q1)
        q2 = from_dict(data)

        assert q2.target == q1.target
        assert q2.mode == q1.mode


class TestRoundtrip:
    """Test roundtrip serialization."""

    def test_roundtrip_minimal_query(self):
        """Minimal query survives roundtrip."""
        q1 = query().claims().given().visible().default_frame().now().build()
        q2 = from_json(to_json(q1))

        assert q1 == q2

    def test_roundtrip_complex_query(self):
        """Complex query survives roundtrip."""
        q1 = (
            query()
            .claims()
            .given()
            .visible()
            .under_frame("F_official", version="2.0")
            .between("2025-01-01", "2025-12-31")
            .where("claim_type", "=", "temperature")
            .where("certainty", ">=", 0.8)
            .with_trace(max_depth=3)
            .build()
        )
        q2 = from_json(to_json(q1))

        assert q1 == q2

    def test_roundtrip_absence_query(self):
        """Absence query survives roundtrip."""
        q1 = (
            query()
            .absences()
            .given()
            .visible()
            .default_frame()
            .between("2025-01-01", "2025-12-31")
            .expecting("EXP_monthly_report", version="1.0")
            .build()
        )
        q2 = from_json(to_json(q1))

        assert q1 == q2
        assert q2.absence.expectation.expectation_id == "EXP_monthly_report"


class TestDiff:
    """Test query diffing."""

    def test_identical_queries(self):
        """Identical queries show no diff."""
        q1 = query().claims().given().visible().default_frame().now().build()
        q2 = query().claims().given().visible().default_frame().now().build()

        # Note: time will differ slightly, so we need to be careful
        # For this test, let's use explicit time
        q1 = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .as_of("2025-12-27T00:00:00Z")
            .build()
        )
        q2 = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .as_of("2025-12-27T00:00:00Z")
            .build()
        )

        diff = diff_queries(q1, q2)
        assert diff["same"] is True

    def test_different_mode(self):
        """Detects mode difference."""
        q1 = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .as_of("2025-12-27T00:00:00Z")
            .build()
        )
        q2 = (
            query()
            .claims()
            .meant()
            .visible()
            .default_frame()
            .as_of("2025-12-27T00:00:00Z")
            .build()
        )

        diff = diff_queries(q1, q2)
        assert diff["same"] is False
        assert "mode" in diff["differences"]

    def test_different_frame(self):
        """Detects frame difference."""
        q1 = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .as_of("2025-12-27T00:00:00Z")
            .build()
        )
        q2 = (
            query()
            .claims()
            .given()
            .visible()
            .under_frame("F_official")
            .as_of("2025-12-27T00:00:00Z")
            .build()
        )

        diff = diff_queries(q1, q2)
        assert diff["same"] is False
        assert any("frame" in k for k in diff["differences"].keys())


class TestValidation:
    """Test JSON validation."""

    def test_valid_json(self):
        """Valid JSON passes validation."""
        q = query().claims().given().visible().default_frame().now().build()
        json_str = to_json(q)

        is_valid, errors = validate_json(json_str)
        assert is_valid is True
        assert len(errors) == 0

    def test_invalid_json(self):
        """Invalid JSON fails validation."""
        is_valid, errors = validate_json("not json")
        assert is_valid is False
        assert len(errors) > 0

    def test_missing_required_field(self):
        """Missing required field is detected."""
        json_str = json.dumps({
            "target": "CLAIMS",
            "mode": "GIVEN",
            # Missing: visibility, frame, time
        })

        is_valid, errors = validate_json(json_str)
        assert is_valid is False
        assert any("visibility" in e for e in errors)
        assert any("frame" in e for e in errors)
        assert any("time" in e for e in errors)

    def test_invalid_enum(self):
        """Invalid enum value is detected."""
        json_str = json.dumps({
            "target": "INVALID",
            "mode": "GIVEN",
            "visibility": "VISIBLE",
            "frame": {"frame_id": "F_default"},
            "time": {"kind": "AS_OF", "as_of": "2025-12-27"},
        })

        is_valid, errors = validate_json(json_str)
        assert is_valid is False
        assert any("target" in e.lower() for e in errors)
