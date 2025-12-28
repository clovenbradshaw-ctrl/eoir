"""
Tests for the EOQL Fluent Query Builder

The builder enforces totality: you cannot build an incomplete query.
It makes the epistemic requirements explicit through the API design.
"""

import pytest
from datetime import datetime

from eoql.builder import QueryBuilder, query, IncompleteQueryError
from eoql.ir.model import (
    ConflictPolicy,
    Mode,
    Target,
    TimeKind,
    Visibility,
)
from eoql.ir.validation import EOQLValidationError


class TestBuilderBasics:
    """Test basic builder functionality."""

    def test_minimal_query(self):
        """Can build a minimal valid query."""
        q = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .now()
            .build()
        )

        assert q.target == Target.CLAIMS
        assert q.mode == Mode.GIVEN
        assert q.visibility == Visibility.VISIBLE
        assert q.frame.frame_id == "F_default"
        assert q.time.kind == TimeKind.AS_OF

    def test_incomplete_query_fails(self):
        """Incomplete queries raise IncompleteQueryError."""
        with pytest.raises(IncompleteQueryError) as exc_info:
            query().claims().build()

        # Should list what's missing
        assert "mode" in str(exc_info.value)
        assert "visibility" in str(exc_info.value)
        assert "frame" in str(exc_info.value)
        assert "time" in str(exc_info.value)

    def test_missing_target(self):
        """Query without target fails."""
        with pytest.raises(IncompleteQueryError) as exc_info:
            (
                query()
                .given()
                .visible()
                .default_frame()
                .now()
                .build()
            )
        assert "target" in str(exc_info.value)


class TestTargets:
    """Test different query targets."""

    def test_claims_target(self):
        """Can query for claims."""
        q = query().claims().given().visible().default_frame().now().build()
        assert q.target == Target.CLAIMS

    def test_entities_target(self):
        """Can query for entities."""
        q = query().entities().given().visible().default_frame().now().build()
        assert q.target == Target.ENTITIES

    def test_assertions_target(self):
        """Can query for assertions."""
        q = query().assertions().given().visible().default_frame().now().build()
        assert q.target == Target.ASSERTIONS

    def test_absences_require_expectation(self):
        """Absence queries require an expectation."""
        with pytest.raises(IncompleteQueryError) as exc_info:
            query().absences().given().visible().default_frame().now().build()
        assert "expectation" in str(exc_info.value)

    def test_absences_with_expectation(self):
        """Can query for absences with expectation."""
        q = (
            query()
            .absences()
            .given()
            .visible()
            .default_frame()
            .now()
            .expecting("EXP_monthly_report", version="1.0")
            .build()
        )
        assert q.target == Target.ABSENCES
        assert q.absence is not None
        assert q.absence.expectation.expectation_id == "EXP_monthly_report"


class TestTimeProjection:
    """Test time projection (AS_OF vs BETWEEN)."""

    def test_as_of_with_string(self):
        """AS_OF accepts ISO string."""
        q = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .as_of("2025-12-27T00:00:00Z")
            .build()
        )
        assert q.time.kind == TimeKind.AS_OF
        assert q.time.as_of == "2025-12-27T00:00:00Z"

    def test_as_of_with_datetime(self):
        """AS_OF accepts datetime object."""
        dt = datetime(2025, 12, 27, 12, 0, 0)
        q = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .as_of(dt)
            .build()
        )
        assert q.time.kind == TimeKind.AS_OF
        assert "2025-12-27" in q.time.as_of

    def test_between(self):
        """BETWEEN sets time range."""
        q = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .between("2025-01-01", "2025-12-31")
            .build()
        )
        assert q.time.kind == TimeKind.BETWEEN
        assert q.time.start == "2025-01-01"
        assert q.time.end == "2025-12-31"


class TestFrames:
    """Test frame specification."""

    def test_default_frame(self):
        """Default frame uses F_default."""
        q = query().claims().given().visible().default_frame().now().build()
        assert q.frame.frame_id == "F_default"
        assert q.frame.version == "latest"

    def test_custom_frame(self):
        """Can specify custom frame."""
        q = (
            query()
            .claims()
            .given()
            .visible()
            .under_frame("F_official", version="2.0")
            .now()
            .build()
        )
        assert q.frame.frame_id == "F_official"
        assert q.frame.version == "2.0"


class TestPatternFiltering:
    """Test pattern matching and WHERE clauses."""

    def test_where_predicate(self):
        """Can add WHERE predicates."""
        q = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .now()
            .where("claim_type", "=", "temperature")
            .build()
        )
        assert len(q.pattern.where) == 1
        assert q.pattern.where[0].field == "claim_type"
        assert q.pattern.where[0].op == "="
        assert q.pattern.where[0].value == "temperature"

    def test_multiple_predicates(self):
        """Can chain multiple WHERE predicates."""
        q = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .now()
            .where("claim_type", "=", "temperature")
            .where("certainty", ">=", 0.8)
            .build()
        )
        assert len(q.pattern.where) == 2

    def test_where_in(self):
        """Can use IN operator."""
        q = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .now()
            .where_in("claim_type", ["temperature", "humidity"])
            .build()
        )
        assert q.pattern.where[0].op == "IN"


class TestGrounding:
    """Test grounding/trace configuration."""

    def test_with_trace(self):
        """Can enable tracing."""
        q = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .now()
            .with_trace(max_depth=3)
            .build()
        )
        assert q.grounding.trace is True
        assert q.grounding.max_depth == 3

    def test_grounded_by(self):
        """Can add grounding filters."""
        q = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .now()
            .with_trace(max_depth=2)
            .grounded_by("source.type", "=", "primary")
            .build()
        )
        assert len(q.grounding.grounded_by) == 1


class TestConflictPolicy:
    """Test conflict policy configuration."""

    def test_expose_all_default(self):
        """Default conflict policy is EXPOSE_ALL."""
        q = query().claims().given().visible().default_frame().now().build()
        assert q.returns.conflict_policy == ConflictPolicy.EXPOSE_ALL

    def test_pick_one_requires_rule(self):
        """PICK_ONE requires selection rule with non-empty rule_id."""
        from eoql.ir.validation import EOQLValidationError

        with pytest.raises(EOQLValidationError) as exc_info:
            (
                query()
                .claims()
                .given()
                .visible()
                .default_frame()
                .now()
                .pick_one("")  # Empty rule_id - fails validation
                .build()
            )
        assert "selection_rule" in str(exc_info.value)

    def test_pick_one_with_rule(self):
        """PICK_ONE with valid rule works."""
        q = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .now()
            .pick_one("highest_certainty")
            .build()
        )
        assert q.returns.conflict_policy == ConflictPolicy.PICK_ONE
        assert q.returns.selection_rule.rule_id == "highest_certainty"


class TestBuilderCopy:
    """Test builder copy functionality."""

    def test_copy_creates_independent_builder(self):
        """Copy creates independent builder."""
        b1 = query().claims().given()
        b2 = b1.copy().meant()

        q1 = b1.visible().default_frame().now().build()
        q2 = b2.visible().default_frame().now().build()

        assert q1.mode == Mode.GIVEN
        assert q2.mode == Mode.MEANT
