"""
Compiler Contract Tests for Postgres Backend

These tests ensure the compiler preserves epistemic shape.

The question is never: "Does this SQL run?"
The question is: "Does this SQL preserve the uncertainty, time, frame,
and grounding the IR demanded?"

What We Test For:

The compiler must:
- Emit time replay logic
- NOT emit snapshot shortcuts
- NOT emit conflict-collapsing SQL
- NOT imply default frames
- NOT silently drop grounding or ambiguity

If someone tries to "optimize" EOQL into lying, CI breaks.
That's the moment EOQL becomes real.
"""

import pytest

from eoql.backends.sql.postgres import PostgresCompiler
from eoql.ir.model import (
    ConflictPolicy,
    EOQLQuery,
    FrameRef,
    Mode,
    ReturnSpec,
    SelectionRule,
    Target,
    TimeWindow,
    Visibility,
)


def base_query(**overrides) -> EOQLQuery:
    """Create a valid base query for testing."""
    base = EOQLQuery(
        target=Target.CLAIMS,
        mode=Mode.GIVEN,
        visibility=Visibility.VISIBLE,
        frame=FrameRef(frame_id="F_default", version="latest"),
        time=TimeWindow.asof("2025-12-27T00:00:00Z"),
    )
    return EOQLQuery(**{**base.__dict__, **overrides})


def compile_sql(q: EOQLQuery) -> str:
    """Compile query and return lowercase SQL for easier assertions."""
    compiler = PostgresCompiler()
    plan = compiler.compile(q)
    return plan.sql.lower()


class TestTimeProjection:
    """Time is projection, not filtering."""

    def test_time_projection_is_explicit(self):
        """Time projection must use event replay CTE, not WHERE shortcut."""
        sql = compile_sql(base_query())
        assert "event_replay" in sql
        assert "asserted_at <=" in sql or "between" in sql

    def test_asof_uses_replay_cte(self):
        """AS_OF time must use replay CTE pattern."""
        sql = compile_sql(base_query(time=TimeWindow.asof("2025-06-15T00:00:00Z")))
        assert "event_replay as" in sql
        assert "2025-06-15" in sql

    def test_between_uses_replay_cte(self):
        """BETWEEN time must use replay CTE pattern."""
        sql = compile_sql(
            base_query(time=TimeWindow.between("2025-01-01", "2025-12-31"))
        )
        assert "event_replay as" in sql
        assert "between" in sql
        assert "2025-01-01" in sql
        assert "2025-12-31" in sql


class TestFrameApplication:
    """Frames are claims, not settings."""

    def test_frame_is_not_implicit(self):
        """Frame must appear explicitly in SQL."""
        sql = compile_sql(base_query())
        assert "frame_id" in sql
        assert "f_default" in sql

    def test_frame_uses_explicit_filter(self):
        """Frame must use explicit WHERE clause."""
        sql = compile_sql(base_query(frame=FrameRef(frame_id="F_official")))
        assert "frame_id = 'f_official'" in sql


class TestNoConflictCollapse:
    """No silent conflict collapse allowed."""

    def test_no_distinct_allowed(self):
        """DISTINCT is forbidden - it silently collapses conflicts."""
        sql = compile_sql(base_query())
        assert "distinct" not in sql

    def test_no_max_or_min_allowed(self):
        """MAX/MIN are forbidden - they pick winners silently."""
        sql = compile_sql(base_query())
        assert "max(" not in sql
        assert "min(" not in sql

    def test_no_latest_row_shortcut(self):
        """ORDER BY ... LIMIT 1 is forbidden - it's a hidden PICK_ONE."""
        sql = compile_sql(base_query())
        # These patterns would indicate silent conflict resolution
        forbidden = ["order by", "limit 1"]
        for f in forbidden:
            assert f not in sql


class TestConflictPolicyPreservation:
    """Conflict policy must be preserved, not ignored."""

    def test_conflict_policy_pick_one_requires_annotation(self):
        """PICK_ONE must leave a trace in the SQL."""
        q = base_query(
            returns=ReturnSpec(
                conflict_policy=ConflictPolicy.PICK_ONE,
                selection_rule=SelectionRule(rule_id="highest_certainty"),
            )
        )
        sql = compile_sql(q)
        # The SQL should indicate that single selection is happening explicitly
        assert "single selection" in sql or "explicit rule" in sql

    def test_expose_all_preserves_all_rows(self):
        """EXPOSE_ALL must not add any limiting clauses."""
        sql = compile_sql(
            base_query(returns=ReturnSpec(conflict_policy=ConflictPolicy.EXPOSE_ALL))
        )
        # Should not have any conflict-collapsing patterns
        assert "distinct" not in sql
        assert "limit" not in sql
        assert "group by" not in sql


class TestVisibilityConstraints:
    """VISIBLE vs EXISTS must be explicit."""

    def test_visible_filters_are_explicit(self):
        """VISIBLE must add explicit visibility filter."""
        sql = compile_sql(base_query(visibility=Visibility.VISIBLE))
        assert "visibility_scope" in sql

    def test_exists_does_not_filter_visibility(self):
        """EXISTS must not filter on visibility (but can annotate)."""
        sql = compile_sql(base_query(visibility=Visibility.EXISTS))
        # EXISTS should not have WHERE visibility_scope = 'visible'
        # But it CAN have visibility_scope in SELECT/CASE for annotation
        assert "where visibility_scope = 'visible'" not in sql


class TestEpistemicNotes:
    """Compiler must document epistemic guarantees."""

    def test_plan_includes_time_note(self):
        """Plan notes should document time projection."""
        compiler = PostgresCompiler()
        plan = compiler.compile(base_query())
        assert any("time projection" in note.lower() for note in plan.notes)

    def test_plan_includes_frame_note(self):
        """Plan notes should document frame application."""
        compiler = PostgresCompiler()
        plan = compiler.compile(base_query())
        assert any("frame" in note.lower() for note in plan.notes)


class TestNoOptimizationThatLies:
    """
    Ensure common SQL optimizations that break epistemic guarantees
    are not present.
    """

    def test_no_first_value(self):
        """FIRST_VALUE window function can hide conflicts."""
        sql = compile_sql(base_query())
        assert "first_value" not in sql

    def test_no_row_number_limit(self):
        """ROW_NUMBER() ... = 1 is a hidden PICK_ONE."""
        sql = compile_sql(base_query())
        assert "row_number" not in sql

    def test_no_coalesce_on_core_values(self):
        """COALESCE can manufacture certainty from absence."""
        sql = compile_sql(base_query())
        # COALESCE is allowed in general, but should not appear in core selection
        # This test is intentionally lenient - a stricter version would
        # parse the SQL and check context
        pass  # Placeholder for more sophisticated check
