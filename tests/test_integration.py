"""
Integration Tests for EOQL

These tests verify the full flow:
Builder → IR → Validation → Compilation → Execution context

The goal is to ensure that epistemic guarantees are preserved
throughout the entire pipeline.
"""

import pytest

from eoql.builder import query
from eoql.ir import validate_query, to_json, from_json
from eoql.ir.model import ConflictPolicy, Target
from eoql.backends.sql.postgres import PostgresCompiler
from eoql.registry.frames import FrameRegistry, FrameDefinition
from eoql.registry.expectations import (
    ExpectationRegistry,
    ExpectationDefinition,
    ExpectationRule,
    ExpectationFrequency,
)


class TestFullPipeline:
    """Test the full query pipeline."""

    def test_builder_to_compilation(self):
        """Query flows from builder through compilation."""
        # Build
        q = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .as_of("2025-12-27T00:00:00Z")
            .where("claim_type", "=", "temperature")
            .build()
        )

        # Validate (already done in build, but explicit)
        validate_query(q)

        # Compile
        compiler = PostgresCompiler()
        plan = compiler.compile(q)

        # Verify SQL has required structure
        sql = plan.sql.lower()
        assert "event_replay" in sql
        assert "frame_id" in sql
        assert "f_default" in sql
        assert "temperature" in sql

        # Verify notes document guarantees
        assert any("time projection" in n.lower() for n in plan.notes)
        assert any("frame" in n.lower() for n in plan.notes)

    def test_serialization_preserves_compilation(self):
        """Serialized and deserialized query compiles identically."""
        q1 = (
            query()
            .claims()
            .given()
            .visible()
            .under_frame("F_official", version="2.0")
            .between("2025-01-01", "2025-12-31")
            .where("certainty", ">=", 0.8)
            .build()
        )

        # Serialize and deserialize
        json_str = to_json(q1)
        q2 = from_json(json_str)

        # Compile both
        compiler = PostgresCompiler()
        plan1 = compiler.compile(q1)
        plan2 = compiler.compile(q2)

        # SQL should be identical
        assert plan1.sql == plan2.sql

    def test_grounding_trace_compilation(self):
        """Grounding trace is properly compiled."""
        q = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .as_of("2025-12-27T00:00:00Z")
            .with_trace(max_depth=3)
            .build()
        )

        compiler = PostgresCompiler()
        plan = compiler.compile(q)

        sql = plan.sql.lower()

        # Should have grounding traversal CTE
        assert "grounding_traverse" in sql
        assert "grounding_depth" in sql

        # Should document trace in notes
        assert any("grounding" in n.lower() or "trace" in n.lower() for n in plan.notes)

    def test_absence_query_compilation(self):
        """Absence query compiles with expectation."""
        q = (
            query()
            .absences()
            .given()
            .visible()
            .default_frame()
            .between("2025-01-01", "2025-12-31")
            .expecting("EXP_monthly_report", version="1.0")
            .build()
        )

        compiler = PostgresCompiler()
        plan = compiler.compile(q)

        sql = plan.sql.lower()

        # Should have absence computation CTEs
        assert "expected_entities" in sql
        assert "actual_claims" in sql
        assert "computed_absences" in sql
        assert "exp_monthly_report" in sql

        # Should document absence in notes
        assert any("absence" in n.lower() for n in plan.notes)

    def test_conflict_policy_in_context(self):
        """Conflict policy is passed to execution context."""
        q = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .as_of("2025-12-27T00:00:00Z")
            .pick_one("highest_certainty")
            .build()
        )

        compiler = PostgresCompiler()
        plan = compiler.compile(q)

        # Context should have conflict policy
        assert plan.context["conflict_policy"] == "PICK_ONE"


class TestWithRegistries:
    """Test integration with registries."""

    def test_frame_registry_integration(self):
        """Queries work with frame registry."""
        registry = FrameRegistry()

        # Register custom frame
        frame = FrameDefinition(
            frame_id="F_strict",
            version="1.0",
            name="Strict Frame",
            config={"thresholds": {"certainty_minimum": 0.9}},
        )
        registry.register(frame)

        # Build query with that frame
        q = (
            query()
            .claims()
            .given()
            .visible()
            .under_frame("F_strict", version="1.0")
            .now()
            .build()
        )

        # Verify frame can be resolved
        from eoql.ir.model import FrameRef
        resolved = registry.resolve(FrameRef(frame_id="F_strict", version="1.0"))
        assert resolved.get_threshold("certainty_minimum") == 0.9

    def test_expectation_registry_integration(self):
        """Absence queries work with expectation registry."""
        registry = ExpectationRegistry()

        # Register expectation
        exp = ExpectationDefinition(
            expectation_id="EXP_daily_check",
            version="1.0",
            name="Daily Health Check",
            rule=ExpectationRule(
                claim_type="health_check",
                frequency=ExpectationFrequency.DAILY,
                entity_filter={"type": "sensor"},
            ),
        )
        registry.register(exp)

        # Build absence query
        q = (
            query()
            .absences()
            .given()
            .visible()
            .default_frame()
            .between("2025-12-01", "2025-12-31")
            .expecting("EXP_daily_check", version="1.0")
            .build()
        )

        # Verify expectation can be resolved
        resolved = registry.resolve("EXP_daily_check", "1.0")
        assert resolved.rule.claim_type == "health_check"


class TestEpistemicGuarantees:
    """Test that epistemic guarantees are maintained."""

    def test_no_silent_collapse_in_sql(self):
        """SQL never contains collapse patterns (except where semantically correct)."""
        # Test CLAIMS queries - these should never collapse
        queries = [
            query().claims().given().visible().default_frame().now().build(),
            query().claims().given().visible().default_frame()
                .now().with_trace(max_depth=2).build(),
        ]

        compiler = PostgresCompiler()
        forbidden_patterns = ["max(", "min(", "limit 1", "row_number"]

        for q in queries:
            plan = compiler.compile(q)
            sql = plan.sql.lower()

            for pattern in forbidden_patterns:
                assert pattern not in sql, f"Found forbidden pattern '{pattern}' in SQL"

            # DISTINCT is forbidden for CLAIMS, but allowed for ENTITIES
            if q.target == Target.CLAIMS:
                # DISTINCT ON is allowed for grounding
                if "distinct on" not in sql:
                    assert "distinct" not in sql, "DISTINCT not allowed for CLAIMS"

    def test_time_always_explicit(self):
        """Time projection is always explicit in SQL."""
        q = query().claims().given().visible().default_frame().now().build()

        compiler = PostgresCompiler()
        plan = compiler.compile(q)

        sql = plan.sql.lower()

        # Must have time-based filtering
        assert "asserted_at" in sql

    def test_frame_always_explicit(self):
        """Frame is always explicit in SQL."""
        q = query().claims().given().visible().default_frame().now().build()

        compiler = PostgresCompiler()
        plan = compiler.compile(q)

        sql = plan.sql.lower()

        # Must filter by frame
        assert "frame_id" in sql

    def test_mode_always_explicit(self):
        """Mode (GIVEN/MEANT) is always explicit in SQL."""
        q = query().claims().given().visible().default_frame().now().build()

        compiler = PostgresCompiler()
        plan = compiler.compile(q)

        sql = plan.sql.lower()

        # Must have mode filtering
        assert "assertion_mode" in sql or "mode_filtered" in sql

    def test_visibility_always_explicit(self):
        """Visibility is always explicit in SQL."""
        q = query().claims().given().visible().default_frame().now().build()

        compiler = PostgresCompiler()
        plan = compiler.compile(q)

        sql = plan.sql.lower()

        # Must have visibility handling
        assert "visibility" in sql


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_pattern_compiles(self):
        """Query with no pattern filters compiles."""
        q = query().claims().given().visible().default_frame().now().build()

        compiler = PostgresCompiler()
        plan = compiler.compile(q)

        assert plan.sql is not None
        assert len(plan.notes) > 0

    def test_between_time_compiles(self):
        """BETWEEN time projection compiles correctly."""
        q = (
            query()
            .claims()
            .given()
            .visible()
            .default_frame()
            .between("2025-01-01", "2025-12-31")
            .build()
        )

        compiler = PostgresCompiler()
        plan = compiler.compile(q)

        sql = plan.sql.lower()
        assert "between" in sql
        assert "2025-01-01" in sql
        assert "2025-12-31" in sql

    def test_exists_visibility_compiles(self):
        """EXISTS visibility includes hidden items with annotation."""
        q = query().claims().given().exists().default_frame().now().build()

        compiler = PostgresCompiler()
        plan = compiler.compile(q)

        sql = plan.sql.lower()

        # Should not filter on visibility
        assert "visibility_note" in sql or "visibility_scope != 'visible'" in sql
