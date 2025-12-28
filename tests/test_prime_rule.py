"""
Tests for the Prime Rule (EOQL Soundness Invariants)

These tests ensure that the IR validator enforces the Prime Invariant:
> EOQL may not manufacture certainty the data model refused to store.

Test Strategy:
- IR validation tests: unit tests that ensure invalid queries fail *before* compilation.
- These failures are not UX bugs. They are epistemic guardrails.
"""

import pytest

from eoql.ir.model import (
    AbsenceSpec,
    ConflictPolicy,
    EOQLQuery,
    ExpectationRef,
    FrameRef,
    GroundingSpec,
    Mode,
    Predicate,
    ReturnSpec,
    SelectionRule,
    Target,
    TimeKind,
    TimeWindow,
    Visibility,
)
from eoql.ir.validation import EOQLValidationError, validate_query


def base_query(**overrides) -> EOQLQuery:
    """Create a valid base query for testing."""
    base = EOQLQuery(
        target=Target.CLAIMS,
        mode=Mode.GIVEN,
        visibility=Visibility.VISIBLE,
        frame=FrameRef(frame_id="F_default", version="latest"),
        time=TimeWindow.asof("2025-12-27T00:00:00-05:00"),
    )
    return EOQLQuery(**{**base.__dict__, **overrides})


class TestTimeInvariants:
    """I2: Time is mandatory and must be well-formed."""

    def test_requires_asof_timestamp(self):
        """AS_OF time requires time.as_of to be set."""
        q = base_query()
        # break AS_OF invariant by constructing invalid time
        q_bad = EOQLQuery(
            **{**q.__dict__, "time": TimeWindow(kind=TimeKind.AS_OF)}  # as_of missing
        )
        with pytest.raises(EOQLValidationError) as e:
            validate_query(q_bad)
        assert "AS_OF time requires time.as_of" in str(e.value)

    def test_asof_must_not_have_start_end(self):
        """AS_OF must not include start/end fields."""
        q_bad = base_query(
            time=TimeWindow(
                kind=TimeKind.AS_OF,
                as_of="2025-12-27T00:00:00Z",
                start="2025-01-01",
            )
        )
        with pytest.raises(EOQLValidationError) as e:
            validate_query(q_bad)
        assert "AS_OF must not include start/end" in str(e.value)

    def test_between_requires_start_and_end(self):
        """BETWEEN time requires both start and end."""
        q_bad = base_query(
            time=TimeWindow(kind=TimeKind.BETWEEN, start="2025-01-01")
        )
        with pytest.raises(EOQLValidationError) as e:
            validate_query(q_bad)
        assert "BETWEEN time requires time.start and time.end" in str(e.value)

    def test_between_must_not_have_asof(self):
        """BETWEEN must not include as_of field."""
        q_bad = base_query(
            time=TimeWindow(
                kind=TimeKind.BETWEEN,
                start="2025-01-01",
                end="2025-12-31",
                as_of="2025-06-15",
            )
        )
        with pytest.raises(EOQLValidationError) as e:
            validate_query(q_bad)
        assert "BETWEEN must not include as_of" in str(e.value)

    def test_valid_between_passes(self):
        """Valid BETWEEN time should pass validation."""
        q = base_query(time=TimeWindow.between("2025-01-01", "2025-12-31"))
        validate_query(q)  # should not raise


class TestFrameInvariants:
    """I1: Frame is mandatory."""

    def test_requires_frame_id(self):
        """Frame must have a non-empty frame_id."""
        q_bad = base_query(frame=FrameRef(frame_id=""))
        with pytest.raises(EOQLValidationError) as e:
            validate_query(q_bad)
        assert "frame.frame_id must be non-empty" in str(e.value)


class TestGroundingInvariants:
    """I6: Trace/grounding contracts."""

    def test_trace_requires_depth(self):
        """TRACE requires grounding.max_depth >= 1."""
        q_bad = base_query(grounding=GroundingSpec(trace=True, max_depth=0))
        with pytest.raises(EOQLValidationError) as e:
            validate_query(q_bad)
        assert "TRACE requires grounding.max_depth >= 1" in str(e.value)

    def test_trace_with_valid_depth_passes(self):
        """TRACE with max_depth >= 1 should pass."""
        q = base_query(grounding=GroundingSpec(trace=True, max_depth=3))
        validate_query(q)  # should not raise

    def test_grounded_by_requires_field_and_op(self):
        """GROUNDED BY predicates must have field and op."""
        q_bad = base_query(
            grounding=GroundingSpec(
                grounded_by=(Predicate(field="", op="=", value="test"),)
            )
        )
        with pytest.raises(EOQLValidationError) as e:
            validate_query(q_bad)
        assert "GROUNDED BY predicates must have field and op" in str(e.value)


class TestAbsenceInvariants:
    """I5: Absence requires an expectation."""

    def test_absences_require_expectation(self):
        """ABSENCES target requires absence spec."""
        q_bad = base_query(target=Target.ABSENCES, absence=None)
        with pytest.raises(EOQLValidationError) as e:
            validate_query(q_bad)
        assert "ABSENCES target requires absence spec" in str(e.value)

    def test_absence_spec_requires_expectation_id(self):
        """Absence spec must have expectation.expectation_id."""
        q_bad = base_query(
            target=Target.ABSENCES,
            absence=AbsenceSpec(expectation=ExpectationRef(expectation_id="")),
        )
        with pytest.raises(EOQLValidationError) as e:
            validate_query(q_bad)
        assert "absence.expectation.expectation_id must be provided" in str(e.value)

    def test_valid_absence_passes(self):
        """Valid absence spec should pass validation."""
        q = base_query(
            target=Target.ABSENCES,
            absence=AbsenceSpec(
                expectation=ExpectationRef(
                    expectation_id="EXP_monthly_report",
                    version="1.0",
                )
            ),
        )
        validate_query(q)  # should not raise


class TestConflictPolicyInvariants:
    """I7: No silent conflict collapse."""

    def test_pick_one_requires_selection_rule(self):
        """PICK_ONE requires returns.selection_rule."""
        q_bad = base_query(
            returns=ReturnSpec(
                conflict_policy=ConflictPolicy.PICK_ONE,
                selection_rule=None,
            )
        )
        with pytest.raises(EOQLValidationError) as e:
            validate_query(q_bad)
        assert "PICK_ONE requires returns.selection_rule" in str(e.value)

    def test_pick_one_requires_nonempty_rule_id(self):
        """Selection rule must have non-empty rule_id."""
        q_bad = base_query(
            returns=ReturnSpec(
                conflict_policy=ConflictPolicy.PICK_ONE,
                selection_rule=SelectionRule(rule_id=""),
            )
        )
        with pytest.raises(EOQLValidationError) as e:
            validate_query(q_bad)
        assert "selection_rule.rule_id must be non-empty" in str(e.value)

    def test_pick_one_with_valid_rule_passes(self):
        """PICK_ONE with valid selection rule should pass."""
        q = base_query(
            returns=ReturnSpec(
                conflict_policy=ConflictPolicy.PICK_ONE,
                selection_rule=SelectionRule(rule_id="highest_certainty"),
            )
        )
        validate_query(q)  # should not raise

    def test_expose_all_passes_without_rule(self):
        """EXPOSE_ALL does not require selection rule."""
        q = base_query(
            returns=ReturnSpec(conflict_policy=ConflictPolicy.EXPOSE_ALL)
        )
        validate_query(q)  # should not raise


class TestValidQueries:
    """Ensure valid queries pass validation."""

    def test_minimal_valid_query_passes(self):
        """Minimal valid query should pass validation."""
        q = base_query()
        validate_query(q)  # should not raise

    def test_fully_specified_query_passes(self):
        """Fully specified query should pass validation."""
        q = EOQLQuery(
            target=Target.CLAIMS,
            mode=Mode.GIVEN,
            visibility=Visibility.VISIBLE,
            frame=FrameRef(frame_id="F_official", version="2.0"),
            time=TimeWindow.asof("2025-12-27T12:00:00Z"),
            grounding=GroundingSpec(
                trace=True,
                max_depth=5,
                grounded_by=(Predicate(field="source.type", op="=", value="primary"),),
            ),
            returns=ReturnSpec(
                include_context=True,
                include_frame=True,
                conflict_policy=ConflictPolicy.RANK,
            ),
        )
        validate_query(q)  # should not raise
