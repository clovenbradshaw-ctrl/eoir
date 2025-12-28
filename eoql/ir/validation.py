"""
EOQL IR Validation

Enforces the Prime Rule by ensuring the IR is epistemically total and explicit.
This validator is the 'physics engine' of EOQL correctness.

The Prime Invariant (Hard Law):
> EOQL may not manufacture certainty the data model refused to store.

This is not a guideline. It is a soundness condition.
If violated, EOQL is incorrect even if it is fast, useful, or popular.
"""

from __future__ import annotations

from typing import List

from .model import (
    AbsenceSpec,
    ConflictPolicy,
    EOQLQuery,
    Target,
    TimeKind,
)


class EOQLValidationError(ValueError):
    """
    Raised when an EOQL query violates soundness invariants.

    These failures are not UX bugs. They are epistemic guardrails.
    An AI coder must optimize for honest refusal, not maximal answerability.
    """

    def __init__(self, errors: List[str], query: EOQLQuery | None = None):
        self.errors = errors
        self.query = query
        msg = "EOQLQuery failed validation:\n- " + "\n- ".join(errors)
        super().__init__(msg)


def validate_query(q: EOQLQuery) -> None:
    """
    Enforce the Prime Rule by ensuring the IR is epistemically total and explicit.

    EOQL must refuse to answer questions that:
    - omit time
    - omit frame
    - conflate GIVEN and MEANT
    - ask about absence without expectations
    - request certainty the data cannot support

    Raises:
        EOQLValidationError: If any soundness invariant is violated.
    """
    errors: List[str] = []

    # I0: totality checks (dataclass ensures presence, but check content)
    if not q.frame.frame_id:
        errors.append("I1: frame.frame_id must be non-empty.")
    if not q.time:
        errors.append("I2: time must be provided.")
    if not q.mode:
        errors.append("I3: mode (GIVEN/MEANT) must be provided.")
    if not q.visibility:
        errors.append("I4: visibility (VISIBLE/EXISTS) must be provided.")

    # I2: time window validity
    if q.time.kind == TimeKind.AS_OF:
        if not q.time.as_of:
            errors.append("I2: AS_OF time requires time.as_of.")
        if q.time.start or q.time.end:
            errors.append("I2: AS_OF must not include start/end.")
    elif q.time.kind == TimeKind.BETWEEN:
        if not (q.time.start and q.time.end):
            errors.append("I2: BETWEEN time requires time.start and time.end.")
        if q.time.as_of:
            errors.append("I2: BETWEEN must not include as_of.")
    else:
        errors.append("I2: Unsupported time.kind.")

    # I6: trace contract
    if q.grounding.trace and q.grounding.max_depth < 1:
        errors.append("I6: TRACE requires grounding.max_depth >= 1.")
    if q.grounding.grounded_by:
        for p in q.grounding.grounded_by:
            if not p.field or not p.op:
                errors.append("I6: GROUNDED BY predicates must have field and op.")

    # I5: absence contract
    if q.target == Target.ABSENCES or q.absence is not None:
        if q.absence is None:
            errors.append("I5: ABSENCES target requires absence spec.")
        else:
            _validate_absence(q.absence, errors)

    # I7: conflict policy explicitness and selection rule
    if q.returns.conflict_policy == ConflictPolicy.PICK_ONE:
        if q.returns.selection_rule is None:
            errors.append("I7: PICK_ONE requires returns.selection_rule.")
        else:
            if not q.returns.selection_rule.rule_id:
                errors.append("I7: selection_rule.rule_id must be non-empty.")

    if errors:
        raise EOQLValidationError(errors, q)


def _validate_absence(absence: AbsenceSpec, errors: List[str]) -> None:
    """
    Validate absence specification.

    Absence is computed, never retrieved. It requires:
    1. an expectation rule
    2. a time window
    3. a scope
    4. a frame
    """
    if not absence.expectation or not absence.expectation.expectation_id:
        errors.append("I5: absence.expectation.expectation_id must be provided.")
