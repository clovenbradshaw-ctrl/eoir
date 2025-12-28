"""
EOQL Fluent Query Builder

Provides a fluent API for constructing EOQL queries safely.

The builder enforces totality: you cannot build an incomplete query.
It makes the epistemic requirements explicit through the API design.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from eoql.ir.model import (
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
from eoql.ir.validation import validate_query


class QueryBuilderError(Exception):
    """Raised when query building fails."""

    pass


class IncompleteQueryError(QueryBuilderError):
    """Raised when trying to build an incomplete query."""

    def __init__(self, missing_fields: List[str]):
        self.missing_fields = missing_fields
        super().__init__(f"Query is incomplete. Missing: {', '.join(missing_fields)}")


@dataclass
class _BuilderState:
    """Internal state for the builder."""

    target: Optional[Target] = None
    mode: Optional[Mode] = None
    visibility: Optional[Visibility] = None
    frame_id: Optional[str] = None
    frame_version: Optional[str] = None
    time_kind: Optional[TimeKind] = None
    time_as_of: Optional[str] = None
    time_start: Optional[str] = None
    time_end: Optional[str] = None
    match_pattern: Optional[str] = None
    predicates: List[Predicate] = None
    trace: bool = False
    max_depth: int = 0
    grounded_by: List[Predicate] = None
    expectation_id: Optional[str] = None
    expectation_version: Optional[str] = None
    absence_scope: Optional[Dict[str, Any]] = None
    absence_deadline_hours: Optional[int] = None
    include_context: bool = True
    include_frame: bool = True
    include_visibility_notes: bool = True
    include_conflicts: bool = True
    conflict_policy: ConflictPolicy = ConflictPolicy.EXPOSE_ALL
    selection_rule_id: Optional[str] = None
    selection_rule_params: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.predicates is None:
            self.predicates = []
        if self.grounded_by is None:
            self.grounded_by = []


class QueryBuilder:
    """
    Fluent builder for EOQL queries.

    Usage:
        query = (
            QueryBuilder()
            .claims()
            .given()
            .visible()
            .under_frame("F_official", version="2.0")
            .as_of("2025-12-27T00:00:00Z")
            .where("claim_type", "=", "temperature")
            .build()
        )

    The builder enforces that all required fields are set before building.
    """

    def __init__(self) -> None:
        self._state = _BuilderState()

    # ========== Target selection ==========

    def claims(self) -> "QueryBuilder":
        """Query for claims."""
        self._state.target = Target.CLAIMS
        return self

    def entities(self) -> "QueryBuilder":
        """Query for entities."""
        self._state.target = Target.ENTITIES
        return self

    def edges(self) -> "QueryBuilder":
        """Query for edges/relationships."""
        self._state.target = Target.EDGES
        return self

    def assertions(self) -> "QueryBuilder":
        """Query for raw assertions."""
        self._state.target = Target.ASSERTIONS
        return self

    def absences(self) -> "QueryBuilder":
        """Query for computed absences."""
        self._state.target = Target.ABSENCES
        return self

    # ========== Mode (GIVEN vs MEANT) ==========

    def given(self) -> "QueryBuilder":
        """Only return directly asserted facts (GIVEN mode)."""
        self._state.mode = Mode.GIVEN
        return self

    def meant(self) -> "QueryBuilder":
        """Include derived/inferred claims (MEANT mode)."""
        self._state.mode = Mode.MEANT
        return self

    # ========== Visibility (VISIBLE vs EXISTS) ==========

    def visible(self) -> "QueryBuilder":
        """Only return items visible in current scope."""
        self._state.visibility = Visibility.VISIBLE
        return self

    def exists(self) -> "QueryBuilder":
        """Return all items including scoped-out ones."""
        self._state.visibility = Visibility.EXISTS
        return self

    # ========== Frame (required) ==========

    def under_frame(
        self, frame_id: str, version: Optional[str] = None
    ) -> "QueryBuilder":
        """
        Specify the interpretation frame.

        Selecting a frame is making a claim about how to interpret the data.
        """
        self._state.frame_id = frame_id
        self._state.frame_version = version or "latest"
        return self

    def default_frame(self) -> "QueryBuilder":
        """Use the default interpretation frame."""
        return self.under_frame("F_default", "latest")

    # ========== Time (required) ==========

    def as_of(self, timestamp: Union[str, datetime]) -> "QueryBuilder":
        """
        Set point-in-time projection.

        This is world reconstruction, not row filtering.
        The system will replay events up to this time.
        """
        self._state.time_kind = TimeKind.AS_OF
        if isinstance(timestamp, datetime):
            self._state.time_as_of = timestamp.isoformat()
        else:
            self._state.time_as_of = timestamp
        self._state.time_start = None
        self._state.time_end = None
        return self

    def between(
        self,
        start: Union[str, datetime],
        end: Union[str, datetime],
    ) -> "QueryBuilder":
        """
        Set time range projection.

        Returns events that occurred within this window.
        """
        self._state.time_kind = TimeKind.BETWEEN
        self._state.time_as_of = None
        if isinstance(start, datetime):
            self._state.time_start = start.isoformat()
        else:
            self._state.time_start = start
        if isinstance(end, datetime):
            self._state.time_end = end.isoformat()
        else:
            self._state.time_end = end
        return self

    def now(self) -> "QueryBuilder":
        """Set time projection to current moment."""
        return self.as_of(datetime.now().isoformat())

    # ========== Pattern matching ==========

    def matching(self, pattern: str) -> "QueryBuilder":
        """Set a match pattern for the query."""
        self._state.match_pattern = pattern
        return self

    def where(
        self, field: str, op: str, value: Any
    ) -> "QueryBuilder":
        """
        Add a filter predicate.

        Args:
            field: The field to filter on (e.g., "claim_type", "source.type")
            op: The operator ("=", "!=", "IN", ">=", "CONTAINS", etc.)
            value: The value to compare against
        """
        self._state.predicates.append(Predicate(field=field, op=op, value=value))
        return self

    def where_eq(self, field: str, value: Any) -> "QueryBuilder":
        """Shorthand for where(field, "=", value)."""
        return self.where(field, "=", value)

    def where_in(self, field: str, values: List[Any]) -> "QueryBuilder":
        """Shorthand for where(field, "IN", values)."""
        return self.where(field, "IN", values)

    # ========== Grounding/Trace ==========

    def with_trace(self, max_depth: int = 1) -> "QueryBuilder":
        """
        Enable provenance tracing.

        Grounding is a traversal mode, not a join.
        If a claim cannot be grounded, EOQL will say so.
        """
        self._state.trace = True
        self._state.max_depth = max_depth
        return self

    def grounded_by(
        self, field: str, op: str, value: Any
    ) -> "QueryBuilder":
        """
        Filter by grounding criteria.

        Only return claims grounded by sources matching this predicate.
        """
        self._state.grounded_by.append(Predicate(field=field, op=op, value=value))
        return self

    # ========== Absence (requires expectation) ==========

    def expecting(
        self,
        expectation_id: str,
        version: Optional[str] = None,
        scope: Optional[Dict[str, Any]] = None,
        deadline_hours: Optional[int] = None,
    ) -> "QueryBuilder":
        """
        Set expectation for absence computation.

        Absences are computed, never retrieved.
        They require an expectation rule to define what should have happened.
        """
        self._state.expectation_id = expectation_id
        self._state.expectation_version = version
        self._state.absence_scope = scope
        self._state.absence_deadline_hours = deadline_hours
        return self

    # ========== Return specification ==========

    def with_context(self, include: bool = True) -> "QueryBuilder":
        """Include or exclude context in results."""
        self._state.include_context = include
        return self

    def with_frame_info(self, include: bool = True) -> "QueryBuilder":
        """Include or exclude frame info in results."""
        self._state.include_frame = include
        return self

    def with_visibility_notes(self, include: bool = True) -> "QueryBuilder":
        """Include or exclude visibility notes in results."""
        self._state.include_visibility_notes = include
        return self

    def with_conflicts(self, include: bool = True) -> "QueryBuilder":
        """Include or exclude conflict information in results."""
        self._state.include_conflicts = include
        return self

    # ========== Conflict policy ==========

    def expose_all_conflicts(self) -> "QueryBuilder":
        """Return all conflicting claims (default)."""
        self._state.conflict_policy = ConflictPolicy.EXPOSE_ALL
        return self

    def cluster_conflicts(self) -> "QueryBuilder":
        """Group conflicts into clusters."""
        self._state.conflict_policy = ConflictPolicy.CLUSTER
        return self

    def rank_conflicts(self) -> "QueryBuilder":
        """Return ranked list, keep alternates."""
        self._state.conflict_policy = ConflictPolicy.RANK
        return self

    def pick_one(
        self, rule_id: str, **params: Any
    ) -> "QueryBuilder":
        """
        Return single choice with explicit selection rule.

        This is the only way to get a single answer from conflicting claims.
        The rule must be explicit - no silent collapse allowed.
        """
        self._state.conflict_policy = ConflictPolicy.PICK_ONE
        self._state.selection_rule_id = rule_id
        self._state.selection_rule_params = params
        return self

    # ========== Build ==========

    def _check_completeness(self) -> List[str]:
        """Check what required fields are missing."""
        missing = []

        if self._state.target is None:
            missing.append("target (use .claims(), .entities(), etc.)")
        if self._state.mode is None:
            missing.append("mode (use .given() or .meant())")
        if self._state.visibility is None:
            missing.append("visibility (use .visible() or .exists())")
        if self._state.frame_id is None:
            missing.append("frame (use .under_frame() or .default_frame())")
        if self._state.time_kind is None:
            missing.append("time (use .as_of() or .between())")

        # Absence requires expectation
        if self._state.target == Target.ABSENCES and self._state.expectation_id is None:
            missing.append("expectation (use .expecting() for absence queries)")

        # PICK_ONE requires selection rule
        if (
            self._state.conflict_policy == ConflictPolicy.PICK_ONE
            and self._state.selection_rule_id is None
        ):
            missing.append("selection_rule (required for .pick_one())")

        return missing

    def build(self, validate: bool = True) -> EOQLQuery:
        """
        Build the EOQL query.

        Args:
            validate: Whether to validate the query (default True)

        Returns:
            A fully constructed EOQLQuery

        Raises:
            IncompleteQueryError: If required fields are missing
            EOQLValidationError: If the query fails validation
        """
        missing = self._check_completeness()
        if missing:
            raise IncompleteQueryError(missing)

        # Build time window
        if self._state.time_kind == TimeKind.AS_OF:
            time = TimeWindow(
                kind=TimeKind.AS_OF,
                as_of=self._state.time_as_of,
            )
        else:
            time = TimeWindow(
                kind=TimeKind.BETWEEN,
                start=self._state.time_start,
                end=self._state.time_end,
            )

        # Build pattern
        pattern = Pattern(
            match=self._state.match_pattern,
            where=tuple(self._state.predicates),
        )

        # Build grounding spec
        grounding = GroundingSpec(
            trace=self._state.trace,
            max_depth=self._state.max_depth,
            grounded_by=tuple(self._state.grounded_by),
        )

        # Build absence spec if needed
        absence = None
        if self._state.expectation_id is not None:
            absence = AbsenceSpec(
                expectation=ExpectationRef(
                    expectation_id=self._state.expectation_id,
                    version=self._state.expectation_version,
                ),
                scope=self._state.absence_scope,
                deadline_hours=self._state.absence_deadline_hours,
            )

        # Build selection rule if needed
        selection_rule = None
        if self._state.selection_rule_id is not None:
            selection_rule = SelectionRule(
                rule_id=self._state.selection_rule_id,
                params=self._state.selection_rule_params or {},
            )

        # Build return spec
        returns = ReturnSpec(
            include_context=self._state.include_context,
            include_frame=self._state.include_frame,
            include_visibility_notes=self._state.include_visibility_notes,
            include_conflicts=self._state.include_conflicts,
            conflict_policy=self._state.conflict_policy,
            selection_rule=selection_rule,
        )

        # Construct the query
        query = EOQLQuery(
            target=self._state.target,
            mode=self._state.mode,
            visibility=self._state.visibility,
            frame=FrameRef(
                frame_id=self._state.frame_id,
                version=self._state.frame_version,
            ),
            time=time,
            pattern=pattern,
            grounding=grounding,
            absence=absence,
            returns=returns,
        )

        # Validate if requested
        if validate:
            validate_query(query)

        return query

    def copy(self) -> "QueryBuilder":
        """Create a copy of this builder with the same state."""
        new_builder = QueryBuilder()
        new_builder._state = _BuilderState(
            target=self._state.target,
            mode=self._state.mode,
            visibility=self._state.visibility,
            frame_id=self._state.frame_id,
            frame_version=self._state.frame_version,
            time_kind=self._state.time_kind,
            time_as_of=self._state.time_as_of,
            time_start=self._state.time_start,
            time_end=self._state.time_end,
            match_pattern=self._state.match_pattern,
            predicates=list(self._state.predicates),
            trace=self._state.trace,
            max_depth=self._state.max_depth,
            grounded_by=list(self._state.grounded_by),
            expectation_id=self._state.expectation_id,
            expectation_version=self._state.expectation_version,
            absence_scope=self._state.absence_scope,
            absence_deadline_hours=self._state.absence_deadline_hours,
            include_context=self._state.include_context,
            include_frame=self._state.include_frame,
            include_visibility_notes=self._state.include_visibility_notes,
            include_conflicts=self._state.include_conflicts,
            conflict_policy=self._state.conflict_policy,
            selection_rule_id=self._state.selection_rule_id,
            selection_rule_params=self._state.selection_rule_params,
        )
        return new_builder


# Convenience function for starting a query
def query() -> QueryBuilder:
    """Start building a new EOQL query."""
    return QueryBuilder()
