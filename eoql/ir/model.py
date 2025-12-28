"""
EOQL Intermediate Representation Model

The EOQL-IR is the single source of truth for correctness.
If an AI coder gets this right, everything else follows.

Required Properties of EOQL-IR:
- Total: no implicit defaults
- Explicit: frame, time, visibility always present
- Serializable: inspectable, diffable, auditable
- Rejecting: invalid questions must fail early
- Backend-agnostic: no SQL/graph assumptions baked in
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Sequence


# ---------- Enums (closed-world) ----------


class Target(str, Enum):
    """What kind of objects we are querying for."""

    CLAIMS = "CLAIMS"
    EDGES = "EDGES"
    ENTITIES = "ENTITIES"
    ASSERTIONS = "ASSERTIONS"
    ABSENCES = "ABSENCES"


class Mode(str, Enum):
    """
    GIVEN vs MEANT: Did this happen, or was it inferred?

    GIVEN: Only return directly asserted facts
    MEANT: Include derived/inferred claims
    """

    GIVEN = "GIVEN"
    MEANT = "MEANT"


class Visibility(str, Enum):
    """
    VISIBLE vs EXISTS: Is it absent, or merely hidden?

    VISIBLE: Only return items visible in current scope
    EXISTS: Return all items including scoped-out ones
    """

    VISIBLE = "VISIBLE"
    EXISTS = "EXISTS"


class TimeKind(str, Enum):
    """
    AS_OF: Point-in-time projection
    BETWEEN: Range-based projection
    """

    AS_OF = "AS_OF"
    BETWEEN = "BETWEEN"


class ConflictPolicy(str, Enum):
    """
    How to handle conflicting claims.

    EXPOSE_ALL: Return all conflicting claims
    CLUSTER: Group conflicts into clusters
    RANK: Return ranked list, keep alternates
    PICK_ONE: Return single choice with explicit rule
    """

    EXPOSE_ALL = "EXPOSE_ALL"
    CLUSTER = "CLUSTER"
    RANK = "RANK"
    PICK_ONE = "PICK_ONE"


# ---------- Core structs ----------


@dataclass(frozen=True)
class FrameRef:
    """
    A frame is a named, versioned interpretation policy.

    Frames are epistemic actors - selecting a frame is making a claim.
    """

    frame_id: str
    version: Optional[str] = None  # '1.4', 'latest', commit hash, etc.


@dataclass(frozen=True)
class TimeWindow:
    """
    Time is projection, not filtering.

    AS OF t means:
    - replay all events up to t
    - reconstruct the world as it could be known then
    - apply frames and synthesis as of that time

    This is fundamentally different from WHERE timestamp <= t
    """

    kind: TimeKind
    # ISO 8601 strings (backend can parse). Keep simple for portability.
    as_of: Optional[str] = None
    start: Optional[str] = None
    end: Optional[str] = None

    @staticmethod
    def asof(ts: str) -> "TimeWindow":
        """Create an AS_OF time window for point-in-time queries."""
        return TimeWindow(kind=TimeKind.AS_OF, as_of=ts)

    @staticmethod
    def between(start: str, end: str) -> "TimeWindow":
        """Create a BETWEEN time window for range queries."""
        return TimeWindow(kind=TimeKind.BETWEEN, start=start, end=end)


@dataclass(frozen=True)
class Predicate:
    """A single filter condition."""

    field: str  # e.g. "term", "epistemic.method", "source.type"
    op: str  # "=", "!=", "IN", ">=", "CONTAINS", etc.
    value: Any


@dataclass(frozen=True)
class Pattern:
    """What we are matching. Keep this generic for multiple backends."""

    match: Optional[str] = None
    where: Sequence[Predicate] = field(default_factory=tuple)


@dataclass(frozen=True)
class GroundingSpec:
    """
    Grounding is a traversal mode, not a join.

    If a claim cannot be grounded to assertions, sources, methods,
    or prior claims, EOQL must be able to say:
    "This exists, but is weakly grounded."
    """

    trace: bool = False
    max_depth: int = 0
    grounded_by: Sequence[Predicate] = field(default_factory=tuple)


@dataclass(frozen=True)
class SelectionRule:
    """
    Required if conflict_policy == PICK_ONE.

    Examples:
    - rule_id="highest_certainty"
    - rule_id="latest_asserted"
    - rule_id="prefer_frame", params={"frame": "official"}
    """

    rule_id: str  # e.g. "highest_certainty", "latest_asserted"
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReturnSpec:
    """Specifies what to include in query results."""

    include_context: bool = True
    include_frame: bool = True
    include_visibility_notes: bool = True
    include_conflicts: bool = True
    conflict_policy: ConflictPolicy = ConflictPolicy.EXPOSE_ALL
    selection_rule: Optional[SelectionRule] = None


# ---------- Absence (NUL) ----------


@dataclass(frozen=True)
class ExpectationRef:
    """
    Absence is relative to an expectation rule.

    Absence does not live in storage. Absence is the result of:
    1. an expectation rule
    2. a time window
    3. a scope
    4. a frame
    """

    expectation_id: str  # points to an expectation artifact
    version: Optional[str] = None  # versioned expectations are best practice


@dataclass(frozen=True)
class AbsenceSpec:
    """
    Specification for querying meaningful absences.

    Absence queries cannot be sugar for NULL.
    Absence cannot be inferred from empty result sets.
    Absence always returns objects, not blanks.
    """

    expectation: ExpectationRef
    # Optional parameters that refine expectation evaluation
    scope: Optional[Dict[str, Any]] = None
    deadline_hours: Optional[int] = None


# ---------- The EOQL Query IR ----------


@dataclass(frozen=True)
class EOQLQuery:
    """
    The EOQL Query Intermediate Representation.

    This is the contract between EOQL and backends.
    The IR represents: "Here is exactly what kind of answer is allowed."

    Backends are forbidden from:
    - strengthening the answer
    - weakening ambiguity
    - discarding conflicts
    - erasing provenance

    If a backend cannot honor the IR, the correct behavior is
    to refuse execution, not to approximate.
    """

    target: Target
    mode: Mode
    visibility: Visibility
    frame: FrameRef
    time: TimeWindow
    pattern: Pattern = field(default_factory=Pattern)
    grounding: GroundingSpec = field(default_factory=GroundingSpec)
    absence: Optional[AbsenceSpec] = None
    returns: ReturnSpec = field(default_factory=ReturnSpec)
