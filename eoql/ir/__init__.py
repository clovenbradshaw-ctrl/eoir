"""EOQL Intermediate Representation - the single source of truth for correctness."""

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
from .validation import EOQLValidationError, validate_query
from .serialize import (
    to_json,
    to_dict,
    from_json,
    from_dict,
    diff_queries,
    validate_json,
)

__all__ = [
    "AbsenceSpec",
    "ConflictPolicy",
    "EOQLQuery",
    "EOQLValidationError",
    "ExpectationRef",
    "FrameRef",
    "GroundingSpec",
    "Mode",
    "Pattern",
    "Predicate",
    "ReturnSpec",
    "SelectionRule",
    "Target",
    "TimeKind",
    "TimeWindow",
    "Visibility",
    "validate_query",
    # Serialization
    "to_json",
    "to_dict",
    "from_json",
    "from_dict",
    "diff_queries",
    "validate_json",
]
