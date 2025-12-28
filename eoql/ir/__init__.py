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
]
