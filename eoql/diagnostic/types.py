"""
Core types for the EOIR Compliance Diagnostic.

This module defines the data structures used throughout the diagnostic
system for representing check results, levels, and the final report.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class CheckLevel(Enum):
    """
    The diagnostic level at which a check operates.

    Level 0 checks are immediate disqualifiers - if any fail, the repo
    is definitively not EOIR-compliant regardless of other factors.

    Higher levels represent increasingly subtle aspects of EOIR compliance.
    """
    LEVEL_0_DISQUALIFIER = 0
    LEVEL_1_ONTOLOGICAL = 1
    LEVEL_2_EPISTEMIC = 2
    LEVEL_3_TEMPORAL = 3
    LEVEL_4_ACCOUNTABILITY = 4
    LEVEL_5_ABSENCE = 5
    LEVEL_6_QUERY = 6
    LEVEL_7_REGRESSION = 7


class CheckStatus(Enum):
    """Status of a single compliance check."""
    PASS = "pass"
    FAIL = "fail"
    WARN = "warn"  # Check passed but with concerns
    SKIP = "skip"  # Check could not be performed
    NOT_APPLICABLE = "n/a"  # Check doesn't apply to this repo


class Disqualifier(Enum):
    """
    Immediate disqualifiers (Level 0) that indicate definitive non-compliance.

    If any of these are triggered, the repo fails EOIR compliance regardless
    of how well it performs on other checks.
    """
    DQ0_1_IMPLICIT_CURRENT_STATE = "DQ0.1"
    DQ0_2_DELETION_WITHOUT_TRACE = "DQ0.2"
    DQ0_3_ABSENCE_AS_NULL = "DQ0.3"


@dataclass(frozen=True)
class Evidence:
    """
    Evidence supporting a check result.

    Each piece of evidence points to a specific location in the codebase
    and describes what was found there.
    """
    location: str  # File path, table name, function name, etc.
    line_number: Optional[int] = None
    description: str = ""
    code_snippet: Optional[str] = None
    severity: str = "info"  # info, warning, error


@dataclass
class CheckResult:
    """
    Result of a single compliance check.

    Includes the check identifier, its status, and evidence supporting
    the determination.
    """
    check_id: str
    name: str
    level: CheckLevel
    status: CheckStatus
    description: str
    evidence: List[Evidence] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        """Check passed or is not applicable."""
        return self.status in (CheckStatus.PASS, CheckStatus.NOT_APPLICABLE)

    @property
    def failed(self) -> bool:
        """Check definitively failed."""
        return self.status == CheckStatus.FAIL

    @property
    def is_disqualifier(self) -> bool:
        """This is a Level 0 disqualifier check."""
        return self.level == CheckLevel.LEVEL_0_DISQUALIFIER


@dataclass
class LevelSummary:
    """Summary of all checks at a given level."""
    level: CheckLevel
    level_name: str
    total_checks: int
    passed: int
    failed: int
    warnings: int
    skipped: int

    @property
    def pass_rate(self) -> float:
        """Percentage of checks that passed (excluding skipped)."""
        applicable = self.total_checks - self.skipped
        if applicable == 0:
            return 1.0
        return self.passed / applicable


@dataclass
class DiagnosticReport:
    """
    Complete diagnostic report for a repository.

    Contains results for all checks across all levels, summaries,
    and the final compliance verdict.
    """
    repository_path: str
    timestamp: str
    eoql_version: Optional[str] = None

    # All check results organized by level
    results: Dict[CheckLevel, List[CheckResult]] = field(default_factory=dict)

    # Summaries
    level_summaries: List[LevelSummary] = field(default_factory=list)

    # Triggered disqualifiers
    disqualifiers: List[Disqualifier] = field(default_factory=list)

    # Final verdict
    is_compliant: bool = False
    compliance_score: Optional[int] = None  # Number of failures

    # Narrative
    verdict_text: str = ""

    @property
    def has_disqualifiers(self) -> bool:
        """Are there any triggered disqualifiers?"""
        return len(self.disqualifiers) > 0

    @property
    def total_failures(self) -> int:
        """Total number of failed checks across all levels."""
        return sum(
            sum(1 for r in results if r.failed)
            for results in self.results.values()
        )

    @property
    def total_checks(self) -> int:
        """Total number of checks run."""
        return sum(len(results) for results in self.results.values())

    def get_failures(self) -> List[CheckResult]:
        """Get all failed checks."""
        failures = []
        for results in self.results.values():
            failures.extend(r for r in results if r.failed)
        return failures

    def get_warnings(self) -> List[CheckResult]:
        """Get all checks with warnings."""
        warnings = []
        for results in self.results.values():
            warnings.extend(r for r in results if r.status == CheckStatus.WARN)
        return warnings


@dataclass
class ComplianceVerdict:
    """
    The final compliance verdict in structured form.

    Used to generate the one-paragraph verdict template.
    """
    # Required fields (no defaults) - all must come first
    is_compliant: bool
    preserves_time: bool
    preserves_disagreement: bool
    models_absence: bool
    certainty_explicit: bool

    # Optional fields with defaults
    time_notes: str = ""
    disagreement_notes: str = ""
    absence_notes: str = ""
    certainty_notes: str = ""

    # Disqualifiers triggered
    disqualifiers: List[str] = field(default_factory=list)

    # Additional notes
    additional_notes: List[str] = field(default_factory=list)
