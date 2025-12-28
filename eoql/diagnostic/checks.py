"""
EO Compliance Check Implementations.

This module implements all checks across Levels 0-7 of the EO Compliance
Diagnostic. Each check evaluates a specific aspect of EO compliance.

Level 0: Immediate Disqualifiers (Fail Fast)
Level 1: Ontological Structure Checks (Data Model)
Level 2: Epistemic Integrity Checks (Meaning Preservation)
Level 3: Temporal Honesty Checks (ALT)
Level 4: Accountability Checks (REC)
Level 5: Absence and Obligation Checks (NUL)
Level 6: Query Layer Honesty (EOQL Compliance)
Level 7: Regression Resistance (The Final Test)
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, Set
import re

from .types import (
    CheckLevel,
    CheckResult,
    CheckStatus,
    Disqualifier,
    Evidence,
)


class TargetRepository(Protocol):
    """Protocol for the repository being analyzed."""

    def get_python_files(self) -> List[str]:
        """Return all Python files in the repository."""
        ...

    def get_sql_files(self) -> List[str]:
        """Return all SQL files in the repository."""
        ...

    def read_file(self, path: str) -> str:
        """Read the contents of a file."""
        ...

    def has_file(self, path: str) -> bool:
        """Check if a file exists."""
        ...

    def search_pattern(self, pattern: str, file_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Search for a pattern in files, returning matches with location info."""
        ...

    def get_schema_definitions(self) -> List[Dict[str, Any]]:
        """Get database schema definitions if present."""
        ...

    def get_query_patterns(self) -> List[Dict[str, Any]]:
        """Get query patterns used in the codebase."""
        ...


class Check(ABC):
    """Base class for all compliance checks."""

    @property
    @abstractmethod
    def check_id(self) -> str:
        """Unique identifier for this check."""
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name for this check."""
        ...

    @property
    @abstractmethod
    def level(self) -> CheckLevel:
        """The diagnostic level of this check."""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """What this check evaluates."""
        ...

    @abstractmethod
    def run(self, repo: TargetRepository) -> CheckResult:
        """Execute the check against the target repository."""
        ...

    def _pass(self, evidence: List[Evidence] = None, details: Dict[str, Any] = None) -> CheckResult:
        """Helper to create a passing result."""
        return CheckResult(
            check_id=self.check_id,
            name=self.name,
            level=self.level,
            status=CheckStatus.PASS,
            description=self.description,
            evidence=evidence or [],
            details=details or {},
        )

    def _fail(
        self,
        evidence: List[Evidence] = None,
        recommendations: List[str] = None,
        details: Dict[str, Any] = None,
    ) -> CheckResult:
        """Helper to create a failing result."""
        return CheckResult(
            check_id=self.check_id,
            name=self.name,
            level=self.level,
            status=CheckStatus.FAIL,
            description=self.description,
            evidence=evidence or [],
            recommendations=recommendations or [],
            details=details or {},
        )

    def _warn(
        self,
        evidence: List[Evidence] = None,
        recommendations: List[str] = None,
        details: Dict[str, Any] = None,
    ) -> CheckResult:
        """Helper to create a warning result."""
        return CheckResult(
            check_id=self.check_id,
            name=self.name,
            level=self.level,
            status=CheckStatus.WARN,
            description=self.description,
            evidence=evidence or [],
            recommendations=recommendations or [],
            details=details or {},
        )

    def _skip(self, reason: str) -> CheckResult:
        """Helper to create a skipped result."""
        return CheckResult(
            check_id=self.check_id,
            name=self.name,
            level=self.level,
            status=CheckStatus.SKIP,
            description=self.description,
            details={"skip_reason": reason},
        )

    def _not_applicable(self, reason: str) -> CheckResult:
        """Helper to create a not-applicable result."""
        return CheckResult(
            check_id=self.check_id,
            name=self.name,
            level=self.level,
            status=CheckStatus.NOT_APPLICABLE,
            description=self.description,
            details={"na_reason": reason},
        )


# =============================================================================
# LEVEL 0 — IMMEDIATE DISQUALIFIERS (FAIL FAST)
# =============================================================================


class DisqualifierCheck(Check):
    """Base class for Level 0 disqualifier checks."""

    @property
    def level(self) -> CheckLevel:
        return CheckLevel.LEVEL_0_DISQUALIFIER


class DQ01_ImplicitCurrentState(DisqualifierCheck):
    """
    DQ0.1 — Implicit "Current State"

    The system exposes a notion of:
      - current_state
      - latest
      - now() without replay semantics

    Queries default to "latest record wins".

    Diagnosis: Time has been collapsed into state. EO's ALT operator is violated.
    """

    @property
    def check_id(self) -> str:
        return "DQ0.1"

    @property
    def name(self) -> str:
        return "Implicit Current State"

    @property
    def description(self) -> str:
        return "Check that the system does not collapse time into implicit 'current state'"

    # Patterns that indicate implicit current state
    DANGEROUS_PATTERNS = [
        # Direct current state references
        (r'\bcurrent_state\b', "Direct 'current_state' reference"),
        (r'\bget_current\b', "get_current() function - implies single truth"),
        (r'\bgetLatest\b', "getLatest() function - implies single truth"),
        (r'\bget_latest\b', "get_latest() function - implies single truth"),
        # Dangerous SQL patterns
        (r'ORDER\s+BY\s+\w+\s+DESC\s+LIMIT\s+1', "ORDER BY ... DESC LIMIT 1 - selects 'latest' arbitrarily"),
        (r'ORDER\s+BY\s+\w+\s+ASC\s+LIMIT\s+1', "ORDER BY ... ASC LIMIT 1 - selects 'earliest' arbitrarily"),
        # NOW() without proper context
        (r'\bNOW\(\)\s*(?!.*replay)', "NOW() without replay semantics"),
        (r'\bCURRENT_TIMESTAMP\s*(?!.*replay)', "CURRENT_TIMESTAMP without replay semantics"),
        # MAX timestamps treated as canonical
        (r'MAX\s*\(\s*\w*(?:time|timestamp|date|created|updated)\w*\s*\)', "MAX(timestamp) - collapses time"),
        # "latest record wins" patterns
        (r'\blatest_record\b', "'latest_record' implies single truth"),
        (r'\bmost_recent\b', "'most_recent' implies single truth without time context"),
    ]

    # Safe patterns - these are acceptable
    SAFE_PATTERNS = [
        r'as_of',           # Proper time projection
        r'between',         # Proper time range
        r'time_projection', # Explicit time handling
        r'replay',          # Replay semantics
        r'at_time',         # Explicit time parameter
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        has_safe_patterns = False

        # Check for dangerous patterns
        for pattern, explanation in self.DANGEROUS_PATTERNS:
            matches = repo.search_pattern(pattern, file_types=["py", "sql"])
            for match in matches:
                # Check if safe patterns exist nearby (same file)
                file_content = repo.read_file(match["file"])
                for safe in self.SAFE_PATTERNS:
                    if re.search(safe, file_content, re.IGNORECASE):
                        has_safe_patterns = True
                        break

                evidence.append(Evidence(
                    location=match["file"],
                    line_number=match.get("line"),
                    description=explanation,
                    code_snippet=match.get("snippet"),
                    severity="error",
                ))

        if evidence:
            return self._fail(
                evidence=evidence,
                recommendations=[
                    "Replace implicit 'latest' with explicit AS_OF or BETWEEN time projections",
                    "Require time parameters in all query functions",
                    "Use EOQL's TimeWindow instead of ORDER BY LIMIT 1",
                    "If NOW() is needed, ensure replay semantics are preserved",
                ],
                details={
                    "has_safe_patterns": has_safe_patterns,
                    "disqualifier": Disqualifier.DQ0_1_IMPLICIT_CURRENT_STATE,
                },
            )

        return self._pass(details={"verified_patterns": len(self.DANGEROUS_PATTERNS)})


class DQ02_DeletionWithoutTrace(DisqualifierCheck):
    """
    DQ0.2 — Deletion Without Trace

    Records can be deleted without:
      - immutable tombstones
      - explicit retraction events
      - traceable justification

    Diagnosis: SEG is being faked as INS reversal. EO forbids erasure without memory.
    """

    @property
    def check_id(self) -> str:
        return "DQ0.2"

    @property
    def name(self) -> str:
        return "Deletion Without Trace"

    @property
    def description(self) -> str:
        return "Check that deletions are always traced (tombstones, retractions)"

    DANGEROUS_PATTERNS = [
        # Hard deletes
        (r'\bDELETE\s+FROM\b(?!.*tombstone)(?!.*retract)', "DELETE without tombstone/retraction"),
        (r'\.delete\(\)(?!.*soft)', "ORM delete without soft-delete"),
        (r'\bTRUNCATE\b', "TRUNCATE destroys history"),
        (r'\bDROP\s+TABLE\b', "DROP TABLE destroys history"),
        # Destructive updates
        (r'\bUPDATE\b.*\bSET\b(?!.*version)(?!.*previous)', "UPDATE without versioning"),
    ]

    SAFE_PATTERNS = [
        (r'soft_delete', "Uses soft delete"),
        (r'tombstone', "Uses tombstones"),
        (r'retract', "Uses retractions"),
        (r'is_deleted', "Tracks deletion state"),
        (r'deleted_at', "Tracks deletion timestamp"),
        (r'visibility_scope', "Uses visibility scoping (SEG)"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        safe_mechanisms_found = set()

        # First, check for safe patterns
        for pattern, explanation in self.SAFE_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                safe_mechanisms_found.add(explanation)

        # Check for dangerous patterns
        for pattern, explanation in self.DANGEROUS_PATTERNS:
            matches = repo.search_pattern(pattern, file_types=["py", "sql"])
            for match in matches:
                evidence.append(Evidence(
                    location=match["file"],
                    line_number=match.get("line"),
                    description=explanation,
                    code_snippet=match.get("snippet"),
                    severity="error",
                ))

        if evidence and not safe_mechanisms_found:
            return self._fail(
                evidence=evidence,
                recommendations=[
                    "Implement soft-delete with deleted_at timestamp",
                    "Use tombstone records instead of DELETE",
                    "Create retraction events for removed data",
                    "Implement visibility_scope (SEG) for hiding without deletion",
                ],
                details={
                    "disqualifier": Disqualifier.DQ0_2_DELETION_WITHOUT_TRACE,
                },
            )
        elif evidence and safe_mechanisms_found:
            # Has both dangerous and safe patterns - warning
            return self._warn(
                evidence=evidence,
                recommendations=[
                    "Review DELETE statements to ensure they work with safe mechanisms",
                    f"Found safe mechanisms: {', '.join(safe_mechanisms_found)}",
                ],
                details={"safe_mechanisms": list(safe_mechanisms_found)},
            )

        return self._pass(
            evidence=[Evidence(
                location="<codebase>",
                description=f"Safe mechanisms found: {', '.join(safe_mechanisms_found) or 'No deletion patterns found'}",
            )],
        )


class DQ03_AbsenceAsNull(DisqualifierCheck):
    """
    DQ0.3 — Absence Treated as NULL / Empty Result

    Missing rows are interpreted as "nothing happened".
    No explicit expectation or absence object exists.

    Diagnosis: NUL is unmodeled. Harm, delay, and silence are invisible.
    """

    @property
    def check_id(self) -> str:
        return "DQ0.3"

    @property
    def name(self) -> str:
        return "Absence Treated as NULL"

    @property
    def description(self) -> str:
        return "Check that absence is explicitly modeled, not just NULL/empty"

    DANGEROUS_PATTERNS = [
        # NULL as absence
        (r'IS\s+NULL\b(?!.*absence)(?!.*expectation)', "IS NULL without absence handling"),
        (r'COALESCE\s*\(.*,\s*(?:0|\'\'|NULL)\)', "COALESCE hiding absence with defaults"),
        (r'IFNULL\s*\(', "IFNULL hiding absence"),
        (r'NVL\s*\(', "NVL hiding absence"),
        (r'\.get\([^,]+,\s*(?:None|0|\'\'|\[\]|\{\})\)', "dict.get() with default hiding absence"),
        # Empty as nothing
        (r'if\s+(?:not\s+)?(?:len\(|result|rows)', "Length/emptiness check may hide meaningful absence"),
        (r'return\s+\[\]', "Returning empty list may hide meaningful absence"),
        (r'return\s+None(?!.*absence)', "Returning None may hide meaningful absence"),
    ]

    SAFE_PATTERNS = [
        (r'expectation', "Has expectations"),
        (r'absence', "Has absence concept"),
        (r'AbsenceSpec', "Uses EOQL AbsenceSpec"),
        (r'AbsenceRecord', "Uses AbsenceRecord"),
        (r'expected_claim', "Models expected claims"),
        (r'NUL\b', "References NUL operator"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        safe_mechanisms_found = set()

        # Check for safe patterns first
        for pattern, explanation in self.SAFE_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                safe_mechanisms_found.add(explanation)

        # If no absence modeling found at all, that's a disqualifier
        if not safe_mechanisms_found:
            return self._fail(
                evidence=[Evidence(
                    location="<codebase>",
                    description="No explicit absence modeling found (no expectations, no absence records)",
                    severity="error",
                )],
                recommendations=[
                    "Implement ExpectationRegistry for defining what 'should' happen",
                    "Create absence_records table for computed meaningful absences",
                    "Use EOQL's AbsenceSpec for absence queries",
                    "Model NUL operator: expectations are first-class artifacts",
                ],
                details={
                    "disqualifier": Disqualifier.DQ0_3_ABSENCE_AS_NULL,
                },
            )

        # Check for dangerous patterns even if safe mechanisms exist
        for pattern, explanation in self.DANGEROUS_PATTERNS:
            matches = repo.search_pattern(pattern, file_types=["py", "sql"])
            for match in matches[:5]:  # Limit to 5 examples per pattern
                evidence.append(Evidence(
                    location=match["file"],
                    line_number=match.get("line"),
                    description=explanation,
                    code_snippet=match.get("snippet"),
                    severity="warning",
                ))

        if evidence:
            return self._warn(
                evidence=evidence,
                recommendations=[
                    "Review NULL/empty handling to ensure absence is not silently ignored",
                    f"Safe mechanisms found: {', '.join(safe_mechanisms_found)}",
                ],
                details={"safe_mechanisms": list(safe_mechanisms_found)},
            )

        return self._pass(
            evidence=[Evidence(
                location="<codebase>",
                description=f"Absence modeling found: {', '.join(safe_mechanisms_found)}",
            )],
        )


# =============================================================================
# LEVEL 1 — ONTOLOGICAL STRUCTURE CHECKS (DATA MODEL)
# =============================================================================


class Level1Check(Check):
    """Base class for Level 1 ontological structure checks."""

    @property
    def level(self) -> CheckLevel:
        return CheckLevel.LEVEL_1_ONTOLOGICAL


class L11_ImmutableEventLayer(Level1Check):
    """
    L1.1 — Immutable Event Layer (INS)

    Check:
      - Is there an append-only event/assertion log?
      - Are mutations modeled as new events, not overwrites?

    Fail if:
      - Tables represent "facts" without event history
      - Updates overwrite previous values without trace
    """

    @property
    def check_id(self) -> str:
        return "L1.1"

    @property
    def name(self) -> str:
        return "Immutable Event Layer (INS)"

    @property
    def description(self) -> str:
        return "Verify append-only event/assertion log exists; mutations are new events"

    REQUIRED_PATTERNS = [
        (r'assertions?\b', "assertions table/model"),
        (r'events?\b.*(?:log|store|table)', "event log/store"),
        (r'append.only|immutable', "append-only/immutable annotation"),
        (r'asserted_at|event_time|recorded_at', "timestamp for assertions"),
    ]

    VIOLATION_PATTERNS = [
        (r'UPDATE.*SET(?!.*version)(?!.*valid_until)', "UPDATE without versioning"),
        (r'ON\s+CONFLICT.*DO\s+UPDATE', "UPSERT may violate immutability"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        found_immutable = False

        # Check for required patterns
        for pattern, explanation in self.REQUIRED_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                found_immutable = True
                evidence.append(Evidence(
                    location=matches[0]["file"],
                    line_number=matches[0].get("line"),
                    description=f"Found: {explanation}",
                    severity="info",
                ))

        if not found_immutable:
            return self._fail(
                recommendations=[
                    "Create an assertions table with immutable records",
                    "Model mutations as new events, not updates",
                    "Add asserted_at timestamp to all assertions",
                ],
            )

        # Check for violations
        violations = []
        for pattern, explanation in self.VIOLATION_PATTERNS:
            matches = repo.search_pattern(pattern, file_types=["py", "sql"])
            for match in matches[:3]:
                violations.append(Evidence(
                    location=match["file"],
                    line_number=match.get("line"),
                    description=explanation,
                    severity="warning",
                ))

        if violations:
            return self._warn(
                evidence=evidence + violations,
                recommendations=[
                    "Review UPDATE statements - should create new versions, not overwrite",
                    "UPSERT patterns should be replaced with INSERT + versioning",
                ],
            )

        return self._pass(evidence=evidence)


class L12_ExplicitIdentity(Level1Check):
    """
    L1.2 — Explicit Identity (DES)

    Check:
      - Are entities, claims, assertions, frames explicitly identified?
      - Are IDs stable and referenceable?

    Fail if:
      - Identity is inferred from content
      - Natural keys silently substitute for identity
    """

    @property
    def check_id(self) -> str:
        return "L1.2"

    @property
    def name(self) -> str:
        return "Explicit Identity (DES)"

    @property
    def description(self) -> str:
        return "Verify entities have explicit, stable IDs (not content-derived)"

    REQUIRED_PATTERNS = [
        (r'(?:entity|assertion|claim|frame)_id\b', "explicit ID fields"),
        (r'UUID|uuid|GUID', "UUID usage"),
        (r'PRIMARY\s+KEY', "explicit primary keys"),
    ]

    VIOLATION_PATTERNS = [
        (r'UNIQUE\s*\((?!.*_id).*\)', "UNIQUE on content fields (natural key)"),
        (r'hash\(.*content', "ID derived from content hash"),
        (r'PRIMARY\s+KEY\s*\(\s*(?!.*_id)', "PK on non-ID fields"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        found_explicit_ids = False

        for pattern, explanation in self.REQUIRED_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                found_explicit_ids = True
                evidence.append(Evidence(
                    location=matches[0]["file"],
                    description=f"Found: {explanation}",
                    severity="info",
                ))

        if not found_explicit_ids:
            return self._fail(
                recommendations=[
                    "Add explicit UUID-based IDs for all entities",
                    "Use entity_id, assertion_id, frame_id patterns",
                    "Avoid deriving identity from content",
                ],
            )

        # Check for violations
        violations = []
        for pattern, explanation in self.VIOLATION_PATTERNS:
            matches = repo.search_pattern(pattern, file_types=["py", "sql"])
            for match in matches[:3]:
                violations.append(Evidence(
                    location=match["file"],
                    line_number=match.get("line"),
                    description=explanation,
                    severity="warning",
                ))

        if violations:
            return self._warn(evidence=evidence + violations)

        return self._pass(evidence=evidence)


class L13_VisibilityNotExistence(Level1Check):
    """
    L1.3 — Visibility ≠ Existence (SEG)

    Check:
      - Is there a distinction between:
        - exists globally
        - visible to actor / scope

    Fail if:
      - Access control is implemented as deletion or filtering without annotation
      - Hidden data is indistinguishable from nonexistent data
    """

    @property
    def check_id(self) -> str:
        return "L1.3"

    @property
    def name(self) -> str:
        return "Visibility ≠ Existence (SEG)"

    @property
    def description(self) -> str:
        return "Verify visibility is tracked separately from existence"

    REQUIRED_PATTERNS = [
        (r'visibility_scope', "visibility_scope field"),
        (r'Visibility\b', "Visibility enum/type"),
        (r'VISIBLE|EXISTS\b', "VISIBLE/EXISTS distinction"),
        (r'is_visible|can_see', "visibility predicates"),
        (r'scope\b.*(?:filter|restrict)', "scope-based visibility"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        found_visibility = False

        for pattern, explanation in self.REQUIRED_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                found_visibility = True
                evidence.append(Evidence(
                    location=matches[0]["file"],
                    description=f"Found: {explanation}",
                    severity="info",
                ))

        if not found_visibility:
            return self._fail(
                recommendations=[
                    "Add visibility_scope field to assertions and entities",
                    "Implement VISIBLE vs EXISTS distinction in queries",
                    "Use SEG operator: scope without deletion",
                ],
            )

        return self._pass(evidence=evidence)


class L14_RelationshipsFirstClass(Level1Check):
    """
    L1.4 — Relationships Are First-Class (CON)

    Check:
      - Are relationships stored as explicit edges/links?
      - Can relationships have provenance and time?

    Fail if:
      - Semantics live only in denormalized fields
      - Relationships are implied but not modeled
    """

    @property
    def check_id(self) -> str:
        return "L1.4"

    @property
    def name(self) -> str:
        return "Relationships First-Class (CON)"

    @property
    def description(self) -> str:
        return "Verify relationships are explicit edges with provenance"

    REQUIRED_PATTERNS = [
        (r'(?:subject|source)_id.*(?:object|target)_id', "explicit edge structure"),
        (r'edges?\b.*table', "edges table"),
        (r'relationship\b', "relationship concept"),
        (r'grounding.*(?:ref|id)', "relationship provenance"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        found_edges = False

        for pattern, explanation in self.REQUIRED_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                found_edges = True
                evidence.append(Evidence(
                    location=matches[0]["file"],
                    description=f"Found: {explanation}",
                    severity="info",
                ))

        if not found_edges:
            return self._fail(
                recommendations=[
                    "Create explicit edges/links table for relationships",
                    "Include subject_id and object_id in assertions",
                    "Add provenance (grounding_ref) to relationships",
                ],
            )

        return self._pass(evidence=evidence)


# =============================================================================
# LEVEL 2 — EPISTEMIC INTEGRITY CHECKS (MEANING PRESERVATION)
# =============================================================================


class Level2Check(Check):
    """Base class for Level 2 epistemic integrity checks."""

    @property
    def level(self) -> CheckLevel:
        return CheckLevel.LEVEL_2_EPISTEMIC


class L21_FactVsInterpretation(Level2Check):
    """
    L2.1 — Fact vs Interpretation Is Preserved

    Check:
      - Can the system distinguish:
        - directly asserted events
        - inferred / derived / labeled claims

    Fail if:
      - Metrics, scores, and inferences are indistinguishable from facts
      - Columns imply factuality without epistemic status

    (EOQL: GIVEN vs MEANT)
    """

    @property
    def check_id(self) -> str:
        return "L2.1"

    @property
    def name(self) -> str:
        return "Fact vs Interpretation (GIVEN/MEANT)"

    @property
    def description(self) -> str:
        return "Verify the system distinguishes directly asserted vs inferred claims"

    REQUIRED_PATTERNS = [
        (r'\bMode\b.*(?:GIVEN|MEANT)', "Mode enum with GIVEN/MEANT"),
        (r'assertion_mode', "assertion_mode field"),
        (r'(?:is_)?derived|inferred', "derived/inferred flag"),
        (r'epistemic.*status', "epistemic status"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        found_distinction = False

        for pattern, explanation in self.REQUIRED_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                found_distinction = True
                evidence.append(Evidence(
                    location=matches[0]["file"],
                    description=f"Found: {explanation}",
                    severity="info",
                ))

        if not found_distinction:
            return self._fail(
                recommendations=[
                    "Add assertion_mode field with GIVEN/MEANT values",
                    "Track whether claims are direct observations or inferences",
                    "Preserve epistemic status through all transformations",
                ],
            )

        return self._pass(evidence=evidence)


class L22_MultipleTruthsCoexist(Level2Check):
    """
    L2.2 — Multiple Truths Can Coexist (SUP)

    Check:
      - Can conflicting claims exist without one being overwritten?
      - Are disagreements preserved, not resolved by default?

    Fail if:
      - Constraints enforce uniqueness where disagreement is valid
      - Aggregation collapses disagreement automatically
    """

    @property
    def check_id(self) -> str:
        return "L2.2"

    @property
    def name(self) -> str:
        return "Multiple Truths Coexist (SUP)"

    @property
    def description(self) -> str:
        return "Verify conflicting claims can coexist without forced resolution"

    REQUIRED_PATTERNS = [
        (r'conflict', "conflict handling"),
        (r'ConflictPolicy', "ConflictPolicy type"),
        (r'EXPOSE_ALL|CLUSTER|RANK', "conflict policies"),
        (r'frame_id.*frame_version', "frame-scoped claims"),
    ]

    VIOLATION_PATTERNS = [
        (r'UNIQUE\s*\(.*(?:claim|assertion)', "UNIQUE on claims forces resolution"),
        (r'ON\s+CONFLICT.*DO\s+UPDATE', "UPSERT forces single truth"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        found_coexistence = False

        for pattern, explanation in self.REQUIRED_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                found_coexistence = True
                evidence.append(Evidence(
                    location=matches[0]["file"],
                    description=f"Found: {explanation}",
                    severity="info",
                ))

        if not found_coexistence:
            return self._fail(
                recommendations=[
                    "Implement ConflictPolicy with EXPOSE_ALL, CLUSTER, RANK options",
                    "Allow multiple claims about the same subject",
                    "Use frame_id to scope interpretations",
                ],
            )

        # Check for violations
        violations = []
        for pattern, explanation in self.VIOLATION_PATTERNS:
            matches = repo.search_pattern(pattern, file_types=["py", "sql"])
            for match in matches[:3]:
                violations.append(Evidence(
                    location=match["file"],
                    line_number=match.get("line"),
                    description=explanation,
                    severity="warning",
                ))

        if violations:
            return self._warn(evidence=evidence + violations)

        return self._pass(evidence=evidence)


class L23_SynthesisExplicit(Level2Check):
    """
    L2.3 — Synthesis Is Explicit and Pre-Query (SYN)

    Check:
      - Are equivalence decisions:
        - explicit
        - versioned
        - attributable

    Fail if:
      - Deduplication happens silently at query time
      - Canonicalization is irreversible
    """

    @property
    def check_id(self) -> str:
        return "L2.3"

    @property
    def name(self) -> str:
        return "Synthesis Explicit (SYN)"

    @property
    def description(self) -> str:
        return "Verify deduplication/canonicalization is explicit and versioned"

    REQUIRED_PATTERNS = [
        (r'synthesis', "synthesis handling"),
        (r'SYN\b', "SYN operator"),
        (r'equivalen', "equivalence tracking"),
        (r'merge.*record', "merge records"),
        (r'canonical.*version', "canonicalization versioning"),
    ]

    VIOLATION_PATTERNS = [
        (r'DISTINCT\b(?!.*annotated)', "DISTINCT without annotation"),
        (r'GROUP\s+BY(?!.*preserve)', "GROUP BY may collapse disagreement"),
        (r'dedupe|dedup(?!.*record)', "silent deduplication"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        found_synthesis = False

        for pattern, explanation in self.REQUIRED_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                found_synthesis = True
                evidence.append(Evidence(
                    location=matches[0]["file"],
                    description=f"Found: {explanation}",
                    severity="info",
                ))

        if not found_synthesis:
            return self._fail(
                recommendations=[
                    "Create synthesis_records table for tracking equivalences",
                    "Make deduplication explicit and attributable",
                    "Ensure canonicalization is reversible",
                ],
            )

        # Check for violations
        violations = []
        for pattern, explanation in self.VIOLATION_PATTERNS:
            matches = repo.search_pattern(pattern, file_types=["py", "sql"])
            for match in matches[:3]:
                violations.append(Evidence(
                    location=match["file"],
                    line_number=match.get("line"),
                    description=explanation,
                    severity="warning",
                ))

        if violations:
            return self._warn(evidence=evidence + violations)

        return self._pass(evidence=evidence)


# =============================================================================
# LEVEL 3 — TEMPORAL HONESTY CHECKS (ALT)
# =============================================================================


class Level3Check(Check):
    """Base class for Level 3 temporal honesty checks."""

    @property
    def level(self) -> CheckLevel:
        return CheckLevel.LEVEL_3_TEMPORAL


class L31_ReplayableWorld(Level3Check):
    """
    L3.1 — Replayable World

    Check:
      - Can the system reconstruct the world as known at time T?
      - Is time treated as projection, not filtering?

    Fail if:
      - Time is just a column used in WHERE clauses
      - "As of" semantics are approximated
    """

    @property
    def check_id(self) -> str:
        return "L3.1"

    @property
    def name(self) -> str:
        return "Replayable World (ALT)"

    @property
    def description(self) -> str:
        return "Verify the system can reconstruct world-state at any point in time"

    REQUIRED_PATTERNS = [
        (r'as_of|AS_OF', "AS_OF time projection"),
        (r'TimeWindow|time_window', "TimeWindow type"),
        (r'valid_from.*valid_until', "bi-temporal fields"),
        (r'time.*projection', "time projection concept"),
        (r'asserted_at', "assertion timestamp"),
    ]

    VIOLATION_PATTERNS = [
        (r'WHERE.*(?:time|date|timestamp)\s*[<>=]', "Time as filter, not projection"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        found_replay = False

        for pattern, explanation in self.REQUIRED_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                found_replay = True
                evidence.append(Evidence(
                    location=matches[0]["file"],
                    description=f"Found: {explanation}",
                    severity="info",
                ))

        if not found_replay:
            return self._fail(
                recommendations=[
                    "Implement AS_OF and BETWEEN time projections",
                    "Add valid_from/valid_until to temporal records",
                    "Treat time as projection, not filtering",
                ],
            )

        return self._pass(evidence=evidence)


class L32_DurabilityRecurrence(Level3Check):
    """
    L3.2 — Durability and Recurrence

    Check:
      - Can the system represent:
        - persistence
        - decay
        - cycles
        - missed cycles

    Fail if:
      - Time only represents point events
      - Recurrence is external or implicit
    """

    @property
    def check_id(self) -> str:
        return "L3.2"

    @property
    def name(self) -> str:
        return "Durability and Recurrence"

    @property
    def description(self) -> str:
        return "Verify support for persistence, decay, and cycles"

    REQUIRED_PATTERNS = [
        (r'valid_until|expires', "expiration/decay"),
        (r'recurrence|frequency', "recurrence handling"),
        (r'DAILY|WEEKLY|MONTHLY|CONTINUOUS', "frequency patterns"),
        (r'deadline', "deadline tracking"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        found_durability = False

        for pattern, explanation in self.REQUIRED_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                found_durability = True
                evidence.append(Evidence(
                    location=matches[0]["file"],
                    description=f"Found: {explanation}",
                    severity="info",
                ))

        if not found_durability:
            return self._fail(
                recommendations=[
                    "Add valid_until for temporal bounds",
                    "Implement ExpectationFrequency for recurrence",
                    "Track missed cycles explicitly",
                ],
            )

        return self._pass(evidence=evidence)


# =============================================================================
# LEVEL 4 — ACCOUNTABILITY CHECKS (REC)
# =============================================================================


class Level4Check(Check):
    """Base class for Level 4 accountability checks."""

    @property
    def level(self) -> CheckLevel:
        return CheckLevel.LEVEL_4_ACCOUNTABILITY


class L41_GroundingTraversable(Level4Check):
    """
    L4.1 — Grounding Is Traversable

    Check:
      - Can you ask "why does this exist?"
      - Does the system return a justification chain?

    Fail if:
      - Provenance is metadata-only
      - "Explain" is not queryable
    """

    @property
    def check_id(self) -> str:
        return "L4.1"

    @property
    def name(self) -> str:
        return "Grounding Traversable (REC)"

    @property
    def description(self) -> str:
        return "Verify provenance chains are queryable and traversable"

    REQUIRED_PATTERNS = [
        (r'grounding', "grounding concept"),
        (r'GroundingSpec', "GroundingSpec type"),
        (r'grounded_by', "grounded_by relationship"),
        (r'source_id', "source tracking"),
        (r'trace|TRACE', "trace capability"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        found_grounding = False

        for pattern, explanation in self.REQUIRED_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                found_grounding = True
                evidence.append(Evidence(
                    location=matches[0]["file"],
                    description=f"Found: {explanation}",
                    severity="info",
                ))

        if not found_grounding:
            return self._fail(
                recommendations=[
                    "Create grounding_chains table",
                    "Implement GroundingSpec for provenance queries",
                    "Link all claims to sources",
                ],
            )

        return self._pass(evidence=evidence)


class L42_NothingFloats(Level4Check):
    """
    L4.2 — Nothing Floats

    Check:
      - Every claim can point back to:
        - assertions
        - sources
        - methods
        - agents

    Fail if:
      - Some claims are accepted "because the system says so"
    """

    @property
    def check_id(self) -> str:
        return "L4.2"

    @property
    def name(self) -> str:
        return "Nothing Floats"

    @property
    def description(self) -> str:
        return "Verify all claims have traceable provenance"

    REQUIRED_PATTERNS = [
        (r'source_id', "source reference"),
        (r'method\b', "method tracking"),
        (r'created_by|agent', "agent tracking"),
        (r'grounding_ref', "grounding reference"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        found_provenance = 0

        for pattern, explanation in self.REQUIRED_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                found_provenance += 1
                evidence.append(Evidence(
                    location=matches[0]["file"],
                    description=f"Found: {explanation}",
                    severity="info",
                ))

        # Need at least 3 of 4 provenance aspects
        if found_provenance < 3:
            return self._fail(
                recommendations=[
                    "Ensure all claims have source_id",
                    "Track method/agent for all assertions",
                    "Add grounding_ref for justification",
                ],
            )

        return self._pass(evidence=evidence)


# =============================================================================
# LEVEL 5 — ABSENCE AND OBLIGATION (NUL)
# =============================================================================


class Level5Check(Check):
    """Base class for Level 5 absence/obligation checks."""

    @property
    def level(self) -> CheckLevel:
        return CheckLevel.LEVEL_5_ABSENCE


class L51_ExpectationsModeled(Level5Check):
    """
    L5.1 — Expectations Are Modeled

    Check:
      - Are expectations first-class artifacts?
      - Can they be versioned, scoped, grounded?

    Fail if:
      - Absence is inferred from missing data
      - Expectations live only in code or docs
    """

    @property
    def check_id(self) -> str:
        return "L5.1"

    @property
    def name(self) -> str:
        return "Expectations Modeled (NUL)"

    @property
    def description(self) -> str:
        return "Verify expectations are first-class, versioned artifacts"

    REQUIRED_PATTERNS = [
        (r'expectations?\b.*(?:table|store|registry)', "expectations storage"),
        (r'ExpectationDefinition', "ExpectationDefinition type"),
        (r'expectation_id.*version', "versioned expectations"),
        (r'ExpectationRule', "expectation rules"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        found_expectations = False

        for pattern, explanation in self.REQUIRED_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                found_expectations = True
                evidence.append(Evidence(
                    location=matches[0]["file"],
                    description=f"Found: {explanation}",
                    severity="info",
                ))

        if not found_expectations:
            return self._fail(
                recommendations=[
                    "Create expectations table for storing expectation definitions",
                    "Implement ExpectationRegistry for managing expectations",
                    "Version expectations like frames",
                ],
            )

        return self._pass(evidence=evidence)


class L52_AbsenceQueryable(Level5Check):
    """
    L5.2 — Absence Is Queryable

    Check:
      - Can you ask:
        "What failed to occur that should have?"

    Fail if:
      - There is no way to query non-events
      - SLAs, obligations, or duties are external
    """

    @property
    def check_id(self) -> str:
        return "L5.2"

    @property
    def name(self) -> str:
        return "Absence Queryable"

    @property
    def description(self) -> str:
        return "Verify non-events can be queried (what should have happened but didn't)"

    REQUIRED_PATTERNS = [
        (r'absence_records?', "absence records"),
        (r'AbsenceSpec', "AbsenceSpec type"),
        (r'ABSENCES?\b', "ABSENCES target"),
        (r'compute.*absence', "absence computation"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        found_absence = False

        for pattern, explanation in self.REQUIRED_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                found_absence = True
                evidence.append(Evidence(
                    location=matches[0]["file"],
                    description=f"Found: {explanation}",
                    severity="info",
                ))

        if not found_absence:
            return self._fail(
                recommendations=[
                    "Create absence_records table for computed absences",
                    "Implement ABSENCES as a query target",
                    "Add compute_absence_window for detection",
                ],
            )

        return self._pass(evidence=evidence)


# =============================================================================
# LEVEL 6 — QUERY LAYER HONESTY (EOQL COMPLIANCE)
# =============================================================================


class Level6Check(Check):
    """Base class for Level 6 query layer checks."""

    @property
    def level(self) -> CheckLevel:
        return CheckLevel.LEVEL_6_QUERY


class L61_NoImplicitDefaults(Level6Check):
    """
    L6.1 — No Implicit Defaults in Queries

    Check:
      - Does the query layer require:
        - time
        - frame
        - epistemic mode
        - visibility mode

    Fail if:
      - Queries silently assume defaults
      - "Latest" or "default frame" is invisible
    """

    @property
    def check_id(self) -> str:
        return "L6.1"

    @property
    def name(self) -> str:
        return "No Implicit Defaults"

    @property
    def description(self) -> str:
        return "Verify queries require explicit time, frame, mode, visibility"

    REQUIRED_PATTERNS = [
        (r'IncompleteQueryError', "incomplete query handling"),
        (r'validate_query', "query validation"),
        (r'I0.*[Tt]otality', "I0 Totality invariant"),
        (r'(?:time|frame|mode|visibility).*(?:required|mandatory)', "required fields"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        found_validation = False

        for pattern, explanation in self.REQUIRED_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                found_validation = True
                evidence.append(Evidence(
                    location=matches[0]["file"],
                    description=f"Found: {explanation}",
                    severity="info",
                ))

        if not found_validation:
            return self._fail(
                recommendations=[
                    "Implement query validation that rejects incomplete queries",
                    "Require time, frame, mode, visibility explicitly",
                    "Raise IncompleteQueryError for missing required fields",
                ],
            )

        return self._pass(evidence=evidence)


class L62_QueriesCanRefuse(Level6Check):
    """
    L6.2 — Queries Can Refuse to Answer

    Check:
      - Can the system say:
        "This question is ill-posed"?

    Fail if:
      - Queries always return something, even if misleading
    """

    @property
    def check_id(self) -> str:
        return "L6.2"

    @property
    def name(self) -> str:
        return "Queries Can Refuse"

    @property
    def description(self) -> str:
        return "Verify queries can refuse ill-posed questions"

    REQUIRED_PATTERNS = [
        (r'ValidationError|QueryError', "error types for refusal"),
        (r'honest.*refusal', "honest refusal concept"),
        (r'ill.posed|cannot.*answer', "refusal language"),
        (r'raise.*(?:Error|Exception)', "exception raising"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        found_refusal = False

        for pattern, explanation in self.REQUIRED_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                found_refusal = True
                evidence.append(Evidence(
                    location=matches[0]["file"],
                    description=f"Found: {explanation}",
                    severity="info",
                ))

        if not found_refusal:
            return self._fail(
                recommendations=[
                    "Implement EOQLValidationError for ill-posed queries",
                    "Prioritize honest refusal over approximation",
                    "Return clear error messages for unanswerable queries",
                ],
            )

        return self._pass(evidence=evidence)


class L63_ConflictSurfaced(Level6Check):
    """
    L6.3 — Conflict Is Surfaced, Not Hidden

    Check:
      - Do query results indicate:
        - disagreement
        - competing frames
        - alternative interpretations

    Fail if:
      - Results are flattened without annotation
    """

    @property
    def check_id(self) -> str:
        return "L6.3"

    @property
    def name(self) -> str:
        return "Conflict Surfaced"

    @property
    def description(self) -> str:
        return "Verify query results surface disagreement and alternatives"

    REQUIRED_PATTERNS = [
        (r'has_conflicts', "conflict flag in results"),
        (r'conflict_ids', "conflict ID tracking"),
        (r'EpistemicMetadata', "epistemic metadata on results"),
        (r'with_conflicts', "conflict inclusion option"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        found_surfacing = False

        for pattern, explanation in self.REQUIRED_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                found_surfacing = True
                evidence.append(Evidence(
                    location=matches[0]["file"],
                    description=f"Found: {explanation}",
                    severity="info",
                ))

        if not found_surfacing:
            return self._fail(
                recommendations=[
                    "Add has_conflicts flag to result rows",
                    "Include conflict_ids in results",
                    "Attach EpistemicMetadata to all results",
                ],
            )

        return self._pass(evidence=evidence)


# =============================================================================
# LEVEL 7 — REGRESSION RESISTANCE (THE FINAL TEST)
# =============================================================================


class Level7Check(Check):
    """Base class for Level 7 regression resistance checks."""

    @property
    def level(self) -> CheckLevel:
        return CheckLevel.LEVEL_7_REGRESSION


class L71_CertaintyNotAddedByOptimization(Level7Check):
    """
    L7.1 — Certainty Cannot Be Added by Optimization

    Check:
      - Are there tests that fail if:
        - DISTINCT is introduced
        - MAX/MIN collapses results
        - LIMIT 1 implies truth

    Fail if:
      - Performance optimizations can change epistemic meaning

    (Compiler contract tests live here.)
    """

    @property
    def check_id(self) -> str:
        return "L7.1"

    @property
    def name(self) -> str:
        return "Certainty Not Added by Optimization"

    @property
    def description(self) -> str:
        return "Verify compiler contract tests prevent epistemic meaning changes"

    REQUIRED_PATTERNS = [
        (r'test.*compiler.*contract', "compiler contract tests"),
        (r'DISTINCT.*forbidden|cannot.*DISTINCT', "DISTINCT prohibition"),
        (r'MAX.*forbidden|cannot.*MAX', "MAX prohibition"),
        (r'LIMIT\s+1.*forbidden', "LIMIT 1 prohibition"),
        (r'epistemic.*shape', "epistemic shape checking"),
    ]

    def run(self, repo: TargetRepository) -> CheckResult:
        evidence = []
        found_tests = False

        for pattern, explanation in self.REQUIRED_PATTERNS:
            matches = repo.search_pattern(pattern)
            if matches:
                found_tests = True
                evidence.append(Evidence(
                    location=matches[0]["file"],
                    description=f"Found: {explanation}",
                    severity="info",
                ))

        if not found_tests:
            return self._fail(
                recommendations=[
                    "Add compiler contract tests",
                    "Test that DISTINCT cannot be introduced by optimization",
                    "Test that MAX/MIN cannot collapse epistemic meaning",
                    "Verify LIMIT 1 cannot imply single truth",
                ],
            )

        return self._pass(evidence=evidence)


# =============================================================================
# CHECK REGISTRY
# =============================================================================


def get_all_checks() -> List[Check]:
    """Return all compliance checks in order."""
    return [
        # Level 0 - Disqualifiers
        DQ01_ImplicitCurrentState(),
        DQ02_DeletionWithoutTrace(),
        DQ03_AbsenceAsNull(),

        # Level 1 - Ontological
        L11_ImmutableEventLayer(),
        L12_ExplicitIdentity(),
        L13_VisibilityNotExistence(),
        L14_RelationshipsFirstClass(),

        # Level 2 - Epistemic
        L21_FactVsInterpretation(),
        L22_MultipleTruthsCoexist(),
        L23_SynthesisExplicit(),

        # Level 3 - Temporal
        L31_ReplayableWorld(),
        L32_DurabilityRecurrence(),

        # Level 4 - Accountability
        L41_GroundingTraversable(),
        L42_NothingFloats(),

        # Level 5 - Absence
        L51_ExpectationsModeled(),
        L52_AbsenceQueryable(),

        # Level 6 - Query Layer
        L61_NoImplicitDefaults(),
        L62_QueriesCanRefuse(),
        L63_ConflictSurfaced(),

        # Level 7 - Regression
        L71_CertaintyNotAddedByOptimization(),
    ]


def get_disqualifier_checks() -> List[DisqualifierCheck]:
    """Return only Level 0 disqualifier checks."""
    return [
        DQ01_ImplicitCurrentState(),
        DQ02_DeletionWithoutTrace(),
        DQ03_AbsenceAsNull(),
    ]
