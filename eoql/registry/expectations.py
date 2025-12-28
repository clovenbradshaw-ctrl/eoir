"""
Expectation Registry

Absence is computed, never retrieved.

Absence is the result of:
1. an expectation rule
2. a time window
3. a scope
4. a frame

Only after those are defined can EOQL ask:
> Did the expected thing fail to occur?

Absence queries cannot be sugar for NULL.
Absence cannot be inferred from empty result sets.
Absence always returns objects, not blanks.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Protocol, Sequence
from enum import Enum


class ExpectationFrequency(str, Enum):
    """How often the expected event should occur."""

    ONCE = "once"              # Should happen exactly once
    DAILY = "daily"            # Should happen every day
    WEEKLY = "weekly"          # Should happen every week
    MONTHLY = "monthly"        # Should happen every month
    RECURRING = "recurring"    # Custom recurrence pattern
    CONTINUOUS = "continuous"  # Should always be true


@dataclass(frozen=True)
class ExpectationRule:
    """
    The executable rule that defines an expectation.

    An expectation defines what "should" happen, allowing
    EOQL to compute meaningful absences.
    """

    # What entities this applies to
    entity_filter: Dict[str, Any] = field(default_factory=dict)

    # What type of claim we expect
    claim_type: Optional[str] = None

    # How often
    frequency: ExpectationFrequency = ExpectationFrequency.ONCE

    # Deadline (for ONCE) or period (for recurring)
    deadline_hours: Optional[int] = None

    # Additional scope constraints
    scope: Dict[str, Any] = field(default_factory=dict)

    # For recurring: cron-like pattern or interval
    recurrence_pattern: Optional[str] = None

    def get_deadline(self) -> Optional[timedelta]:
        """Get the deadline as a timedelta."""
        if self.deadline_hours is not None:
            return timedelta(hours=self.deadline_hours)
        return None


@dataclass(frozen=True)
class ExpectationDefinition:
    """
    A fully resolved expectation definition.

    Expectations define what should occur so that EOQL can
    compute meaningful absences when they don't.
    """

    expectation_id: str
    version: str
    name: str
    description: Optional[str] = None

    # The rule that defines this expectation
    rule: ExpectationRule = field(default_factory=ExpectationRule)

    # When this expectation is active
    active_from: Optional[datetime] = None
    active_until: Optional[datetime] = None

    # Frame context (expectations can be frame-relative)
    frame_id: Optional[str] = None
    frame_version: Optional[str] = None

    # Provenance
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None
    grounding_ref: Optional[str] = None

    def is_active_at(self, timestamp: datetime) -> bool:
        """Check if this expectation is active at a given time."""
        if self.active_from and timestamp < self.active_from:
            return False
        if self.active_until and timestamp > self.active_until:
            return False
        return True


@dataclass
class AbsenceObject:
    """
    A computed absence - the result of an expectation not being met.

    This is a first-class object, not a null or empty result.
    """

    absence_id: str
    expectation_id: str
    expectation_version: str

    # What was expected
    expected_entity_id: Optional[str] = None
    expected_claim_type: Optional[str] = None

    # Time window of the absence
    window_start: Optional[datetime] = None
    window_end: Optional[datetime] = None

    # Frame context
    frame_id: str = "F_default"
    frame_version: str = "latest"

    # When this absence was computed
    computed_at: Optional[datetime] = None

    # The scope that was checked
    scope: Dict[str, Any] = field(default_factory=dict)

    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)


class ExpectationNotFoundError(Exception):
    """Raised when a referenced expectation cannot be resolved."""

    def __init__(self, expectation_id: str, version: Optional[str] = None):
        self.expectation_id = expectation_id
        self.version = version
        msg = f"Expectation not found: {expectation_id}"
        if version:
            msg += f" (version: {version})"
        super().__init__(msg)


class ExpectationStore(Protocol):
    """Protocol for expectation storage backends."""

    def get(
        self, expectation_id: str, version: Optional[str] = None
    ) -> Optional[ExpectationDefinition]:
        """Retrieve an expectation by ID and optional version."""
        ...

    def list_versions(self, expectation_id: str) -> List[str]:
        """List all versions of an expectation."""
        ...

    def put(self, expectation: ExpectationDefinition) -> None:
        """Store an expectation definition."""
        ...

    def exists(self, expectation_id: str, version: Optional[str] = None) -> bool:
        """Check if an expectation exists."""
        ...

    def list_active(self, at_time: datetime) -> List[ExpectationDefinition]:
        """List all expectations active at a given time."""
        ...


class InMemoryExpectationStore:
    """In-memory expectation store for testing and simple use cases."""

    def __init__(self) -> None:
        self._expectations: Dict[str, Dict[str, ExpectationDefinition]] = {}

    def get(
        self, expectation_id: str, version: Optional[str] = None
    ) -> Optional[ExpectationDefinition]:
        """Retrieve an expectation by ID and optional version."""
        if expectation_id not in self._expectations:
            return None

        versions = self._expectations[expectation_id]

        if version is None or version == "latest":
            if "latest" in versions:
                return versions["latest"]
            sorted_versions = sorted(versions.keys(), reverse=True)
            return versions[sorted_versions[0]] if sorted_versions else None

        return versions.get(version)

    def list_versions(self, expectation_id: str) -> List[str]:
        """List all versions of an expectation."""
        if expectation_id not in self._expectations:
            return []
        return list(self._expectations[expectation_id].keys())

    def put(self, expectation: ExpectationDefinition) -> None:
        """Store an expectation definition."""
        if expectation.expectation_id not in self._expectations:
            self._expectations[expectation.expectation_id] = {}
        self._expectations[expectation.expectation_id][expectation.version] = expectation

    def exists(self, expectation_id: str, version: Optional[str] = None) -> bool:
        """Check if an expectation exists."""
        return self.get(expectation_id, version) is not None

    def list_active(self, at_time: datetime) -> List[ExpectationDefinition]:
        """List all expectations active at a given time."""
        active = []
        for versions in self._expectations.values():
            for exp in versions.values():
                if exp.is_active_at(at_time):
                    active.append(exp)
        return active


class ExpectationRegistry:
    """
    Central registry for expectation resolution and absence computation.

    This is where EOQL asks: "Did the expected thing fail to occur?"
    """

    def __init__(self, store: Optional[ExpectationStore] = None) -> None:
        self._store: ExpectationStore = store or InMemoryExpectationStore()
        self._cache: Dict[str, ExpectationDefinition] = {}

    def resolve(
        self, expectation_id: str, version: Optional[str] = None
    ) -> ExpectationDefinition:
        """
        Resolve an expectation ID to its full definition.

        Raises:
            ExpectationNotFoundError: If the expectation cannot be found.
        """
        cache_key = f"{expectation_id}:{version or 'latest'}"

        if cache_key in self._cache:
            return self._cache[cache_key]

        exp = self._store.get(expectation_id, version)
        if exp is None:
            raise ExpectationNotFoundError(expectation_id, version)

        self._cache[cache_key] = exp
        return exp

    def register(self, expectation: ExpectationDefinition) -> None:
        """Register a new expectation definition."""
        self._store.put(expectation)
        # Invalidate cache for this expectation
        keys_to_remove = [
            k for k in self._cache if k.startswith(f"{expectation.expectation_id}:")
        ]
        for k in keys_to_remove:
            del self._cache[k]

    def exists(self, expectation_id: str, version: Optional[str] = None) -> bool:
        """Check if an expectation exists."""
        return self._store.exists(expectation_id, version)

    def list_active(self, at_time: Optional[datetime] = None) -> List[ExpectationDefinition]:
        """List all expectations active at a given time."""
        return self._store.list_active(at_time or datetime.now())

    def compute_absence_window(
        self,
        expectation: ExpectationDefinition,
        reference_time: datetime,
    ) -> tuple[datetime, datetime]:
        """
        Compute the time window for absence detection.

        Returns (window_start, window_end) based on expectation rule.
        """
        rule = expectation.rule
        deadline = rule.get_deadline()

        if rule.frequency == ExpectationFrequency.ONCE:
            # For one-time expectations, window is from active_from to deadline
            start = expectation.active_from or reference_time
            if deadline:
                end = start + deadline
            else:
                end = reference_time
            return (start, end)

        elif rule.frequency == ExpectationFrequency.DAILY:
            # Window is the current day
            start = reference_time.replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=1)
            return (start, end)

        elif rule.frequency == ExpectationFrequency.WEEKLY:
            # Window is the current week (Monday to Sunday)
            days_since_monday = reference_time.weekday()
            start = (reference_time - timedelta(days=days_since_monday)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            end = start + timedelta(days=7)
            return (start, end)

        elif rule.frequency == ExpectationFrequency.MONTHLY:
            # Window is the current month
            start = reference_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            # Next month
            if reference_time.month == 12:
                end = start.replace(year=start.year + 1, month=1)
            else:
                end = start.replace(month=start.month + 1)
            return (start, end)

        else:
            # Default: use deadline or current time
            if deadline:
                return (reference_time - deadline, reference_time)
            return (reference_time - timedelta(hours=24), reference_time)

    def clear_cache(self) -> None:
        """Clear the resolution cache."""
        self._cache.clear()
