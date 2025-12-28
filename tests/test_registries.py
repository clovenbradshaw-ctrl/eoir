"""
Tests for Frame and Expectation Registries

Frames are epistemic actors - selecting a frame is making a claim.
Expectations define what should occur so EOQL can compute meaningful absences.
"""

import pytest
from datetime import datetime, timedelta

from eoql.ir.model import FrameRef
from eoql.registry.frames import (
    FrameRegistry,
    FrameDefinition,
    FrameNotFoundError,
    InMemoryFrameStore,
)
from eoql.registry.expectations import (
    ExpectationRegistry,
    ExpectationDefinition,
    ExpectationRule,
    ExpectationFrequency,
    ExpectationNotFoundError,
    InMemoryExpectationStore,
)


class TestFrameRegistry:
    """Tests for Frame Registry."""

    def test_default_frame_exists(self):
        """Default frame is always present."""
        registry = FrameRegistry()
        frame = registry.get_default()

        assert frame.frame_id == "F_default"
        assert frame.name == "Default Frame"

    def test_resolve_frame(self):
        """Can resolve a frame reference."""
        registry = FrameRegistry()
        ref = FrameRef(frame_id="F_default", version="latest")
        frame = registry.resolve(ref)

        assert frame.frame_id == "F_default"

    def test_resolve_missing_frame_fails(self):
        """Resolving missing frame raises error."""
        registry = FrameRegistry()
        ref = FrameRef(frame_id="F_nonexistent")

        with pytest.raises(FrameNotFoundError):
            registry.resolve(ref)

    def test_register_frame(self):
        """Can register a new frame."""
        registry = FrameRegistry()
        frame = FrameDefinition(
            frame_id="F_custom",
            version="1.0",
            name="Custom Frame",
            config={
                "thresholds": {"certainty_minimum": 0.8},
                "definitions": {"active": {"min_activity": 5}},
            },
        )
        registry.register(frame)

        resolved = registry.resolve(FrameRef(frame_id="F_custom", version="1.0"))
        assert resolved.frame_id == "F_custom"
        assert resolved.get_threshold("certainty_minimum") == 0.8

    def test_frame_comparison(self):
        """Can compare two frames."""
        registry = FrameRegistry()

        frame1 = FrameDefinition(
            frame_id="F_v1",
            version="1.0",
            name="Version 1",
            config={"thresholds": {"min": 0.5}},
        )
        frame2 = FrameDefinition(
            frame_id="F_v2",
            version="1.0",
            name="Version 2",
            config={"thresholds": {"min": 0.8, "max": 1.0}},
        )
        registry.register(frame1)
        registry.register(frame2)

        diff = registry.compare(
            FrameRef(frame_id="F_v1"),
            FrameRef(frame_id="F_v2"),
        )

        assert diff["same"] is False
        assert "threshold_diff" in diff

    def test_list_versions(self):
        """Can list all versions of a frame."""
        registry = FrameRegistry()

        for v in ["1.0", "1.1", "2.0"]:
            frame = FrameDefinition(
                frame_id="F_versioned",
                version=v,
                name=f"Frame v{v}",
            )
            registry.register(frame)

        versions = registry.list_versions("F_versioned")
        assert "1.0" in versions
        assert "1.1" in versions
        assert "2.0" in versions

    def test_frame_helpers(self):
        """Frame definition helper methods work."""
        frame = FrameDefinition(
            frame_id="F_test",
            version="1.0",
            name="Test Frame",
            config={
                "thresholds": {"certainty_minimum": 0.7},
                "definitions": {"active": {"min_days": 30}},
                "exclusions": ["archived", "deleted"],
                "synthesis_preferences": {"merge_strategy": "latest_wins"},
            },
        )

        assert frame.get_threshold("certainty_minimum") == 0.7
        assert frame.get_threshold("nonexistent", 0.5) == 0.5
        assert frame.get_definition("active") == {"min_days": 30}
        assert frame.is_excluded("archived") is True
        assert frame.is_excluded("active") is False
        assert frame.get_synthesis_preference("merge_strategy") == "latest_wins"


class TestExpectationRegistry:
    """Tests for Expectation Registry."""

    def test_register_and_resolve(self):
        """Can register and resolve expectations."""
        registry = ExpectationRegistry()

        expectation = ExpectationDefinition(
            expectation_id="EXP_daily_report",
            version="1.0",
            name="Daily Report Expectation",
            rule=ExpectationRule(
                claim_type="daily_report",
                frequency=ExpectationFrequency.DAILY,
            ),
        )
        registry.register(expectation)

        resolved = registry.resolve("EXP_daily_report", "1.0")
        assert resolved.expectation_id == "EXP_daily_report"
        assert resolved.rule.frequency == ExpectationFrequency.DAILY

    def test_resolve_missing_expectation_fails(self):
        """Resolving missing expectation raises error."""
        registry = ExpectationRegistry()

        with pytest.raises(ExpectationNotFoundError):
            registry.resolve("EXP_nonexistent")

    def test_expectation_active_check(self):
        """Can check if expectation is active at a time."""
        now = datetime.now()

        active_exp = ExpectationDefinition(
            expectation_id="EXP_active",
            version="1.0",
            name="Active Expectation",
            active_from=now - timedelta(days=30),
            active_until=now + timedelta(days=30),
        )

        expired_exp = ExpectationDefinition(
            expectation_id="EXP_expired",
            version="1.0",
            name="Expired Expectation",
            active_from=now - timedelta(days=60),
            active_until=now - timedelta(days=30),
        )

        assert active_exp.is_active_at(now) is True
        assert expired_exp.is_active_at(now) is False

    def test_compute_absence_window_daily(self):
        """Computes correct window for daily expectation."""
        registry = ExpectationRegistry()

        expectation = ExpectationDefinition(
            expectation_id="EXP_daily",
            version="1.0",
            name="Daily",
            rule=ExpectationRule(frequency=ExpectationFrequency.DAILY),
        )

        ref_time = datetime(2025, 12, 27, 14, 30, 0)
        start, end = registry.compute_absence_window(expectation, ref_time)

        # Should be the current day
        assert start.day == 27
        assert start.hour == 0
        assert end.day == 28

    def test_compute_absence_window_weekly(self):
        """Computes correct window for weekly expectation."""
        registry = ExpectationRegistry()

        expectation = ExpectationDefinition(
            expectation_id="EXP_weekly",
            version="1.0",
            name="Weekly",
            rule=ExpectationRule(frequency=ExpectationFrequency.WEEKLY),
        )

        # December 27, 2025 is a Saturday
        ref_time = datetime(2025, 12, 27, 14, 30, 0)
        start, end = registry.compute_absence_window(expectation, ref_time)

        # Should be Monday to Sunday
        assert start.weekday() == 0  # Monday
        assert (end - start).days == 7

    def test_compute_absence_window_with_deadline(self):
        """Computes correct window with deadline."""
        registry = ExpectationRegistry()

        expectation = ExpectationDefinition(
            expectation_id="EXP_once",
            version="1.0",
            name="One-time",
            rule=ExpectationRule(
                frequency=ExpectationFrequency.ONCE,
                deadline_hours=48,
            ),
            active_from=datetime(2025, 12, 25, 0, 0, 0),
        )

        ref_time = datetime(2025, 12, 27, 14, 30, 0)
        start, end = registry.compute_absence_window(expectation, ref_time)

        # Should be from active_from to deadline
        assert start == datetime(2025, 12, 25, 0, 0, 0)
        assert (end - start).total_seconds() == 48 * 3600

    def test_list_active_expectations(self):
        """Can list active expectations."""
        registry = ExpectationRegistry()
        now = datetime.now()

        # Register active expectation
        active = ExpectationDefinition(
            expectation_id="EXP_active",
            version="1.0",
            name="Active",
            active_from=now - timedelta(days=1),
            active_until=now + timedelta(days=1),
        )
        registry.register(active)

        # Register expired expectation
        expired = ExpectationDefinition(
            expectation_id="EXP_expired",
            version="1.0",
            name="Expired",
            active_from=now - timedelta(days=30),
            active_until=now - timedelta(days=1),
        )
        registry.register(expired)

        active_list = registry.list_active(now)
        active_ids = [e.expectation_id for e in active_list]

        assert "EXP_active" in active_ids
        assert "EXP_expired" not in active_ids
