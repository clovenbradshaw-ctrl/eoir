"""
EO Compliance Diagnostic

A system for evaluating whether a repository truly complies with
Experiential Ontology (EO) principles or is just pretending.

This diagnostic asks one question, repeatedly:
    Where does certainty get introduced—and is it earned?

A repo is EO-compliant only if every introduction of certainty is:
    1. Explicit
    2. Traceable
    3. Optional
    4. Reversible

If certainty appears by default, the repo fails.
"""

from .types import (
    CheckLevel,
    CheckResult,
    CheckStatus,
    DiagnosticReport,
    Disqualifier,
)
from .checks import (
    Check,
    DisqualifierCheck,
    Level1Check,
    Level2Check,
    Level3Check,
    Level4Check,
    Level5Check,
    Level6Check,
    Level7Check,
)
from .runner import DiagnosticRunner
from .report import generate_report, generate_verdict

__all__ = [
    # Types
    "CheckLevel",
    "CheckResult",
    "CheckStatus",
    "DiagnosticReport",
    "Disqualifier",
    # Checks
    "Check",
    "DisqualifierCheck",
    "Level1Check",
    "Level2Check",
    "Level3Check",
    "Level4Check",
    "Level5Check",
    "Level6Check",
    "Level7Check",
    # Runner
    "DiagnosticRunner",
    # Report
    "generate_report",
    "generate_verdict",
]
