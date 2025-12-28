"""
EO Compliance Diagnostic Report Generation.

This module generates human-readable reports and verdicts from
diagnostic results, including the one-paragraph verdict template.
"""

from typing import Dict, List, Optional
from datetime import datetime

from .types import (
    CheckLevel,
    CheckResult,
    CheckStatus,
    ComplianceVerdict,
    DiagnosticReport,
    Evidence,
    LevelSummary,
)


def generate_verdict(report: DiagnosticReport) -> str:
    """
    Generate the one-paragraph verdict for a diagnostic report.

    This follows the template:
        This repository ⟨does / does not⟩ comply with Experiential Ontology principles.
        It ⟨preserves / collapses⟩ time as projection, ⟨preserves / collapses⟩ epistemic
        disagreement, and ⟨models / fails to model⟩ meaningful absence.
        Any certainty introduced by the system is ⟨explicit and traceable / implicit
        and manufactured⟩.
        Therefore, the repository is ⟨EO-compliant / not EO-compliant⟩.
    """
    # Analyze results to fill in the template
    verdict = _analyze_for_verdict(report)

    # Build the verdict paragraph
    compliance = "does" if verdict.is_compliant else "does not"
    time_handling = "preserves" if verdict.preserves_time else "collapses"
    disagreement_handling = "preserves" if verdict.preserves_disagreement else "collapses"
    absence_handling = "models" if verdict.models_absence else "fails to model"
    certainty_handling = "explicit and traceable" if verdict.certainty_explicit else "implicit and manufactured"
    final_status = "EO-compliant" if verdict.is_compliant else "not EO-compliant"

    paragraph = (
        f"This repository {compliance} comply with Experiential Ontology principles. "
        f"It {time_handling} time as projection, {disagreement_handling} epistemic disagreement, "
        f"and {absence_handling} meaningful absence. "
        f"Any certainty introduced by the system is {certainty_handling}. "
        f"Therefore, the repository is {final_status}."
    )

    # Add disqualifier notes if any
    if verdict.disqualifiers:
        dq_list = ", ".join(verdict.disqualifiers)
        paragraph += f"\n\nDisqualifiers triggered: {dq_list}"

    # Add additional notes
    if verdict.additional_notes:
        paragraph += "\n\nNotes:\n" + "\n".join(f"  - {note}" for note in verdict.additional_notes)

    return paragraph


def _analyze_for_verdict(report: DiagnosticReport) -> ComplianceVerdict:
    """Analyze the report to build a structured verdict."""
    verdict = ComplianceVerdict(
        is_compliant=report.is_compliant,
        preserves_time=True,
        preserves_disagreement=True,
        models_absence=True,
        certainty_explicit=True,
    )

    # Check for disqualifiers
    for dq in report.disqualifiers:
        verdict.disqualifiers.append(dq.value)

    # Analyze by level
    for level, results in report.results.items():
        for result in results:
            if result.failed:
                _update_verdict_from_failure(verdict, result)

    return verdict


def _update_verdict_from_failure(verdict: ComplianceVerdict, result: CheckResult) -> None:
    """Update verdict based on a specific failure."""
    check_id = result.check_id

    # Level 0 - Disqualifiers
    if check_id == "DQ0.1":
        verdict.preserves_time = False
        verdict.certainty_explicit = False
        verdict.time_notes = "Implicit 'current state' detected"
    elif check_id == "DQ0.2":
        verdict.certainty_explicit = False
        verdict.additional_notes.append("Deletion without trace detected")
    elif check_id == "DQ0.3":
        verdict.models_absence = False
        verdict.absence_notes = "Absence treated as NULL"

    # Level 2 - Epistemic
    elif check_id == "L2.1":
        verdict.certainty_explicit = False
    elif check_id == "L2.2":
        verdict.preserves_disagreement = False
        verdict.disagreement_notes = "Conflicting claims cannot coexist"
    elif check_id == "L2.3":
        verdict.certainty_explicit = False

    # Level 3 - Temporal
    elif check_id in ("L3.1", "L3.2"):
        verdict.preserves_time = False

    # Level 5 - Absence
    elif check_id in ("L5.1", "L5.2"):
        verdict.models_absence = False

    # Level 7 - Regression
    elif check_id == "L7.1":
        verdict.certainty_explicit = False
        verdict.additional_notes.append("Optimization may introduce certainty")


def generate_report(report: DiagnosticReport, format: str = "markdown") -> str:
    """
    Generate a full diagnostic report in the specified format.

    Args:
        report: The diagnostic report to format
        format: Output format ("markdown", "text", "json")

    Returns:
        Formatted report string
    """
    if format == "markdown":
        return _generate_markdown_report(report)
    elif format == "text":
        return _generate_text_report(report)
    elif format == "json":
        import json
        return _generate_json_report(report)
    else:
        raise ValueError(f"Unknown format: {format}")


def _generate_markdown_report(report: DiagnosticReport) -> str:
    """Generate a Markdown-formatted report."""
    lines = []

    # Header
    lines.append("# EO Compliance Diagnostic Report")
    lines.append("")
    lines.append(f"**Repository:** `{report.repository_path}`")
    lines.append(f"**Timestamp:** {report.timestamp}")
    if report.eoql_version:
        lines.append(f"**EOQL Version:** {report.eoql_version}")
    lines.append("")

    # Verdict
    lines.append("## Verdict")
    lines.append("")
    if report.is_compliant:
        lines.append("✅ **EO-COMPLIANT**")
    else:
        lines.append("❌ **NOT EO-COMPLIANT**")
    lines.append("")
    lines.append(report.verdict_text)
    lines.append("")

    # Summary
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- **Total Checks:** {report.total_checks}")
    lines.append(f"- **Failures:** {report.total_failures}")
    lines.append(f"- **Disqualifiers Triggered:** {len(report.disqualifiers)}")
    lines.append("")

    # Scoring
    lines.append("### Scoring")
    lines.append("")
    if report.total_failures <= 2:
        lines.append("- **0–2 failures:** EO-compliant (v0) ✓")
    elif report.total_failures <= 6:
        lines.append("- **3–6 failures:** EO-adjacent (dangerous) ⚠")
    else:
        lines.append("- **7+ failures or any disqualifier:** Not EO-compliant ✗")
    lines.append("")

    # Level Summaries
    lines.append("## Level Results")
    lines.append("")

    for summary in report.level_summaries:
        status_icon = "✅" if summary.failed == 0 else "❌"
        lines.append(f"### Level {summary.level.value}: {summary.level_name} {status_icon}")
        lines.append("")
        lines.append(f"- Passed: {summary.passed}/{summary.total_checks}")
        lines.append(f"- Failed: {summary.failed}")
        lines.append(f"- Warnings: {summary.warnings}")
        if summary.skipped > 0:
            lines.append(f"- Skipped: {summary.skipped}")
        lines.append("")

        # Detail each result
        if summary.level in report.results:
            for result in report.results[summary.level]:
                _append_result_markdown(lines, result)

    # Failures Detail
    failures = report.get_failures()
    if failures:
        lines.append("## Failure Details")
        lines.append("")
        for failure in failures:
            lines.append(f"### {failure.check_id}: {failure.name}")
            lines.append("")
            lines.append(f"**Description:** {failure.description}")
            lines.append("")
            if failure.evidence:
                lines.append("**Evidence:**")
                for ev in failure.evidence[:5]:  # Limit to 5
                    loc = ev.location
                    if ev.line_number:
                        loc += f":{ev.line_number}"
                    lines.append(f"- `{loc}`: {ev.description}")
                    if ev.code_snippet:
                        lines.append(f"  ```")
                        lines.append(f"  {ev.code_snippet}")
                        lines.append(f"  ```")
                lines.append("")
            if failure.recommendations:
                lines.append("**Recommendations:**")
                for rec in failure.recommendations:
                    lines.append(f"- {rec}")
                lines.append("")

    # Warnings
    warnings = report.get_warnings()
    if warnings:
        lines.append("## Warnings")
        lines.append("")
        for warning in warnings:
            lines.append(f"### {warning.check_id}: {warning.name}")
            lines.append("")
            if warning.evidence:
                for ev in warning.evidence[:3]:
                    loc = ev.location
                    if ev.line_number:
                        loc += f":{ev.line_number}"
                    lines.append(f"- `{loc}`: {ev.description}")
            if warning.recommendations:
                lines.append("")
                lines.append("**Recommendations:**")
                for rec in warning.recommendations:
                    lines.append(f"- {rec}")
            lines.append("")

    return "\n".join(lines)


def _append_result_markdown(lines: List[str], result: CheckResult) -> None:
    """Append a single result to the markdown output."""
    if result.status == CheckStatus.PASS:
        icon = "✅"
    elif result.status == CheckStatus.FAIL:
        icon = "❌"
    elif result.status == CheckStatus.WARN:
        icon = "⚠️"
    elif result.status == CheckStatus.SKIP:
        icon = "⏭️"
    else:
        icon = "➖"

    lines.append(f"- {icon} **{result.check_id}**: {result.name}")


def _generate_text_report(report: DiagnosticReport) -> str:
    """Generate a plain-text report."""
    lines = []

    lines.append("=" * 60)
    lines.append("EO COMPLIANCE DIAGNOSTIC REPORT")
    lines.append("=" * 60)
    lines.append("")
    lines.append(f"Repository: {report.repository_path}")
    lines.append(f"Timestamp:  {report.timestamp}")
    if report.eoql_version:
        lines.append(f"EOQL Ver:   {report.eoql_version}")
    lines.append("")

    # Verdict
    lines.append("-" * 60)
    lines.append("VERDICT")
    lines.append("-" * 60)
    if report.is_compliant:
        lines.append("[PASS] EO-COMPLIANT")
    else:
        lines.append("[FAIL] NOT EO-COMPLIANT")
    lines.append("")
    lines.append(report.verdict_text)
    lines.append("")

    # Summary
    lines.append("-" * 60)
    lines.append("SUMMARY")
    lines.append("-" * 60)
    lines.append(f"Total Checks:   {report.total_checks}")
    lines.append(f"Failures:       {report.total_failures}")
    lines.append(f"Disqualifiers:  {len(report.disqualifiers)}")
    lines.append("")

    # Level summaries
    for summary in report.level_summaries:
        status = "PASS" if summary.failed == 0 else "FAIL"
        lines.append(f"[{status}] Level {summary.level.value}: {summary.level_name}")
        lines.append(f"       Passed: {summary.passed}/{summary.total_checks}, "
                    f"Failed: {summary.failed}, Warnings: {summary.warnings}")
        lines.append("")

    return "\n".join(lines)


def _generate_json_report(report: DiagnosticReport) -> str:
    """Generate a JSON report."""
    import json
    from dataclasses import asdict
    from enum import Enum

    def serialize(obj):
        if isinstance(obj, Enum):
            return obj.value
        elif hasattr(obj, '__dict__'):
            return {k: serialize(v) for k, v in obj.__dict__.items()}
        elif isinstance(obj, dict):
            return {str(k): serialize(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [serialize(v) for v in obj]
        return obj

    data = {
        "repository_path": report.repository_path,
        "timestamp": report.timestamp,
        "eoql_version": report.eoql_version,
        "is_compliant": report.is_compliant,
        "compliance_score": report.compliance_score,
        "total_checks": report.total_checks,
        "total_failures": report.total_failures,
        "disqualifiers": [dq.value for dq in report.disqualifiers],
        "verdict": report.verdict_text,
        "level_summaries": [serialize(s) for s in report.level_summaries],
        "results": {
            level.value: [serialize(r) for r in results]
            for level, results in report.results.items()
        },
    }

    return json.dumps(data, indent=2)


def generate_compliance_badge(report: DiagnosticReport) -> str:
    """
    Generate a compliance badge in Shields.io format.

    Note: This is NOT what EO compliance is about. The badge is a convenience
    for repos that want to display their status, but the diagnostic itself
    is what matters.
    """
    if report.is_compliant:
        color = "brightgreen"
        status = "EO--Compliant"
    elif report.total_failures <= 6:
        color = "yellow"
        status = "EO--Adjacent"
    else:
        color = "red"
        status = "Not%20EO--Compliant"

    return f"![EO Compliance](https://img.shields.io/badge/{status}-{color})"


def print_summary(report: DiagnosticReport) -> None:
    """Print a brief summary to stdout."""
    print()
    print("=" * 50)
    if report.is_compliant:
        print("✅ EO-COMPLIANT")
    else:
        print("❌ NOT EO-COMPLIANT")
    print("=" * 50)
    print()
    print(f"Total: {report.total_checks} checks")
    print(f"Passed: {report.total_checks - report.total_failures}")
    print(f"Failed: {report.total_failures}")
    if report.disqualifiers:
        print(f"Disqualifiers: {', '.join(dq.value for dq in report.disqualifiers)}")
    print()
    print("Verdict:")
    print(report.verdict_text)
    print()
