"""
EO Compliance Diagnostic CLI.

Command-line interface for running EO compliance diagnostics
against repositories.

Usage:
    python -m eoql.diagnostic /path/to/repo
    python -m eoql.diagnostic /path/to/repo --format markdown
    python -m eoql.diagnostic /path/to/repo --output report.md
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

from .runner import run_diagnostic
from .report import generate_report, print_summary
from .types import CheckLevel


def main(args: Optional[list] = None) -> int:
    """
    Main entry point for the CLI.

    Returns:
        Exit code: 0 if compliant, 1 if not compliant, 2 if error
    """
    parser = argparse.ArgumentParser(
        prog="eoql-diagnostic",
        description="EO Compliance Diagnostic - Verify Experiential Ontology compliance",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    %(prog)s /path/to/repo
    %(prog)s . --format markdown --output report.md
    %(prog)s /path/to/repo --quiet
    %(prog)s /path/to/repo --level 3

Exit codes:
    0 - Repository is EO-compliant
    1 - Repository is NOT EO-compliant
    2 - Error running diagnostic
        """,
    )

    parser.add_argument(
        "repository",
        type=str,
        help="Path to the repository to analyze",
    )

    parser.add_argument(
        "--format", "-f",
        type=str,
        choices=["text", "markdown", "json"],
        default="text",
        help="Output format (default: text)",
    )

    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Output file (default: stdout)",
    )

    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Only output the verdict, no details",
    )

    parser.add_argument(
        "--level", "-l",
        type=int,
        choices=[0, 1, 2, 3, 4, 5, 6, 7],
        default=None,
        help="Stop at this level (default: run all levels)",
    )

    parser.add_argument(
        "--no-fail-fast",
        action="store_true",
        help="Continue running checks after disqualifier failure",
    )

    parser.add_argument(
        "--version", "-v",
        action="version",
        version="%(prog)s 0.1.0",
    )

    parsed = parser.parse_args(args)

    # Validate repository path
    repo_path = Path(parsed.repository).resolve()
    if not repo_path.exists():
        print(f"Error: Repository path does not exist: {repo_path}", file=sys.stderr)
        return 2

    if not repo_path.is_dir():
        print(f"Error: Repository path is not a directory: {repo_path}", file=sys.stderr)
        return 2

    # Determine stop level
    stop_at_level = None
    if parsed.level is not None:
        stop_at_level = CheckLevel(parsed.level)

    # Run diagnostic
    try:
        report = run_diagnostic(
            str(repo_path),
            fail_fast=not parsed.no_fail_fast,
            stop_at_level=stop_at_level,
        )
    except Exception as e:
        print(f"Error running diagnostic: {e}", file=sys.stderr)
        return 2

    # Output results
    if parsed.quiet:
        if report.is_compliant:
            print("EO-COMPLIANT")
        else:
            print("NOT EO-COMPLIANT")
    else:
        output = generate_report(report, format=parsed.format)

        if parsed.output:
            output_path = Path(parsed.output)
            output_path.write_text(output)
            print(f"Report written to: {output_path}")
            print()
            print_summary(report)
        else:
            print(output)

    # Return exit code based on compliance
    return 0 if report.is_compliant else 1


if __name__ == "__main__":
    sys.exit(main())
