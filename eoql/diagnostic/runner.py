"""
EOIR Compliance Diagnostic Runner.

Executes all compliance checks against a target repository and produces
a comprehensive diagnostic report.
"""

import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from .types import (
    CheckLevel,
    CheckResult,
    CheckStatus,
    DiagnosticReport,
    Disqualifier,
    LevelSummary,
)
from .checks import (
    Check,
    get_all_checks,
    get_disqualifier_checks,
    TargetRepository,
)


@dataclass
class FileSystemRepository:
    """
    Implementation of TargetRepository for file system access.

    Provides pattern searching and file reading capabilities for
    running compliance checks against a local repository.
    """

    path: Path
    _file_cache: Dict[str, str] = field(default_factory=dict)
    _file_list_cache: Dict[str, List[str]] = field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.path, str):
            self.path = Path(self.path)
        if not self.path.exists():
            raise ValueError(f"Repository path does not exist: {self.path}")
        if not self.path.is_dir():
            raise ValueError(f"Repository path is not a directory: {self.path}")

    def _get_files_by_extension(self, extension: str) -> List[str]:
        """Get all files with a given extension."""
        if extension in self._file_list_cache:
            return self._file_list_cache[extension]

        files = []
        for root, dirs, filenames in os.walk(self.path):
            # Skip hidden directories and common non-source directories
            dirs[:] = [d for d in dirs if not d.startswith('.') and d not in (
                '__pycache__', 'node_modules', '.git', 'venv', '.venv', 'env'
            )]
            for filename in filenames:
                if filename.endswith(f'.{extension}'):
                    files.append(os.path.join(root, filename))

        self._file_list_cache[extension] = files
        return files

    def get_python_files(self) -> List[str]:
        """Return all Python files in the repository."""
        return self._get_files_by_extension('py')

    def get_sql_files(self) -> List[str]:
        """Return all SQL files in the repository."""
        return self._get_files_by_extension('sql')

    def read_file(self, path: str) -> str:
        """Read the contents of a file."""
        if path in self._file_cache:
            return self._file_cache[path]

        try:
            with open(path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            self._file_cache[path] = content
            return content
        except Exception:
            return ""

    def has_file(self, path: str) -> bool:
        """Check if a file exists."""
        full_path = self.path / path if not Path(path).is_absolute() else Path(path)
        return full_path.exists()

    def search_pattern(
        self,
        pattern: str,
        file_types: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Search for a pattern in files, returning matches with location info.

        Args:
            pattern: Regular expression pattern to search for
            file_types: List of file extensions to search (e.g., ["py", "sql"])
                       If None, searches all Python and SQL files

        Returns:
            List of dicts with 'file', 'line', 'snippet' keys
        """
        if file_types is None:
            file_types = ["py", "sql", "md"]

        matches = []
        compiled_pattern = re.compile(pattern, re.IGNORECASE | re.MULTILINE)

        for ext in file_types:
            files = self._get_files_by_extension(ext)
            for filepath in files:
                content = self.read_file(filepath)
                for match in compiled_pattern.finditer(content):
                    # Calculate line number
                    line_num = content[:match.start()].count('\n') + 1
                    # Get the line containing the match
                    lines = content.split('\n')
                    if 0 <= line_num - 1 < len(lines):
                        snippet = lines[line_num - 1].strip()[:100]
                    else:
                        snippet = match.group(0)[:100]

                    matches.append({
                        "file": filepath,
                        "line": line_num,
                        "snippet": snippet,
                        "match": match.group(0),
                    })

        return matches

    def get_schema_definitions(self) -> List[Dict[str, Any]]:
        """Get database schema definitions if present."""
        schemas = []

        # Look for schema files
        schema_patterns = [
            "schema", "tables", "models", "migrations"
        ]

        py_files = self.get_python_files()
        for filepath in py_files:
            for pattern in schema_patterns:
                if pattern in filepath.lower():
                    content = self.read_file(filepath)
                    schemas.append({
                        "file": filepath,
                        "content": content,
                    })
                    break

        return schemas

    def get_query_patterns(self) -> List[Dict[str, Any]]:
        """Get query patterns used in the codebase."""
        patterns = []

        # Search for SQL-like patterns
        sql_patterns = [
            r'SELECT\s+',
            r'INSERT\s+INTO',
            r'UPDATE\s+',
            r'DELETE\s+FROM',
        ]

        for sql_pattern in sql_patterns:
            matches = self.search_pattern(sql_pattern)
            for match in matches:
                patterns.append({
                    "type": "sql",
                    "file": match["file"],
                    "line": match["line"],
                    "snippet": match["snippet"],
                })

        return patterns


class DiagnosticRunner:
    """
    Runs EOIR Compliance Diagnostic checks against a repository.

    The runner executes checks in order by level, with Level 0 disqualifiers
    running first. If any disqualifier fails, the diagnostic stops immediately.

    Usage:
        runner = DiagnosticRunner()
        report = runner.run("/path/to/repo")
        print(report.verdict_text)
    """

    def __init__(
        self,
        checks: Optional[List[Check]] = None,
        fail_fast: bool = True,
    ):
        """
        Initialize the diagnostic runner.

        Args:
            checks: List of checks to run. If None, uses all default checks.
            fail_fast: If True, stop at first disqualifier failure.
        """
        self.checks = checks or get_all_checks()
        self.fail_fast = fail_fast

    def run(
        self,
        repository_path: str,
        stop_at_level: Optional[CheckLevel] = None,
    ) -> DiagnosticReport:
        """
        Run the diagnostic against a repository.

        Args:
            repository_path: Path to the repository to analyze
            stop_at_level: Optional level to stop at (for partial diagnostics)

        Returns:
            DiagnosticReport with all results and verdict
        """
        repo = FileSystemRepository(Path(repository_path))

        # Initialize report
        report = DiagnosticReport(
            repository_path=repository_path,
            timestamp=datetime.utcnow().isoformat(),
        )

        # Try to get EOQL version if this is an EOQL repo
        try:
            init_content = repo.read_file(str(repo.path / "eoql" / "__init__.py"))
            version_match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', init_content)
            if version_match:
                report.eoql_version = version_match.group(1)
        except Exception:
            pass

        # Group checks by level
        checks_by_level: Dict[CheckLevel, List[Check]] = {}
        for check in self.checks:
            level = check.level
            if level not in checks_by_level:
                checks_by_level[level] = []
            checks_by_level[level].append(check)

        # Run checks in level order
        disqualified = False
        for level in sorted(checks_by_level.keys(), key=lambda l: l.value):
            if stop_at_level and level.value > stop_at_level.value:
                break

            level_results = []
            for check in checks_by_level[level]:
                try:
                    result = check.run(repo)
                    level_results.append(result)

                    # Check for disqualifier
                    if result.is_disqualifier and result.failed:
                        dq = result.details.get("disqualifier")
                        if dq:
                            report.disqualifiers.append(dq)
                        disqualified = True

                        if self.fail_fast:
                            break

                except Exception as e:
                    # Record the error as a skip
                    level_results.append(CheckResult(
                        check_id=check.check_id,
                        name=check.name,
                        level=check.level,
                        status=CheckStatus.SKIP,
                        description=check.description,
                        details={"error": str(e)},
                    ))

            report.results[level] = level_results

            if disqualified and self.fail_fast:
                break

        # Generate summaries
        report.level_summaries = self._generate_summaries(report.results)

        # Determine compliance
        report.is_compliant = self._determine_compliance(report)
        report.compliance_score = report.total_failures

        # Generate verdict text
        from .report import generate_verdict
        report.verdict_text = generate_verdict(report)

        return report

    def _generate_summaries(
        self,
        results: Dict[CheckLevel, List[CheckResult]],
    ) -> List[LevelSummary]:
        """Generate level summaries from results."""
        summaries = []

        level_names = {
            CheckLevel.LEVEL_0_DISQUALIFIER: "Immediate Disqualifiers",
            CheckLevel.LEVEL_1_ONTOLOGICAL: "Ontological Structure",
            CheckLevel.LEVEL_2_EPISTEMIC: "Epistemic Integrity",
            CheckLevel.LEVEL_3_TEMPORAL: "Temporal Honesty",
            CheckLevel.LEVEL_4_ACCOUNTABILITY: "Accountability",
            CheckLevel.LEVEL_5_ABSENCE: "Absence and Obligation",
            CheckLevel.LEVEL_6_QUERY: "Query Layer Honesty",
            CheckLevel.LEVEL_7_REGRESSION: "Regression Resistance",
        }

        for level in sorted(results.keys(), key=lambda l: l.value):
            level_results = results[level]
            summary = LevelSummary(
                level=level,
                level_name=level_names.get(level, f"Level {level.value}"),
                total_checks=len(level_results),
                passed=sum(1 for r in level_results if r.status == CheckStatus.PASS),
                failed=sum(1 for r in level_results if r.status == CheckStatus.FAIL),
                warnings=sum(1 for r in level_results if r.status == CheckStatus.WARN),
                skipped=sum(1 for r in level_results if r.status == CheckStatus.SKIP),
            )
            summaries.append(summary)

        return summaries

    def _determine_compliance(self, report: DiagnosticReport) -> bool:
        """Determine if the repository is EOIR-compliant."""
        # Any disqualifier = not compliant
        if report.has_disqualifiers:
            return False

        # Score-based: 0-2 failures = compliant (v0)
        if report.total_failures <= 2:
            return True

        return False


def run_diagnostic(
    repository_path: str,
    fail_fast: bool = True,
    stop_at_level: Optional[CheckLevel] = None,
) -> DiagnosticReport:
    """
    Convenience function to run the diagnostic.

    Args:
        repository_path: Path to the repository to analyze
        fail_fast: If True, stop at first disqualifier failure
        stop_at_level: Optional level to stop at

    Returns:
        DiagnosticReport with all results and verdict
    """
    runner = DiagnosticRunner(fail_fast=fail_fast)
    return runner.run(repository_path, stop_at_level=stop_at_level)
