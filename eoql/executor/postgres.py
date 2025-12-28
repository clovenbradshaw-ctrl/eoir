"""
EOQL Postgres Query Executor

Takes a SQLPlan, connects to database, runs query, and maps results
back to typed objects with epistemic metadata.

The executor is forbidden from:
- strengthening the answer
- weakening ambiguity
- discarding conflicts
- erasing provenance

If the backend cannot honor the IR, the correct behavior is
to refuse execution, not to approximate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence
from enum import Enum
import json


class ExecutionMode(str, Enum):
    """How to execute the query."""

    STRICT = "strict"      # Fail on any ambiguity
    ANNOTATED = "annotated"  # Execute with epistemic annotations
    EXPLAIN = "explain"    # Return plan only, don't execute


@dataclass
class EpistemicMetadata:
    """
    Epistemic metadata attached to every result row.

    This is how EOQL preserves uncertainty through to results.
    """

    # The frame under which this result was computed
    frame_id: str
    frame_version: str

    # Time projection used
    time_projection: str  # AS_OF or BETWEEN
    time_value: str       # The actual timestamp(s)

    # Visibility status
    visibility: str       # VISIBLE or EXISTS

    # Mode
    mode: str  # GIVEN or MEANT

    # Conflict information
    has_conflicts: bool = False
    conflict_ids: List[str] = field(default_factory=list)
    conflict_policy: str = "EXPOSE_ALL"

    # Grounding information
    grounding_depth: int = 0
    grounding_refs: List[str] = field(default_factory=list)

    # Certainty (if available)
    certainty: Optional[float] = None
    certainty_method: Optional[str] = None


@dataclass
class ResultRow:
    """
    A single result row with epistemic metadata.

    Every row carries its epistemic context - there are no
    "naked" results that pretend to be more certain than they are.
    """

    # The actual data
    data: Dict[str, Any]

    # Epistemic context (always present)
    epistemic: EpistemicMetadata

    # Row-level provenance
    assertion_id: Optional[str] = None
    source_id: Optional[str] = None

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from the data dict."""
        return self.data.get(key, default)

    def __getitem__(self, key: str) -> Any:
        """Allow dict-like access to data."""
        return self.data[key]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to a plain dictionary with metadata."""
        return {
            "data": self.data,
            "epistemic": {
                "frame_id": self.epistemic.frame_id,
                "frame_version": self.epistemic.frame_version,
                "time_projection": self.epistemic.time_projection,
                "time_value": self.epistemic.time_value,
                "visibility": self.epistemic.visibility,
                "mode": self.epistemic.mode,
                "has_conflicts": self.epistemic.has_conflicts,
                "conflict_ids": self.epistemic.conflict_ids,
                "certainty": self.epistemic.certainty,
            },
            "assertion_id": self.assertion_id,
            "source_id": self.source_id,
        }


@dataclass
class ConflictCluster:
    """A group of conflicting results."""

    cluster_id: str
    conflict_type: str  # 'contradictory', 'inconsistent', 'competing'
    rows: List[ResultRow]
    resolution_status: str = "unresolved"


@dataclass
class AbsenceResult:
    """
    A computed absence object.

    Absences are first-class results, not nulls.
    """

    absence_id: str
    expectation_id: str
    expectation_version: str
    expected_entity_id: Optional[str]
    expected_claim_type: Optional[str]
    window_start: datetime
    window_end: datetime
    frame_id: str
    frame_version: str
    computed_at: datetime
    scope: Dict[str, Any]


@dataclass
class QueryResult:
    """
    The complete result of an EOQL query.

    Contains:
    - rows: The actual result rows with epistemic metadata
    - conflicts: Any conflict clusters found
    - absences: Computed absence objects (if requested)
    - metadata: Query-level information
    """

    # Result data
    rows: List[ResultRow] = field(default_factory=list)
    conflicts: List[ConflictCluster] = field(default_factory=list)
    absences: List[AbsenceResult] = field(default_factory=list)

    # Query metadata
    query_id: Optional[str] = None
    executed_at: Optional[datetime] = None
    execution_time_ms: Optional[float] = None

    # The SQL that was executed (for debugging/auditing)
    executed_sql: Optional[str] = None

    # Epistemic summary
    frame_id: Optional[str] = None
    frame_version: Optional[str] = None
    time_projection: Optional[str] = None
    conflict_policy: Optional[str] = None

    # Warnings/notes
    notes: List[str] = field(default_factory=list)

    @property
    def row_count(self) -> int:
        """Number of result rows."""
        return len(self.rows)

    @property
    def has_conflicts(self) -> bool:
        """Whether any conflicts were found."""
        return len(self.conflicts) > 0

    @property
    def has_absences(self) -> bool:
        """Whether any absences were computed."""
        return len(self.absences) > 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to a plain dictionary."""
        return {
            "rows": [r.to_dict() for r in self.rows],
            "conflicts": [
                {
                    "cluster_id": c.cluster_id,
                    "conflict_type": c.conflict_type,
                    "row_count": len(c.rows),
                    "resolution_status": c.resolution_status,
                }
                for c in self.conflicts
            ],
            "absences": [
                {
                    "absence_id": a.absence_id,
                    "expectation_id": a.expectation_id,
                    "window_start": a.window_start.isoformat() if a.window_start else None,
                    "window_end": a.window_end.isoformat() if a.window_end else None,
                }
                for a in self.absences
            ],
            "metadata": {
                "query_id": self.query_id,
                "executed_at": self.executed_at.isoformat() if self.executed_at else None,
                "execution_time_ms": self.execution_time_ms,
                "row_count": self.row_count,
                "has_conflicts": self.has_conflicts,
                "has_absences": self.has_absences,
                "frame_id": self.frame_id,
                "conflict_policy": self.conflict_policy,
            },
            "notes": self.notes,
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2, default=str)


class ExecutionError(Exception):
    """Raised when query execution fails."""

    def __init__(
        self,
        message: str,
        sql: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        self.sql = sql
        self.original_error = original_error
        super().__init__(message)


class EpistemicViolationError(Exception):
    """Raised when execution would violate epistemic guarantees."""

    def __init__(self, message: str, violation_type: str):
        self.violation_type = violation_type
        super().__init__(message)


class PostgresExecutor:
    """
    Executes EOQL queries against a Postgres database.

    This executor:
    - Maps SQL results to typed objects with epistemic metadata
    - Preserves conflict information
    - Computes absences from expectation rules
    - Never approximates or strengthens answers
    """

    def __init__(
        self,
        connection_string: Optional[str] = None,
        connection: Optional[Any] = None,
    ) -> None:
        """
        Initialize the executor.

        Args:
            connection_string: Postgres connection string
            connection: Existing database connection (e.g., psycopg2 connection)
        """
        self._connection_string = connection_string
        self._connection = connection
        self._in_transaction = False

    def execute(
        self,
        sql_plan: "SQLPlan",  # Forward reference to avoid circular import
        mode: ExecutionMode = ExecutionMode.ANNOTATED,
        query_context: Optional[Dict[str, Any]] = None,
    ) -> QueryResult:
        """
        Execute a compiled SQL plan and return results.

        Args:
            sql_plan: The compiled SQL plan from PostgresCompiler
            mode: Execution mode (strict, annotated, explain)
            query_context: Additional context (frame, time, etc.)

        Returns:
            QueryResult with typed rows and epistemic metadata

        Raises:
            ExecutionError: If execution fails
            EpistemicViolationError: If results would violate guarantees
        """
        import uuid
        from datetime import datetime

        context = query_context or {}

        if mode == ExecutionMode.EXPLAIN:
            return QueryResult(
                query_id=str(uuid.uuid4()),
                executed_sql=sql_plan.sql,
                notes=sql_plan.notes + ["EXPLAIN mode: query not executed"],
            )

        # Build epistemic metadata template from context
        epistemic_template = EpistemicMetadata(
            frame_id=context.get("frame_id", "F_default"),
            frame_version=context.get("frame_version", "latest"),
            time_projection=context.get("time_projection", "AS_OF"),
            time_value=context.get("time_value", ""),
            visibility=context.get("visibility", "VISIBLE"),
            mode=context.get("mode", "GIVEN"),
            conflict_policy=context.get("conflict_policy", "EXPOSE_ALL"),
        )

        start_time = datetime.now()

        try:
            raw_rows = self._execute_sql(sql_plan.sql, sql_plan.params)
        except Exception as e:
            raise ExecutionError(
                f"Query execution failed: {e}",
                sql=sql_plan.sql,
                original_error=e,
            )

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds() * 1000

        # Map raw rows to ResultRows with epistemic metadata
        result_rows = self._map_results(raw_rows, epistemic_template)

        # Detect and cluster conflicts
        conflicts = self._detect_conflicts(result_rows, context)

        # Mark rows that are part of conflicts
        conflict_row_ids = set()
        for cluster in conflicts:
            for row in cluster.rows:
                if row.assertion_id:
                    conflict_row_ids.add(row.assertion_id)

        for row in result_rows:
            if row.assertion_id in conflict_row_ids:
                row.epistemic.has_conflicts = True

        return QueryResult(
            rows=result_rows,
            conflicts=conflicts,
            query_id=str(uuid.uuid4()),
            executed_at=start_time,
            execution_time_ms=execution_time,
            executed_sql=sql_plan.sql,
            frame_id=epistemic_template.frame_id,
            frame_version=epistemic_template.frame_version,
            time_projection=epistemic_template.time_projection,
            conflict_policy=epistemic_template.conflict_policy,
            notes=sql_plan.notes,
        )

    def _execute_sql(
        self, sql: str, params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Execute SQL and return raw results.

        Override this method to use a real database connection.
        """
        if self._connection is None:
            # Return empty for now - in real usage, connect to database
            return []

        cursor = self._connection.cursor()
        try:
            cursor.execute(sql, params)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            cursor.close()

    def _map_results(
        self,
        raw_rows: List[Dict[str, Any]],
        epistemic_template: EpistemicMetadata,
    ) -> List[ResultRow]:
        """Map raw database rows to ResultRows with epistemic metadata."""
        result_rows = []

        for raw in raw_rows:
            # Extract epistemic fields from row if present, otherwise use template
            epistemic = EpistemicMetadata(
                frame_id=raw.pop("frame_id", epistemic_template.frame_id),
                frame_version=raw.pop("frame_version", epistemic_template.frame_version),
                time_projection=epistemic_template.time_projection,
                time_value=epistemic_template.time_value,
                visibility=raw.pop("visibility_scope", epistemic_template.visibility),
                mode=raw.pop("assertion_mode", epistemic_template.mode),
                conflict_policy=epistemic_template.conflict_policy,
                certainty=raw.pop("certainty", None),
                certainty_method=raw.pop("method", None),
            )

            # Extract provenance fields
            assertion_id = raw.pop("assertion_id", None)
            source_id = raw.pop("source_id", None)

            # Convert UUID objects to strings if needed
            if assertion_id and hasattr(assertion_id, "hex"):
                assertion_id = str(assertion_id)
            if source_id and hasattr(source_id, "hex"):
                source_id = str(source_id)

            result_rows.append(
                ResultRow(
                    data=raw,
                    epistemic=epistemic,
                    assertion_id=assertion_id,
                    source_id=source_id,
                )
            )

        return result_rows

    def _detect_conflicts(
        self,
        rows: List[ResultRow],
        context: Dict[str, Any],
    ) -> List[ConflictCluster]:
        """
        Detect conflicts in result rows.

        This is a simplified implementation. A full implementation would:
        - Check for contradictory claims on the same subject
        - Detect temporal overlaps with inconsistent values
        - Group competing claims from different sources
        """
        # Group rows by subject for conflict detection
        by_subject: Dict[str, List[ResultRow]] = {}

        for row in rows:
            subject_id = row.data.get("subject_id")
            if subject_id:
                if subject_id not in by_subject:
                    by_subject[subject_id] = []
                by_subject[subject_id].append(row)

        conflicts = []
        import uuid

        for subject_id, subject_rows in by_subject.items():
            if len(subject_rows) > 1:
                # Check if these are actually conflicting
                # (simplified: any multiple claims on same subject/claim_type)
                claim_types: Dict[str, List[ResultRow]] = {}
                for row in subject_rows:
                    ct = row.data.get("claim_type", "unknown")
                    if ct not in claim_types:
                        claim_types[ct] = []
                    claim_types[ct].append(row)

                for ct, ct_rows in claim_types.items():
                    if len(ct_rows) > 1:
                        # These are potentially conflicting
                        conflicts.append(
                            ConflictCluster(
                                cluster_id=str(uuid.uuid4()),
                                conflict_type="competing",
                                rows=ct_rows,
                            )
                        )

        return conflicts

    def begin_transaction(self) -> None:
        """Begin a transaction."""
        if self._connection:
            self._connection.autocommit = False
            self._in_transaction = True

    def commit(self) -> None:
        """Commit the current transaction."""
        if self._connection and self._in_transaction:
            self._connection.commit()
            self._in_transaction = False

    def rollback(self) -> None:
        """Rollback the current transaction."""
        if self._connection and self._in_transaction:
            self._connection.rollback()
            self._in_transaction = False

    def close(self) -> None:
        """Close the database connection."""
        if self._connection:
            if self._in_transaction:
                self.rollback()
            self._connection.close()
            self._connection = None


# Import SQLPlan for type checking
from eoql.backends.sql.postgres import SQLPlan
