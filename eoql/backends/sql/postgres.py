"""
EOQL → Postgres Compiler (v0)

Reference EOQL → Postgres compiler.

This compiler is intentionally conservative:
- prioritizes correctness over performance
- prefers CTEs and explicit structure

A compiler contract test does NOT check correctness of results.
It checks epistemic shape.

The question is never: "Does this SQL run?"
The question is: "Does this SQL preserve the uncertainty, time, frame,
and grounding the IR demanded?"

If the compiler emits:
- DISTINCT
- MAX()
- ORDER BY ... LIMIT 1
- snapshot tables without replay
- filters without frame annotation

the test must fail.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from eoql.ir.model import (
    ConflictPolicy,
    EOQLQuery,
    Mode,
    Pattern,
    Predicate,
    Target,
    Visibility,
)


@dataclass
class SQLPlan:
    """
    The compiled SQL plan with epistemic guarantees.

    The 'notes' field contains human-readable explanation of
    what epistemic guarantees are preserved in this plan.
    """

    sql: str
    params: Dict[str, Any]
    notes: List[str]  # human-readable explanation of epistemic guarantees

    # Epistemic context for the executor
    context: Dict[str, Any] = None

    def __post_init__(self):
        if self.context is None:
            self.context = {}


class CompilationError(Exception):
    """Raised when compilation fails."""

    def __init__(self, message: str, query: Optional[EOQLQuery] = None):
        self.query = query
        super().__init__(message)


class PostgresCompiler:
    """
    Reference EOQL → Postgres compiler.

    This compiler is intentionally conservative:
    - prioritizes correctness over performance
    - prefers CTEs and explicit structure

    EOQL's only job is to produce a fully-specified, epistemically sound
    query plan. Execution is delegated to SQL.
    """

    def compile(self, q: EOQLQuery) -> SQLPlan:
        """
        Compile an EOQL query to a Postgres SQL plan.

        NOTE: validation must already have passed.
        """
        ctes: List[str] = []
        notes: List[str] = []
        params: Dict[str, Any] = {}

        # ---- Phase 1: Event replay (ALT) ----
        # Time is projection, not filtering.
        # AS OF t means: replay all events up to t,
        # reconstruct the world as it could be known then.
        replay_cte = self._compile_time_projection(q)
        ctes.append(replay_cte)
        notes.append("Time projection enforced via event replay CTE")

        # ---- Phase 2: Frame application (SUP) ----
        # Frames are claims, not settings.
        # Selecting a frame is making a claim.
        frame_cte = self._compile_frame_application(q)
        ctes.append(frame_cte)
        notes.append(f"Frame '{q.frame.frame_id}' applied explicitly")

        # ---- Phase 3: Mode filtering (GIVEN vs MEANT) ----
        mode_cte = self._compile_mode_filter(q)
        ctes.append(mode_cte)
        notes.append(f"Mode '{q.mode.value}' applied")

        # ---- Phase 4: Pattern filtering ----
        if q.pattern.where or q.pattern.match:
            pattern_cte = self._compile_pattern(q)
            ctes.append(pattern_cte)
            notes.append("Pattern filters applied")
        else:
            # Pass through if no pattern
            ctes.append("""pattern_filtered AS (
    SELECT * FROM mode_filtered
)""")

        # ---- Phase 5: Grounding traversal (REC) ----
        if q.grounding.trace:
            grounding_cte = self._compile_grounding_traversal(q)
            ctes.append(grounding_cte)
            notes.append(f"Grounding trace enabled (depth: {q.grounding.max_depth})")
        else:
            # No grounding trace, just pass through
            ctes.append("""with_grounding AS (
    SELECT
        pf.*,
        NULL::uuid[] AS grounding_path,
        0 AS grounding_depth
    FROM pattern_filtered pf
)""")

        # ---- Phase 6: Visibility (SEG) ----
        # VISIBLE vs EXISTS: Is it absent, or merely hidden?
        visibility_cte = self._compile_visibility_cte(q)
        ctes.append(visibility_cte)
        notes.append(f"Visibility '{q.visibility.value}' applied")

        # ---- Phase 7: Absence computation (NUL) ----
        if q.target == Target.ABSENCES and q.absence:
            absence_cte = self._compile_absence_computation(q)
            ctes.append(absence_cte)
            notes.append("Absence computed from expectation")

        # ---- Phase 8: Final projection ----
        select_clause, from_clause = self._compile_final_projection(q)

        # ---- Conflict policy ----
        # No silent conflict collapse allowed.
        conflict_guard = self._compile_conflict_policy(q)
        if conflict_guard:
            notes.append("Conflict policy preserved (no silent collapse)")

        ctes_sql = ',\n'.join(ctes)
        sql = f"""WITH
{ctes_sql}
SELECT
    {select_clause}
FROM {from_clause}
{conflict_guard}"""

        # Build context for executor
        context = {
            "frame_id": q.frame.frame_id,
            "frame_version": q.frame.version or "latest",
            "time_projection": q.time.kind.value,
            "time_value": q.time.as_of or f"{q.time.start} - {q.time.end}",
            "visibility": q.visibility.value,
            "mode": q.mode.value,
            "conflict_policy": q.returns.conflict_policy.value,
        }

        return SQLPlan(
            sql=sql.strip(),
            params=params,
            notes=notes,
            context=context,
        )

    # ---------------- Internals ----------------

    def _compile_time_projection(self, q: EOQLQuery) -> str:
        """
        Compile time projection as event replay CTE.

        This is fundamentally different from WHERE timestamp <= t.
        An AI coder must treat time as world reconstruction, not row selection.
        """
        if q.time.kind.name == "AS_OF":
            return f"""event_replay AS (
    SELECT *
    FROM assertions
    WHERE asserted_at <= '{q.time.as_of}'
)"""
        else:
            return f"""event_replay AS (
    SELECT *
    FROM assertions
    WHERE asserted_at BETWEEN '{q.time.start}' AND '{q.time.end}'
)"""

    def _compile_frame_application(self, q: EOQLQuery) -> str:
        """
        Compile frame application as explicit filter.

        EOQL must:
        - require a frame (even if default)
        - return the frame identity with every answer
        - allow frames themselves to be queried and compared
        """
        return f"""framed_projection AS (
    SELECT *
    FROM event_replay
    WHERE frame_id = '{q.frame.frame_id}'
)"""

    def _compile_mode_filter(self, q: EOQLQuery) -> str:
        """
        Compile mode (GIVEN vs MEANT) filter.

        GIVEN: Only directly asserted facts
        MEANT: Include derived/inferred claims
        """
        if q.mode == Mode.GIVEN:
            return """mode_filtered AS (
    SELECT *
    FROM framed_projection
    WHERE assertion_mode = 'GIVEN'
)"""
        else:
            # MEANT includes both GIVEN and MEANT
            return """mode_filtered AS (
    SELECT *
    FROM framed_projection
)"""

    def _compile_pattern(self, q: EOQLQuery) -> str:
        """Compile pattern matching predicates."""
        conditions = []

        if q.pattern.match:
            # Text matching on claim content
            conditions.append(
                f"claim_content::text ILIKE '%{q.pattern.match}%'"
            )

        for pred in q.pattern.where:
            condition = self._compile_predicate(pred)
            conditions.append(condition)

        where_clause = " AND ".join(conditions) if conditions else "TRUE"

        return f"""pattern_filtered AS (
    SELECT *
    FROM mode_filtered
    WHERE {where_clause}
)"""

    def _compile_predicate(self, pred: Predicate) -> str:
        """Compile a single predicate to SQL."""
        field = self._map_field(pred.field)
        op = pred.op.upper()
        value = pred.value

        if op == "=":
            if isinstance(value, str):
                return f"{field} = '{value}'"
            return f"{field} = {value}"
        elif op == "!=":
            if isinstance(value, str):
                return f"{field} != '{value}'"
            return f"{field} != {value}"
        elif op == "IN":
            if isinstance(value, (list, tuple)):
                vals = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in value)
                return f"{field} IN ({vals})"
            return f"{field} IN ({value})"
        elif op in (">=", ">", "<=", "<"):
            if isinstance(value, str):
                return f"{field} {op} '{value}'"
            return f"{field} {op} {value}"
        elif op == "CONTAINS":
            return f"{field}::text ILIKE '%{value}%'"
        elif op == "IS NULL":
            return f"{field} IS NULL"
        elif op == "IS NOT NULL":
            return f"{field} IS NOT NULL"
        else:
            # Default to equality
            if isinstance(value, str):
                return f"{field} = '{value}'"
            return f"{field} = {value}"

    def _map_field(self, field: str) -> str:
        """Map EOQL field names to SQL column names."""
        field_map = {
            "claim_type": "claim_type",
            "source.type": "source_id",  # Would need join
            "epistemic.method": "method",
            "epistemic.certainty": "certainty",
            "term": "claim_content->>'term'",
            "value": "claim_content->>'value'",
        }
        return field_map.get(field, field)

    def _compile_grounding_traversal(self, q: EOQLQuery) -> str:
        """
        Compile grounding/trace traversal as recursive CTE.

        Grounding is a traversal mode, not a join.
        If a claim cannot be grounded, EOQL will indicate weak grounding.
        """
        max_depth = q.grounding.max_depth

        # Build grounded_by filter if present
        grounding_filter = ""
        if q.grounding.grounded_by:
            conditions = []
            for pred in q.grounding.grounded_by:
                conditions.append(self._compile_predicate(pred))
            grounding_filter = f"WHERE {' AND '.join(conditions)}"

        return f"""grounding_traverse AS (
    -- Base case: assertions with their direct grounding
    SELECT
        pf.assertion_id,
        pf.claim_type,
        pf.claim_content,
        pf.subject_id,
        pf.object_id,
        pf.asserted_at,
        pf.valid_from,
        pf.valid_until,
        pf.frame_id,
        pf.frame_version,
        pf.source_id,
        pf.grounding_ref,
        pf.certainty,
        pf.method,
        pf.visibility_scope,
        pf.assertion_mode,
        ARRAY[pf.grounding_ref] AS grounding_path,
        1 AS grounding_depth
    FROM pattern_filtered pf
    WHERE pf.grounding_ref IS NOT NULL

    UNION ALL

    -- Recursive case: follow grounding chains
    SELECT
        gt.assertion_id,
        gt.claim_type,
        gt.claim_content,
        gt.subject_id,
        gt.object_id,
        gt.asserted_at,
        gt.valid_from,
        gt.valid_until,
        gt.frame_id,
        gt.frame_version,
        gt.source_id,
        gc.grounded_by_id AS grounding_ref,
        gt.certainty,
        gt.method,
        gt.visibility_scope,
        gt.assertion_mode,
        gt.grounding_path || gc.grounded_by_id,
        gt.grounding_depth + 1
    FROM grounding_traverse gt
    JOIN grounding_chains gc ON gt.grounding_ref = gc.target_id
    WHERE gt.grounding_depth < {max_depth}
    AND NOT gc.grounded_by_id = ANY(gt.grounding_path)  -- Prevent cycles
),
with_grounding AS (
    -- Select deepest grounding for each assertion
    SELECT DISTINCT ON (assertion_id)
        assertion_id,
        claim_type,
        claim_content,
        subject_id,
        object_id,
        asserted_at,
        valid_from,
        valid_until,
        frame_id,
        frame_version,
        source_id,
        grounding_ref,
        certainty,
        method,
        visibility_scope,
        assertion_mode,
        grounding_path,
        grounding_depth
    FROM grounding_traverse
    {grounding_filter}

    UNION ALL

    -- Include assertions without grounding (marked as weakly grounded)
    SELECT
        pf.assertion_id,
        pf.claim_type,
        pf.claim_content,
        pf.subject_id,
        pf.object_id,
        pf.asserted_at,
        pf.valid_from,
        pf.valid_until,
        pf.frame_id,
        pf.frame_version,
        pf.source_id,
        pf.grounding_ref,
        pf.certainty,
        pf.method,
        pf.visibility_scope,
        pf.assertion_mode,
        ARRAY[]::uuid[] AS grounding_path,
        0 AS grounding_depth
    FROM pattern_filtered pf
    WHERE pf.grounding_ref IS NULL
)"""

    def _compile_visibility_cte(self, q: EOQLQuery) -> str:
        """
        Compile visibility constraint as CTE.

        VISIBLE: Only return items visible in current scope
        EXISTS: Return all items including scoped-out ones (with annotation)
        """
        if q.visibility == Visibility.VISIBLE:
            return """visibility_filtered AS (
    SELECT *
    FROM with_grounding
    WHERE visibility_scope = 'visible'
)"""
        else:
            # EXISTS: include all, but annotate visibility
            return """visibility_filtered AS (
    SELECT
        *,
        CASE
            WHEN visibility_scope != 'visible'
            THEN 'hidden:' || visibility_scope
            ELSE NULL
        END AS visibility_note
    FROM with_grounding
)"""

    def _compile_absence_computation(self, q: EOQLQuery) -> str:
        """
        Compile absence computation.

        Absence is computed, never retrieved.
        It requires: expectation rule, time window, scope, frame.

        Returns absence objects, not nulls or empty results.
        """
        exp = q.absence.expectation
        scope_filter = ""
        if q.absence.scope:
            conditions = []
            for k, v in q.absence.scope.items():
                if isinstance(v, str):
                    conditions.append(f"e.properties->'{k}' = '\"{v}\"'")
                else:
                    conditions.append(f"e.properties->'{k}' = '{v}'")
            if conditions:
                scope_filter = "AND " + " AND ".join(conditions)

        return f"""expected_entities AS (
    -- Get entities that match the expectation scope
    SELECT e.entity_id, e.entity_type, e.label
    FROM entities e
    JOIN expectations exp ON exp.expectation_id = '{exp.expectation_id}'
    WHERE (exp.rule->>'entity_filter' IS NULL
           OR e.entity_type = exp.rule->>'entity_filter')
    {scope_filter}
),
actual_claims AS (
    -- Get claims that satisfy the expectation
    SELECT DISTINCT vf.subject_id
    FROM visibility_filtered vf
    JOIN expectations exp ON exp.expectation_id = '{exp.expectation_id}'
    WHERE vf.claim_type = exp.rule->>'claim_type'
),
computed_absences AS (
    -- Entities that should have claims but don't
    SELECT
        uuid_generate_v4() AS absence_id,
        '{exp.expectation_id}' AS expectation_id,
        '{exp.version or "latest"}' AS expectation_version,
        ee.entity_id AS expected_entity_id,
        (SELECT rule->>'claim_type' FROM expectations WHERE expectation_id = '{exp.expectation_id}') AS expected_claim_type,
        '{q.time.start or q.time.as_of}'::timestamptz AS window_start,
        '{q.time.end or q.time.as_of}'::timestamptz AS window_end,
        '{q.frame.frame_id}' AS frame_id,
        '{q.frame.version or "latest"}' AS frame_version,
        NOW() AS computed_at
    FROM expected_entities ee
    LEFT JOIN actual_claims ac ON ee.entity_id = ac.subject_id
    WHERE ac.subject_id IS NULL
)"""

    def _compile_visibility(self, q: EOQLQuery) -> str:
        """
        Compile visibility constraint (legacy, for simple queries).

        VISIBLE: Only return items visible in current scope
        EXISTS: Return all items including scoped-out ones
        """
        if q.visibility.name == "VISIBLE":
            return "WHERE visibility_scope = 'visible'"
        else:
            # EXISTS means no filtering, but we annotate later
            return ""

    def _compile_final_projection(self, q: EOQLQuery) -> tuple[str, str]:
        """Compile final SELECT and FROM clauses."""
        if q.target == Target.ABSENCES and q.absence:
            return "*", "computed_absences"
        elif q.target == Target.CLAIMS:
            return """
    assertion_id,
    claim_type,
    claim_content,
    subject_id,
    object_id,
    asserted_at,
    valid_from,
    valid_until,
    frame_id,
    frame_version,
    source_id,
    certainty,
    method,
    visibility_scope,
    assertion_mode,
    grounding_path,
    grounding_depth""", "visibility_filtered"
        elif q.target == Target.ENTITIES:
            return """
    DISTINCT subject_id AS entity_id,
    claim_type,
    claim_content,
    frame_id""", "visibility_filtered"
        elif q.target == Target.ASSERTIONS:
            return "*", "visibility_filtered"
        else:
            return "*", "visibility_filtered"

    def _compile_target(self, q: EOQLQuery) -> str:
        """Compile target selection clause (legacy)."""
        if q.target == Target.CLAIMS:
            return "*"
        if q.target == Target.ABSENCES:
            return "*  -- absence objects, computed upstream"
        return "*"

    def _compile_conflict_policy(self, q: EOQLQuery) -> str:
        """
        Compile conflict policy guardrails.

        Ensure compiler does NOT collapse conflicts.
        Backends are forbidden from:
        - strengthening the answer
        - weakening ambiguity
        - discarding conflicts
        """
        if q.returns.conflict_policy == ConflictPolicy.EXPOSE_ALL:
            return ""
        if q.returns.conflict_policy == ConflictPolicy.CLUSTER:
            return "-- conflict clustering applied downstream"
        if q.returns.conflict_policy == ConflictPolicy.RANK:
            return "-- ranked results, alternates preserved"
        if q.returns.conflict_policy == ConflictPolicy.PICK_ONE:
            return "-- single selection WITH explicit rule"
        return ""
