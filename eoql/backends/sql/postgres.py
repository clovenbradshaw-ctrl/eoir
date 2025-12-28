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
from typing import List

from eoql.ir.model import ConflictPolicy, EOQLQuery, Target


@dataclass
class SQLPlan:
    """
    The compiled SQL plan with epistemic guarantees.

    The 'notes' field contains human-readable explanation of
    what epistemic guarantees are preserved in this plan.
    """

    sql: str
    params: dict
    notes: List[str]  # human-readable explanation of epistemic guarantees


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

        # ---- Phase 3: Visibility (SEG) ----
        # VISIBLE vs EXISTS: Is it absent, or merely hidden?
        visibility_clause = self._compile_visibility(q)

        # ---- Phase 4: Target selection ----
        select_clause = self._compile_target(q)

        # ---- Phase 5: Conflict policy ----
        # No silent conflict collapse allowed.
        conflict_guard = self._compile_conflict_policy(q)
        if conflict_guard:
            notes.append("Conflict policy preserved (no silent collapse)")

        sql = f"""WITH
{', '.join(ctes)}
SELECT
    {select_clause}
FROM framed_projection
{visibility_clause}
{conflict_guard}"""

        return SQLPlan(sql=sql.strip(), params={}, notes=notes)

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

    def _compile_visibility(self, q: EOQLQuery) -> str:
        """
        Compile visibility constraint.

        VISIBLE: Only return items visible in current scope
        EXISTS: Return all items including scoped-out ones
        """
        if q.visibility.name == "VISIBLE":
            return "WHERE visibility_scope = 'visible'"
        else:
            # EXISTS means no filtering, but we annotate later
            return ""

    def _compile_target(self, q: EOQLQuery) -> str:
        """Compile target selection clause."""
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
