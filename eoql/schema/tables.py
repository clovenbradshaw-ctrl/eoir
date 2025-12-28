"""
EOQL Postgres Schema

This schema implements the data model required by Experiential Ontology (EOIR).
Every table is designed to preserve:
- Immutability of events (INS)
- Explicit identity (DES)
- Scoping without deletion (SEG)
- Temporal persistence (ALT)
- Coexisting interpretations (SUP)
- Grounding chains (REC)

CRITICAL: This schema never deletes. "Deletion" is scoping (visibility change).
"""

POSTGRES_SCHEMA = """
-- ============================================================================
-- EOQL Core Schema
-- ============================================================================

-- Extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- FRAMES: Interpretation policies (SUP)
-- ============================================================================
-- A frame is a named, versioned interpretation policy.
-- Selecting a frame is making a claim.

CREATE TABLE IF NOT EXISTS frames (
    frame_id        TEXT NOT NULL,
    version         TEXT NOT NULL,

    -- Frame metadata
    name            TEXT NOT NULL,
    description     TEXT,

    -- Frame configuration (JSONB for flexibility)
    -- Contains: definitions, thresholds, exclusions, synthesis preferences
    config          JSONB NOT NULL DEFAULT '{}',

    -- Provenance
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by      TEXT,
    supersedes      TEXT,  -- Previous version this replaces

    -- Frames themselves are subject to provenance
    grounding_ref   UUID,  -- Points to grounding chain

    PRIMARY KEY (frame_id, version)
);

-- Default frame that must always exist
INSERT INTO frames (frame_id, version, name, description, config)
VALUES ('F_default', 'latest', 'Default Frame', 'System default interpretation frame', '{}')
ON CONFLICT (frame_id, version) DO NOTHING;

-- ============================================================================
-- SOURCES: Where claims originate (REC)
-- ============================================================================

CREATE TABLE IF NOT EXISTS sources (
    source_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Source identification
    source_type     TEXT NOT NULL,  -- 'document', 'sensor', 'human', 'system', 'derived'
    source_uri      TEXT,           -- External reference if applicable
    source_name     TEXT NOT NULL,

    -- Source metadata
    metadata        JSONB NOT NULL DEFAULT '{}',

    -- Provenance
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    registered_by   TEXT
);

-- ============================================================================
-- ENTITIES: Things that exist (DES)
-- ============================================================================
-- Entities have explicit identity. References are not accidental.

CREATE TABLE IF NOT EXISTS entities (
    entity_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Entity type (extensible)
    entity_type     TEXT NOT NULL,

    -- Human-readable label (not identity)
    label           TEXT,

    -- Additional properties
    properties      JSONB NOT NULL DEFAULT '{}',

    -- When this entity was first asserted
    first_asserted  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Visibility scope (SEG): invisibility ≠ non-existence
    visibility_scope TEXT NOT NULL DEFAULT 'visible',

    -- Grounding
    grounding_ref   UUID
);

-- ============================================================================
-- ASSERTIONS: Immutable event records (INS + ALT)
-- ============================================================================
-- Once asserted, never erased. This is the event log.

CREATE TABLE IF NOT EXISTS assertions (
    assertion_id    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- What is being asserted
    claim_type      TEXT NOT NULL,      -- 'attribute', 'relationship', 'event', 'state'
    claim_content   JSONB NOT NULL,     -- The actual claim data

    -- Subject of the assertion
    subject_id      UUID NOT NULL REFERENCES entities(entity_id),

    -- Optional object (for relationships)
    object_id       UUID REFERENCES entities(entity_id),

    -- Temporal (ALT): time is structural
    asserted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_from      TIMESTAMPTZ,        -- When the claim becomes true
    valid_until     TIMESTAMPTZ,        -- When the claim ceases to be true (if known)

    -- Frame context (SUP): which interpretation this belongs to
    frame_id        TEXT NOT NULL DEFAULT 'F_default',
    frame_version   TEXT NOT NULL DEFAULT 'latest',

    -- Source and provenance (REC)
    source_id       UUID REFERENCES sources(source_id),
    grounding_ref   UUID,               -- Points to grounding chain

    -- Epistemic metadata
    certainty       REAL CHECK (certainty >= 0 AND certainty <= 1),
    method          TEXT,               -- How this was determined

    -- Visibility (SEG)
    visibility_scope TEXT NOT NULL DEFAULT 'visible',

    -- Mode (GIVEN vs MEANT)
    assertion_mode  TEXT NOT NULL DEFAULT 'GIVEN' CHECK (assertion_mode IN ('GIVEN', 'MEANT')),

    FOREIGN KEY (frame_id, frame_version) REFERENCES frames(frame_id, version)
);

-- ============================================================================
-- GROUNDING_CHAINS: Provenance links (REC)
-- ============================================================================
-- Nothing floats. Every claim can be traced.

CREATE TABLE IF NOT EXISTS grounding_chains (
    grounding_id    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- What this grounds
    target_type     TEXT NOT NULL,      -- 'assertion', 'entity', 'frame', 'synthesis'
    target_id       UUID NOT NULL,

    -- What it's grounded by
    grounded_by_type TEXT NOT NULL,     -- 'assertion', 'source', 'rule', 'derivation'
    grounded_by_id  UUID NOT NULL,

    -- Relationship type
    grounding_type  TEXT NOT NULL,      -- 'derived_from', 'supported_by', 'contradicted_by', 'supersedes'

    -- Strength of grounding
    strength        REAL CHECK (strength >= 0 AND strength <= 1),

    -- When this grounding was established
    established_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Metadata
    metadata        JSONB NOT NULL DEFAULT '{}'
);

-- ============================================================================
-- EXPECTATIONS: Rules for meaningful absence (NUL)
-- ============================================================================
-- Absence is computed, never retrieved. These define what "should" happen.

CREATE TABLE IF NOT EXISTS expectations (
    expectation_id  TEXT NOT NULL,
    version         TEXT NOT NULL,

    -- What we expect
    name            TEXT NOT NULL,
    description     TEXT,

    -- Expectation rule (executable)
    -- Contains: entity_filter, claim_type, frequency, deadline, scope
    rule            JSONB NOT NULL,

    -- When is this expectation active?
    active_from     TIMESTAMPTZ,
    active_until    TIMESTAMPTZ,

    -- Frame context (expectations can be frame-relative)
    frame_id        TEXT,
    frame_version   TEXT,

    -- Provenance
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by      TEXT,
    grounding_ref   UUID,

    PRIMARY KEY (expectation_id, version)
);

-- ============================================================================
-- SYNTHESIS_RECORDS: Pre-query equivalence (SYN)
-- ============================================================================
-- Equivalence happens before querying, not during.

CREATE TABLE IF NOT EXISTS synthesis_records (
    synthesis_id    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- What was synthesized
    synthesis_type  TEXT NOT NULL,      -- 'merge', 'split', 'reclassify', 'link'

    -- Entities involved
    input_entities  UUID[] NOT NULL,
    output_entities UUID[] NOT NULL,

    -- Synthesis rule/method
    method          TEXT NOT NULL,
    rule_ref        TEXT,

    -- Frame context
    frame_id        TEXT NOT NULL,
    frame_version   TEXT NOT NULL,

    -- Temporal
    synthesized_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_from      TIMESTAMPTZ,

    -- Grounding
    grounding_ref   UUID,

    FOREIGN KEY (frame_id, frame_version) REFERENCES frames(frame_id, version)
);

-- ============================================================================
-- CONFLICTS: Preserved disagreement (SUP)
-- ============================================================================
-- Disagreement is preserved, not resolved by default.

CREATE TABLE IF NOT EXISTS conflicts (
    conflict_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Conflicting assertions
    assertion_ids   UUID[] NOT NULL,

    -- Type of conflict
    conflict_type   TEXT NOT NULL,      -- 'contradictory', 'inconsistent', 'competing'

    -- Description
    description     TEXT,

    -- When detected
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Resolution status (if any)
    resolution_status TEXT NOT NULL DEFAULT 'unresolved',
    resolution_frame TEXT,              -- Frame under which resolved
    resolution_method TEXT,
    resolved_at     TIMESTAMPTZ,

    -- The resolved assertion (if PICK_ONE was used)
    resolved_assertion_id UUID REFERENCES assertions(assertion_id)
);

-- ============================================================================
-- ABSENCE_RECORDS: Computed meaningful absences (NUL)
-- ============================================================================
-- Absences are objects, not nulls.

CREATE TABLE IF NOT EXISTS absence_records (
    absence_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Which expectation was violated
    expectation_id  TEXT NOT NULL,
    expectation_version TEXT NOT NULL,

    -- What was expected
    expected_entity_id UUID REFERENCES entities(entity_id),
    expected_claim_type TEXT,

    -- Time window of the absence
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,

    -- Frame context
    frame_id        TEXT NOT NULL,
    frame_version   TEXT NOT NULL,

    -- When computed
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Scope that was checked
    scope           JSONB NOT NULL DEFAULT '{}',

    FOREIGN KEY (expectation_id, expectation_version) REFERENCES expectations(expectation_id, version),
    FOREIGN KEY (frame_id, frame_version) REFERENCES frames(frame_id, version)
);
"""

POSTGRES_INDEXES = """
-- ============================================================================
-- Performance Indexes
-- ============================================================================

-- Assertions: primary query patterns
CREATE INDEX IF NOT EXISTS idx_assertions_asserted_at ON assertions(asserted_at);
CREATE INDEX IF NOT EXISTS idx_assertions_subject ON assertions(subject_id);
CREATE INDEX IF NOT EXISTS idx_assertions_frame ON assertions(frame_id, frame_version);
CREATE INDEX IF NOT EXISTS idx_assertions_visibility ON assertions(visibility_scope);
CREATE INDEX IF NOT EXISTS idx_assertions_mode ON assertions(assertion_mode);
CREATE INDEX IF NOT EXISTS idx_assertions_claim_type ON assertions(claim_type);

-- Temporal range queries
CREATE INDEX IF NOT EXISTS idx_assertions_valid_range ON assertions(valid_from, valid_until);

-- Grounding traversal
CREATE INDEX IF NOT EXISTS idx_grounding_target ON grounding_chains(target_type, target_id);
CREATE INDEX IF NOT EXISTS idx_grounding_source ON grounding_chains(grounded_by_type, grounded_by_id);

-- Entity lookups
CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_entities_visibility ON entities(visibility_scope);

-- Conflict lookups
CREATE INDEX IF NOT EXISTS idx_conflicts_status ON conflicts(resolution_status);

-- Absence lookups
CREATE INDEX IF NOT EXISTS idx_absence_expectation ON absence_records(expectation_id, expectation_version);
CREATE INDEX IF NOT EXISTS idx_absence_window ON absence_records(window_start, window_end);
"""

POSTGRES_VIEWS = """
-- ============================================================================
-- Utility Views
-- ============================================================================

-- Current visible assertions (convenience, but use with caution)
CREATE OR REPLACE VIEW visible_assertions AS
SELECT * FROM assertions
WHERE visibility_scope = 'visible';

-- Latest frame versions
CREATE OR REPLACE VIEW latest_frames AS
SELECT DISTINCT ON (frame_id) *
FROM frames
ORDER BY frame_id, created_at DESC;

-- Unresolved conflicts
CREATE OR REPLACE VIEW unresolved_conflicts AS
SELECT * FROM conflicts
WHERE resolution_status = 'unresolved';

-- Grounding depth helper (for trace queries)
CREATE OR REPLACE VIEW grounding_with_depth AS
WITH RECURSIVE grounding_tree AS (
    -- Base case: direct groundings
    SELECT
        grounding_id,
        target_type,
        target_id,
        grounded_by_type,
        grounded_by_id,
        grounding_type,
        strength,
        1 as depth,
        ARRAY[grounding_id] as path
    FROM grounding_chains

    UNION ALL

    -- Recursive case: follow the chain
    SELECT
        gc.grounding_id,
        gc.target_type,
        gc.target_id,
        gc.grounded_by_type,
        gc.grounded_by_id,
        gc.grounding_type,
        gc.strength,
        gt.depth + 1,
        gt.path || gc.grounding_id
    FROM grounding_chains gc
    JOIN grounding_tree gt ON gc.target_id = gt.grounded_by_id
    WHERE NOT gc.grounding_id = ANY(gt.path)  -- Prevent cycles
    AND gt.depth < 100  -- Safety limit
)
SELECT * FROM grounding_tree;
"""


def get_full_schema() -> str:
    """Return the complete schema as a single SQL string."""
    return f"{POSTGRES_SCHEMA}\n\n{POSTGRES_INDEXES}\n\n{POSTGRES_VIEWS}"
