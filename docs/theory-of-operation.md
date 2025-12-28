# The Theory of EOQL

### *How to Build a Query System That Does Not Lie When the World Refuses to Settle*

---

## 0. What EOQL Is, in One Sentence (for the Coder)

> **EOQL is a planning layer that constrains how queries may be interpreted so that no answer can imply certainty that was not explicitly stored, justified, and scoped in the underlying system.**

Everything else flows from that.

---

## 1. The Fundamental Problem EOQL Solves

### 1.1 The Hidden Assumption in All Query Languages

All mainstream query languages (SQL, Cypher, Datalog, GraphQL, search DSLs) share an unspoken assumption:

> *The data already represents a settled world.*

That assumption leaks in through:

* snapshot semantics ("current state")
* row/record finality
* aggregation defaults
* absence-as-emptiness
* implicit time
* implicit perspective

Once that assumption is present, **certainty is manufactured automatically**, even if the data itself is ambiguous, contested, incomplete, or provisional.

EOQL exists because **EOIR-based systems explicitly reject the premise of a settled world**.

EOQL is therefore not a query language in the traditional sense.
It is a **constraint system on meaning extraction**.

---

## 2. The Separation Principle: World-Forming vs World-Questioning

EOQL rests on a strict separation:

| Layer           | Responsibility                                                   |
| --------------- | ---------------------------------------------------------------- |
| EOIR (ontology) | How reality is allowed to be formed, recorded, and preserved     |
| EOQL            | How questions about that reality may be asked without distortion |

This separation is **non-negotiable**.

If EOQL is allowed to:

* infer equivalence
* collapse time
* resolve conflict
* hide absence
* invent defaults

then EOQL becomes a *second ontology*, and the system forks into contradiction.

**EOQL must never do ontology work.**
It may only *respect* ontology work already done.

---

## 3. The Prime Invariant (Hard Law)

> **EOQL may not manufacture certainty the data model refused to store.**

This is not a guideline.
It is a *soundness condition*.

If violated, EOQL is incorrect even if it is fast, useful, or popular.

### 3.1 What "Manufacturing Certainty" Means Precisely

EOQL manufactures certainty if it:

* returns a single answer where multiple incompatible claims exist
* implies "nothing happened" when an expected event failed to occur
* implies global truth where truth is frame-relative
* implies present truth without declaring a temporal projection
* implies fact where only interpretation exists
* implies irrelevance where only invisibility exists

This definition is operational: it can be tested.

---

## 4. EOQL's Ontological Dependencies (What It Must Assume)

EOQL assumes the underlying system respects **Experiential Ontology (EOIR)**, which provides the following guarantees:

1. **Immutability of events** (INS): once asserted, not erased
2. **Explicit identity** (DES): references are not accidental
3. **Scoping without deletion** (SEG): invisibility ≠ non-existence
4. **Relational meaning** (CON): semantics live in edges
5. **Pre-query synthesis** (SYN): equivalence happens before querying
6. **Temporal persistence** (ALT): time is structural
7. **Coexisting interpretations** (SUP): disagreement is preserved
8. **Grounding chains** (REC): nothing floats
9. **Meaningful absence** (NUL): non-events exert pressure

EOQL does **not** implement these operators.
It **assumes they have already shaped the data**.

---

## 5. The Compression Principle (Why EOQL Has Only 6 Primitives)

Not all EOIR operators correspond to distinct *questions*.

EOQL exposes only operators that create **epistemic distinctions at query time**.

This yields six EOQL primitives:

| EOQL Primitive      | Underlying EOIR Operators | Question It Forces                    |
| ------------------- | ----------------------- | ------------------------------------- |
| GIVEN / MEANT       | DES + INS               | Did this happen, or was it inferred?  |
| EXISTS / VISIBLE    | SEG                     | Is it absent, or merely hidden?       |
| UNDER FRAME         | SUP                     | Under whose assumptions is this true? |
| AS OF / BETWEEN     | ALT                     | When is this projection valid?        |
| TRACE / GROUNDED BY | REC                     | Why does this exist?                  |
| ABSENCE             | NUL                     | What failed to occur that mattered?   |

An AI coder must internalize this:
**EOQL primitives are epistemic questions, not operators.**

---

## 6. EOQL Is a Planner, Not an Executor

### 6.1 Why This Matters

If EOQL executes queries directly, it will be tempted to:

* optimize early
* collapse structure
* hide complexity
* pick defaults

That is exactly how certainty sneaks back in.

Therefore:

> **EOQL's only job is to produce a fully-specified, epistemically sound query plan.**

Execution is delegated to:

* SQL
* graph engines
* log replay systems
* search indexes
* batch processors

EOQL constrains *interpretation*, not *retrieval*.

---

## 7. The EOQL Intermediate Representation (IR) Is the System

The EOQL-IR is the **single source of truth** for correctness.

If an AI coder gets this right, everything else follows.

### 7.1 Required Properties of EOQL-IR

EOQL-IR must be:

* **Total**: no implicit defaults
* **Explicit**: frame, time, visibility always present
* **Serializable**: inspectable, diffable, auditable
* **Rejecting**: invalid questions must fail early
* **Backend-agnostic**: no SQL/graph assumptions baked in

### 7.2 EOQL-IR as a Contract

The IR represents:

> *"Here is exactly what kind of answer is allowed."*

Backends are forbidden from:

* strengthening the answer
* weakening ambiguity
* discarding conflicts
* erasing provenance

If a backend cannot honor the IR, the correct behavior is **to refuse execution**, not to approximate.

---

## 8. Time Is Projection, Not Filtering

A critical conceptual point for implementers:

> **EOQL time is not a WHERE clause.**

`AS OF t` means:

* replay all events up to t
* reconstruct the world *as it could be known then*
* apply frames and synthesis as of that time

This is fundamentally different from:

```sql
WHERE timestamp <= t
```

An AI coder must treat time as **world reconstruction**, not row selection.

---

## 9. Frames Are Claims, Not Settings

A frame is not a UI toggle or config flag.

A frame is:

* a named, versioned interpretation policy
* defining definitions, thresholds, exclusions, synthesis preferences
* itself subject to provenance and disagreement

Therefore:

> **Selecting a frame is making a claim.**

EOQL must:

* require a frame (even if default)
* return the frame identity with every answer
* allow frames themselves to be queried and compared

Frames are epistemic actors.

---

## 10. Absence Is Computed, Never Retrieved

This is where most implementations fail.

Absence does **not** live in storage.

Absence is the result of:

1. an expectation rule
2. a time window
3. a scope
4. a frame

Only after those are defined can EOQL ask:

> *Did the expected thing fail to occur?*

Therefore:

* absence queries cannot be sugar for `NULL`
* absence cannot be inferred from empty result sets
* absence always returns *objects*, not blanks

An AI coder must treat absence as **derived structure**, not missing data.

---

## 11. Grounding Is a Traversal Mode, Not a Join

EOQL's grounding (`TRACE`, `GROUNDED BY`) is not:

* a debugging feature
* a metadata afterthought
* an optional explain plan

It is an epistemic requirement.

If a claim cannot be grounded to:

* assertions
* sources
* methods
* prior claims

then EOQL must be able to say:

> *"This exists, but is weakly grounded."*

This is how EOQL encodes accountability.

---

## 12. Failure Is a Feature

EOQL must refuse to answer questions that:

* omit time
* omit frame
* conflate GIVEN and MEANT
* ask about absence without expectations
* request certainty the data cannot support

These failures are not UX bugs.
They are **epistemic guardrails**.

An AI coder must optimize for *honest refusal*, not maximal answerability.

---

## 13. Why EOQL Cannot Be "Extended SQL"

This is the final conceptual trap to avoid.

SQL:

* assumes snapshot state
* treats rows as facts
* collapses absence into emptiness
* collapses disagreement via aggregation
* has no native concept of frame or grounding

No amount of syntax extension fixes this, because:

> **The defaults are wrong.**

EOQL inverts the defaults:

* ambiguity is preserved unless explicitly resolved
* time is mandatory
* perspective is explicit
* absence is meaningful
* explanations are first-class

This inversion must live *above* SQL, not inside it.

---

## 14. What an AI Coder Must Optimize For

If you were instructing an AI coder, the success criteria would be:

1. **Soundness over convenience**
2. **Explicitness over brevity**
3. **Refusal over silent approximation**
4. **Traceability over performance**
5. **Faithfulness over decisiveness**

If those values are violated, EOQL will devolve into a dashboard generator.

---

## 15. Final Synthesis (The One Thing to Remember)

> **EOQL is not about getting answers.
> It is about preventing answers from pretending to be more certain than the world allows.**

If an AI coder internalizes that, they will:

* design the IR correctly
* enforce the prime rule
* keep EOQL orthogonal to storage
* resist collapsing ambiguity for "usability"

And EOQL will survive contact with reality.

---

# Formal Invariants

## EOQL Soundness Invariants (v0)

**I0. Totality (no implicit defaults)**

* A query must explicitly carry: `target`, `mode`, `visibility`, `frame`, `time`.
* "Default frame/time" is allowed **only if it is materialized into the IR** before validation passes.

**I1. Frame is mandatory**

* `frame` must be present and version-resolved (or resolvable by registry at planning time).

**I2. Time is mandatory**

* `time` must be present (`AS_OF` or `BETWEEN`).

**I3. Mode is mandatory (GIVEN vs MEANT)**

* `mode` must be present.
* Backends may not "upgrade" MEANT results into GIVEN.

**I4. Visibility is mandatory (VISIBLE vs EXISTS)**

* `visibility` must be present.
* If `EXISTS`, results must be annotated with visibility metadata (backend responsibility, but IR must request it).

**I5. Absence requires an expectation**

* If `target == ABSENCES` or `absence_spec != None`, then:
  * `absence.expectation` must exist
  * a `time` window must exist (`BETWEEN` strongly preferred; `AS_OF` allowed only if expectation supplies its own window semantics)
  * expectation must be grounded or traceable (at least a pointer)

**I6. Trace/grounding contracts**

* If `trace == True`, then `grounding.max_depth >= 1`.
* If `grounded_by` filters exist, they must be well-formed (no empty predicates).

**I7. No silent conflict collapse**

* `return_spec.conflict_policy` must be explicit:
  * `EXPOSE_ALL | CLUSTER | RANK | PICK_ONE`
* If `PICK_ONE`, the IR must carry a `selection_rule` (e.g., "highest certainty under frame", "most recent asserted_at", etc.). Otherwise invalid.

**I8. Backend non-strengthening**

* Compilers must not emit plans that reduce ambiguity beyond IR's conflict policy.
* (This is enforced by compiler tests: given the same IR, SQL plan must include conflict-preserving grouping rather than max/distinct.)

---

## Test Strategy

* **IR validation tests**: unit tests that ensure invalid queries fail *before* compilation.
* **Compiler contract tests**: golden tests that ensure compiled plans preserve:
  * time projection requirement
  * frame constraints
  * conflict policy behavior
  * trace behavior (joins/CTEs/graph traversals)
* **Roundtrip tests**: IR → JSON → IR retains meaning.
