# Layer10 Take-Home: Grounded Long-Term Memory via Structured Extraction, Deduplication, and a Context Graph

## Overview

This submission builds a complete end-to-end memory graph system over a synthetic corpus that mirrors the **Enron Email Dataset** (CMU/Kaggle) combined with **GitHub Issues**. The synthetic corpus was necessary because the evaluation environment has no internet access; every design decision, schema, and pipeline stage is identical to what would run on the real Enron corpus — swapping the generator for the real loader requires no other changes.

---

## Corpus

**Corpus Name**: Novex Engineering Communications (Synthetic)  
**Mirrors**: Enron Email Dataset + GitHub Issues  
**Contents**: 43 emails across 12 threads, 8 issues with events and comments  
**Challenges modelled**:
- Identity aliasing (`alice.hartmann@novex.com` vs `ahartmann@novex.com` vs `"A. Hartmann"`)
- Email quoting/forwarding chains (dedup challenge)
- Decision reversals (PostgreSQL → MySQL after cost review)
- Issue state transitions (open → in_progress → closed)
- Redacted messages (tombstone preservation)
- Cross-posted announcements (same content, different threads)

**To reproduce with real Enron corpus**: Download from [CMU Enron](https://www.cs.cmu.edu/~enron/) or Kaggle mirror, then replace `corpus_generator.py::generate_corpus()` with `src/enron_loader.py::load_enron(path)`. The extraction pipeline, dedup logic, and graph store are corpus-agnostic.

---

## Ontology / Schema

### Entity Types

| Type | Description | Key Fields |
|------|-------------|------------|
| `PersonEntity` | Individual humans | email_addresses, display_names, role, organization |
| `ProjectEntity` | Software projects | status, description |
| `TechnologyEntity` | Tech choices | category (database/messaging/language/infra) |
| `DecisionEntity` | Explicit decisions | decision_text, decided_by, superseded_by |
| `IncidentEntity` | Security/perf incidents | severity, affected_systems |

### Claim Types

| Type | Meaning |
|------|---------|
| `DECISION_MADE` | An explicit decision was recorded |
| `DECISION_REVERSED` | A prior decision was overturned |
| `HAS_STATUS` | Entity has a status value (with validity interval) |
| `ASSIGNED_TO` | Issue/project assigned to a person |
| `AUTHORED_BY` | Artifact was created by a person |
| `USES_TECHNOLOGY` | Project uses a technology |
| `INCIDENT_OCCURRED` | A security or performance incident |
| `PERFORMANCE_METRIC` | A measured numeric metric |
| `SCOPE_CHANGE` | Project scope or timeline changed |

### Evidence

Every claim points to ≥1 `Evidence` record containing:
- `source_id` (e.g., `EMAIL-T001-04`, `I003-CMT-abc123`)
- `source_type` (email | issue | issue_comment | issue_event)
- `excerpt` (verbatim ≤512 chars)
- `offset_start` / `offset_end` (char offsets in source)
- `timestamp` (event time from source)
- `extraction_version` + `schema_version` (for backfill tracking)

---

## Extraction Pipeline

### Architecture

```
CorpusLoader → ArtifactExtractor → EntityExtractor → ClaimExtractor → ExtractionValidator
```

### Grounding Contract

Every extracted `Claim` **must** reference ≥1 `Evidence` record with a non-empty `excerpt`. Claims failing this gate are rejected (not silently stored). This is enforced in `ExtractionValidator.validate_claim()`.

### Validation & Repair

1. **Missing evidence IDs**: Repair by removing stale evidence pointers; if none remain, reject the claim.
2. **Empty predicates**: Repair by constructing `"{claim_type}: {value[:80]}"`.
3. **Low confidence** (< 0.5 threshold): Reject to prevent noisy memory.
4. **Schema drift**: All objects carry `schema_version` + `extraction_version`. When the ontology changes, a backfill job re-extracts only records with an older version.

### Versioning

Each extracted object stores:
- `schema_version: "1.0.0"` — ontology version
- `extraction_version: "1.0.0"` — prompt/model/rule version

A backfill triggers when `extraction_version` in the DB is older than the current version. Only affected records need re-extraction.

### Quality Gates

- Confidence scoring: single-source claims start at 0.85; corroborating evidence boosts up to min(1.0, base + 0.05 × n).
- Claims with `confidence < 0.5` are rejected before storage.
- Redacted artifacts produce tombstone `Artifact` records only — no claims are extracted from redacted content.

---

## Deduplication and Canonicalization

### Level 1 — Artifact Dedup

- **Exact dedup**: SHA-256 content hash. Same hash → earlier artifact is canonical; later ones get `dedup_canonical_id`.
- **Near-dedup (quoting/forwarding)**: If artifact B's excerpt appears verbatim in artifact A's body, A is a forward of B. A gets `dedup_canonical_id = B`.
- **Reversibility**: Original artifacts are never deleted. The `dedup_canonical_id` pointer can be cleared to undo any merge.

### Level 2 — Entity Canonicalization

- **Person identity**: Email addresses are normalised (`lower().strip()`). Entities sharing an email are merged; the canonical entity accumulates all aliases from both.
- **Technology aliases**: Known alias map (`TECH-POSTGRES → TECH-POSTGRESQL`, `TECH-K8S → TECH-KUBERNETES`).
- **Merge provenance**: Every merged entity records `merged_from: [entity_id, ...]` and `merge_reason`.
- **Reversibility**: The `merged_from` list plus `merge_reason` provides a full audit trail. Undoing a merge re-creates the original entities and re-links their claims.

### Level 3 — Claim Dedup

- Claims grouped by `(claim_type, subject_entity_id, object_entity_id, value[:50])`.
- Groups of size > 1 are merged: highest-confidence claim is canonical; others' `evidence_ids` are unioned onto it.
- `merged_from_claim_ids` + `merge_reason` on the canonical claim enable reversal.

### Conflicts & Reversals

Decision reversals are handled without erasure:
1. The original `DECISION_MADE` claim gets `is_current = False`, `valid_until = reversal_timestamp`, `superseded_by_claim_id`.
2. The new `DECISION_REVERSED` claim gets `supersedes_claim_id` pointing to the original.
3. Both are queryable: `get_history()` returns the full chain; `get_claims_for_entity(current_only=True)` returns only the active decision.

**Example in this corpus**: The PostgreSQL → MySQL reversal. The original PostgreSQL decision (Day 1) is marked historical; the MySQL decision (Day 8) is current. Both are grounded in email evidence.

---

## Memory Graph Design

### Storage

- **SQLite** (trivially replaceable with Postgres + pgvector, or Neo4j).
- **NetworkX DiGraph** for in-memory traversal, neighbourhood expansion, and path queries.
- Schema: `nodes`, `claims`, `evidence`, `artifacts`, `claim_evidence`, `dedup_audit`.

### Bi-temporal Model

| Dimension | Field | Meaning |
|-----------|-------|---------|
| Event time | `valid_from`, `valid_until` | When the fact was true in the world |
| Ingestion time | `stored_at` | When we recorded it |

`is_current = True` means `valid_until` is empty (no end known). This means "current as of last update" — not necessarily "current right now" for rapidly-changing systems.

### Incremental Updates

- All inserts use `INSERT OR REPLACE` (upsert by primary key).
- Re-ingesting the same artifact is idempotent.
- Edits produce a new extraction run with a higher `extraction_version`; the old claims are superseded.
- Deletions set `is_deleted = True` on the artifact; all claims whose only evidence points to that artifact get `is_current = False`.
- Redactions set `is_redacted = True`; evidence records are hidden from retrieval; claims using only redacted evidence are suppressed.

### Permissions (Conceptual)

Each `Evidence` record carries `source_id`. In production:
- Source artifacts are tagged with an ACL (e.g., `{team: "engineering", visibility: "internal"}`).
- At retrieval time, the user's permission set is intersected with source ACLs.
- Claims whose evidence set has no permitted sources are excluded from the context pack.
- This means memory retrieval is constrained by underlying source access — a user cannot learn a fact from a source they cannot read.

### Observability

Logged/measured for degradation detection:
- `validation_report`: claims_validated, claims_rejected, claims_repaired, rejection_reasons
- Per-extraction: entity count, claim count, evidence count, rejected count
- Dedup audit log: every merge with reason and timestamp
- Confidence distribution: alert if median confidence drops below threshold
- Reversal rate: spike in reversals may indicate extraction noise or real corpus churn

---

## Retrieval and Grounding

### Query → Context Pack

```
Query string
  → tokenize (lower, alphanum ≥2 chars)
  → entity matching (keyword overlap on canonical_name + aliases)
  → evidence scoring (TF-IDF cosine, IDF built over all evidence excerpts)
  → claim retrieval (from matched entities + evidence-linked claims)
  → conflict detection (find superseded/superseding pairs in result set)
  → context pack assembly
```

### Grounding Guarantee

Every item in a context pack's `evidence_snippets` carries:
- `evidence_id` → `Evidence` record → `source_id` + `excerpt` + `timestamp`
- `citation` string: `[EMAIL EMAIL-T001-04 @ 2024-01-16]`

No claim appears in a context pack without at least one evidence pointer.

### Ambiguity and Conflicts

When conflicting claims are detected (e.g., "use PostgreSQL" and "switch to MySQL"), both appear in `conflicts_detected` with:
- `is_current` flag distinguishing old vs new
- `conflict_note` explaining the relationship
- `valid_from` / `valid_until` for the temporal scope of each

The retrieval layer does not silently suppress historical claims — it surfaces them with clear labelling.

### Production Upgrade Path

Replace the TF-IDF scorer with a vector embedding model:
1. At ingestion, embed each `Evidence.excerpt` → store vector in pgvector.
2. At query time, embed query → ANN search (HNSW) → top-k evidence.
3. Re-rank with cross-encoder for precision.
4. The rest of the pipeline (claim retrieval, conflict detection, context pack) is unchanged.

---

## Visualization Layer

The standalone `viz/index.html` provides four views:

1. **Graph View**: Force-directed canvas graph. Nodes are colour-coded by entity type (blue=person, green=project, purple=technology, orange=issue). Edges are solid for current claims, dashed/red for historical. Click any node to inspect its claims and evidence.

2. **Retrieval View**: Type any question or click a preset query. Returns answer hint, ranked claims with confidence bars, grounded evidence snippets with citations, and conflict detection panel.

3. **Dedup Audit**: Full audit trail of artifact merges, entity canonicalization, and claim reversals. Every merge shows the from/to IDs and the reason.

4. **Stats View**: System statistics dashboard plus a complete decision timeline showing all `DECISION_MADE` / `DECISION_REVERSED` claims with current/historical labelling.

---

## Layer10 Adaptation

### Ontology Changes for Email + Slack + Jira/Linear

| Addition | Reason |
|----------|--------|
| `ThreadEntity` | Slack threads / email conversations are first-class; decisions happen in threads |
| `MeetingEntity` | Calendar events with participants and outcomes |
| `TicketEntity` | Jira/Linear tickets with status, sprint, priority, story points |
| `ComponentEntity` | Code components / services referenced across tickets and chat |
| `CustomerEntity` | Customer mentions (with PII scrubbing) |
| `OKR / GoalEntity` | Quarterly goals that decisions are made against |

New claim types: `MENTIONED_IN`, `BLOCKS`, `DEPENDS_ON`, `ESCALATED_TO`, `CUSTOMER_REPORTED`.

### Unstructured + Structured Fusion

The key challenge is connecting Slack discussions to Jira tickets to code changes:
- Link a Slack message to a Jira ticket via URL mention → `MENTIONED_IN` claim with both as evidence.
- When a ticket changes status, propagate `HAS_STATUS` claims with event_time from the webhook.
- PR merges linked to tickets → `RESOLVED_BY` claim with commit SHA as evidence.
- This creates a traversable evidence chain: customer complaint → Slack triage → Jira ticket → PR → deployment.

### What Becomes Durable Memory vs Ephemeral Context

| Durable (permanent memory) | Ephemeral (session context) |
|----------------------------|-----------------------------|
| Decisions with explicit `DECISION_MADE` signal | Water-cooler chat |
| Incident timelines | WIP messages ("let me check…") |
| Status transitions on tickets | Draft messages |
| Architecture choices in ADRs | Reaction emojis |
| Ownership assignments | Meeting chatter without outcome |
| Performance baselines | Redundant forwards/replies |

A claim enters durable memory only when it passes confidence threshold AND has ≥1 non-ephemeral evidence source. A decay function can demote claims whose only supporting evidence is older than N days with no corroboration.

### Long-Term Drift Prevention

- **Schema versioning** ensures backfills are targeted.
- **Cross-evidence support**: a fact must be supported by evidence from ≥2 independent sources to be considered "settled memory" (configurable threshold).
- **Decay with revival**: claims not mentioned for 90 days lose confidence; a new mention revives them.
- **Human review hooks**: claims at borderline confidence (0.5–0.7) are queued for human review before becoming durable.

### Grounding & Safety

- **Deletions**: Soft-delete only. Claims pointing exclusively to deleted sources get `is_current = False`. No information is "forgotten" — it becomes inaccessible via normal retrieval.
- **Redactions**: Redacted evidence is hidden from all retrieval. Claims relying only on redacted evidence are suppressed (not deleted). The claim and evidence_id are preserved for audit, but the excerpt is zeroed out.
- **PII**: Email addresses, phone numbers, and personal financial data are scrubbed from excerpts at ingestion time before storage.
- **Provenance citations** are mandatory in every context pack response.

### Permissions

In production:
- Every source artifact inherits ACL from the originating system (Slack channel visibility, Jira project permissions, email domain policies).
- Evidence records carry source ACL hash.
- Retrieval is filtered at the SQL level: `WHERE source_acl_hash IN (user_permitted_set)`.
- A user in `#public-engineering` cannot receive a memory claim grounded only in `#executive-compensation`.

### Operational Reality

| Concern | Approach |
|---------|----------|
| **Scaling** | Partition SQLite by org; migrate to Postgres with pgvector for embeddings; async extraction workers per source type |
| **Cost** | LLM extraction runs only on new/changed artifacts; hashing + dedup prevents re-extraction; cheaper models for structured artifacts (issues), stronger models for unstructured (emails) |
| **Incremental updates** | Webhook-driven ingestion per artifact change; idempotent upserts; extraction only triggered when content hash changes |
| **Evaluation/regression** | Golden test set of (artifact, expected_claims) pairs; extraction accuracy (precision/recall on claim type + evidence id); dedup recall on known alias pairs; retrieval relevance on labelled queries |
| **Latency** | Pre-built IDF index; HNSW ANN for vector search; result caching with TTL keyed on query hash + corpus version |

---

## What We Did Not Build (and Why)

- **Real LLM extraction calls**: Disabled due to no network in evaluation environment. The extraction contract, schema, and validation layer are production-quality; the rule-based extractor is a deterministic stand-in. Replacing it with Claude/GPT-4 structured outputs requires only swapping `ClaimExtractor.extract_from_email()` — the interface is identical.
- **Vector embeddings**: TF-IDF is a faithful functional substitute for offline evaluation. The retrieval interface is designed for drop-in replacement with a vector scorer.
- **Real-time streaming ingestion**: Would use Kafka + a worker pool. The pipeline is structured to be stateless and idempotent, so it's ready for streaming.
