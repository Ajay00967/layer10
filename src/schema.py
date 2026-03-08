"""
schema.py
---------
Ontology / schema for the Layer10 memory graph.

Design principles:
  1. Every claim is grounded — it points to at least one Evidence record.
  2. All objects are versioned — schema_version + extraction_version travel with each record.
  3. Validity intervals distinguish "current" from "historical" facts.
  4. Soft-delete / redaction is first-class.
  5. Merge provenance is recorded for dedup audit trail.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid


SCHEMA_VERSION = "1.0.0"


def _new_id() -> str:
    return str(uuid.uuid4())


# ══════════════════════════════════════════════════════════
#  EVIDENCE  (the base of everything — claims point here)
# ══════════════════════════════════════════════════════════

@dataclass
class Evidence:
    """
    A grounded pointer to a source artifact.
    Every Claim must reference ≥ 1 Evidence record.
    """
    evidence_id: str = field(default_factory=_new_id)
    source_id: str = ""            # e.g. "EMAIL-T001-04", "I003-CMT-abc"
    source_type: str = ""          # email | issue | issue_comment | issue_event
    excerpt: str = ""              # verbatim excerpt (≤ 512 chars)
    offset_start: int = 0          # char offset in source body
    offset_end: int = 0
    timestamp: str = ""            # ISO-8601 event time from source
    ingested_at: str = ""          # ISO-8601 when we ingested it
    redacted: bool = False
    schema_version: str = SCHEMA_VERSION
    extraction_version: str = "1.0.0"

    def to_dict(self) -> Dict:
        return self.__dict__.copy()


# ══════════════════════════════════════════════════════════
#  ENTITY TYPES
# ══════════════════════════════════════════════════════════

@dataclass
class Entity:
    """Base entity — everything that can appear as a node."""
    entity_id: str = field(default_factory=_new_id)
    entity_type: str = ""      # person | project | component | technology | decision | incident
    canonical_name: str = ""
    aliases: List[str] = field(default_factory=list)
    evidence_ids: List[str] = field(default_factory=list)
    # Merge provenance
    merged_from: List[str] = field(default_factory=list)   # entity_ids this was merged from
    merge_reason: str = ""
    # Lifecycle
    created_at: str = ""
    updated_at: str = ""
    deleted: bool = False
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> Dict:
        d = self.__dict__.copy()
        return d


@dataclass
class PersonEntity(Entity):
    entity_type: str = "person"
    email_addresses: List[str] = field(default_factory=list)
    display_names: List[str] = field(default_factory=list)
    role: str = ""
    organization: str = ""


@dataclass
class ProjectEntity(Entity):
    entity_type: str = "project"
    description: str = ""
    status: str = ""              # active | completed | cancelled | deferred


@dataclass
class TechnologyEntity(Entity):
    entity_type: str = "technology"
    category: str = ""            # database | messaging | language | infrastructure


@dataclass
class DecisionEntity(Entity):
    entity_type: str = "decision"
    decision_text: str = ""
    decided_by: str = ""          # entity_id of person
    decided_at: str = ""
    superseded_by: str = ""       # entity_id of newer decision (reversal)
    valid_from: str = ""
    valid_until: str = ""         # empty = still current


@dataclass
class IncidentEntity(Entity):
    entity_type: str = "incident"
    severity: str = ""            # critical | high | medium | low
    status: str = ""
    affected_systems: List[str] = field(default_factory=list)


# ══════════════════════════════════════════════════════════
#  ARTIFACT  (source documents — email messages, issues)
# ══════════════════════════════════════════════════════════

@dataclass
class Artifact:
    """
    A raw source artifact from the corpus.
    Artifacts are never modified; we only add is_deleted / is_redacted flags.
    """
    artifact_id: str = field(default_factory=_new_id)
    artifact_type: str = ""       # email | issue | issue_comment
    source_id: str = ""           # original corpus ID
    content_hash: str = ""
    timestamp: str = ""
    author_entity_id: str = ""
    thread_id: str = ""
    subject: str = ""
    body_excerpt: str = ""        # first 500 chars
    is_deleted: bool = False
    is_redacted: bool = False
    dedup_canonical_id: str = ""  # if this is a duplicate, points to the canonical artifact
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> Dict:
        return self.__dict__.copy()


# ══════════════════════════════════════════════════════════
#  CLAIM  (the core memory unit)
# ══════════════════════════════════════════════════════════

CLAIM_TYPES = {
    # Relational
    "ASSIGNED_TO":     "Entity X is assigned to person Y",
    "AUTHORED_BY":     "Artifact was authored by person Y",
    "MEMBER_OF":       "Person is member of project/team",
    "USES_TECHNOLOGY": "Project uses a technology",
    # Factual / state
    "HAS_STATUS":      "Entity has a given status",
    "HAS_ROLE":        "Person has a role",
    "DECISION_MADE":   "A decision was made",
    "DECISION_REVERSED": "A prior decision was reversed",
    "INCIDENT_OCCURRED": "A security/perf incident occurred",
    "PERFORMANCE_METRIC": "A measured metric value",
    "OWNERSHIP_CHANGE": "Ownership/assignment changed",
    "SCOPE_CHANGE":    "Project scope or timeline changed",
}


@dataclass
class Claim:
    """
    A typed, grounded, versioned statement about entities/artifacts.
    A Claim can have multiple supporting Evidence records.
    Conflicts are tracked by linking superseding claims.
    """
    claim_id: str = field(default_factory=_new_id)
    claim_type: str = ""           # one of CLAIM_TYPES
    subject_entity_id: str = ""   # entity this claim is about
    object_entity_id: str = ""    # optional second entity (for relations)
    predicate: str = ""           # human-readable statement
    value: str = ""               # free-form value / detail
    confidence: float = 1.0       # 0–1
    evidence_ids: List[str] = field(default_factory=list)
    # Validity (bi-temporal)
    valid_from: str = ""          # when this became true (event time)
    valid_until: str = ""         # when it stopped being true (empty = current)
    is_current: bool = True
    # Conflict tracking
    supersedes_claim_id: str = "" # if this reverses a prior claim
    superseded_by_claim_id: str = ""
    # Merge provenance
    merged_from_claim_ids: List[str] = field(default_factory=list)
    merge_reason: str = ""
    # Metadata
    extracted_at: str = ""
    schema_version: str = SCHEMA_VERSION
    extraction_version: str = "1.0.0"
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return self.__dict__.copy()


# ══════════════════════════════════════════════════════════
#  CONTEXT PACK  (retrieval output)
# ══════════════════════════════════════════════════════════

@dataclass
class ContextPack:
    """
    The grounded context pack returned by the retrieval layer.
    Every item in evidence_snippets is traceable to a source artifact.
    """
    query: str = ""
    retrieved_at: str = ""
    claims: List[Dict] = field(default_factory=list)
    evidence_snippets: List[Dict] = field(default_factory=list)
    entities: List[Dict] = field(default_factory=list)
    conflicts_detected: List[Dict] = field(default_factory=list)
    answer_hint: str = ""

    def to_dict(self) -> Dict:
        return self.__dict__.copy()
