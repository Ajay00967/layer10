"""
dedup.py
--------
Deduplication and Canonicalization Pipeline.

Three levels of deduplication:
  1. Artifact dedup     — identical/near-identical messages (hash + content similarity)
  2. Entity canonicalization — person aliases, project renames, tech name variants
  3. Claim dedup        — merge repeated statements of the same fact

All merges are REVERSIBLE:
  - Artifacts get dedup_canonical_id pointing to the representative
  - Entities record merged_from[] and merge_reason
  - Claims record merged_from_claim_ids[] and merge_reason

Conflicts are PRESERVED, not erased:
  - A "DECISION_REVERSED" claim supersedes but does not delete the original
  - Ownership changes create new ASSIGNED_TO claims with validity intervals
"""

import re
import hashlib
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Set, Any
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))
from schema import Entity, Claim, Artifact, Evidence, SCHEMA_VERSION

NOW = datetime.utcnow().isoformat() + "Z"


# ══════════════════════════════════════════════════════════
# 1 — Artifact Deduplication
# ══════════════════════════════════════════════════════════

class ArtifactDeduplicator:
    """
    Detects:
      a) Exact duplicates by content_hash
      b) Near-duplicates from email quoting (body contains another body)
      c) Cross-posts (same body, different thread_id)

    Outputs a merge_map: {duplicate_artifact_id → canonical_artifact_id}
    """

    def run(self, artifacts: Dict[str, Artifact]) -> Tuple[Dict[str, str], List[Dict]]:
        merge_map: Dict[str, str] = {}
        audit_log: List[Dict] = []

        # Group by content_hash
        hash_groups: Dict[str, List[str]] = {}
        for aid, art in artifacts.items():
            h = art.content_hash
            if h:
                hash_groups.setdefault(h, []).append(aid)

        for h, aids in hash_groups.items():
            if len(aids) > 1:
                # Earliest artifact wins (canonical)
                canonical = sorted(aids, key=lambda a: artifacts[a].timestamp or "")[0]
                for dup in aids:
                    if dup != canonical:
                        merge_map[dup] = canonical
                        audit_log.append({
                            "type": "exact_hash_dedup",
                            "duplicate": dup,
                            "canonical": canonical,
                            "reason": f"identical content_hash {h[:8]}",
                            "merged_at": NOW,
                        })

        # Near-duplicate: forwarded/quoted email detection
        # If artifact body_excerpt of B is contained in A's body_excerpt (A is the forward)
        artifact_list = [(aid, art) for aid, art in artifacts.items()
                         if art.artifact_type == "email" and not art.is_redacted and art.body_excerpt]

        for i, (aid_a, art_a) in enumerate(artifact_list):
            for j, (aid_b, art_b) in enumerate(artifact_list):
                if i >= j:
                    continue
                if aid_a in merge_map or aid_b in merge_map:
                    continue
                # Check if art_b excerpt appears in art_a (art_a is the forward)
                excerpt_b = art_b.body_excerpt[:100].strip()
                if len(excerpt_b) > 30 and excerpt_b.lower() in art_a.body_excerpt.lower():
                    # art_a is the forward — mark it as a near-dup pointing to art_b (the original)
                    merge_map[aid_a] = aid_b
                    audit_log.append({
                        "type": "quoted_forward_dedup",
                        "duplicate": aid_a,
                        "canonical": aid_b,
                        "reason": "body_a contains body_b excerpt (forwarded/quoted email)",
                        "merged_at": NOW,
                    })
                    break

        # Apply merge_map back to artifact objects
        for dup_id, canonical_id in merge_map.items():
            if dup_id in artifacts:
                artifacts[dup_id].dedup_canonical_id = canonical_id

        return merge_map, audit_log


# ══════════════════════════════════════════════════════════
# 2 — Entity Canonicalization
# ══════════════════════════════════════════════════════════

class EntityCanonicalizer:
    """
    Merges entity duplicates:
      - PersonEntity: same email in aliases → merge
      - TechnologyEntity: known aliases (Postgres = PostgreSQL)
      - ProjectEntity: renames (tracked via merge_reason)

    Returns:
      entity_merge_map: {old_entity_id → canonical_entity_id}
      updated entities dict
    """

    # Known technology aliases
    TECH_ALIASES: Dict[str, str] = {
        "TECH-POSTGRES":    "TECH-POSTGRESQL",
        "TECH-PG":          "TECH-POSTGRESQL",
        "TECH-K8S":         "TECH-KUBERNETES",
        "TECH-RABBIT":      "TECH-RABBITMQ",
    }

    def run(self, entities: Dict[str, Entity]) -> Tuple[Dict[str, str], Dict[str, Entity], List[Dict]]:
        merge_map: Dict[str, str] = {}
        audit_log: List[Dict] = []
        updated = dict(entities)

        # ─ Technology alias merge ─
        for alias_id, canonical_id in self.TECH_ALIASES.items():
            if alias_id in updated and canonical_id in updated:
                canonical = updated[canonical_id]
                dup = updated[alias_id]
                canonical.aliases = list(set(canonical.aliases + dup.aliases + [alias_id]))
                canonical.merged_from.append(alias_id)
                canonical.merge_reason = f"technology alias: {alias_id} → {canonical_id}"
                canonical.updated_at = NOW
                del updated[alias_id]
                merge_map[alias_id] = canonical_id
                audit_log.append({
                    "type": "tech_alias_merge",
                    "merged": alias_id,
                    "canonical": canonical_id,
                    "reason": "known technology alias",
                    "merged_at": NOW,
                })

        # ─ Person dedup: merge entities sharing email ─
        email_to_eid: Dict[str, str] = {}
        for eid, ent in list(updated.items()):
            if ent.entity_type != "person":
                continue
            for email in getattr(ent, "email_addresses", []):
                norm = email.lower().strip()
                if norm in email_to_eid:
                    # Merge eid into email_to_eid[norm]
                    canonical_id = email_to_eid[norm]
                    canonical = updated[canonical_id]
                    canonical.aliases = list(set(canonical.aliases + ent.aliases))
                    canonical.merged_from.append(eid)
                    canonical.merge_reason = f"shared email: {norm}"
                    canonical.updated_at = NOW
                    if eid in updated:
                        del updated[eid]
                    merge_map[eid] = canonical_id
                    audit_log.append({
                        "type": "person_email_merge",
                        "merged": eid,
                        "canonical": canonical_id,
                        "reason": f"shared email {norm}",
                        "merged_at": NOW,
                    })
                else:
                    email_to_eid[norm] = eid

        return merge_map, updated, audit_log


# ══════════════════════════════════════════════════════════
# 3 — Claim Deduplication & Conflict Resolution
# ══════════════════════════════════════════════════════════

class ClaimDeduplicator:
    """
    Merges claims that express the same fact.
    Handles:
      - Exact duplicate claims (same type, subject, object, value)
      - Decision reversals: DECISION_MADE superseded by DECISION_REVERSED
      - Status transitions: maintains history chain
    """

    def run(self, claims: List[Claim], entity_merge_map: Dict[str, str]) -> Tuple[List[Claim], List[Dict]]:
        # First, remap entity IDs to canonicals
        for c in claims:
            c.subject_entity_id = entity_merge_map.get(c.subject_entity_id, c.subject_entity_id)
            c.object_entity_id = entity_merge_map.get(c.object_entity_id, c.object_entity_id)

        audit_log: List[Dict] = []
        merged: List[Claim] = []

        # Group by (claim_type, subject, object, value-hash)
        groups: Dict[str, List[Claim]] = {}
        for c in claims:
            key = f"{c.claim_type}|{c.subject_entity_id}|{c.object_entity_id}|{c.value[:50]}"
            groups.setdefault(key, []).append(c)

        for key, group in groups.items():
            if len(group) == 1:
                merged.append(group[0])
                continue

            # Merge into representative (highest confidence, earliest valid_from)
            group.sort(key=lambda c: (-(c.confidence), c.valid_from or ""))
            canonical = group[0]
            for dup in group[1:]:
                canonical.evidence_ids = list(set(canonical.evidence_ids + dup.evidence_ids))
                canonical.merged_from_claim_ids.append(dup.claim_id)
                canonical.merge_reason = "duplicate claim merge"
                # Boost confidence from corroborating evidence
                canonical.confidence = min(1.0, canonical.confidence + 0.05 * len(group))
                audit_log.append({
                    "type": "claim_dedup",
                    "merged": dup.claim_id,
                    "canonical": canonical.claim_id,
                    "reason": "duplicate claim (same type/subject/object/value)",
                    "merged_at": NOW,
                })
            merged.append(canonical)

        # ─ Decision reversal linking ─
        decisions_made = {c.claim_id: c for c in merged if c.claim_type == "DECISION_MADE"}
        decisions_reversed = [c for c in merged if c.claim_type == "DECISION_REVERSED"]

        for rev_claim in decisions_reversed:
            # Find the most recent DECISION_MADE claim with overlapping subject/value
            best_match = None
            best_score = 0
            rev_words = set(rev_claim.predicate.lower().split())
            for dm_id, dm_claim in decisions_made.items():
                dm_words = set(dm_claim.predicate.lower().split())
                overlap = len(rev_words & dm_words)
                if overlap > best_score:
                    best_score = overlap
                    best_match = dm_claim

            if best_match and best_score >= 2:
                # Link reversal
                best_match.is_current = False
                best_match.valid_until = rev_claim.valid_from
                best_match.superseded_by_claim_id = rev_claim.claim_id
                rev_claim.supersedes_claim_id = best_match.claim_id
                audit_log.append({
                    "type": "decision_reversal_linked",
                    "original_decision": best_match.claim_id,
                    "reversal": rev_claim.claim_id,
                    "reason": f"word overlap score {best_score}",
                    "merged_at": NOW,
                })

        return merged, audit_log


# ══════════════════════════════════════════════════════════
# Orchestrator
# ══════════════════════════════════════════════════════════

class DeduplicationPipeline:
    def __init__(self, extracted: Dict[str, Any]):
        self.extracted = extracted
        # Rebuild typed objects from dicts
        self.artifacts = {k: _dict_to_artifact(v) for k, v in extracted["artifacts"].items()}
        self.entities = {k: _dict_to_entity(v) for k, v in extracted["entities"].items()}
        self.claims = [_dict_to_claim(c) for c in extracted["claims"]]
        self.evidence_index = extracted["evidence_index"]

    def run(self) -> Dict[str, Any]:
        print("  [1/3] Artifact deduplication...")
        art_dedup = ArtifactDeduplicator()
        artifact_merge_map, art_audit = art_dedup.run(self.artifacts)

        print(f"         Artifact duplicates found: {len(artifact_merge_map)}")

        print("  [2/3] Entity canonicalization...")
        ent_canon = EntityCanonicalizer()
        entity_merge_map, canonical_entities, ent_audit = ent_canon.run(self.entities)
        print(f"         Entity merges: {len(entity_merge_map)}")

        print("  [3/3] Claim deduplication & conflict resolution...")
        claim_dedup = ClaimDeduplicator()
        deduped_claims, claim_audit = claim_dedup.run(self.claims, entity_merge_map)
        reversals = sum(1 for e in claim_audit if e["type"] == "decision_reversal_linked")
        print(f"         Claims after dedup: {len(deduped_claims)} | Reversal links: {reversals}")

        return {
            "artifacts": {k: v.to_dict() for k, v in self.artifacts.items()},
            "evidence_index": self.evidence_index,
            "entities": {k: v.to_dict() for k, v in canonical_entities.items()},
            "claims": [c.to_dict() for c in deduped_claims],
            "dedup_audit": {
                "artifact_merge_map": artifact_merge_map,
                "artifact_audit_log": art_audit,
                "entity_merge_map": entity_merge_map,
                "entity_audit_log": ent_audit,
                "claim_audit_log": claim_audit,
                "summary": {
                    "artifact_duplicates_removed": len(artifact_merge_map),
                    "entity_merges": len(entity_merge_map),
                    "claim_merges": sum(1 for e in claim_audit if e["type"] == "claim_dedup"),
                    "reversal_links": reversals,
                }
            }
        }


# ══════════════════════════════════════════════════════════
# Dict → typed object helpers (for round-trip serialization)
# ══════════════════════════════════════════════════════════

def _dict_to_artifact(d: Dict) -> Artifact:
    a = Artifact()
    for k, v in d.items():
        if hasattr(a, k):
            setattr(a, k, v)
    return a


def _dict_to_entity(d: Dict) -> Entity:
    et = d.get("entity_type", "")
    if et == "person":
        e = Entity.__new__(Entity)
        e.__dict__.update({
            "entity_id": "", "entity_type": "person", "canonical_name": "",
            "aliases": [], "evidence_ids": [], "merged_from": [],
            "merge_reason": "", "created_at": "", "updated_at": "",
            "deleted": False, "schema_version": SCHEMA_VERSION,
            "email_addresses": [], "display_names": [], "role": "", "organization": "",
        })
    elif et == "technology":
        e = Entity.__new__(Entity)
        e.__dict__.update({
            "entity_id": "", "entity_type": "technology", "canonical_name": "",
            "aliases": [], "evidence_ids": [], "merged_from": [],
            "merge_reason": "", "created_at": "", "updated_at": "",
            "deleted": False, "schema_version": SCHEMA_VERSION, "category": "",
        })
    else:
        e = Entity()
    for k, v in d.items():
        try:
            setattr(e, k, v)
        except AttributeError:
            e.__dict__[k] = v
    return e


def _dict_to_claim(d: Dict) -> Claim:
    c = Claim()
    for k, v in d.items():
        if hasattr(c, k):
            setattr(c, k, v)
    return c
