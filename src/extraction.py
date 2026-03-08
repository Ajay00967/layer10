"""
extraction.py — rule-based, fully offline, no API key needed.
"""

import json
import re
import hashlib
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any
import sys

sys.path.insert(0, str(Path(__file__).parent))
from schema import (
    Evidence, Artifact, Entity, PersonEntity, ProjectEntity,
    TechnologyEntity, DecisionEntity, IncidentEntity, Claim,
    SCHEMA_VERSION
)

EXTRACTION_VERSION = "1.0.0"
NOW = datetime.utcnow().isoformat() + "Z"

def _excerpt(text: str, max_len: int = 300) -> str:
    return text[:max_len].strip()

def _normalize_email(s: str) -> str:
    return s.lower().strip()

TECH_PATTERNS = {
    "PostgreSQL": r"\bpostgresql\b|\bpostgres\b",
    "MySQL":      r"\bmysql\b",
    "Redis":      r"\bredis\b",
    "Kafka":      r"\bkafka\b",
    "Kubernetes": r"\bkubernetes\b|\bk8s\b",
    "Docker":     r"\bdocker\b",
    "Python":     r"\bpython\b",
    "Go":         r"\bgolang\b|\bgo\b(?= \d|\slanguage)",
    "React":      r"\breact\b",
    "RabbitMQ":   r"\brabbitmq\b",
    "PagerDuty":  r"\bpagerduty\b",
}

REVERSAL_KEYWORDS = ["switching to", "re-opening", "reverses", "reversal", "changed to", "migrat"]


class ArtifactExtractor:
    def extract_email(self, raw, person_alias_map):
        author_eid = person_alias_map.get(_normalize_email(raw.get("from", "")), "UNKNOWN")
        body = raw.get("body", "")
        art = Artifact(
            artifact_id=f"ART-{raw['id']}", artifact_type="email",
            source_id=raw["id"], content_hash=raw.get("hash", ""),
            timestamp=raw.get("timestamp", ""), author_entity_id=author_eid,
            thread_id=raw.get("thread_id", ""), subject=raw.get("subject", ""),
            body_excerpt=_excerpt(body), is_redacted=raw.get("redacted", False),
            schema_version=SCHEMA_VERSION,
        )
        ev = Evidence(
            source_id=raw["id"], source_type="email",
            excerpt=_excerpt(body, 400), offset_start=0,
            offset_end=min(len(body), 400), timestamp=raw.get("timestamp", ""),
            ingested_at=NOW, redacted=raw.get("redacted", False),
            schema_version=SCHEMA_VERSION, extraction_version=EXTRACTION_VERSION,
        )
        return art, ev

    def extract_issue(self, raw, person_alias_map):
        author_eid = person_alias_map.get(raw.get("author", ""), raw.get("author", "UNKNOWN"))
        desc = raw.get("description", "")
        art = Artifact(
            artifact_id=f"ART-{raw['id']}", artifact_type="issue",
            source_id=raw["id"], content_hash=raw.get("hash", ""),
            timestamp=raw.get("created_at", ""), author_entity_id=author_eid,
            thread_id=raw["id"], subject=raw.get("title", ""),
            body_excerpt=_excerpt(desc), schema_version=SCHEMA_VERSION,
        )
        ev = Evidence(
            source_id=raw["id"], source_type="issue",
            excerpt=_excerpt(desc, 400), offset_start=0,
            offset_end=min(len(desc), 400), timestamp=raw.get("created_at", ""),
            ingested_at=NOW, schema_version=SCHEMA_VERSION,
            extraction_version=EXTRACTION_VERSION,
        )
        comment_pairs = []
        for cmt in raw.get("comments", []):
            cmt_id = cmt.get("comment_id", f"{raw['id']}-cmt")
            cmt_body = cmt.get("body", "")
            cmt_art = Artifact(
                artifact_id=f"ART-{cmt_id}", artifact_type="issue_comment",
                source_id=cmt_id, content_hash=cmt.get("hash", ""),
                timestamp=cmt.get("timestamp", ""),
                author_entity_id=person_alias_map.get(cmt.get("author", ""), cmt.get("author", "UNKNOWN")),
                thread_id=raw["id"], subject=f"Comment on {raw.get('title','')}",
                body_excerpt=_excerpt(cmt_body), schema_version=SCHEMA_VERSION,
            )
            cmt_ev = Evidence(
                source_id=cmt_id, source_type="issue_comment",
                excerpt=_excerpt(cmt_body, 300), offset_start=0,
                offset_end=min(len(cmt_body), 300), timestamp=cmt.get("timestamp", ""),
                ingested_at=NOW, schema_version=SCHEMA_VERSION,
                extraction_version=EXTRACTION_VERSION,
            )
            comment_pairs.append((cmt_art, cmt_ev))
        return art, ev, comment_pairs


class EntityExtractor:
    def extract_people(self, corpus):
        entities = {}
        for p in corpus["meta"]["people"]:
            eid = f"PERS-{p['id']}"
            aliases = list(p["aliases"]) + [p["name"]]
            entities[eid] = PersonEntity(
                entity_id=eid, canonical_name=p["name"], aliases=aliases,
                email_addresses=[a for a in aliases if "@" in a],
                display_names=[a for a in aliases if "@" not in a],
                role=p["role"], organization="Novex Engineering",
                evidence_ids=[], created_at=NOW, updated_at=NOW,
                schema_version=SCHEMA_VERSION,
            )
        return entities

    def extract_projects(self, corpus):
        entities = {}
        for proj in corpus["meta"]["projects"]:
            eid = f"PROJ-{proj.replace(' ', '_').upper()}"
            entities[eid] = ProjectEntity(
                entity_id=eid, canonical_name=proj, aliases=[proj],
                evidence_ids=[], created_at=NOW, updated_at=NOW,
                schema_version=SCHEMA_VERSION,
            )
        return entities

    def extract_technologies(self, all_text, evidence_ids):
        entities = {}
        categories = {
            "PostgreSQL": "database", "MySQL": "database", "Redis": "cache",
            "Kafka": "messaging", "RabbitMQ": "messaging",
            "Kubernetes": "infrastructure", "Docker": "infrastructure",
            "Python": "language", "Go": "language", "React": "frontend",
            "PagerDuty": "operations",
        }
        for tech, pattern in TECH_PATTERNS.items():
            if re.search(pattern, all_text, re.IGNORECASE):
                eid = f"TECH-{tech.upper().replace(' ', '_')}"
                entities[eid] = TechnologyEntity(
                    entity_id=eid, canonical_name=tech, aliases=[tech],
                    category=categories.get(tech, "other"),
                    evidence_ids=evidence_ids[:3],
                    created_at=NOW, updated_at=NOW, schema_version=SCHEMA_VERSION,
                )
        return entities


class ClaimExtractor:
    def extract_from_email(self, raw, ev, person_alias_map, entity_map, tech_entities):
        claims = []
        body = raw.get("body", "")
        author_eid = person_alias_map.get(_normalize_email(raw.get("from", "")), "UNKNOWN")
        ts = raw.get("timestamp", "")

        for tech, pattern in TECH_PATTERNS.items():
            if re.search(pattern, body, re.IGNORECASE):
                tech_eid = f"TECH-{tech.upper().replace(' ', '_')}"
                for proj_name in ["novexcore", "datapipeline", "authservice", "reportingengine", "infrav2"]:
                    if proj_name in body.lower():
                        claims.append(Claim(
                            claim_type="USES_TECHNOLOGY",
                            subject_entity_id=f"PROJ-{proj_name.upper()}",
                            object_entity_id=tech_eid,
                            predicate=f"{proj_name.title()} uses {tech}",
                            value=tech, confidence=0.85,
                            evidence_ids=[ev.evidence_id], valid_from=ts,
                            is_current=True, extracted_at=NOW,
                            schema_version=SCHEMA_VERSION,
                            extraction_version=EXTRACTION_VERSION,
                            tags=["technology", "project"],
                        ))

        if any(kw in body.lower() for kw in ["decision", "we go with", "approved", "switching to", "decided"]):
            is_reversal = any(kw in body.lower() for kw in REVERSAL_KEYWORDS)
            excerpt_short = _excerpt(body, 200)
            dec_id = f"DEC-{ev.evidence_id[:8]}"
            claims.append(Claim(
                claim_type="DECISION_REVERSED" if is_reversal else "DECISION_MADE",
                subject_entity_id=dec_id, object_entity_id=author_eid,
                predicate=excerpt_short[:120], value=excerpt_short,
                confidence=0.95 if is_reversal else 0.9,
                evidence_ids=[ev.evidence_id], valid_from=ts,
                is_current=True, extracted_at=NOW,
                schema_version=SCHEMA_VERSION, extraction_version=EXTRACTION_VERSION,
                tags=["decision"] + (["reversal"] if is_reversal else []),
            ))
        return claims

    def extract_from_issue(self, raw, ev, comment_evs, person_alias_map, project_entities):
        claims = []
        proj_eid = f"PROJ-{raw.get('project', 'UNKNOWN').upper()}"
        assignee_raw = raw.get("assignee", "")
        assignee_eid = person_alias_map.get(assignee_raw, f"PERS-{assignee_raw}")
        author_eid = person_alias_map.get(raw.get("author", ""), f"PERS-{raw.get('author','')}")
        ts_created = raw.get("created_at", "")
        ts_updated = raw.get("updated_at", "")
        current_status = raw.get("status", "")
        issue_eid = f"ISSUE-{raw['id']}"
        all_ev_ids = [ev.evidence_id] + [e.evidence_id for e in comment_evs]

        for i, evt in enumerate(raw.get("events", [])):
            is_current = (i == len(raw["events"]) - 1)
            valid_until = raw["events"][i+1]["timestamp"] if i < len(raw["events"])-1 else ""
            claims.append(Claim(
                claim_type="HAS_STATUS", subject_entity_id=issue_eid,
                predicate=f"Issue {raw['id']} status: {evt['status']}",
                value=evt["status"], confidence=1.0,
                evidence_ids=[ev.evidence_id], valid_from=evt["timestamp"],
                valid_until=valid_until, is_current=is_current,
                extracted_at=NOW, schema_version=SCHEMA_VERSION,
                extraction_version=EXTRACTION_VERSION, tags=["issue", "status"],
            ))

        if assignee_eid:
            claims.append(Claim(
                claim_type="ASSIGNED_TO", subject_entity_id=issue_eid,
                object_entity_id=assignee_eid,
                predicate=f"Issue {raw['id']} assigned to {assignee_eid}",
                value=assignee_eid, confidence=1.0,
                evidence_ids=[ev.evidence_id], valid_from=ts_created,
                is_current=True, extracted_at=NOW,
                schema_version=SCHEMA_VERSION, extraction_version=EXTRACTION_VERSION,
                tags=["issue", "assignment"],
            ))

        claims.append(Claim(
            claim_type="AUTHORED_BY", subject_entity_id=issue_eid,
            object_entity_id=author_eid,
            predicate=f"Issue {raw['id']} authored by {author_eid}",
            value=author_eid, confidence=1.0,
            evidence_ids=[ev.evidence_id], valid_from=ts_created,
            is_current=True, extracted_at=NOW,
            schema_version=SCHEMA_VERSION, extraction_version=EXTRACTION_VERSION,
            tags=["issue", "authorship"],
        ))

        labels = raw.get("labels", [])
        if "critical" in labels or "security" in labels or "incident" in labels:
            claims.append(Claim(
                claim_type="INCIDENT_OCCURRED", subject_entity_id=issue_eid,
                object_entity_id=proj_eid,
                predicate=f"Security/incident: {raw['title']}",
                value=raw.get("description", "")[:200], confidence=0.95,
                evidence_ids=all_ev_ids[:5], valid_from=ts_created,
                valid_until=ts_updated if current_status == "closed" else "",
                is_current=(current_status != "closed"),
                extracted_at=NOW, schema_version=SCHEMA_VERSION,
                extraction_version=EXTRACTION_VERSION, tags=["incident", "security"],
            ))

        all_text = raw.get("description", "") + " ".join(c.get("body", "") for c in raw.get("comments", []))
        for pat in [r"(\d+)(?:ms|\bms\b)", r"(\d+)k?\s*msg/sec",
                    r"(\d+)\s*%\s*(?:improvement|faster|slower)", r"p99[:\s]+(\d+)"]:
            m = re.search(pat, all_text, re.IGNORECASE)
            if m:
                claims.append(Claim(
                    claim_type="PERFORMANCE_METRIC", subject_entity_id=issue_eid,
                    object_entity_id=proj_eid,
                    predicate=f"Performance metric in {raw['id']}: {m.group(0)}",
                    value=m.group(0), confidence=0.8,
                    evidence_ids=[ev.evidence_id], valid_from=ts_updated,
                    is_current=True, extracted_at=NOW,
                    schema_version=SCHEMA_VERSION, extraction_version=EXTRACTION_VERSION,
                    tags=["performance", "metric"],
                ))
                break
        return claims


class ExtractionValidator:
    def __init__(self, min_confidence: float = 0.5):
        self.min_confidence = min_confidence
        self.report: Dict[str, Any] = {
            "claims_validated": 0, "claims_rejected": 0, "claims_repaired": 0,
            "entities_validated": 0, "entities_rejected": 0, "rejection_reasons": [],
        }

    def validate_evidence(self, ev):
        if not ev.excerpt.strip(): return False, "empty_excerpt"
        if not ev.source_id: return False, "missing_source_id"
        if not ev.timestamp: return False, "missing_timestamp"
        return True, "ok"

    def validate_claim(self, claim, evidence_index):
        self.report["claims_validated"] += 1
        if not claim.evidence_ids:
            self.report["claims_rejected"] += 1
            return False, "no_evidence"
        claim.evidence_ids = [e for e in claim.evidence_ids if e in evidence_index]
        if not claim.evidence_ids:
            self.report["claims_rejected"] += 1
            return False, "all_evidence_missing"
        if claim.confidence < self.min_confidence:
            self.report["claims_rejected"] += 1
            return False, "low_confidence"
        if not claim.predicate.strip():
            if claim.value:
                claim.predicate = f"{claim.claim_type}: {claim.value[:80]}"
                self.report["claims_repaired"] += 1
            else:
                self.report["claims_rejected"] += 1
                return False, "missing_predicate"
        return True, "ok"

    def validate_entity(self, entity):
        self.report["entities_validated"] += 1
        if not entity.canonical_name.strip():
            self.report["entities_rejected"] += 1
            return False, "missing_canonical_name"
        return True, "ok"


class ExtractionPipeline:
    def __init__(self, corpus):
        self.corpus = corpus
        self.artifacts: Dict[str, Artifact] = {}
        self.evidence_index: Dict[str, Evidence] = {}
        self.entities: Dict[str, Entity] = {}
        self.claims: List[Claim] = []
        self.tech_entities: Dict[str, TechnologyEntity] = {}

        self.person_alias_map: Dict[str, str] = {}
        for p in corpus["meta"]["people"]:
            eid = f"PERS-{p['id']}"
            for alias in p["aliases"]:
                self.person_alias_map[alias.lower()] = eid
            self.person_alias_map[p["id"].lower()] = eid

        self.art_extractor = ArtifactExtractor()
        self.ent_extractor = EntityExtractor()
        self.claim_extractor = ClaimExtractor()
        self.validator = ExtractionValidator()

    def run(self):
        print("  [1/5] Extracting entities...")
        self.entities.update(self.ent_extractor.extract_people(self.corpus))
        self.entities.update(self.ent_extractor.extract_projects(self.corpus))

        print("  [2/5] Extracting artifacts & evidence from emails...")
        for raw in self.corpus["emails"]:
            if raw.get("redacted"):
                self.artifacts[f"ART-{raw['id']}"] = Artifact(
                    artifact_id=f"ART-{raw['id']}", artifact_type="email",
                    source_id=raw["id"], content_hash=raw.get("hash",""),
                    timestamp=raw.get("timestamp",""), is_redacted=True,
                    schema_version=SCHEMA_VERSION,
                )
                continue
            art, ev = self.art_extractor.extract_email(raw, self.person_alias_map)
            ok, _ = self.validator.validate_evidence(ev)
            if ok:
                self.evidence_index[ev.evidence_id] = ev
                self.artifacts[art.artifact_id] = art
                self.claims.extend(self.claim_extractor.extract_from_email(
                    raw, ev, self.person_alias_map, self.entities, {}
                ))

        print("  [3/5] Extracting artifacts & evidence from issues...")
        for raw in self.corpus["issues"]:
            art, ev, cmt_pairs = self.art_extractor.extract_issue(raw, self.person_alias_map)
            ok, _ = self.validator.validate_evidence(ev)
            if ok:
                self.evidence_index[ev.evidence_id] = ev
                self.artifacts[art.artifact_id] = art
            cmt_evs = []
            for cmt_art, cmt_ev in cmt_pairs:
                ok2, _ = self.validator.validate_evidence(cmt_ev)
                if ok2:
                    self.evidence_index[cmt_ev.evidence_id] = cmt_ev
                    self.artifacts[cmt_art.artifact_id] = cmt_art
                    cmt_evs.append(cmt_ev)
            self.claims.extend(self.claim_extractor.extract_from_issue(
                raw, ev, cmt_evs, self.person_alias_map,
                {k: v for k, v in self.entities.items() if v.entity_type == "project"}
            ))

        print("  [4/5] Extracting technology entities from corpus text...")
        all_text = " ".join(e.get("body","") for e in self.corpus["emails"]) + \
                   " ".join(i.get("description","") + " ".join(c.get("body","") for c in i.get("comments",[]))
                            for i in self.corpus["issues"])
        self.tech_entities = self.ent_extractor.extract_technologies(all_text, list(self.evidence_index.keys())[:5])
        self.entities.update(self.tech_entities)

        print("  [5/5] Validating and quality-gating...")
        valid, rejected = [], 0
        for c in self.claims:
            ok, _ = self.validator.validate_claim(c, self.evidence_index)
            if ok: valid.append(c)
            else: rejected += 1
        self.claims = valid
        print(f"         Claims valid: {len(valid)} | Rejected: {rejected}")
        print(f"         Entities: {len(self.entities)} | Artifacts: {len(self.artifacts)} | Evidence: {len(self.evidence_index)}")

        return {
            "artifacts": {k: v.to_dict() for k, v in self.artifacts.items()},
            "evidence_index": {k: v.to_dict() for k, v in self.evidence_index.items()},
            "entities": {k: v.to_dict() for k, v in self.entities.items()},
            "claims": [c.to_dict() for c in self.claims],
            "validation_report": self.validator.report,
            "meta": {
                "extraction_version": EXTRACTION_VERSION,
                "schema_version": SCHEMA_VERSION,
                "extracted_at": NOW,
                "artifact_count": len(self.artifacts),
                "evidence_count": len(self.evidence_index),
                "entity_count": len(self.entities),
                "claim_count": len(self.claims),
            }
        }
