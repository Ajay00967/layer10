"""
retrieval.py
------------
Retrieval and Grounding Engine.

Architecture
============
  1. Query → candidate entities via keyword matching on canonical_name + aliases
  2. Evidence scoring via TF-IDF cosine similarity (no external embeddings needed)
  3. Claim + evidence aggregation with confidence-weighted ranking
  4. Context pack assembly: ranked evidence snippets + linked entities + conflict detection
  5. Every returned item is grounded: claim_id → evidence_id → source_id → excerpt

In production, step 2 would use a vector embedding model (e.g. text-embedding-3-small)
stored in pgvector or a dedicated vector store. The interface is identical — only the
scorer changes.
"""

import re
import math
import json
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))
from schema import ContextPack
from graph import MemoryGraph

NOW = datetime.utcnow().isoformat() + "Z"


# ══════════════════════════════════════════════════════════
# TF-IDF scorer (no external deps)
# ══════════════════════════════════════════════════════════

def _tokenize(text: str) -> List[str]:
    return re.findall(r'\b[a-z0-9]{2,}\b', text.lower())


def _tfidf_score(query_tokens: List[str], doc_tokens: List[str], idf: Dict[str, float]) -> float:
    if not query_tokens or not doc_tokens:
        return 0.0
    doc_freq: Dict[str, int] = {}
    for t in doc_tokens:
        doc_freq[t] = doc_freq.get(t, 0) + 1
    score = 0.0
    for qt in set(query_tokens):
        tf = doc_freq.get(qt, 0) / max(len(doc_tokens), 1)
        idf_val = idf.get(qt, math.log(100))
        score += tf * idf_val
    return score


def _build_idf(docs: List[str]) -> Dict[str, float]:
    N = max(len(docs), 1)
    df: Dict[str, int] = {}
    for doc in docs:
        for t in set(_tokenize(doc)):
            df[t] = df.get(t, 0) + 1
    return {t: math.log(N / v) for t, v in df.items()}


# ══════════════════════════════════════════════════════════
# Retrieval Engine
# ══════════════════════════════════════════════════════════

class RetrievalEngine:

    def __init__(self, graph: MemoryGraph):
        self.graph = graph
        self._build_index()

    def _build_index(self):
        """Pre-compute IDF over all evidence excerpts."""
        self.all_evidence = self.graph.get_all_evidence()
        docs = [e.get("excerpt","") for e in self.all_evidence]
        self.idf = _build_idf(docs)
        # Index evidence by source_id for fast lookup
        self.evidence_by_source: Dict[str, List[Dict]] = {}
        for ev in self.all_evidence:
            sid = ev.get("source_id","")
            self.evidence_by_source.setdefault(sid, []).append(ev)

    def retrieve(self, query: str, top_k: int = 8) -> ContextPack:
        """
        Main retrieval entry point.
        Returns a ContextPack with grounded claims, evidence, and conflict detection.
        """
        query_tokens = _tokenize(query)

        # 1. Entity matching
        entity_hits = self._match_entities(query, query_tokens)

        # 2. Evidence scoring
        scored_evidence = self._score_evidence(query_tokens)

        # 3. Claim retrieval from matched entities + scored evidence
        claims = self._retrieve_claims(entity_hits, scored_evidence, top_k)

        # 4. Conflict detection
        conflicts = self._detect_conflicts(claims)

        # 5. Build context pack
        pack = self._build_context_pack(query, claims, scored_evidence[:top_k], entity_hits, conflicts)

        return pack

    def _match_entities(self, query: str, query_tokens: List[str]) -> List[Dict]:
        """Keyword match against entity canonical names and aliases."""
        hits = []

        # Exact/substring match first
        for ent in self.graph.get_all_entities():
            name = ent.get("canonical_name","").lower()
            aliases = " ".join(ent.get("aliases", [])).lower()
            full_text = f"{name} {aliases}"

            score = 0.0
            for qt in query_tokens:
                if qt in full_text:
                    score += 2.0 if qt in name else 1.0

            if score > 0:
                hits.append((score, ent))

        hits.sort(key=lambda x: -x[0])
        return [h[1] for h in hits[:6]]

    def _score_evidence(self, query_tokens: List[str]) -> List[Tuple[float, Dict]]:
        """TF-IDF score each evidence excerpt against query."""
        scored = []
        for ev in self.all_evidence:
            doc_tokens = _tokenize(ev.get("excerpt",""))
            score = _tfidf_score(query_tokens, doc_tokens, self.idf)
            if score > 0:
                scored.append((score, ev))
        scored.sort(key=lambda x: -x[0])
        return scored

    def _retrieve_claims(
        self,
        entity_hits: List[Dict],
        scored_evidence: List[Tuple[float, Dict]],
        top_k: int,
    ) -> List[Dict]:
        """Gather claims from matched entities + evidence-linked claims."""
        claim_scores: Dict[str, float] = {}
        claim_objects: Dict[str, Dict] = {}

        # Claims from entity neighbourhood
        for ent in entity_hits:
            eid = ent["entity_id"]
            for claim in self.graph.get_claims_for_entity(eid, current_only=False):
                cid = claim["claim_id"]
                claim_scores[cid] = claim_scores.get(cid, 0) + claim.get("confidence", 0.5) * 2
                claim_objects[cid] = claim

        # Claims linked to top evidence
        for score, ev in scored_evidence[:20]:
            evid = ev.get("evidence_id","")
            # Find claims using this evidence
            rows = self.graph.conn.execute("""
                SELECT c.json_data FROM claims c
                JOIN claim_evidence ce ON ce.claim_id = c.claim_id
                WHERE ce.evidence_id = ?
            """, (evid,)).fetchall()
            for row in rows:
                claim = json.loads(row["json_data"])
                cid = claim["claim_id"]
                claim_scores[cid] = claim_scores.get(cid, 0) + score
                claim_objects[cid] = claim

        # Sort by composite score, prefer current claims
        ranked = sorted(
            claim_objects.items(),
            key=lambda x: (
                -int(x[1].get("is_current", True)),  # current first
                -claim_scores.get(x[0], 0),
                -x[1].get("confidence", 0),
            )
        )[:top_k]

        return [c for _, c in ranked]

    def _detect_conflicts(self, claims: List[Dict]) -> List[Dict]:
        """Find pairs of conflicting/superseded claims in the result set."""
        conflicts = []
        superseded_ids = {c.get("superseded_by_claim_id") for c in claims if c.get("superseded_by_claim_id")}
        superseding_ids = {c.get("supersedes_claim_id") for c in claims if c.get("supersedes_claim_id")}

        for c in claims:
            if c["claim_id"] in superseded_ids or c.get("superseded_by_claim_id"):
                conflicts.append({
                    "claim_id": c["claim_id"],
                    "predicate": c.get("predicate",""),
                    "valid_from": c.get("valid_from",""),
                    "valid_until": c.get("valid_until",""),
                    "is_current": c.get("is_current", True),
                    "superseded_by": c.get("superseded_by_claim_id",""),
                    "supersedes": c.get("supersedes_claim_id",""),
                    "conflict_note": "This claim was later reversed/superseded" if not c.get("is_current") else "This claim reverses an earlier decision",
                })

        return conflicts

    def _build_context_pack(
        self,
        query: str,
        claims: List[Dict],
        top_evidence: List[Tuple[float, Dict]],
        entity_hits: List[Dict],
        conflicts: List[Dict],
    ) -> ContextPack:
        # Assemble evidence snippets with grounding
        evidence_snippets = []
        seen_ev = set()
        for score, ev in top_evidence:
            evid = ev.get("evidence_id","")
            if evid in seen_ev:
                continue
            seen_ev.add(evid)
            evidence_snippets.append({
                "evidence_id": evid,
                "source_id": ev.get("source_id",""),
                "source_type": ev.get("source_type",""),
                "timestamp": ev.get("timestamp",""),
                "excerpt": ev.get("excerpt","")[:350],
                "relevance_score": round(score, 4),
                "citation": f"[{ev.get('source_type','').upper()} {ev.get('source_id','')} @ {ev.get('timestamp','')[:10]}]",
            })

        # Add evidence from claims not already covered
        for claim in claims:
            for evid in claim.get("evidence_ids",[])[:2]:
                if evid not in seen_ev:
                    ev_data = self.graph.conn.execute(
                        "SELECT json_data FROM evidence WHERE evidence_id=?", (evid,)
                    ).fetchone()
                    if ev_data:
                        ev = json.loads(ev_data["json_data"])
                        seen_ev.add(evid)
                        evidence_snippets.append({
                            "evidence_id": evid,
                            "source_id": ev.get("source_id",""),
                            "source_type": ev.get("source_type",""),
                            "timestamp": ev.get("timestamp",""),
                            "excerpt": ev.get("excerpt","")[:350],
                            "relevance_score": 0.0,
                            "citation": f"[{ev.get('source_type','').upper()} {ev.get('source_id','')} @ {ev.get('timestamp','')[:10]}]",
                        })

        # Answer hint: concatenate top claim predicates
        hints = [c.get("predicate","") for c in claims[:3] if c.get("is_current")]
        answer_hint = " | ".join(hints) if hints else "No current claims found."

        return ContextPack(
            query=query,
            retrieved_at=NOW,
            claims=[{
                "claim_id": c["claim_id"],
                "claim_type": c["claim_type"],
                "predicate": c["predicate"],
                "value": c.get("value",""),
                "confidence": c.get("confidence",1.0),
                "is_current": c.get("is_current", True),
                "valid_from": c.get("valid_from",""),
                "valid_until": c.get("valid_until",""),
                "evidence_ids": c.get("evidence_ids",[]),
                "tags": c.get("tags",[]),
            } for c in claims],
            evidence_snippets=evidence_snippets[:10],
            entities=[{
                "entity_id": e["entity_id"],
                "entity_type": e["entity_type"],
                "canonical_name": e["canonical_name"],
                "aliases": e.get("aliases",[])[:3],
            } for e in entity_hits[:5]],
            conflicts_detected=conflicts,
            answer_hint=answer_hint,
        )


def run_example_queries(engine: RetrievalEngine) -> List[Dict]:
    """Run the standard set of example queries and return context packs."""
    queries = [
        "What database was chosen for NovexCore v2?",
        "Who is responsible for the auth token security incident?",
        "What is the status of the Kubernetes migration?",
        "What decisions were reversed or changed?",
        "What performance metrics were reported for Kafka?",
        "Who owns the DataPipeline project?",
        "What happened with Python version upgrades?",
    ]

    results = []
    for q in queries:
        print(f"  Q: {q[:60]}...")
        pack = engine.retrieve(q)
        results.append({
            "query": q,
            "context_pack": pack.to_dict(),
        })
    return results
