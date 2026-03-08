"""
graph.py
--------
Memory Graph — SQLite-backed store + NetworkX graph for traversal.

Schema
======
  nodes (entity_id, entity_type, canonical_name, json_data)
  edges (edge_id, claim_id, src_id, dst_id, claim_type, valid_from, valid_until, is_current)
  artifacts (artifact_id, artifact_type, source_id, content_hash, timestamp, json_data)
  evidence (evidence_id, source_id, source_type, timestamp, excerpt, json_data)
  claims (claim_id, claim_type, subject_id, object_id, predicate, value, confidence,
          valid_from, valid_until, is_current, json_data)

Design decisions:
  - SQLite for queryable persistent store (trivially replaceable with Postgres/Neo4j)
  - NetworkX DiGraph for in-memory traversal and neighbourhood expansion
  - Idempotent upserts: INSERT OR REPLACE everywhere
  - Soft deletes: is_deleted column, never hard-deletes
  - Bi-temporal: event_time (valid_from/valid_until) vs ingestion_time (stored_at)
"""

import sqlite3
import json
import networkx as nx
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

NOW = datetime.utcnow().isoformat() + "Z"

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS nodes (
    entity_id      TEXT PRIMARY KEY,
    entity_type    TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    aliases        TEXT,          -- JSON array
    is_deleted     INTEGER DEFAULT 0,
    stored_at      TEXT,
    json_data      TEXT           -- full serialized entity
);

CREATE TABLE IF NOT EXISTS claims (
    claim_id         TEXT PRIMARY KEY,
    claim_type       TEXT NOT NULL,
    subject_id       TEXT NOT NULL,
    object_id        TEXT,
    predicate        TEXT NOT NULL,
    value            TEXT,
    confidence       REAL DEFAULT 1.0,
    valid_from       TEXT,
    valid_until      TEXT,
    is_current       INTEGER DEFAULT 1,
    supersedes_id    TEXT,
    superseded_by_id TEXT,
    stored_at        TEXT,
    json_data        TEXT
);

CREATE TABLE IF NOT EXISTS evidence (
    evidence_id  TEXT PRIMARY KEY,
    source_id    TEXT NOT NULL,
    source_type  TEXT NOT NULL,
    timestamp    TEXT,
    excerpt      TEXT,
    is_redacted  INTEGER DEFAULT 0,
    stored_at    TEXT,
    json_data    TEXT
);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id    TEXT PRIMARY KEY,
    artifact_type  TEXT NOT NULL,
    source_id      TEXT NOT NULL,
    content_hash   TEXT,
    timestamp      TEXT,
    thread_id      TEXT,
    subject        TEXT,
    is_redacted    INTEGER DEFAULT 0,
    is_deleted     INTEGER DEFAULT 0,
    dedup_id       TEXT,          -- canonical artifact_id if this is a dup
    stored_at      TEXT,
    json_data      TEXT
);

CREATE TABLE IF NOT EXISTS claim_evidence (
    claim_id    TEXT NOT NULL,
    evidence_id TEXT NOT NULL,
    PRIMARY KEY (claim_id, evidence_id)
);

CREATE TABLE IF NOT EXISTS dedup_audit (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type  TEXT,
    merged_id   TEXT,
    canonical_id TEXT,
    reason      TEXT,
    merged_at   TEXT
);

CREATE INDEX IF NOT EXISTS idx_claims_subject ON claims(subject_id);
CREATE INDEX IF NOT EXISTS idx_claims_type    ON claims(claim_type);
CREATE INDEX IF NOT EXISTS idx_claims_current ON claims(is_current);
CREATE INDEX IF NOT EXISTS idx_artifacts_hash ON artifacts(content_hash);
CREATE INDEX IF NOT EXISTS idx_evidence_source ON evidence(source_id);
"""


class MemoryGraph:
    """
    The central memory store.

    Public API:
      ingest(deduped_data)         — loads a full dedup output into the graph
      get_entity(entity_id)        — fetch entity by ID
      get_claims_for_entity(eid)   — all current claims about an entity
      get_evidence(claim_id)       — all evidence for a claim
      search_entities(query)       — keyword search over canonical_name + aliases
      neighbourhood(eid, depth)    — NetworkX neighbourhood expansion
      get_history(eid)             — all historical claims (including superseded)
      get_conflicts()              — claims with decision reversals
      query_current_state(eid)     — snapshot of current facts about entity
    """

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()
        self.graph = nx.DiGraph()

    def _init_schema(self):
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()

    # ─────────────────── Ingestion ───────────────────

    def ingest(self, data: Dict[str, Any]):
        """Idempotent ingest of full dedup pipeline output."""
        print("  Ingesting entities...")
        for eid, ent in data["entities"].items():
            self._upsert_entity(ent)

        print("  Ingesting evidence...")
        for evid, ev in data["evidence_index"].items():
            self._upsert_evidence(ev)

        print("  Ingesting artifacts...")
        for aid, art in data["artifacts"].items():
            self._upsert_artifact(art)

        print("  Ingesting claims...")
        for c in data["claims"]:
            self._upsert_claim(c)

        print("  Ingesting dedup audit log...")
        if "dedup_audit" in data:
            self._ingest_dedup_audit(data["dedup_audit"])

        print("  Building in-memory NetworkX graph...")
        self._build_nx_graph()

        self.conn.commit()

    def _upsert_entity(self, ent: Dict):
        self.conn.execute("""
            INSERT OR REPLACE INTO nodes
            (entity_id, entity_type, canonical_name, aliases, is_deleted, stored_at, json_data)
            VALUES (?,?,?,?,?,?,?)
        """, (
            ent["entity_id"], ent["entity_type"], ent["canonical_name"],
            json.dumps(ent.get("aliases", [])),
            int(ent.get("deleted", False)),
            NOW,
            json.dumps(ent),
        ))

    def _upsert_evidence(self, ev: Dict):
        self.conn.execute("""
            INSERT OR REPLACE INTO evidence
            (evidence_id, source_id, source_type, timestamp, excerpt, is_redacted, stored_at, json_data)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            ev["evidence_id"], ev["source_id"], ev["source_type"],
            ev.get("timestamp",""), ev.get("excerpt",""),
            int(ev.get("redacted", False)),
            NOW, json.dumps(ev),
        ))

    def _upsert_artifact(self, art: Dict):
        self.conn.execute("""
            INSERT OR REPLACE INTO artifacts
            (artifact_id, artifact_type, source_id, content_hash, timestamp,
             thread_id, subject, is_redacted, is_deleted, dedup_id, stored_at, json_data)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            art["artifact_id"], art["artifact_type"], art["source_id"],
            art.get("content_hash",""), art.get("timestamp",""),
            art.get("thread_id",""), art.get("subject",""),
            int(art.get("is_redacted", False)), int(art.get("is_deleted", False)),
            art.get("dedup_canonical_id",""),
            NOW, json.dumps(art),
        ))

    def _upsert_claim(self, c: Dict):
        self.conn.execute("""
            INSERT OR REPLACE INTO claims
            (claim_id, claim_type, subject_id, object_id, predicate, value,
             confidence, valid_from, valid_until, is_current,
             supersedes_id, superseded_by_id, stored_at, json_data)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            c["claim_id"], c["claim_type"],
            c.get("subject_entity_id",""), c.get("object_entity_id",""),
            c.get("predicate",""), c.get("value",""),
            c.get("confidence", 1.0),
            c.get("valid_from",""), c.get("valid_until",""),
            int(c.get("is_current", True)),
            c.get("supersedes_claim_id",""), c.get("superseded_by_claim_id",""),
            NOW, json.dumps(c),
        ))
        for evid in c.get("evidence_ids", []):
            self.conn.execute("""
                INSERT OR IGNORE INTO claim_evidence (claim_id, evidence_id) VALUES (?,?)
            """, (c["claim_id"], evid))

    def _ingest_dedup_audit(self, audit: Dict):
        for entry in audit.get("artifact_audit_log", []) + audit.get("entity_audit_log", []) + audit.get("claim_audit_log", []):
            self.conn.execute("""
                INSERT INTO dedup_audit (event_type, merged_id, canonical_id, reason, merged_at)
                VALUES (?,?,?,?,?)
            """, (
                entry.get("type",""),
                entry.get("duplicate", entry.get("merged","")),
                entry.get("canonical",""),
                entry.get("reason",""),
                entry.get("merged_at", NOW),
            ))

    def _build_nx_graph(self):
        """Build NetworkX DiGraph for traversal and path queries."""
        self.graph.clear()

        # Add entity nodes
        for row in self.conn.execute("SELECT entity_id, entity_type, canonical_name FROM nodes WHERE is_deleted=0"):
            self.graph.add_node(
                row["entity_id"],
                entity_type=row["entity_type"],
                label=row["canonical_name"],
            )

        # Add claim edges
        for row in self.conn.execute("""
            SELECT claim_id, claim_type, subject_id, object_id, predicate, confidence, is_current
            FROM claims WHERE object_id != '' AND object_id IS NOT NULL
        """):
            src = row["subject_id"]
            dst = row["object_id"]
            if not self.graph.has_node(src):
                self.graph.add_node(src, entity_type="unknown", label=src)
            if not self.graph.has_node(dst):
                self.graph.add_node(dst, entity_type="unknown", label=dst)
            self.graph.add_edge(
                src, dst,
                claim_id=row["claim_id"],
                claim_type=row["claim_type"],
                predicate=row["predicate"],
                confidence=row["confidence"],
                is_current=bool(row["is_current"]),
            )

    # ─────────────────── Query API ───────────────────

    def get_entity(self, entity_id: str) -> Optional[Dict]:
        row = self.conn.execute(
            "SELECT json_data FROM nodes WHERE entity_id=?", (entity_id,)
        ).fetchone()
        return json.loads(row["json_data"]) if row else None

    def search_entities(self, query: str, limit: int = 10) -> List[Dict]:
        q = f"%{query.lower()}%"
        rows = self.conn.execute("""
            SELECT json_data FROM nodes
            WHERE (lower(canonical_name) LIKE ? OR lower(aliases) LIKE ?)
              AND is_deleted=0
            LIMIT ?
        """, (q, q, limit)).fetchall()
        return [json.loads(r["json_data"]) for r in rows]

    def get_claims_for_entity(self, entity_id: str, current_only: bool = True) -> List[Dict]:
        sql = """
            SELECT json_data FROM claims
            WHERE (subject_id=? OR object_id=?)
        """
        params = [entity_id, entity_id]
        if current_only:
            sql += " AND is_current=1"
        sql += " ORDER BY confidence DESC"
        rows = self.conn.execute(sql, params).fetchall()
        return [json.loads(r["json_data"]) for r in rows]

    def get_evidence_for_claim(self, claim_id: str) -> List[Dict]:
        rows = self.conn.execute("""
            SELECT e.json_data
            FROM evidence e
            JOIN claim_evidence ce ON ce.evidence_id = e.evidence_id
            WHERE ce.claim_id = ?
              AND e.is_redacted = 0
        """, (claim_id,)).fetchall()
        return [json.loads(r["json_data"]) for r in rows]

    def get_all_evidence(self) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT json_data FROM evidence WHERE is_redacted=0"
        ).fetchall()
        return [json.loads(r["json_data"]) for r in rows]

    def get_history(self, entity_id: str) -> List[Dict]:
        """All claims (current + historical) for an entity, ordered by valid_from."""
        rows = self.conn.execute("""
            SELECT json_data FROM claims
            WHERE (subject_id=? OR object_id=?)
            ORDER BY valid_from ASC
        """, (entity_id, entity_id)).fetchall()
        return [json.loads(r["json_data"]) for r in rows]

    def get_conflicts(self) -> List[Dict]:
        """Claims involved in decision reversals."""
        rows = self.conn.execute("""
            SELECT json_data FROM claims
            WHERE claim_type IN ('DECISION_REVERSED', 'DECISION_MADE')
              AND (supersedes_id != '' OR superseded_by_id != '')
            ORDER BY valid_from DESC
        """).fetchall()
        return [json.loads(r["json_data"]) for r in rows]

    def get_all_claims(self, current_only: bool = False) -> List[Dict]:
        sql = "SELECT json_data FROM claims"
        if current_only:
            sql += " WHERE is_current=1"
        sql += " ORDER BY confidence DESC"
        rows = self.conn.execute(sql).fetchall()
        return [json.loads(r["json_data"]) for r in rows]

    def get_all_entities(self) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT json_data FROM nodes WHERE is_deleted=0"
        ).fetchall()
        return [json.loads(r["json_data"]) for r in rows]

    def get_all_artifacts(self) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT json_data FROM artifacts"
        ).fetchall()
        return [json.loads(r["json_data"]) for r in rows]

    def neighbourhood(self, entity_id: str, depth: int = 2) -> Tuple[List[str], List[Dict]]:
        """Return nodes and edges within `depth` hops of entity_id."""
        if entity_id not in self.graph:
            return [], []
        nodes = set()
        try:
            for node in nx.ego_graph(self.graph, entity_id, radius=depth).nodes():
                nodes.add(node)
        except Exception:
            nodes.add(entity_id)
        edges = []
        for src, dst, data in self.graph.edges(data=True):
            if src in nodes and dst in nodes:
                edges.append({"src": src, "dst": dst, **data})
        return list(nodes), edges

    def query_current_state(self, entity_id: str) -> Dict:
        """Snapshot of current facts about an entity."""
        entity = self.get_entity(entity_id)
        claims = self.get_claims_for_entity(entity_id, current_only=True)
        history = self.get_history(entity_id)
        neighbours, edges = self.neighbourhood(entity_id, depth=1)

        ev_all = []
        for c in claims[:5]:
            ev_all.extend(self.get_evidence_for_claim(c["claim_id"]))

        return {
            "entity": entity,
            "current_claims": claims,
            "history_count": len(history),
            "neighbours": neighbours,
            "edges": edges,
            "sample_evidence": ev_all[:5],
        }

    def get_dedup_audit(self) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT * FROM dedup_audit ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]

    def stats(self) -> Dict:
        def count(table, where=""):
            sql = f"SELECT COUNT(*) as n FROM {table}"
            if where:
                sql += f" WHERE {where}"
            return self.conn.execute(sql).fetchone()["n"]

        return {
            "entities": count("nodes", "is_deleted=0"),
            "claims_total": count("claims"),
            "claims_current": count("claims", "is_current=1"),
            "evidence": count("evidence"),
            "artifacts": count("artifacts"),
            "artifacts_redacted": count("artifacts", "is_redacted=1"),
            "artifacts_deduplicated": count("artifacts", "dedup_id != ''"),
            "decision_reversals": count("claims", "claim_type='DECISION_REVERSED'"),
            "graph_nodes": self.graph.number_of_nodes(),
            "graph_edges": self.graph.number_of_edges(),
        }

    def close(self):
        self.conn.close()

    def export_graph_json(self) -> Dict:
        """Export full graph for visualization."""
        nodes = []
        for row in self.conn.execute("SELECT entity_id, entity_type, canonical_name FROM nodes WHERE is_deleted=0"):
            nodes.append({
                "id": row["entity_id"],
                "type": row["entity_type"],
                "label": row["canonical_name"],
            })

        # Add issue nodes from claims
        issue_ids = set()
        for row in self.conn.execute("SELECT DISTINCT subject_id FROM claims WHERE subject_id LIKE 'ISSUE-%'"):
            issue_ids.add(row["subject_id"])
        for iid in issue_ids:
            # get title from claims
            row2 = self.conn.execute(
                "SELECT predicate FROM claims WHERE subject_id=? LIMIT 1", (iid,)
            ).fetchone()
            label = row2["predicate"][:40] if row2 else iid
            nodes.append({"id": iid, "type": "issue", "label": label})

        edges = []
        for row in self.conn.execute("""
            SELECT claim_id, claim_type, subject_id, object_id, predicate, confidence, is_current
            FROM claims WHERE object_id != '' AND object_id IS NOT NULL
        """):
            edges.append({
                "id": row["claim_id"],
                "source": row["subject_id"],
                "target": row["object_id"],
                "claim_type": row["claim_type"],
                "label": row["predicate"][:60],
                "confidence": row["confidence"],
                "is_current": bool(row["is_current"]),
            })

        return {"nodes": nodes, "edges": edges}
