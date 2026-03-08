"""
pipeline.py
-----------
End-to-end pipeline orchestrator.

Usage:
  python pipeline.py

Steps:
  1. Generate corpus
  2. Extract entities, artifacts, claims
  3. Deduplicate and canonicalize
  4. Build and persist memory graph (SQLite)
  5. Run example retrieval queries
  6. Persist all outputs
"""

import json
import sys
from datetime import datetime
from pathlib import Path

# Make src importable
sys.path.insert(0, str(Path(__file__).parent / "src"))

from corpus_generator import generate_corpus
from extraction import ExtractionPipeline
from dedup import DeduplicationPipeline
from graph import MemoryGraph
from retrieval import RetrievalEngine, run_example_queries

OUT = Path("outputs")
DATA = Path("data")

def run():
    print("=" * 60)
    print("  Layer10 Memory Graph Pipeline")
    print("=" * 60)

    # ── Step 1: Generate corpus ──
    print("\n[1/5] Generating corpus...")
    corpus = generate_corpus()
    (DATA / "raw").mkdir(parents=True, exist_ok=True)
    with open(DATA / "raw" / "corpus.json", "w") as f:
        json.dump(corpus, f, indent=2)
    print(f"      Emails: {corpus['meta']['email_count']} | Issues: {corpus['meta']['issue_count']}")

    # ── Step 2: Extract ──
    print("\n[2/5] Running extraction pipeline...")
    ext_pipeline = ExtractionPipeline(corpus)
    extracted = ext_pipeline.run()
    (DATA / "processed").mkdir(parents=True, exist_ok=True)
    with open(DATA / "processed" / "extracted.json", "w") as f:
        json.dump(extracted, f, indent=2)
    print(f"      Entities: {extracted['meta']['entity_count']} | "
          f"Claims: {extracted['meta']['claim_count']} | "
          f"Evidence: {extracted['meta']['evidence_count']}")

    # ── Step 3: Deduplicate ──
    print("\n[3/5] Running deduplication pipeline...")
    dedup_pipeline = DeduplicationPipeline(extracted)
    deduped = dedup_pipeline.run()
    with open(DATA / "processed" / "deduped.json", "w") as f:
        json.dump(deduped, f, indent=2)
    audit = deduped["dedup_audit"]["summary"]
    print(f"      Artifact dups removed: {audit['artifact_duplicates_removed']} | "
          f"Entity merges: {audit['entity_merges']} | "
          f"Claim merges: {audit['claim_merges']} | "
          f"Reversal links: {audit['reversal_links']}")

    # ── Step 4: Build memory graph ──
    print("\n[4/5] Building memory graph...")
    db_path = str(OUT / "graph" / "memory.db")
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    graph = MemoryGraph(db_path=db_path)
    graph.ingest(deduped)
    stats = graph.stats()
    print(f"      Graph nodes: {stats['graph_nodes']} | Edges: {stats['graph_edges']}")
    print(f"      Entities: {stats['entities']} | Claims: {stats['claims_total']} "
          f"(current: {stats['claims_current']}) | Evidence: {stats['evidence']}")
    print(f"      Decision reversals: {stats['decision_reversals']} | "
          f"Redacted artifacts: {stats['artifacts_redacted']}")

    # Export graph JSON for visualization
    graph_json = graph.export_graph_json()
    with open(OUT / "graph" / "graph.json", "w") as f:
        json.dump(graph_json, f, indent=2)
    with open(OUT / "graph" / "stats.json", "w") as f:
        json.dump(stats, f, indent=2)

    # ── Step 5: Retrieval ──
    print("\n[5/5] Running example retrieval queries...")
    engine = RetrievalEngine(graph)
    context_packs = run_example_queries(engine)
    (OUT / "context_packs").mkdir(parents=True, exist_ok=True)
    with open(OUT / "context_packs" / "all_queries.json", "w") as f:
        json.dump(context_packs, f, indent=2)
    # Individual files
    for i, result in enumerate(context_packs):
        fname = f"query_{i+1:02d}.json"
        with open(OUT / "context_packs" / fname, "w") as f:
            json.dump(result, f, indent=2)
    print(f"      Saved {len(context_packs)} context packs")

    graph.close()

    print("\n" + "=" * 60)
    print("  Pipeline complete!")
    print(f"  Graph DB:       outputs/graph/memory.db")
    print(f"  Graph JSON:     outputs/graph/graph.json")
    print(f"  Context packs:  outputs/context_packs/")
    print("=" * 60)

    return stats, context_packs

if __name__ == "__main__":
    run()
