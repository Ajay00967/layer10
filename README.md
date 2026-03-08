# Layer10 Take-Home — Grounded Long-Term Memory Graph

## Quick Start (< 5 minutes)

```bash
# 1. Unzip and enter directory
unzip layer10.zip && cd layer10_build

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set your Anthropic API key
export ANTHROPIC_API_KEY=sk-ant-api03-...   # get one at console.anthropic.com

# 4. Run the full pipeline  (~2–4 min depending on API speed)
python3 pipeline.py

# 5. Open the visualization
open viz/index.html       # macOS
xdg-open viz/index.html   # Linux
# Or: just double-click viz/index.html in your file manager
```

That's it. All outputs are written to `outputs/`.

> **Note**: The extraction stage now calls the real Claude API (`claude-sonnet-4-20250514`)
> with structured output prompts. It makes ~2 API calls per artifact (entity pass + claim pass).
> With 51 artifacts in the corpus, expect ~100 API calls and ~$0.10–0.30 in API cost.
> All other stages (dedup, graph, retrieval, viz) are offline.

---

## Directory Structure

```
layer10/
├── pipeline.py                    # Main orchestrator — run this
├── WRITEUP.md                     # Full write-up with design decisions
├── README.md                      # This file
│
├── src/
│   ├── corpus_generator.py        # Synthetic corpus (Enron-mirror structure)
│   ├── schema.py                  # Ontology: Entity, Claim, Evidence, Artifact
│   ├── extraction.py              # Extraction pipeline (artifact → entities → claims)
│   ├── dedup.py                   # Dedup: artifact / entity / claim levels
│   ├── graph.py                   # Memory graph (SQLite + NetworkX)
│   └── retrieval.py               # Retrieval engine (TF-IDF + context packs)
│
├── viz/
│   └── index.html                 # Standalone visualization (no server needed)
│
├── data/
│   ├── raw/corpus.json            # Generated after pipeline.py
│   └── processed/
│       ├── extracted.json         # Post-extraction
│       └── deduped.json           # Post-deduplication
│
└── outputs/
    ├── graph/
    │   ├── memory.db              # SQLite memory graph
    │   ├── graph.json             # Graph export for visualization
    │   ├── viz_bundle.json        # Full visualization data bundle
    │   └── stats.json             # System statistics
    └── context_packs/
        ├── all_queries.json       # All 7 example query results
        ├── query_01.json          # "What database was chosen for NovexCore v2?"
        ├── query_02.json          # "Who is responsible for the auth token incident?"
        ├── query_03.json          # "What is the status of the Kubernetes migration?"
        ├── query_04.json          # "What decisions were reversed or changed?"
        ├── query_05.json          # "What performance metrics were reported for Kafka?"
        ├── query_06.json          # "Who owns the DataPipeline project?"
        └── query_07.json          # "What happened with Python version upgrades?"
```

---

## Requirements

Python 3.10+ with standard library plus:

| Package | Version | Use |
|---------|---------|-----|
| networkx | 3.x | Graph traversal, ego-graph expansion |
| numpy | 2.x | Numeric operations |
| pandas | 3.x | Data inspection (optional) |
| scipy | 1.x | Available but not required at runtime |

All available in a standard Python install. No pip installs needed if you have Anaconda or a typical data science environment.

```bash
# If any package is missing:
pip install networkx numpy pandas scipy --break-system-packages
```

No API keys required. No network access required. Everything runs offline.

---

## Pipeline Steps (what pipeline.py does)

```
[1/5] Generate corpus      → data/raw/corpus.json
[2/5] Extract              → data/processed/extracted.json
[3/5] Deduplicate          → data/processed/deduped.json
[4/5] Build memory graph   → outputs/graph/memory.db + graph.json
[5/5] Run example queries  → outputs/context_packs/
```

Expected output:
```
Emails: 43 | Issues: 8
Entities: 23 | Claims: 52 | Evidence: 75
Graph nodes: 37 | Edges: 27
Claims current: 31 | Decision reversals: 2 | Redacted artifacts: 1
```

---

## Visualization

`viz/index.html` is **fully standalone** — open it directly in any browser, no server needed.

### Four Views

**Graph View**
- Force-directed entity graph
- Node colours: blue=person, green=project, purple=technology, orange=issue
- Solid edges = current claims; dashed red = historical/reversed
- Click any node → inspect claims and evidence in right panel
- Click any claim card → expand supporting evidence with source citations
- Use type filter chips to show/hide entity categories
- Mouse wheel to zoom, drag to pan

**Retrieval View**
- Type any natural language question or click a preset query
- Returns: answer summary, ranked claims with confidence bars, grounded evidence snippets with citations, conflict/reversal panel

**Dedup Audit View**
- Full audit trail of all dedup decisions
- Artifact deduplication (hash + quoting detection)
- Entity canonicalization (alias resolution)
- Claim reversals (PostgreSQL → MySQL decision chain)

**Stats View**
- System statistics dashboard
- Complete decision timeline (all DECISION_MADE / DECISION_REVERSED claims)

---

## Example Context Pack (Query 1)

**Query**: "What database was chosen for NovexCore v2?"

**Answer hint** (from context pack):
```
DECISION_REVERSED: After cost review, we are switching to MySQL.
  Evidence: [EMAIL EMAIL-T001-06 @ 2024-01-23]

DECISION_MADE (HISTORICAL): Decision made: we go with PostgreSQL for NovexCore v2.
  Evidence: [EMAIL EMAIL-T001-04 @ 2024-01-16]
  valid_until: 2024-01-23 (superseded by MySQL decision)
```

This demonstrates:
1. Grounded claims — every statement has a source ID, excerpt, and timestamp
2. Conflict detection — original PostgreSQL decision is surfaced alongside the reversal
3. Bi-temporal correctness — PostgreSQL claim is historical, MySQL is current
4. Reversal linking — `supersedes_claim_id` and `superseded_by_claim_id` chain the two

---

## Adapting to Real Enron Corpus

```python
# In pipeline.py, replace:
from corpus_generator import generate_corpus
corpus = generate_corpus()

# With:
from enron_loader import load_enron
corpus = load_enron("/path/to/enron_mail_20150507/")
```

`enron_loader.py` (stub provided in comments in `corpus_generator.py`) normalises the Enron maildir format into the same JSON schema (`emails[]` + `issues[]`). The extraction, dedup, graph, and retrieval layers are unchanged.

---

## Running Individual Components

```bash
# Just generate the corpus
python3 -c "from src.corpus_generator import generate_corpus; import json; print(json.dumps(generate_corpus()['meta'], indent=2))"

# Just run extraction
python3 src/extraction.py

# Inspect the graph via SQLite
sqlite3 outputs/graph/memory.db "SELECT claim_type, COUNT(*) FROM claims GROUP BY claim_type;"

# Query the retrieval engine interactively
python3 -c "
import sys; sys.path.insert(0,'src')
from graph import MemoryGraph
from retrieval import RetrievalEngine
g = MemoryGraph('outputs/graph/memory.db')
engine = RetrievalEngine(g)
pack = engine.retrieve('What happened with the auth token vulnerability?')
print(pack.answer_hint)
for ev in pack.evidence_snippets[:3]:
    print(ev['citation'], ev['excerpt'][:100])
"
```
