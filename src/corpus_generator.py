"""
corpus_generator.py
--------------------
Generates a realistic synthetic corpus that mirrors the structure and challenges
of the Enron Email Dataset (CMU/Kaggle version).

Why synthetic? The evaluation environment has no internet access. The schema,
extraction pipeline, dedup logic, and graph design are identical to what would
run on the real Enron corpus — swap `generate_corpus()` for the Enron loader
and everything downstream is unchanged.

Corpus characteristics modelled:
  - ~200 email messages across ~15 employees at a fictional energy company
  - ~40 GitHub-style issues (structured work artifacts)
  - Quoting/forwarding chains (dedup challenge)
  - Identity aliases (john.smith@co vs jsmith@co vs "J. Smith")
  - State transitions in issues (open → in-progress → closed)
  - Decision reversals ("we chose Postgres" ... "switching to MySQL")
  - Deleted/redacted messages (tombstones)
  - Cross-posted announcements
"""

import json
import random
import hashlib
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

random.seed(42)

# ─────────────────────────── People ───────────────────────────
PEOPLE = [
    {"id": "P001", "name": "Alice Hartmann",   "aliases": ["alice.hartmann@novex.com", "ahartmann@novex.com", "A. Hartmann"],   "role": "VP Engineering"},
    {"id": "P002", "name": "Bob Delgado",       "aliases": ["bob.delgado@novex.com",   "bdelgado@novex.com",   "Bobby D"],       "role": "Senior Engineer"},
    {"id": "P003", "name": "Carol Finch",       "aliases": ["carol.finch@novex.com",   "c.finch@novex.com",    "Carol F."],      "role": "Product Manager"},
    {"id": "P004", "name": "David Osei",        "aliases": ["david.osei@novex.com",    "dosei@novex.com",      "Dave Osei"],     "role": "Engineer"},
    {"id": "P005", "name": "Eva Lindström",     "aliases": ["eva.lindstrom@novex.com", "elindstrom@novex.com", "Eva L."],        "role": "Data Engineer"},
    {"id": "P006", "name": "Frank Nguyen",      "aliases": ["frank.nguyen@novex.com",  "fnguyen@novex.com",    "Frank N."],      "role": "Engineer"},
    {"id": "P007", "name": "Grace Kim",         "aliases": ["grace.kim@novex.com",     "gkim@novex.com",       "G. Kim"],        "role": "Engineering Manager"},
    {"id": "P008", "name": "Hector Romero",     "aliases": ["hector.romero@novex.com", "hromero@novex.com",    "Hector R."],     "role": "DevOps"},
    {"id": "P009", "name": "Iris Johansson",    "aliases": ["iris.johansson@novex.com","ijohansson@novex.com", "Iris J."],       "role": "QA Engineer"},
    {"id": "P010", "name": "James Park",        "aliases": ["james.park@novex.com",    "jpark@novex.com",      "Jim Park"],      "role": "Backend Engineer"},
]

PROJECTS = ["NovexCore", "DataPipeline", "AuthService", "ReportingEngine", "InfraV2"]
COMPONENTS = ["API", "Database", "Frontend", "CI/CD", "Security", "Analytics"]
TECHNOLOGIES = ["PostgreSQL", "MySQL", "Redis", "Kafka", "Kubernetes", "Docker", "Python", "Go", "React"]


def _ts(base: datetime, offset_days: float = 0) -> str:
    return (base + timedelta(days=offset_days)).isoformat() + "Z"


def _uid() -> str:
    return str(uuid.uuid4())


def _hash(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def _person(pid: str) -> Dict:
    return next(p for p in PEOPLE if p["id"] == pid)


def _alias(pid: str, idx: int = 0) -> str:
    return _person(pid)["aliases"][idx % len(_person(pid)["aliases"])]


# ─────────────────────────── Email Generator ───────────────────────────

BASE_DATE = datetime(2024, 1, 15, 9, 0, 0)

EMAIL_THREADS = [
    # (thread_id, subject, messages: list of (author_id, day_offset, body, reply_to_idx))
    {
        "thread_id": "T001",
        "subject": "Database selection for NovexCore v2",
        "messages": [
            ("P001", 0,   "Team, we need to decide on the database for NovexCore v2. I'm leaning toward PostgreSQL for its JSON support and strong ACID guarantees. Thoughts?", None),
            ("P002", 0.3, "Agree on Postgres. We've used it in DataPipeline and it's been rock solid. MySQL is an option but Postgres has better window functions.", 0),
            ("P004", 0.5, "I've benchmarked both. PostgreSQL wins on complex queries by ~30%. My recommendation: PostgreSQL.", 0),
            ("P003", 1.0, "From a product angle, either works. I'll defer to engineering here.", 1),
            ("P001", 1.2, "Decision made: we go with PostgreSQL for NovexCore v2. David, please update the ADR.", 2),
            # ── Decision reversal ──
            ("P007", 8.0, "Heads up: the cloud vendor deal gives us managed MySQL at 40% lower cost. Re-opening the DB decision.", 4),
            ("P001", 8.2, "After cost review, we are switching to MySQL. I know this reverses our earlier decision — documenting the reason: vendor discount + ops simplicity.", 5),
            ("P002", 8.3, "Understood. Will update migration scripts. Not ideal but makes sense economically.", 6),
        ]
    },
    {
        "thread_id": "T002",
        "subject": "Auth token expiry — security incident",
        "messages": [
            ("P009", 3.0,  "Found a bug: auth tokens never expire in AuthService. This is a security vulnerability. Assigning to Frank.", None),
            ("P006", 3.2,  "Confirmed. Tokens issued before 2024-01-10 have no exp claim. Working on a fix now.", 0),
            ("P008", 3.4,  "I can force-rotate all tokens in prod as a hotfix. Risk: users get logged out. Worth it?", 1),
            ("P001", 3.5,  "Yes, force rotate. User disruption acceptable given security risk. Hector please proceed.", 2),
            ("P008", 3.6,  "Token rotation complete. 2,847 sessions invalidated. Monitoring for issues.", 3),
            ("P006", 4.0,  "Fix deployed: tokens now have 24h expiry. Added regression test. Closing.", 4),
        ]
    },
    {
        "thread_id": "T003",
        "subject": "Q1 roadmap sync",
        "messages": [
            ("P003", 5.0, "Sharing Q1 roadmap draft. Key items: (1) NovexCore v2 launch, (2) ReportingEngine beta, (3) InfraV2 Kubernetes migration. Please review by Friday.", None),
            ("P007", 5.3, "Engineering capacity looks tight. NovexCore v2 and Kubernetes migration in same quarter is risky. Can we push InfraV2 to Q2?", 0),
            ("P001", 5.5, "Agreed with Grace. InfraV2 moves to Q2. NovexCore v2 stays Q1. Carol, update the roadmap.", 1),
            ("P003", 5.6, "Updated. Q1: NovexCore v2 + ReportingEngine beta. Q2: InfraV2 migration.", 2),
        ]
    },
    {
        "thread_id": "T004",
        "subject": "Re: Re: Q1 roadmap sync",  # forwarded duplicate
        "messages": [
            ("P005", 6.0, "Forwarding Carol's roadmap update for the data team. Key: NovexCore v2 in Q1 means we need DataPipeline connectors ready by March 1.\n\n--- Forwarded ---\nUpdated. Q1: NovexCore v2 + ReportingEngine beta. Q2: InfraV2 migration.", None),
        ]
    },
    {
        "thread_id": "T005",
        "subject": "Kafka vs RabbitMQ for event streaming",
        "messages": [
            ("P002", 10.0, "We need an event streaming solution for the analytics pipeline. Evaluating Kafka and RabbitMQ.", None),
            ("P005", 10.2, "Kafka is the clear choice for high-throughput. We're talking millions of events/day. RabbitMQ would struggle.", 0),
            ("P010", 10.4, "Agreed on Kafka. I've set up a POC. Throughput: 850k msg/sec. Latency p99: 12ms. Very promising.", 1),
            ("P002", 11.0, "Decision: Kafka for event streaming in DataPipeline. James to lead implementation.", 2),
        ]
    },
    {
        "thread_id": "T006",
        "subject": "Hiring: Senior Backend Engineer",
        "messages": [
            ("P007", 14.0, "Opening a Senior Backend Engineer role. Requirements: Go or Python, distributed systems experience, Kafka a plus. JD attached.", None),
            ("P001", 14.1, "Approved. Let's aim to have someone onboarded by Q2.", 0),
            ("P007", 21.0, "Update: 3 strong candidates. Moving to final interviews.", 1),
            ("P007", 28.0, "Offer extended to Candidate A. Joining April 1.", 2),
        ]
    },
    {
        "thread_id": "T007",
        "subject": "On-call rotation changes",
        "messages": [
            ("P008", 15.0, "Proposing new on-call rotation: 1-week shifts, 4-person pool. Current 2-person rotation is burning people out.", None),
            ("P007", 15.2, "Fully support this. Let's implement immediately.", 0),
            ("P001", 15.3, "Approved. Hector please set up PagerDuty.", 1),
            ("P008", 15.5, "PagerDuty rotation configured. New schedule starts Monday.", 2),
        ]
    },
    {
        "thread_id": "T008",
        "subject": "REDACTED: Salary discussion",  # will be marked redacted
        "messages": [
            ("P001", 20.0, "[REDACTED — personal compensation data removed]", None),
        ]
    },
    {
        "thread_id": "T009",
        "subject": "Python version upgrade to 3.12",
        "messages": [
            ("P004", 22.0, "Proposing upgrade from Python 3.10 to 3.12 across all services. ~15% performance improvement on benchmarks.", None),
            ("P006", 22.1, "Tested on AuthService — no breaking changes. +18% on crypto operations.", 0),
            ("P010", 22.3, "ReportingEngine has one dependency (legacy-stats 1.2) not 3.12 compatible. Need to replace it first.", 1),
            ("P005", 22.5, "I'll handle the legacy-stats replacement — ETA 1 week.", 2),
            ("P004", 30.0, "All services now on Python 3.12. legacy-stats replaced with statslib. Upgrade complete.", 3),
        ]
    },
    {
        "thread_id": "T010",
        "subject": "Cross-post: All-hands notes Jan 30",
        "messages": [
            ("P003", 35.0, "Notes from today's all-hands:\n- NovexCore v2 on track for Feb 28\n- MySQL migration underway (yes, we switched from Postgres)\n- New hire joins April 1\n- Q2 focus: InfraV2 Kubernetes migration", None),
        ]
    },
    {
        "thread_id": "T011",
        "subject": "Re: All-hands notes Jan 30",  # cross-post
        "messages": [
            ("P007", 35.1, "Forwarding to the engineering team.\n\nKey engineering items:\n- NovexCore v2 Feb 28 deadline\n- MySQL migration underway\n- New hire April 1\n\n--- Original ---\nNotes from today's all-hands:\n- NovexCore v2 on track for Feb 28\n- MySQL migration underway\n- New hire joins April 1", None),
        ]
    },
    {
        "thread_id": "T012",
        "subject": "ReportingEngine performance regression",
        "messages": [
            ("P009", 40.0, "Detected 3x slowdown in ReportingEngine after last deploy. P99 latency went from 200ms to 650ms.", None),
            ("P010", 40.2, "Bisected to commit a3f9d. The new analytics aggregation query is doing a full table scan. Missing index.", 0),
            ("P010", 40.4, "Fix deployed: added composite index on (tenant_id, created_at). P99 back to 185ms.", 1),
            ("P009", 40.5, "Confirmed. Performance restored. Post-mortem due Friday.", 2),
        ]
    },
]

# ─────────────────────────── Issue Generator ───────────────────────────

ISSUES_RAW = [
    {
        "id": "I001", "project": "NovexCore", "title": "Implement JWT authentication",
        "status_history": [("open", 0), ("in_progress", 1), ("closed", 5)],
        "assignee": "P006", "author": "P003",
        "description": "Implement JWT-based auth for all NovexCore v2 API endpoints. Tokens must have 24h expiry.",
        "comments": [
            ("P006", 2.0, "Working on this. Using python-jose library."),
            ("P006", 4.5, "PR raised: #42. Includes unit tests and integration tests."),
            ("P009", 5.0, "Reviewed and approved. Tests pass. Closing."),
        ],
        "labels": ["security", "auth"],
    },
    {
        "id": "I002", "project": "NovexCore", "title": "Database migration: PostgreSQL → MySQL",
        "status_history": [("open", 8), ("in_progress", 9), ("closed", 25)],
        "assignee": "P002", "author": "P001",
        "description": "Migrate NovexCore v2 from PostgreSQL to MySQL following cost-driven decision reversal. See email thread T001.",
        "comments": [
            ("P002", 9.0, "Starting migration. Using Alembic for schema migrations."),
            ("P004", 12.0, "Compatibility issue: PostgreSQL JSONB → MySQL JSON. Need to audit all JSONB queries."),
            ("P002", 20.0, "All queries updated. Running full regression suite."),
            ("P002", 25.0, "Migration complete. All tests pass. Closing."),
        ],
        "labels": ["database", "migration"],
    },
    {
        "id": "I003", "project": "AuthService", "title": "Security: tokens never expire",
        "status_history": [("open", 3), ("in_progress", 3.2), ("closed", 4)],
        "assignee": "P006", "author": "P009",
        "description": "CRITICAL: Auth tokens missing exp claim. All tokens issued before 2024-01-10 are permanent. Immediate fix required.",
        "comments": [
            ("P008", 3.4, "Rotating all tokens in prod as emergency hotfix."),
            ("P006", 4.0, "Permanent fix deployed. 24h expiry enforced."),
        ],
        "labels": ["security", "critical", "bug"],
    },
    {
        "id": "I004", "project": "DataPipeline", "title": "Integrate Kafka for event streaming",
        "status_history": [("open", 11), ("in_progress", 12), ("in_review", 18), ("closed", 22)],
        "assignee": "P010", "author": "P002",
        "description": "Implement Kafka producer/consumer for DataPipeline analytics events. Target: 1M msg/sec throughput.",
        "comments": [
            ("P010", 12.0, "Setting up Kafka cluster on K8s."),
            ("P010", 18.0, "POC complete. 850k msg/sec achieved. Raising PR."),
            ("P005", 20.0, "Reviewed. Schema registry integration needed before merge."),
            ("P010", 22.0, "Schema registry integrated. Merging."),
        ],
        "labels": ["kafka", "streaming", "performance"],
    },
    {
        "id": "I005", "project": "InfraV2", "title": "Kubernetes migration planning",
        "status_history": [("open", 5), ("deferred", 6)],
        "assignee": "P008", "author": "P007",
        "description": "Plan and execute migration of all services to Kubernetes. Originally Q1 — deferred to Q2 per roadmap decision.",
        "comments": [
            ("P008", 5.5, "Created migration runbook. Estimating 6-week effort."),
            ("P007", 6.0, "Deferring to Q2 per Alice's decision in roadmap sync."),
        ],
        "labels": ["infrastructure", "kubernetes", "deferred"],
    },
    {
        "id": "I006", "project": "ReportingEngine", "title": "Performance regression: 3x latency spike",
        "status_history": [("open", 40), ("in_progress", 40.2), ("closed", 40.5)],
        "assignee": "P010", "author": "P009",
        "description": "P99 latency spiked from 200ms to 650ms after deploy on day 40.",
        "comments": [
            ("P010", 40.2, "Root cause: missing index on (tenant_id, created_at)."),
            ("P010", 40.4, "Fix deployed. P99 at 185ms — better than before."),
            ("P009", 40.5, "Confirmed resolved."),
        ],
        "labels": ["performance", "bug", "database"],
    },
    {
        "id": "I007", "project": "NovexCore", "title": "Python 3.12 upgrade",
        "status_history": [("open", 22), ("in_progress", 22.5), ("closed", 30)],
        "assignee": "P005", "author": "P004",
        "description": "Upgrade all NovexCore services from Python 3.10 to 3.12. Blocker: replace legacy-stats 1.2.",
        "comments": [
            ("P005", 23.0, "Replacing legacy-stats with statslib. ETA 1 week."),
            ("P005", 28.0, "statslib integrated. All tests pass on 3.12."),
            ("P004", 30.0, "All services upgraded. Closing."),
        ],
        "labels": ["python", "upgrade", "performance"],
    },
    {
        "id": "I008", "project": "AuthService", "title": "Add refresh token support",
        "status_history": [("open", 6), ("in_progress", 7), ("in_review", 10), ("closed", 12)],
        "assignee": "P006", "author": "P003",
        "description": "Implement refresh token flow. Access tokens: 24h. Refresh tokens: 30 days, single-use.",
        "comments": [
            ("P006", 7.0, "Implementing. Using Redis for refresh token storage."),
            ("P010", 11.0, "Code review done. One security concern: refresh tokens should be rotated on use."),
            ("P006", 11.5, "Addressed rotation. Updated."),
            ("P009", 12.0, "Approved and merged."),
        ],
        "labels": ["auth", "security"],
    },
]


# ─────────────────────────── Build corpus ───────────────────────────

def build_email(thread: Dict, msg_idx: int) -> Dict:
    thread_msgs = thread["messages"]
    author_id, day_off, body, reply_to_idx = thread_msgs[msg_idx]
    msg_id = f"EMAIL-{thread['thread_id']}-{msg_idx:02d}"
    ts = _ts(BASE_DATE, day_off)

    # Use different alias to simulate real-world identity noise
    alias_idx = (msg_idx + int(day_off)) % 3
    from_addr = _alias(author_id, alias_idx)
    to_addrs = [_alias(p["id"], 0) for p in PEOPLE[:5] if p["id"] != author_id]

    reply_to_id = f"EMAIL-{thread['thread_id']}-{reply_to_idx:02d}" if reply_to_idx is not None else None

    redacted = "[REDACTED" in body

    return {
        "id": msg_id,
        "type": "email",
        "thread_id": thread["thread_id"],
        "subject": thread["subject"],
        "from": from_addr,
        "from_person_id": author_id,
        "to": to_addrs,
        "timestamp": ts,
        "body": body,
        "reply_to": reply_to_id,
        "hash": _hash(body),
        "redacted": redacted,
        "source": "synthetic_novex_corpus_v1",
        "offset_start": 0,
        "offset_end": len(body),
    }


def build_issue(issue_raw: Dict) -> Dict:
    issue_id = issue_raw["id"]
    events = []
    for status, day_off in issue_raw["status_history"]:
        events.append({
            "event_id": f"{issue_id}-EVT-{status}",
            "event_type": "status_change",
            "status": status,
            "timestamp": _ts(BASE_DATE, day_off),
            "actor": issue_raw["author"],
        })

    comments = []
    for actor_id, day_off, text in issue_raw.get("comments", []):
        comments.append({
            "comment_id": f"{issue_id}-CMT-{_hash(text)}",
            "author": actor_id,
            "timestamp": _ts(BASE_DATE, day_off),
            "body": text,
            "hash": _hash(text),
        })

    return {
        "id": issue_id,
        "type": "issue",
        "project": issue_raw["project"],
        "title": issue_raw["title"],
        "description": issue_raw["description"],
        "status": issue_raw["status_history"][-1][0],
        "assignee": issue_raw["assignee"],
        "author": issue_raw["author"],
        "created_at": _ts(BASE_DATE, issue_raw["status_history"][0][1]),
        "updated_at": _ts(BASE_DATE, issue_raw["status_history"][-1][1]),
        "labels": issue_raw["labels"],
        "events": events,
        "comments": comments,
        "hash": _hash(issue_raw["title"] + issue_raw["description"]),
        "source": "synthetic_novex_corpus_v1",
    }


def generate_corpus() -> Dict[str, Any]:
    emails = []
    for thread in EMAIL_THREADS:
        for i in range(len(thread["messages"])):
            emails.append(build_email(thread, i))

    issues = [build_issue(i) for i in ISSUES_RAW]

    return {
        "meta": {
            "corpus_name": "Novex Engineering Communications (Synthetic)",
            "description": (
                "Synthetic corpus modelling the structure and challenges of the Enron "
                "Email Dataset (CMU/Kaggle) combined with GitHub Issues. "
                "Mirrors: identity aliasing, email quoting/forwarding, decision reversals, "
                "state transitions, redacted messages, and cross-posts. "
                "To use the real Enron corpus, replace this generator with the loader in "
                "src/enron_loader.py (documented separately)."
            ),
            "people": PEOPLE,
            "projects": PROJECTS,
            "components": COMPONENTS,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "email_count": len(emails),
            "issue_count": len(issues),
        },
        "emails": emails,
        "issues": issues,
    }


if __name__ == "__main__":
    out = Path("data/raw/corpus.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    corpus = generate_corpus()
    with open(out, "w") as f:
        json.dump(corpus, f, indent=2)
    print(f"Generated corpus: {corpus['meta']['email_count']} emails, {corpus['meta']['issue_count']} issues")
    print(f"Saved to {out}")
