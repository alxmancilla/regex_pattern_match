"""perf_common.py — Shared infrastructure for regex / search performance tests.

Imported by:
    perf_test.py          — sequential benchmark (avg / median / stdev)
    locustfile_regex.py   — Locust load test for MQL $regex
    locustfile_search.py  — Locust load test for Atlas Search $search

Contains:
    • MongoDB connection constants (read from .env / environment)
    • Embedding generation (_make_embedding) — numpy if available, else pure Python
    • Synthetic document generators and generate_documents()
    • setup_perf_collection() — idempotent collection + index setup
    • Query runners — one function per query variant, returns (count, elapsed_ms)
"""

import os
import sys
import time
import random
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.operations import SearchIndexModel
from schema import (SEARCH_INDEX_DEFINITION, wait_for_index,
                    VECTOR_INDEX_DEFINITION, VECTOR_INDEX_NAME, wait_for_vector_index)

load_dotenv()

# =============================================================================
# Configuration  (shared with main.py via .env)
# =============================================================================

MONGODB_URI       = os.getenv("MONGODB_URI", "mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/")
DATABASE_NAME     = os.getenv("DATABASE_NAME", "regex_demo")
PERF_COLLECTION   = "perf_documents"
SEARCH_INDEX_NAME = os.getenv("SEARCH_INDEX_NAME", "content_search")

# =============================================================================
# Embedding generation
# =============================================================================

_EMBEDDING_DIM = 1536   # matches OpenAI ada-002 / reference benchmark

try:
    import numpy as np
    def _make_embedding(dim: int = _EMBEDDING_DIM) -> list[float]:
        """Return a random L2-normalised unit vector (numpy fast path)."""
        vec = np.random.randn(dim).astype(np.float64)
        return (vec / np.linalg.norm(vec)).tolist()
except ImportError:
    def _make_embedding(dim: int = _EMBEDDING_DIM) -> list[float]:
        """Return a random [-1, 1] vector (pure-Python fallback; ~30 s for 100K docs)."""
        return [random.uniform(-1.0, 1.0) for _ in range(dim)]

# =============================================================================
# Synthetic document generation
# =============================================================================

_SERVERS    = ["prod-db-01", "prod-api-02", "auth-service", "cache-01", "worker-03"]
_USERS      = ["admin", "john", "jane", "deploy", "monitor"]
_LEVELS     = ["INFO", "WARN", "ERROR"]
_EMAILS     = ["alice@example.com", "Bob@Test.org", "Carol@CORP.net", "dave@Service.io"]
_IP_TMPLS   = ["192.168.1.{}", "10.0.0.{}", "172.16.0.{}"]
_PORTS      = [5432, 6379, 8080, 27017, 3306]
_EXCEPTIONS = ["NullPointerException", "IllegalArgumentException",
               "RuntimeException", "IOException", "TimeoutException"]


def _rand_date() -> str:
    return (f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d} "
            f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}")


def _rand_ip() -> str:
    return random.choice(_IP_TMPLS).format(random.randint(1, 254))


def _make_log() -> str:
    lines = [f"{random.choice(_LEVELS)} {_rand_date()} {random.choice(_USERS)} on {random.choice(_SERVERS)}"
             for _ in range(random.randint(2, 5))]
    return "\n".join(lines)


def _make_config() -> str:
    port = random.choice(_PORTS)
    return (f'{{"database": {{"host": "localhost", "port": {port}, '
            f'"username": "{random.choice(_USERS)}"}}, '
            f'"cache": {{"enabled": true, "ttl": {random.randint(300, 7200)}}}}}')


def _make_api_response() -> str:
    emails = random.sample(_EMAILS, k=random.randint(1, 3))
    users  = ", ".join(f'{{"id": {i+1}, "email": "{e}"}}' for i, e in enumerate(emails))
    return f'{{"status": "success", "data": {{"users": [{users}]}}}}'


def _make_access_log() -> str:
    lines = [f"INFO {_rand_date()} User {random.choice(_USERS)} logged in from {_rand_ip()}"
             for _ in range(random.randint(2, 4))]
    if random.random() < 0.35:
        lines.append(f"WARN {_rand_date()} Failed login attempt for user unknown")
    return "\n".join(lines)


def _make_stack_trace() -> str:
    e1, e2 = random.choice(_EXCEPTIONS), random.choice(_EXCEPTIONS)
    return (f"Stack trace: {e1} at com.app.Service.process(Service.java:{random.randint(10,200)})\n"
            f"Caused by: {e2} at com.app.Validator.check(Validator.java:{random.randint(10,200)})")


_MAKERS = [_make_log, _make_config, _make_api_response, _make_access_log, _make_stack_trace]

# Rare generators — unique tokens (CRIT, AUDIT, DEPLOY) absent from common docs.
# At 1/167 density each gives ~600 entity records per 100K docs.
_CRIT_CODES = [f"CRIT-{n:04d}" for n in range(1000, 1050)]
_OPS        = ["DELETE", "UPDATE", "CREATE", "READ"]
_SERVICES   = ["svc-api", "svc-auth", "svc-billing", "svc-gateway", "svc-worker"]
_REGIONS    = ["us-east-1", "eu-west-1", "ap-southeast-1", "us-west-2"]
_ENVS       = ["production", "staging", "qa"]


def _make_critical_alert() -> str:
    code = random.choice(_CRIT_CODES)
    host = random.choice(_SERVERS)
    pct  = random.randint(80, 99)
    return (f"{code} {_rand_date()} severity=critical host={host} "
            f"msg=\"disk usage threshold exceeded {pct}%\" pid={random.randint(1000,9999)}")


def _make_audit_event() -> str:
    uid = random.randint(1000, 9999)
    op  = random.choice(_OPS)
    svc = random.choice(_SERVICES)
    return (f"AUDIT-EVENT {_rand_date()} uid={uid} op={op} "
            f"resource=/api/users/{random.randint(1,999)} svc={svc} ip={_rand_ip()}")


def _make_deploy_record() -> str:
    svc    = random.choice(_SERVICES)
    major, minor, patch = random.randint(1,5), random.randint(0,12), random.randint(0,20)
    commit = "".join(random.choices("abcdef0123456789", k=8))
    return (f"DEPLOY {svc} v{major}.{minor}.{patch} env={random.choice(_ENVS)} "
            f"region={random.choice(_REGIONS)} commit={commit} "
            f"triggered_by={random.choice(_USERS)} ts={_rand_date()}")


_RARE_MAKERS = [_make_critical_alert, _make_audit_event, _make_deploy_record]


def generate_documents(n: int) -> list:
    """Return n synthetic documents, each with a 1536-dim embedding vector.

    Density: 3/167 ≈ 1.8% rare docs total (0.6% CRIT, 0.6% AUDIT, 0.6% DEPLOY)
    → ~600 entity records per type at 100K docs (reference benchmark target).
    """
    random.seed(42)          # reproducible; numpy seeded separately if available
    try:
        import numpy as np
        np.random.seed(42)
    except ImportError:
        pass
    docs = []
    for i in range(n):
        if i % 167 < len(_RARE_MAKERS):
            content = _RARE_MAKERS[i % 167]()
        else:
            content = _MAKERS[i % len(_MAKERS)]()
        docs.append({
            "_id":        i + 1,
            "filename":   f"doc_{i+1:05d}.txt",
            "content":    content,
            "content_lc": content.lower(),
            "metadata":   {"seq": i + 1},
            "embeddings": _make_embedding(),
        })
    return docs


# =============================================================================
# Collection / index setup
# =============================================================================

def _index_status(collection) -> str | None:
    """Return the SEARCH index status string, or None if it doesn't exist."""
    for idx in collection.list_search_indexes():
        if idx["name"] == SEARCH_INDEX_NAME:
            return idx["status"]
    return None


def setup_perf_collection(client: MongoClient, num_docs: int, reuse: bool = False):
    """Idempotent collection + index setup.  Returns (collection, actual_docs).

    reuse=True  → skip rebuild when collection exists with a READY index.
    reuse=False → always drop and rebuild (default for clean benchmarks).
    """
    db         = client[DATABASE_NAME]
    collection = db[PERF_COLLECTION]

    existing = collection.estimated_document_count()
    status   = _index_status(collection)

    if reuse and existing > 0 and status == "READY":
        print(f"\nReusing existing collection ({existing:,} docs) and READY index.")
        return collection, existing

    if existing == num_docs and status in ("BUILDING", "PENDING", "INITIAL_SYNC"):
        print(f"\nCollection has {existing:,} docs; index is {status} — waiting...")
        if not wait_for_index(collection, SEARCH_INDEX_NAME):
            sys.exit("Text search index did not reach READY state — aborting.")
        return collection, existing

    # Full rebuild
    print(f"\nGenerating {num_docs:,} synthetic documents...")
    docs = generate_documents(num_docs)
    collection.drop()

    _BATCH = 1_000
    for start in range(0, num_docs, _BATCH):
        collection.insert_many(docs[start:start + _BATCH], ordered=False)
        if (start + _BATCH) % 10_000 == 0 or start + _BATCH >= num_docs:
            print(f"  Inserted {min(start + _BATCH, num_docs):,} / {num_docs:,} documents...")
    print(f"  Done — {num_docs:,} documents in '{PERF_COLLECTION}'")

    collection.create_search_index(
        SearchIndexModel(definition=SEARCH_INDEX_DEFINITION, name=SEARCH_INDEX_NAME)
    )
    collection.create_search_index(
        SearchIndexModel(definition=VECTOR_INDEX_DEFINITION, name=VECTOR_INDEX_NAME,
                         type="vectorSearch")
    )
    print("  Indexes submitted — waiting for Atlas to build them "
          "(typically 60-180 s for 100K docs)...")
    if not wait_for_index(collection, SEARCH_INDEX_NAME):
        sys.exit("Text search index did not reach READY state — aborting.")
    if not wait_for_vector_index(collection, VECTOR_INDEX_NAME):
        sys.exit("Vector search index did not reach READY state — aborting.")
    return collection, num_docs


# =============================================================================
# Query runners — each returns (result_count: int, elapsed_ms: float)
# =============================================================================

def run_mql_regex(collection, pattern: str, options: str = "", limit: int = 0) -> tuple:
    """Plain MQL $regex — COLLSCAN + PCRE.  No index benefit."""
    query  = {"content": {"$regex": pattern}}
    if options:
        query["content"]["$options"] = options
    t0     = time.perf_counter()
    cursor = collection.find(query, {"_id": 1})
    if limit:
        cursor = cursor.limit(limit)
    count = len(list(cursor))
    return count, (time.perf_counter() - t0) * 1000


def run_atlas_search_regex(collection, pattern: str,
                           path: str = "content", limit: int = 0) -> tuple:
    """Atlas Search $search / regex — Lucene index + concurrent segment scan.

    path="content"    → case-sensitive  (lucene.keyword original case)
    path="content_lc" → case-insensitive (pre-lowercased; (?i) unsupported)
    returnStoredSource skips the per-hit mongot→mongod round-trip.
    """
    pipeline = [{"$search": {
        "index": SEARCH_INDEX_NAME,
        "regex": {"path": path, "query": pattern},
        "concurrent": True,
        "returnStoredSource": True,
    }}]
    if limit:
        pipeline.append({"$limit": limit})
    pipeline.append({"$project": {"_id": 1}})
    t0    = time.perf_counter()
    count = len(list(collection.aggregate(pipeline)))
    return count, (time.perf_counter() - t0) * 1000


def run_compound_search_regex(collection, text_query: str, pattern: str,
                               path: str = "content", limit: int = 0) -> tuple:
    """Compound Atlas Search: inverted-index text pre-filter + regex refinement.

    Correct production pattern for selective queries:
      filter[text] → O(matching) inverted-index lookup (100K → ~600 candidates)
      must[regex]  → O(candidates) Lucene regex on the small candidate set
    """
    pipeline = [{"$search": {
        "index": SEARCH_INDEX_NAME,
        "compound": {
            "filter": [{"text": {
                "path": {"value": "content", "multi": "std"},
                "query": text_query,
            }}],
            "must": [{"regex": {"path": path, "query": pattern}}],
        },
        "concurrent": True,
        "returnStoredSource": True,
    }}]
    if limit:
        pipeline.append({"$limit": limit})
    pipeline.append({"$project": {"_id": 1}})
    t0    = time.perf_counter()
    count = len(list(collection.aggregate(pipeline)))
    return count, (time.perf_counter() - t0) * 1000


def run_atlas_text(collection, query: str, limit: int = 0) -> tuple:
    """Atlas Search text operator — O(matching) inverted-index term lookup."""
    pipeline = [{"$search": {
        "index": SEARCH_INDEX_NAME,
        "text": {"path": {"value": "content", "multi": "std"}, "query": query},
        "concurrent": True,
        "returnStoredSource": True,
    }}]
    if limit:
        pipeline.append({"$limit": limit})
    pipeline.append({"$project": {"_id": 1}})
    t0    = time.perf_counter()
    count = len(list(collection.aggregate(pipeline)))
    return count, (time.perf_counter() - t0) * 1000


def run_mql_negation(collection, pattern: str, limit: int = 0) -> tuple:
    """MQL $not $regex — COLLSCAN excluding matches.  No index benefit."""
    t0     = time.perf_counter()
    cursor = collection.find({"content": {"$not": {"$regex": pattern}}}, {"_id": 1})
    if limit:
        cursor = cursor.limit(limit)
    count = len(list(cursor))
    return count, (time.perf_counter() - t0) * 1000


def run_atlas_negation(collection, pattern: str, limit: int = 0) -> tuple:
    """Atlas Search compound mustNot — O(N) like MQL but with IPC overhead."""
    pipeline = [{"$search": {
        "index": SEARCH_INDEX_NAME,
        "compound": {
            "filter": [{"wildcard": {"path": "content", "query": "*",
                                     "allowAnalyzedField": True}}],
            "mustNot": [{"regex": {"path": "content", "query": pattern}}],
        },
        "concurrent": True,
        "returnStoredSource": True,
    }}]
    if limit:
        pipeline.append({"$limit": limit})
    pipeline.append({"$project": {"_id": 1}})
    t0    = time.perf_counter()
    count = len(list(collection.aggregate(pipeline)))
    return count, (time.perf_counter() - t0) * 1000
