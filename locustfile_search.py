"""locustfile_search.py — Locust load test for Atlas Search $search queries.

Measures P50 / P95 / P99 latency of three Atlas Search strategies:
  • plain $search / regex   — full Lucene segment scan
  • compound text + regex   — inverted-index pre-filter → small regex pass
  • text operator only      — pure inverted-index lookup (baseline)

Run alongside locustfile_regex.py to see the MQL $regex vs $search comparison
at high-percentile latencies under concurrent load.

Usage (headless, 10 workers, 90 s total):
    locust -f locustfile_search.py --headless \
           --users 10 --spawn-rate 2 --run-time 90s \
           --host mongodb+srv://<cluster>.mongodb.net/

Usage (interactive web UI):
    locust -f locustfile_search.py --host mongodb+srv://<cluster>.mongodb.net/

Environment variables (read from .env automatically):
    MONGODB_URI          Atlas connection string
    DATABASE_NAME        default: regex_demo
    SEARCH_INDEX_NAME    default: content_search
    SEARCH_LIMIT         rows per query (default: 20, 0 = all hits)

Tag filters:
    plain_regex   bare $search / regex (no pre-filter)
    compound      compound text pre-filter + regex
    text          standalone text operator (inverted-index only)
    selective     low-hit patterns (~600 results)
    dense         high-hit patterns (many results)
    negation      compound mustNot regex
"""

import random
from locust import User, task, tag, constant_pacing
from pymongo import MongoClient

from perf_common import (
    MONGODB_URI, DATABASE_NAME, PERF_COLLECTION,
    run_atlas_search_regex, run_compound_search_regex,
    run_atlas_text, run_atlas_negation,
)

_LIMIT = int(__import__("os").getenv("SEARCH_LIMIT", "20"))

# ---------------------------------------------------------------------------
# Pattern catalogue — mirrors perf_test.py SCENARIOS
# ---------------------------------------------------------------------------

# (label, atlas_pattern, path)
_DENSE_REGEX = [
    ("date_prefix",    r"2024-0[1-9]-.*",    "content"),
    ("ip_subnet",      r"192\.168\..*",       "content"),
    ("log_level_warn", r".*WARN.*",           "content"),
]

# (label, atlas_pattern, path, text_query)
_COMPOUND_SELECTIVE = [
    ("crit_compound",  r"CRIT-10[0-9]{2}.*", "content", "CRIT"),
    ("audit_compound", r".*AUDIT-EVENT.*",    "content", "AUDIT"),
    ("deploy_compound",r".*DEPLOY.*",         "content", "DEPLOY"),
]

# (label, text_query) — standalone text, no regex
_TEXT_QUERIES = [
    ("text_crit",   "CRIT"),
    ("text_audit",  "AUDIT"),
    ("text_deploy", "DEPLOY"),
]

# (label, atlas_pattern) — mustNot compound
_NEG_PATTERNS = [
    ("not_error", r".*ERROR.*"),
]


# ---------------------------------------------------------------------------
# Locust User
# ---------------------------------------------------------------------------

class AtlasSearchUser(User):
    """Simulates a client driving Atlas Search queries at ~1 RPS per user."""

    wait_time = constant_pacing(1)

    def on_start(self):
        self._client = MongoClient(
            MONGODB_URI,
            serverSelectionTimeoutMS=5_000,
            connectTimeoutMS=5_000,
            socketTimeoutMS=15_000,   # search can be slower on cold indexes
        )
        self._col = self._client[DATABASE_NAME][PERF_COLLECTION]

    def on_stop(self):
        self._client.close()

    # ------------------------------------------------------------------
    # Plain $search / regex — full Lucene scan (no pre-filter)
    # ------------------------------------------------------------------

    @tag("plain_regex", "dense")
    @task(3)
    def search_regex_dense(self):
        label, pattern, path = random.choice(_DENSE_REGEX)
        self._fire("search_regex", label,
                   lambda: run_atlas_search_regex(self._col, pattern, path, _LIMIT))

    # ------------------------------------------------------------------
    # Compound text + regex — inverted-index pre-filter → tiny regex pass
    # ------------------------------------------------------------------

    @tag("compound", "selective")
    @task(4)
    def search_compound_selective(self):
        label, pattern, path, text_q = random.choice(_COMPOUND_SELECTIVE)
        self._fire("compound", label,
                   lambda: run_compound_search_regex(self._col, text_q, pattern, path, 0))

    # ------------------------------------------------------------------
    # Standalone text operator — pure inverted-index (reference baseline)
    # ------------------------------------------------------------------

    @tag("text")
    @task(2)
    def search_text(self):
        label, text_q = random.choice(_TEXT_QUERIES)
        self._fire("text", label,
                   lambda: run_atlas_text(self._col, text_q, 0))

    # ------------------------------------------------------------------
    # Atlas Search mustNot negation
    # ------------------------------------------------------------------

    @tag("negation")
    @task(1)
    def search_negation(self):
        label, pattern = random.choice(_NEG_PATTERNS)
        self._fire("search_mustNot", label,
                   lambda: run_atlas_negation(self._col, pattern, _LIMIT))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fire(self, request_type: str, label: str, fn):
        name = f"{request_type}/{label}"
        try:
            count, elapsed_ms = fn()
            self.environment.events.request.fire(
                request_type="Atlas",
                name=name,
                response_time=elapsed_ms,
                response_length=count,
                exception=None,
                context={},
            )
        except Exception as exc:
            self.environment.events.request.fire(
                request_type="Atlas",
                name=name,
                response_time=0,
                response_length=0,
                exception=exc,
                context={},
            )
