"""locustfile_regex.py — Locust load test for MQL $regex queries.

Measures P50 / P95 / P99 latency of plain MQL $regex across five pattern
categories against the 100 K-doc perf_documents collection.

Usage (headless, 10 workers, 30-second ramp, 60-second measurement):
    locust -f locustfile_regex.py --headless \
           --users 10 --spawn-rate 2 --run-time 90s \
           --host mongodb+srv://<cluster>.mongodb.net/

Usage (interactive web UI on port 8089):
    locust -f locustfile_regex.py --host mongodb+srv://<cluster>.mongodb.net/

Environment variables (read from .env automatically):
    MONGODB_URI          Atlas connection string
    DATABASE_NAME        default: regex_demo
    SEARCH_INDEX_NAME    default: content_search
    REGEX_LIMIT          rows per query (default: 20, 0 = all hits)

Tag filters (--tags flag):
    dense      high-hit patterns (date, IP, log-level)  ~many results
    selective  low-hit patterns (CRIT, AUDIT codes)     ~600 results
    negation   $not $regex (always a full collection scan)
"""

import random
from locust import User, task, tag, events, constant_pacing
from pymongo import MongoClient

from perf_common import (
    MONGODB_URI, DATABASE_NAME, PERF_COLLECTION,
    run_mql_regex, run_mql_negation,
)

# ---------------------------------------------------------------------------
# Patterns — same set as perf_test.py SCENARIOS for apple-to-apple comparison
# ---------------------------------------------------------------------------

_DENSE_PATTERNS = [
    ("date_prefix",    r"2024-0[1-9]-",   ""),
    ("ip_subnet",      r"192\.168\.",      ""),
    ("log_level_warn", r"WARN",            ""),
    ("multi_level",    r"(ERROR|WARN)",    ""),
]

_SELECTIVE_PATTERNS = [
    ("crit_code",  r"CRIT-10[0-9]{2}",    ""),
    ("audit_op",   r"AUDIT-EVENT.*DELETE", ""),
]

_NEG_PATTERNS = [
    ("not_error",  r"ERROR"),
]

_LIMIT = int(__import__("os").getenv("REGEX_LIMIT", "20"))


# ---------------------------------------------------------------------------
# Locust User
# ---------------------------------------------------------------------------

class MqlRegexUser(User):
    """Simulates a client issuing MQL $regex queries at ~1 RPS per user."""

    # One request per second per virtual user.  Adjust with --users to control
    # aggregate throughput (e.g. 10 users ≈ 10 RPS).
    wait_time = constant_pacing(1)

    def on_start(self):
        self._client = MongoClient(
            MONGODB_URI,
            serverSelectionTimeoutMS=5_000,
            connectTimeoutMS=5_000,
            socketTimeoutMS=10_000,
        )
        self._col = self._client[DATABASE_NAME][PERF_COLLECTION]

    def on_stop(self):
        self._client.close()

    # ------------------------------------------------------------------
    # Dense patterns — many results, exercises full collection scan cost
    # ------------------------------------------------------------------

    @tag("dense")
    @task(4)
    def regex_dense(self):
        label, pattern, opts = random.choice(_DENSE_PATTERNS)
        self._run(label, pattern, opts, _LIMIT)

    # ------------------------------------------------------------------
    # Selective patterns — ~600 results, shows cost of rare-doc scan
    # ------------------------------------------------------------------

    @tag("selective")
    @task(2)
    def regex_selective(self):
        label, pattern, opts = random.choice(_SELECTIVE_PATTERNS)
        self._run(label, pattern, opts, 0)   # fetch all hits

    # ------------------------------------------------------------------
    # Negation — $not $regex always does a full collection scan
    # ------------------------------------------------------------------

    @tag("negation")
    @task(1)
    def regex_negation(self):
        label, pattern = random.choice(_NEG_PATTERNS)
        name = f"mql_not_regex/{label}"
        try:
            count, elapsed_ms = run_mql_negation(self._col, pattern, limit=_LIMIT)
            self.environment.events.request.fire(
                request_type="MQL",
                name=name,
                response_time=elapsed_ms,
                response_length=count,
                exception=None,
                context={},
            )
        except Exception as exc:
            self.environment.events.request.fire(
                request_type="MQL",
                name=name,
                response_time=0,
                response_length=0,
                exception=exc,
                context={},
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run(self, label: str, pattern: str, options: str, limit: int):
        name = f"mql_regex/{label}"
        try:
            count, elapsed_ms = run_mql_regex(
                self._col, pattern, options=options, limit=limit
            )
            self.environment.events.request.fire(
                request_type="MQL",
                name=name,
                response_time=elapsed_ms,
                response_length=count,
                exception=None,
                context={},
            )
        except Exception as exc:
            self.environment.events.request.fire(
                request_type="MQL",
                name=name,
                response_time=0,
                response_length=0,
                exception=exc,
                context={},
            )
