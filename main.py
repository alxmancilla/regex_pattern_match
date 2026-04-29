"""
MongoDB Atlas Search Regex Examples
Demonstrates grep-style pattern matching similar to PostgreSQL's ~ and ~* operators

Requirements:
    pip install pymongo python-dotenv

Usage:
    python main.py            # reuse existing collection + index (fast)
    python main.py --reset    # drop and rebuild everything (slow, ~60-120 s)
"""

import os
import sys
import argparse
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.operations import SearchIndexModel
from schema import SEARCH_INDEX_DEFINITION, wait_for_index

# Load environment variables from .env file
load_dotenv()

# =============================================================================
# Configuration
# =============================================================================

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://<username>:<password>@<cluster>.mongodb.net/")
DATABASE_NAME = os.getenv("DATABASE_NAME", "regex_demo")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "documents")
SEARCH_INDEX_NAME = os.getenv("SEARCH_INDEX_NAME", "content_search")
# RESULTS_LIMIT removed — use the --limit CLI flag (default 10) passed to each example.


# =============================================================================
# Sample Data
# content_lc is a pre-lowercased copy of content, indexed with lucene.keyword.
# The Atlas Search regex operator does NOT support the (?i) inline Lucene flag;
# case-insensitive matching requires querying a pre-lowercased field.
# =============================================================================

SAMPLE_DOCUMENTS = [
    {
        "filename": "server_logs.txt",
        "content": "ERROR 2024-01-15 10:23:45 Connection timeout on port 5432\nWARN 2024-01-15 10:24:00 Retry attempt 1\nERROR 2024-01-15 10:25:00 Database connection failed",
        "metadata": {"type": "log", "server": "prod-db-01"}
    },
    {
        "filename": "config.json",
        "content": '{"database": {"host": "localhost", "port": 5432, "username": "admin"}, "cache": {"enabled": true, "ttl": 3600}}',
        "metadata": {"type": "config", "server": "prod-api-01"}
    },
    {
        "filename": "api_response.json",
        "content": '{"status": "success", "data": {"users": [{"id": 1, "email": "John@Example.com"}, {"id": 2, "email": "Jane@Test.org"}]}}',
        "metadata": {"type": "api_response", "endpoint": "/users"}
    },
    {
        "filename": "error_dump.txt",
        "content": "Stack trace: NullPointerException at com.app.Service.process(Service.java:42)\nCaused by: IllegalArgumentException at com.app.Validator.check(Validator.java:15)",
        "metadata": {"type": "error", "application": "backend-service"}
    },
    {
        "filename": "access_log.txt",
        "content": "INFO 2024-01-15 08:00:00 User admin logged in from 192.168.1.100\nINFO 2024-01-15 08:05:00 User john logged in from 10.0.0.50\nWARN 2024-01-15 08:10:00 Failed login attempt for user unknown",
        "metadata": {"type": "log", "server": "auth-service"}
    },
    {
        "filename": "deploy_log.txt",
        "content": "INFO 2024-01-15 14:30:00 DEPLOY api-gateway v3.2.1 env=production region=us-east-1 status=SUCCESS",
        "metadata": {"type": "log", "server": "deploy-01"}
    },
    {
        "filename": "network.json",
        "content": '{"network": {"gateway": "192.168.1.1", "dns": ["8.8.8.8", "8.8.4.4"], "subnet": "192.168.1.0/24", "port": 443}}',
        "metadata": {"type": "config", "server": "network-01"}
    },
    {
        "filename": "metrics.txt",
        "content": "METRIC 2024-01-15 09:00:00 cpu_usage=85.3% memory=14.2GB disk_io=1234KB/s host=prod-api-02 ip=10.0.0.22",
        "metadata": {"type": "metric", "server": "prod-api-02"}
    },
    {
        "filename": "audit_trail.json",
        "content": '{"event": "user_action", "user": "dave@corp.net", "action": "DELETE", "resource": "/api/orders/9823", "ip": "10.0.0.15"}',
        "metadata": {"type": "audit", "endpoint": "/api/orders"}
    },
    {
        "filename": "migration_v2.sql.log",
        "content": "INFO 2024-01-15 02:00:00 Starting migration v2.3.0\nINFO 2024-01-15 02:05:00 Applied 14 schema changes\nWARN 2024-01-15 02:10:00 Deprecated column users.legacy_id still present",
        "metadata": {"type": "log", "server": "db-migration-01"}
    },
    {
        "filename": "healthcheck.txt",
        "content": "HEALTH 2024-01-15 12:00:00 service=payment-service status=DEGRADED latency_p99=2847ms endpoint=172.16.0.5:8443",
        "metadata": {"type": "metric", "server": "payment-service"}
    },
    {
        "filename": "app_config.json",
        "content": '{"app": {"name": "billing-service", "version": "4.1.0", "port": 8080, "db_host": "prod-db-01:5432"}}',
        "metadata": {"type": "config", "server": "billing-01"}
    },
    {
        "filename": "clean_ops.txt",
        "content": "INFO 2024-01-15 13:00:00 Backup completed successfully\nINFO 2024-01-15 13:05:00 All health checks passed\nINFO 2024-01-15 13:10:00 Scheduled maintenance window closed",
        "metadata": {"type": "log", "server": "backup-01"}
    },
    # ── additional documents ──────────────────────────────────────────────────
    {
        "filename": "nginx_access.log",
        "content": '192.168.1.42 - alice@example.com [15/Jan/2024:09:12:03 +0000] "GET /api/v2/orders HTTP/1.1" 200 1482\n10.0.0.77 - - [15/Jan/2024:09:12:07 +0000] "POST /api/v2/login HTTP/1.1" 401 112\n172.16.0.9 - bob@example.com [15/Jan/2024:09:12:11 +0000] "DELETE /api/v2/users/881 HTTP/1.1" 204 0\nERROR 2024-01-15 09:12:15 upstream timed out (110) while reading response from upstream 10.0.0.91:8080',
        "metadata": {"type": "access_log", "server": "nginx-01"}
    },
    {
        "filename": "firewall_events.txt",
        "content": "WARN 2024-01-15 03:15:00 Blocked inbound connection from 203.0.113.55 to port 22\nERROR 2024-01-15 03:17:42 Port scan detected from 198.51.100.4 — 1024 probes in 30s\nINFO 2024-01-15 03:20:00 Rule UPDATE: deny src=198.51.100.0/24 dst=any port=22\nWARN 2024-01-15 03:22:10 GeoIP block applied to 203.0.113.0/24 (CN)",
        "metadata": {"type": "security_log", "server": "fw-edge-01"}
    },
    {
        "filename": "k8s_events.txt",
        "content": "WARN 2024-01-15 11:00:05 Pod svc-payment-7d9f8b-xk2pq CrashLoopBackOff restartCount=5 node=worker-03\nERROR 2024-01-15 11:00:08 Liveness probe failed: GET http://10.0.0.33:8080/healthz — timeout after 3s\nINFO 2024-01-15 11:02:00 Pod svc-payment-7d9f8b-xk2pq rescheduled on node worker-04 ip=172.16.0.21\nWARN 2024-01-15 11:04:00 OOMKilled: container billing exceeded memory limit 512Mi",
        "metadata": {"type": "log", "server": "k8s-control-plane"}
    },
    {
        "filename": "slow_query.log",
        "content": 'WARN 2024-01-15 07:44:12 Slow query detected duration=4821ms db=orders collection=transactions\n{"query": {"status": {"$in": ["pending", "failed"]}, "created_at": {"$lt": "2024-01-01"}}, "planSummary": "COLLSCAN", "docsExamined": 2847291, "nReturned": 142}\nWARN 2024-01-15 07:51:03 Slow query duration=2103ms missing index on field user_id — add index to improve performance',
        "metadata": {"type": "slow_query", "server": "prod-db-01"}
    },
    {
        "filename": "cicd_pipeline.log",
        "content": "INFO 2024-01-15 16:00:00 Pipeline #4821 started branch=main commit=a3f9c12d triggered_by=carol@example.com\nINFO 2024-01-15 16:04:30 Stage BUILD passed duration=270s\nERROR 2024-01-15 16:07:15 Stage TEST failed — 3 of 142 tests failed (RuntimeException in PaymentServiceTest)\nWARN 2024-01-15 16:07:16 Pipeline #4821 blocked — deployment to production skipped\nINFO 2024-01-15 16:30:00 DEPLOY svc-billing v4.1.1 env=staging region=eu-west-1 commit=b7e2d09f triggered_by=deploy status=SUCCESS",
        "metadata": {"type": "log", "server": "ci-runner-02"}
    },
    {
        "filename": "critical_alerts.txt",
        "content": "CRIT 2024-01-15 05:30:00 Disk usage 94% on prod-db-01 (172.16.0.3) — threshold 90%\nCRIT 2024-01-15 05:31:00 Replication lag 48s on replica prod-db-02:27017 — primary prod-db-01\nWARN 2024-01-15 05:32:00 Auto-remediation triggered: evicting stale sessions on 10.0.0.5\nERROR 2024-01-15 05:35:00 Auto-remediation FAILED — manual intervention required contact oncall@corp.net",
        "metadata": {"type": "alert", "server": "monitoring-01"}
    },
    {
        "filename": "smtp_delivery.log",
        "content": "INFO 2024-01-15 14:00:01 Accepted from=alice@example.com to=bob@corp.net size=8420 id=<msg-20240115-0001>\nWARN 2024-01-15 14:00:04 Deferred to=carol@Test.org reason=421 Service temporarily unavailable relay=10.0.0.88:25\nERROR 2024-01-15 14:02:30 Bounced from=noreply@example.com to=invalid@nowhere.xyz status=550 5.1.1 User unknown\nINFO 2024-01-15 14:05:00 Delivered from=dave@Service.io to=eve@example.com relay=172.16.0.44:587",
        "metadata": {"type": "log", "server": "smtp-relay-01"}
    },
    {
        "filename": "prometheus_metrics.json",
        "content": '{"timestamp": "2024-01-15T10:00:00Z", "metrics": [{"name": "http_request_duration_seconds", "labels": {"method": "POST", "endpoint": "/api/checkout", "status": "500"}, "value": 4.821}, {"name": "db_connections_active", "labels": {"host": "prod-db-01", "port": "5432"}, "value": 97}, {"name": "cache_hit_ratio", "labels": {"host": "cache-01", "port": "6379"}, "value": 0.61}]}',
        "metadata": {"type": "metric", "server": "prometheus-01"}
    },
    {
        "filename": "tls_config.json",
        "content": '{"tls": {"cert": "/etc/ssl/certs/prod.pem", "key": "/etc/ssl/private/prod.key", "port": 443, "min_version": "TLS1.2", "ciphers": ["TLS_AES_256_GCM_SHA384"], "hsts": true, "ocsp_stapling": true, "contact": "security@corp.net"}}',
        "metadata": {"type": "config", "server": "lb-01"}
    },
    {
        "filename": "user_management.log",
        "content": "INFO 2024-01-15 09:00:00 User created email=frank@example.com role=viewer ip=10.0.0.12\nINFO 2024-01-15 09:05:00 Password reset requested for grace@corp.net from ip=192.168.1.88\nWARN 2024-01-15 09:10:00 Role escalation: henry@Test.org promoted from viewer to admin by admin\nERROR 2024-01-15 09:15:00 Failed login for ivan@example.com — account locked after 5 attempts from 10.0.0.99",
        "metadata": {"type": "audit", "server": "auth-service"}
    },
    {
        "filename": "cron_report.txt",
        "content": "INFO 2024-01-15 00:00:01 cron: starting nightly-cleanup on prod-db-01\nINFO 2024-01-15 00:03:44 Deleted 14823 expired session records\nWARN 2024-01-15 00:04:10 Table archive_events took 87s — exceeds warn threshold 60s\nINFO 2024-01-15 00:04:11 cron: nightly-cleanup finished duration=231s exit=0",
        "metadata": {"type": "log", "server": "prod-db-01"}
    },
    {
        "filename": "service_mesh.log",
        "content": "INFO 2024-01-15 10:10:00 svc-api → svc-auth 200 OK latency=12ms src=10.0.0.8 dst=10.0.0.21:8081\nWARN 2024-01-15 10:10:45 Circuit breaker OPEN svc-api → svc-billing after 5 consecutive failures dst=172.16.0.7:8080\nERROR 2024-01-15 10:11:00 Request dropped svc-api → svc-billing circuit=OPEN fallback=cache\nINFO 2024-01-15 10:15:00 Circuit breaker CLOSED svc-api → svc-billing — service recovered",
        "metadata": {"type": "log", "server": "envoy-proxy-01"}
    },
    {
        "filename": "rate_limiter.log",
        "content": "WARN 2024-01-15 18:00:00 Rate limit exceeded ip=203.0.113.77 endpoint=/api/v2/search requests=1204/min limit=100/min\nERROR 2024-01-15 18:00:01 IP 203.0.113.77 blocked for 300s — excessive rate limit violations\nWARN 2024-01-15 18:05:30 Token bucket exhausted user=bot@example.com endpoint=/api/v2/export\nINFO 2024-01-15 18:10:00 Rate limit policy UPDATE applied: /api/v2/export limit=10/min per user",
        "metadata": {"type": "security_log", "server": "api-gateway-01"}
    },
    {
        "filename": "waf_events.json",
        "content": '{"timestamp": "2024-01-15T20:33:07Z", "event": "waf_block", "severity": "HIGH", "src_ip": "198.51.100.22", "dst_ip": "172.16.0.1", "port": 443, "rule": "SQLi-1045", "payload": "1 OR 1=1--", "action": "BLOCK", "alert_to": "security@corp.net"}',
        "metadata": {"type": "security_log", "server": "waf-01"}
    },
    {
        "filename": "db_replication.log",
        "content": "INFO 2024-01-15 06:00:00 Replication started primary=prod-db-01:27017 replica=prod-db-02:27017\nWARN 2024-01-15 06:04:20 Replication lag 12s — oplog window 48h remaining\nERROR 2024-01-15 06:10:05 Replication INTERRUPTED on prod-db-02 — network partition detected from 10.0.0.5\nERROR 2024-01-15 06:10:06 IOException: connection reset by peer at prod-db-01:27017\nINFO 2024-01-15 06:15:00 Replication resumed lag=0s — partition resolved",
        "metadata": {"type": "log", "server": "prod-db-02"}
    },
]

for _doc in SAMPLE_DOCUMENTS:
    _doc["content_lc"] = _doc["content"].lower()


# =============================================================================
# Search Index Definition — imported from schema.py
# See schema.py for the full definition and design rationale.
# Both main.py and perf_test.py share the same definition to prevent drift.
# =============================================================================


# =============================================================================
# Helper Functions
# =============================================================================

def print_header(title: str) -> None:
    """Print a formatted section header."""
    print("\n" + "=" * 70)
    print(f" {title}")
    print("=" * 70)


def print_subheader(title: str) -> None:
    """Print a formatted sub-section header."""
    print(f"\n--- {title} ---\n")


def print_results(results: list, show_content: bool = True) -> None:
    """Print search results in a readable format."""
    if not results:
        print("No results found.")
        return

    for i, doc in enumerate(results, 1):
        print(f"Result {i}:")
        print(f"  Filename: {doc.get('filename', 'N/A')}")

        if "score" in doc:
            print(f"  Score: {doc['score']:.4f}")

        if "metadata" in doc:
            print(f"  Metadata: {doc['metadata']}")

        if show_content and "content" in doc:
            preview = doc["content"][:100] + "..." if len(doc["content"]) > 100 else doc["content"]
            print(f"  Content: {preview}")

        if "errorLines" in doc:
            print(f"  Error Lines: {doc['errorLines']}")

        if "errorCount" in doc:
            print(f"  Error Count: {doc['errorCount']}")

        if "matchedEmails" in doc:
            print(f"  Matched Emails: {[m['match'] for m in doc['matchedEmails']]}")

        if "ipAddresses" in doc:
            print(f"  IP Addresses: {[m['match'] for m in doc['ipAddresses']]}")

        print()


# =============================================================================
# Setup Functions
# =============================================================================

# wait_for_index() is imported from schema.py (shared with perf_test.py).
# It prints one dot per 5-second tick — much less noisy than the old
# wait_for_index_ready() which printed a status line on every poll.


def _index_is_ready(collection) -> bool:
    """Return True if the search index exists and is in READY state."""
    for idx in collection.list_search_indexes():
        if idx["name"] == SEARCH_INDEX_NAME and idx["status"] == "READY":
            return True
    return False


def _index_needs_rebuild(collection) -> bool:
    """Return True if the READY index is missing storedSource or the content multi-field.

    These were added in a later schema revision. Any index built before them
    must be rebuilt (by dropping the collection) to pick up the new definition.
    """
    for idx in collection.list_search_indexes():
        if idx["name"] == SEARCH_INDEX_NAME and idx["status"] == "READY":
            defn = idx.get("latestDefinition", {})
            if "storedSource" not in defn:
                return True
            content = defn.get("mappings", {}).get("fields", {}).get("content", {})
            if "multi" not in content:
                return True
            if "metadata" not in defn.get("mappings", {}).get("fields", {}):
                return True
            return False
    return True


def setup_database(client: MongoClient, reset: bool = False) -> None:
    """Set up the database and load sample data.

    reset=False (default): skip if the collection already has the expected
    documents — avoids the 60-120 s index rebuild on every run.
    reset=True: always drop and recreate (useful after schema/data changes).
    """
    print_header("Setting Up Database")

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    if not reset:
        count = collection.count_documents({})
        if count == len(SAMPLE_DOCUMENTS) and _index_is_ready(collection):
            schema_ok = (
                collection.find_one({"content_lc": {"$exists": True}}) is not None
                and not _index_needs_rebuild(collection)
            )
            if schema_ok:
                print(f"Collection '{COLLECTION_NAME}' already has {count} docs and index is READY — skipping setup.")
                print("  (run with --reset to force a full rebuild)")
                return
            print("Schema update detected — rebuilding collection and index.")

    # collection.drop() also removes all associated search indexes automatically.
    collection.drop()
    print(f"Dropped existing collection '{COLLECTION_NAME}'")

    collection.insert_many(SAMPLE_DOCUMENTS)
    print(f"Inserted {len(SAMPLE_DOCUMENTS)} sample documents")

    print_subheader("Inserted Documents")
    for doc in collection.find():
        print(f"  - {doc['filename']}: {doc['metadata']['type']}")


def setup_search_index(client: MongoClient, reset: bool = False) -> None:
    """Create the Atlas Search index.

    Skipped automatically when reset=False and the index is already READY.
    When reset=True the collection was already dropped by setup_database(),
    so we just create a fresh index and wait.
    """
    print_header("Setting Up Atlas Search Index")

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    if not reset and _index_is_ready(collection):
        print(f"Index '{SEARCH_INDEX_NAME}' is already READY — skipping.")
        return

    search_index_model = SearchIndexModel(
        definition=SEARCH_INDEX_DEFINITION,
        name=SEARCH_INDEX_NAME
    )
    collection.create_search_index(model=search_index_model)
    print(f"Created search index '{SEARCH_INDEX_NAME}'")

    wait_for_index(collection, SEARCH_INDEX_NAME)


# =============================================================================
# Example Functions
# =============================================================================

def example_1_case_sensitive_grep(client: MongoClient, limit: int = 10) -> None:
    """
    Example 1: Case-Sensitive Grep (like PostgreSQL ~)
    Find documents containing ERROR log entries with timestamps.

    Uses compound.filter (not a bare regex operator) because this is a pure
    grep/filter — there is no relevance ranking intent.  compound.filter skips
    scoring entirely, which is faster and semantically honest.
    """
    print_header("Example 1: Case-Sensitive Grep (like ~)")
    print("PostgreSQL equivalent: SELECT * FROM documents WHERE content ~ 'ERROR [0-9]{4}-[0-9]{2}-[0-9]{2}'")
    print("Atlas Search tip: use compound.filter for grep-style filters — no scoring overhead.")

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$search": {
                "index": SEARCH_INDEX_NAME,
                # compound.filter: the regex must match, but contributes no score.
                # This is the correct framing for a grep-style filter query.
                "compound": {
                    "filter": [
                        {"regex": {"path": "content",
                                   "query": "(.*)ERROR [0-9]{4}-[0-9]{2}-[0-9]{2}(.*)"}}
                    ]
                },
                "returnStoredSource": True,
            }
        },
        {"$limit": limit},
        {"$project": {"filename": 1, "content": 1, "metadata": 1}}
    ]

    results = list(collection.aggregate(pipeline))
    print_results(results)


def example_2_case_insensitive_grep(client: MongoClient, limit: int = 10) -> None:
    """
    Example 2: Case-Insensitive Grep (like PostgreSQL ~*)
    Find documents containing email patterns (case-insensitive).

    The Atlas Search regex operator does NOT support the (?i) inline Lucene
    flag. The correct approach is to query the pre-lowercased "content_lc"
    field (indexed with lucene.keyword) using a fully lowercase pattern.
    """
    print_header("Example 2: Case-Insensitive Grep (like ~*)")
    print("PostgreSQL equivalent: SELECT * FROM documents WHERE content ~* '[a-z]+@[a-z]+\\.(com|org)'")
    print("Atlas Search tip: query the pre-lowercased 'content_lc' field — (?i) is NOT supported.")

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$search": {
                "index": SEARCH_INDEX_NAME,
                "regex": {
                    # content_lc stores content.lower() indexed with lucene.keyword.
                    # Pattern is all lowercase — no (?i) needed and not supported.
                    "path": "content_lc",
                    "query": "(.*)[a-z]+@[a-z]+\\.(com|org)(.*)"
                },
                "returnStoredSource": True,
            }
        },
        {"$limit": limit},
        {
            "$project": {
                "filename": 1,
                "content": 1,
                "metadata": 1,
                "matchedEmails": {
                    "$regexFindAll": {
                        "input": "$content",
                        "regex": "[A-Za-z]+@[A-Za-z]+\\.(com|org)",
                        "options": "i"
                    }
                },
                "score": {"$meta": "searchScore"}
            }
        }
    ]

    results = list(collection.aggregate(pipeline))
    print_results(results)


def example_3_json_field_patterns(client: MongoClient, limit: int = 10) -> None:
    """
    Example 3: Grep for JSON Field Patterns
    Find documents containing specific JSON structures.
    """
    print_header("Example 3: Grep for JSON Field Patterns")
    print("PostgreSQL equivalent: SELECT * FROM documents WHERE content ~ '\"port\":\\s*5432'")
    print('Lucene regex tips: \\s is unsupported — use [ ]*; " must be escaped as \\" to match literally.')

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$search": {
                "index": SEARCH_INDEX_NAME,
                "regex": {
                    "path": "content",
                    # In Lucene regex, " is a string-literal delimiter, not a literal character.
                    # Use \" (escaped) to match a real double-quote in the document content.
                    # Raw string r'...' keeps the backslash intact so Python doesn't consume it.
                    # \s is also unsupported — use [ ]* for optional whitespace.
                    "query": r'(.*)(\"port\":[ ]*5432)(.*)'
                },
                "returnStoredSource": True,
            }
        },
        {"$limit": limit},
        {
            "$project": {
                "filename": 1,
                "content": 1,
                "metadata": 1,
                "score": {"$meta": "searchScore"}
            }
        }
    ]

    results = list(collection.aggregate(pipeline))
    print_results(results)


def example_4_multi_pattern_grep(client: MongoClient, limit: int = 10) -> None:
    """
    Example 4: Multi-Pattern Grep Search
    Find documents matching multiple patterns (like grep -E 'pattern1|pattern2').

    Improvement: A single regex with alternation replaces three separate
    $search compound/should clauses, making the pipeline simpler and cheaper.
    """
    print_header("Example 4: Multi-Pattern Grep (like grep -E)")
    print("PostgreSQL equivalent: SELECT * FROM documents WHERE content ~ '(Exception|Error|WARN)'")
    print("Atlas Search tip: use regex alternation instead of compound/should for simpler pipelines.")

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$search": {
                "index": SEARCH_INDEX_NAME,
                # Single regex with alternation — equivalent to grep -E 'Exception|Error|WARN'
                "regex": {
                    "path": "content",
                    "query": "(.*)(Exception|Error|WARN)(.*)"
                },
                "returnStoredSource": True,
            }
        },
        {"$limit": limit},
        {
            "$project": {
                "filename": 1,
                "content": 1,
                "metadata": 1,
                "score": {"$meta": "searchScore"}
            }
        }
    ]

    results = list(collection.aggregate(pipeline))
    print_results(results)


def example_5_grep_with_context(client: MongoClient, limit: int = 10) -> None:
    """
    Example 5: Grep with Context (like grep -C)
    Find matches and extract surrounding context.
    """
    print_header("Example 5: Grep with Context (like grep -C)")
    print("Find ERROR entries and extract matching lines with count")

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$search": {
                "index": SEARCH_INDEX_NAME,
                "regex": {
                    "path": "content",
                    "query": "(.*)ERROR(.*)"
                },
                "returnStoredSource": True,
            }
        },
        {"$limit": limit},
        {
            "$project": {
                "filename": 1,
                "metadata": 1,
                "score": {"$meta": "searchScore"},
                "errorLines": {
                    "$filter": {
                        "input": {"$split": ["$content", "\n"]},
                        "cond": {"$regexMatch": {"input": "$$this", "regex": "ERROR"}}
                    }
                },
                "errorCount": {
                    "$size": {
                        "$regexFindAll": {
                            "input": "$content",
                            "regex": "ERROR"
                        }
                    }
                }
            }
        }
    ]

    results = list(collection.aggregate(pipeline))
    print_results(results, show_content=False)


def example_6_ip_address_grep(client: MongoClient, limit: int = 10) -> None:
    """
    Example 6: Grep for IP Addresses
    Find documents containing IP address patterns.
    """
    print_header("Example 6: Grep for IP Addresses")
    print("PostgreSQL equivalent: SELECT * FROM documents WHERE content ~ '[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+'")

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$search": {
                "index": SEARCH_INDEX_NAME,
                "regex": {
                    "path": "content",
                    "query": "(.*)[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+(.*)"
                },
                "returnStoredSource": True,
            }
        },
        {"$limit": limit},
        {
            "$project": {
                "filename": 1,
                "content": 1,
                "metadata": 1,
                "ipAddresses": {
                    "$regexFindAll": {
                        "input": "$content",
                        "regex": "[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+"
                    }
                },
                "score": {"$meta": "searchScore"}
            }
        }
    ]

    results = list(collection.aggregate(pipeline))
    print_results(results)


def example_7_negation_grep(client: MongoClient, limit: int = 10) -> None:
    """
    Example 7: Negation Grep (like grep -v or PostgreSQL !~)
    Find documents NOT containing a pattern.

    Improvement: The match-all wildcard uses `filter` (not `must`) so it
    doesn't waste cycles computing a relevance score for the always-true
    clause — only the mustNot exclusion matters here.
    """
    print_header("Example 7: Negation Grep (like !~)")
    print("PostgreSQL equivalent: SELECT * FROM documents WHERE content !~ 'ERROR'")
    print("Atlas Search tip: put non-scoring filters in 'filter', not 'must', for better performance.")

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$search": {
                "index": SEARCH_INDEX_NAME,
                "compound": {
                    # 'filter' matches all docs without scoring overhead (faster than 'must')
                    "filter": [
                        {
                            "wildcard": {
                                "path": "content",
                                "query": "*",
                                "allowAnalyzedField": True
                            }
                        }
                    ],
                    "mustNot": [
                        {
                            "regex": {
                                "path": "content",
                                "query": "(.*)ERROR(.*)"
                            }
                        }
                    ]
                },
                "returnStoredSource": True,
            }
        },
        {"$limit": limit},
        {
            "$project": {
                "filename": 1,
                "content": 1,
                "metadata": 1,
                "score": {"$meta": "searchScore"}
            }
        }
    ]

    results = list(collection.aggregate(pipeline))
    print_results(results)


def example_8_text_vs_regex(client: MongoClient, limit: int = 10) -> None:
    """
    Example 8: Text operator vs Regex — choosing the right tool.

    For plain keyword presence, the text operator uses the inverted index
    (O(matching docs)), while regex must scan every token in the field
    (O(all docs)).  Use regex only when you actually need pattern matching.
    """
    print_header("Example 8: Text vs Regex — Choosing the Right Tool")
    print("For simple keyword presence, prefer the text operator (inverted index).")
    print("Use regex only when a pattern is required — it cannot use the inverted index.")

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    print_subheader("text operator — 'ERROR' via inverted index (content.std, lucene.standard)")
    text_pipeline = [
        {
            "$search": {
                "index": SEARCH_INDEX_NAME,
                # Multi-analyzer field: {"value": field, "multi": name} — NOT "field.name"
                "text": {
                    "path": {"value": "content", "multi": "std"},
                    "query": "ERROR"
                },
                "returnStoredSource": True,
            }
        },
        {"$limit": limit},
        {
            "$project": {
                "filename": 1,
                "metadata": 1,
                "score": {"$meta": "searchScore"}
            }
        }
    ]
    print_results(list(collection.aggregate(text_pipeline)), show_content=False)

    print_subheader("regex operator — same keyword, no index benefit (full token scan)")
    regex_pipeline = [
        {
            "$search": {
                "index": SEARCH_INDEX_NAME,
                "regex": {
                    "path": "content",
                    "query": "(.*)ERROR(.*)"
                },
                "returnStoredSource": True,
            }
        },
        {"$limit": limit},
        {
            "$project": {
                "filename": 1,
                "metadata": 1,
                "score": {"$meta": "searchScore"}
            }
        }
    ]
    print_results(list(collection.aggregate(regex_pipeline)), show_content=False)
    print("  Both return the same documents; text is faster at scale.")
    print("  Use text/phrase for keywords, regex for patterns (timestamps, IPs, emails).")


def example_9_facets(client: MongoClient, limit: int = 10) -> None:  # noqa: ARG001 (limit unused; $searchMeta returns aggregation not docs)
    """
    Example 9: $searchMeta facets — count matched documents by type.

    $searchMeta applies the regex filter and counts bucket membership in one
    query, with no double scan.  Useful for sidebar counts (e.g. "Logs: 2,
    Config: 1") alongside a search results page.
    """
    print_header("Example 9: Faceted Counts with $searchMeta")
    print("How many documents of each type contain an error or warning? One query, no double scan.")
    print("PostgreSQL equivalent: SELECT metadata->>'type', COUNT(*) ... GROUP BY 1")

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$searchMeta": {
                "index": SEARCH_INDEX_NAME,
                "facet": {
                    "operator": {
                        "regex": {
                            "path": "content",
                            # Broad pattern captures log-level tokens AND exception class names,
                            # matching across multiple document types for a richer facet result.
                            "query": "(.*)(Exception|Error|WARN)(.*)"
                        }
                    },
                    "facets": {
                        "byType": {
                            "type": "string",
                            "path": "metadata.type"
                        }
                    }
                }
            }
        }
    ]

    meta = list(collection.aggregate(pipeline))
    if not meta:
        print("No results.")
        return

    result = meta[0]
    total = result.get("count", {}).get("lowerBound", 0)
    print(f"\nDocuments matching (Exception|Error|WARN): {total}")
    print("\nBreakdown by document type:")
    for bucket in result.get("facet", {}).get("byType", {}).get("buckets", []):
        print(f"  {bucket['_id']:<20} {bucket['count']:>4} docs")
    print()


def example_10_compound_text_regex(client: MongoClient, limit: int = 10) -> None:
    """
    Example 10: Compound text pre-filter + regex refinement (production pattern).

    This is the correct way to use $search regex in production workloads where
    a selective keyword can narrow candidate documents before regex is applied:

      Stage 1 — compound.filter[text]: inverted-index term lookup → O(matching docs)
                 Reduces candidates from N to a small set.
      Stage 2 — compound.must[regex]:  regex applied only to those candidates.
                 Skips the full BSON collection scan that $regex (MQL) must do.

    This pattern wins over $regex (MQL) when:
      • The text pre-filter is highly selective (rare token in the corpus).
      • The regex provides additional filtering beyond what the text found.
      • You need ALL matching documents (not just a paginated top-N).
    """
    print_header("Example 10: Compound text pre-filter + regex refinement")
    print("PostgreSQL equivalent: WHERE content ~ 'WARN.*[0-9]{4}-[0-9]{2}-[0-9]{2}'")
    print("Atlas Search tip: use a selective text filter to narrow candidates, then refine with regex.")
    print("  Stage 1 (text): inverted-index lookup — O(matching docs), not O(all docs).")
    print("  Stage 2 (regex): regex applied only to the small candidate set.")

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$search": {
                "index": SEARCH_INDEX_NAME,
                "compound": {
                    # filter[text]: inverted-index lookup for "WARN" — fast, O(matching).
                    # References the lucene.standard multi-field using {"value":…, "multi":…}
                    # NOT "content.std" dot notation — the dot form is not valid for text queries.
                    "filter": [
                        {
                            "text": {
                                "path": {"value": "content", "multi": "std"},
                                "query": "WARN",
                            }
                        }
                    ],
                    # must[regex]: applied only to the WARN candidates, not the whole corpus.
                    "must": [
                        {
                            "regex": {
                                "path": "content",
                                "query": "(.*)WARN [0-9]{4}-[0-9]{2}-[0-9]{2}(.*)",
                            }
                        }
                    ],
                },
                "concurrent": True,           # parallelise Lucene segment scan (S20+)
                "returnStoredSource": True,   # serve from Lucene, skip mongot→mongod hop
            }
        },
        {"$limit": limit},
        {
            "$project": {
                "filename": 1,
                "content": 1,
                "metadata": 1,
                "score": {"$meta": "searchScore"},
            }
        },
    ]

    results = list(collection.aggregate(pipeline))
    print_results(results)
    print("  ✓ compound (text + regex) is the recommended production pattern.")
    print("    text clause uses the inverted index; regex refines only the candidate set.")


# =============================================================================
# Main Execution
# =============================================================================

def main():
    """Run Atlas Search regex examples (all or a specific one)."""
    parser = argparse.ArgumentParser(description="MongoDB Atlas Search Regex Demo")
    parser.add_argument(
        "--reset", action="store_true",
        help="Drop and rebuild the collection + search index (takes 60-120 s). "
             "Default: reuse existing setup for instant startup."
    )
    parser.add_argument(
        "--limit", type=int, default=10, metavar="N",
        help="Max documents returned per example (default: 10)."
    )
    parser.add_argument(
        "--example", type=int, default=None, metavar="N",
        help="Run only example N (1-10). Default: run all examples."
    )
    args = parser.parse_args()

    # Ordered dispatch table — preserves run-all sequence.
    examples = {
        1:  example_1_case_sensitive_grep,
        2:  example_2_case_insensitive_grep,
        3:  example_3_json_field_patterns,
        4:  example_4_multi_pattern_grep,
        5:  example_5_grep_with_context,
        6:  example_6_ip_address_grep,
        7:  example_7_negation_grep,
        8:  example_8_text_vs_regex,
        9:  example_9_facets,
        10: example_10_compound_text_regex,
    }

    if args.example is not None and args.example not in examples:
        sys.exit(f"Unknown example {args.example}. Valid range: 1–{max(examples)}.")

    print("\n" + "=" * 70)
    print(" MongoDB Atlas Search Regex Examples")
    print(" Grep-style Pattern Matching Demo")
    print("=" * 70)
    print(f"\nConnecting to MongoDB...")

    # serverSelectionTimeoutMS: fail fast with a clear error instead of
    # hanging for 30 s if the URI or network is misconfigured.
    with MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000) as client:
        try:
            client.admin.command("ping")
        except Exception as exc:
            sys.exit(f"Connection failed: {exc}")

        print("Successfully connected to MongoDB!")
        setup_database(client, reset=args.reset)
        setup_search_index(client, reset=args.reset)

        to_run = {args.example: examples[args.example]} if args.example else examples
        for fn in to_run.values():
            fn(client, limit=args.limit)

        if args.example is None:
            print_header("Summary — PostgreSQL to Atlas Search Regex Mapping")
            print(f"""
  PostgreSQL operator   Atlas Search equivalent
  --------------------  --------------------------------------------------------
  content ~ 'pat'       compound.filter: [regex {{ path:"content", query:"(.*)pat(.*)" }}]
  content ~* 'pat'      regex: {{ path: "content_lc", query: "(.*)pat(.*)" }}
                        (content_lc = content.lower(), lucene.keyword;
                         (?i) inline flag is NOT supported by Atlas Search regex)
  content !~ 'pat'      compound: {{ filter: [wildcard *], mustNot: [regex pat] }}
  content ~ 'a|b|c'     regex: {{ query: "(.*)(a|b|c)(.*)" }}
                        (alternation beats separate compound/should clauses)
  LIKE '%keyword%'      text: {{ path: {{"value":"content","multi":"std"}}, query:"kw" }}
                        (inverted index; use text for keywords, regex for patterns)
  selective + pattern   compound: {{ filter:[text kw], must:[regex pat] }}  ← Example 10
                        (text pre-filter narrows candidates; regex refines the small set)
  GROUP BY type         $searchMeta + facet (Example 9) — counts in one query

  Key rules:
  • lucene.keyword on the field stores the full value as one token so the
    regex can span the entire string — required for the regex operator.
  • Wrap patterns with (.*) so the match can start/end anywhere in the value.
  • Lucene regex is NOT PCRE: \\s, \\d, \\w unsupported → use [ ]*, [0-9], etc.
  • In Lucene regex, " is a string-literal delimiter — escape it as \\" to match
    a literal double-quote (use a Python raw string r'...' so \\ reaches Lucene).
  • Case-insensitive: store content.lower() in content_lc — (?i) is unsupported.
  • Non-scoring filters go in compound.filter, not must (no relevance overhead).
  • Add returnStoredSource:true + storedSource in the index to skip the extra
    mongot→mongod round-trip per hit.
  • Results limited to {args.limit} per example (--limit flag).
""")


if __name__ == "__main__":
    main()
