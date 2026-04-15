"""
MongoDB Atlas Search Regex Examples
Demonstrates grep-style pattern matching similar to PostgreSQL's ~ and ~* operators

Requirements:
    pip install pymongo python-dotenv
"""

import os
import time
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.operations import SearchIndexModel

# Load environment variables from .env file
load_dotenv()

# =============================================================================
# Configuration
# =============================================================================

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://<username>:<password>@<cluster>.mongodb.net/")
DATABASE_NAME = os.getenv("DATABASE_NAME", "regex_demo")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "documents")
SEARCH_INDEX_NAME = os.getenv("SEARCH_INDEX_NAME", "content_search")


# =============================================================================
# Sample Data
# Note: No "contentLower" field needed — we use the (?i) inline Lucene flag
#       directly in the regex pattern for case-insensitive matching.
# =============================================================================

SAMPLE_DOCUMENTS = [
    {
        "_id": 1,
        "filename": "server_logs.txt",
        "content": "ERROR 2024-01-15 10:23:45 Connection timeout on port 5432\nWARN 2024-01-15 10:24:00 Retry attempt 1\nERROR 2024-01-15 10:25:00 Database connection failed",
        "metadata": {"type": "log", "server": "prod-db-01"}
    },
    {
        "_id": 2,
        "filename": "config.json",
        "content": '{"database": {"host": "localhost", "port": 5432, "username": "admin"}, "cache": {"enabled": true, "ttl": 3600}}',
        "metadata": {"type": "config", "server": "prod-api-01"}
    },
    {
        "_id": 3,
        "filename": "api_response.json",
        "content": '{"status": "success", "data": {"users": [{"id": 1, "email": "John@Example.com"}, {"id": 2, "email": "Jane@Test.org"}]}}',
        "metadata": {"type": "api_response", "endpoint": "/users"}
    },
    {
        "_id": 4,
        "filename": "error_dump.txt",
        "content": "Stack trace: NullPointerException at com.app.Service.process(Service.java:42)\nCaused by: IllegalArgumentException at com.app.Validator.check(Validator.java:15)",
        "metadata": {"type": "error", "application": "backend-service"}
    },
    {
        "_id": 5,
        "filename": "access_log.txt",
        "content": "INFO 2024-01-15 08:00:00 User admin logged in from 192.168.1.100\nINFO 2024-01-15 08:05:00 User john logged in from 10.0.0.50\nWARN 2024-01-15 08:10:00 Failed login attempt for user unknown",
        "metadata": {"type": "log", "server": "auth-service"}
    }
]


# =============================================================================
# Search Index Definition
# Explicit mapping (dynamic: false) for a smaller, faster index.
# Only the fields actually queried are indexed — no unused contentLower field.
# "content" uses lucene.keyword so the regex operator sees the full raw token.
# =============================================================================

SEARCH_INDEX_DEFINITION = {
    "mappings": {
        "dynamic": False,
        "fields": {
            "content": {
                "type": "string",
                "analyzer": "lucene.keyword"
            },
            "filename": {
                "type": "string",
                "analyzer": "lucene.keyword"
            }
        }
    }
}


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

def wait_for_index_ready(collection, index_name: str, max_wait: int = 120) -> bool:
    """Wait for the search index to be ready."""
    print(f"Waiting for search index '{index_name}' to be ready...")

    start_time = time.time()
    while time.time() - start_time < max_wait:
        indexes = list(collection.list_search_indexes())
        for index in indexes:
            if index["name"] == index_name:
                if index["status"] == "READY":
                    print(f"Index '{index_name}' is ready!")
                    return True
                else:
                    print(f"Index status: {index['status']}")
        time.sleep(5)

    print(f"Timeout waiting for index '{index_name}'")
    return False


def setup_database(client: MongoClient) -> None:
    """Set up the database and load sample data."""
    print_header("Setting Up Database")

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    # Drop existing collection
    collection.drop()
    print(f"Dropped existing collection '{COLLECTION_NAME}'")

    # Insert sample documents
    collection.insert_many(SAMPLE_DOCUMENTS)
    print(f"Inserted {len(SAMPLE_DOCUMENTS)} sample documents")

    # Display inserted documents
    print_subheader("Inserted Documents")
    for doc in collection.find():
        print(f"  - {doc['filename']}: {doc['metadata']['type']}")


def setup_search_index(client: MongoClient) -> None:
    """Create the Atlas Search index."""
    print_header("Setting Up Atlas Search Index")

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    # Check if index already exists
    existing_indexes = list(collection.list_search_indexes())
    for index in existing_indexes:
        if index["name"] == SEARCH_INDEX_NAME:
            print(f"Dropping existing index '{SEARCH_INDEX_NAME}'")
            collection.drop_search_index(SEARCH_INDEX_NAME)
            time.sleep(5)
            break

    # Create search index
    search_index_model = SearchIndexModel(
        definition=SEARCH_INDEX_DEFINITION,
        name=SEARCH_INDEX_NAME
    )

    collection.create_search_index(model=search_index_model)
    print(f"Created search index '{SEARCH_INDEX_NAME}'")

    # Wait for index to be ready
    wait_for_index_ready(collection, SEARCH_INDEX_NAME)


# =============================================================================
# Example Functions
# =============================================================================

def example_1_case_sensitive_grep(client: MongoClient) -> None:
    """
    Example 1: Case-Sensitive Grep (like PostgreSQL ~)
    Find documents containing ERROR log entries with timestamps.
    """
    print_header("Example 1: Case-Sensitive Grep (like ~)")
    print("PostgreSQL equivalent: SELECT * FROM documents WHERE content ~ 'ERROR [0-9]{4}-[0-9]{2}-[0-9]{2}'")

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$search": {
                "index": SEARCH_INDEX_NAME,
                "regex": {
                    "path": "content",
                    "query": "(.*)ERROR [0-9]{4}-[0-9]{2}-[0-9]{2}(.*)"
                }
            }
        },
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


def example_2_case_insensitive_grep(client: MongoClient) -> None:
    """
    Example 2: Case-Insensitive Grep (like PostgreSQL ~*)
    Find documents containing email patterns (case-insensitive).

    Improvement: Use the (?i) inline Lucene flag directly on the "content"
    field instead of maintaining a redundant "contentLower" field in every
    document. This halves storage cost and keeps documents clean.
    """
    print_header("Example 2: Case-Insensitive Grep (like ~*)")
    print("PostgreSQL equivalent: SELECT * FROM documents WHERE content ~* '[a-z]+@[a-z]+\\.(com|org)'")
    print("Atlas Search tip: prefix the pattern with (?i) for case-insensitive matching — no extra field needed.")

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$search": {
                "index": SEARCH_INDEX_NAME,
                "regex": {
                    "path": "content",
                    # (?i) is the Lucene inline case-insensitive flag —
                    # eliminates the need for a separate "contentLower" field.
                    "query": "(?i)(.*)[a-z]+@[a-z]+\\.(com|org)(.*)"
                }
            }
        },
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


def example_3_json_field_patterns(client: MongoClient) -> None:
    """
    Example 3: Grep for JSON Field Patterns
    Find documents containing specific JSON structures.
    """
    print_header("Example 3: Grep for JSON Field Patterns")
    print("PostgreSQL equivalent: SELECT * FROM documents WHERE content ~ '\"port\":\\s*5432'")

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$search": {
                "index": SEARCH_INDEX_NAME,
                "regex": {
                    "path": "content",
                    "query": "(.*)(\"port\":\\s*5432)(.*)"
                }
            }
        },
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


def example_4_multi_pattern_grep(client: MongoClient) -> None:
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
                }
            }
        },
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


def example_5_grep_with_context(client: MongoClient) -> None:
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
                }
            }
        },
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


def example_6_ip_address_grep(client: MongoClient) -> None:
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
                }
            }
        },
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


def example_7_negation_grep(client: MongoClient) -> None:
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
                }
            }
        },
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


# =============================================================================
# Main Execution
# =============================================================================

def main():
    """Main function to run all examples."""
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
            print("Successfully connected to MongoDB!")

            setup_database(client)
            setup_search_index(client)

            print("\nWaiting 5 seconds for index synchronization...")
            time.sleep(5)

            example_1_case_sensitive_grep(client)
            example_2_case_insensitive_grep(client)
            example_3_json_field_patterns(client)
            example_4_multi_pattern_grep(client)
            example_5_grep_with_context(client)
            example_6_ip_address_grep(client)
            example_7_negation_grep(client)

            print_header("Summary — PostgreSQL to Atlas Search Regex Mapping")
            print("""
  PostgreSQL operator   Atlas Search equivalent
  --------------------  --------------------------------------------------------
  content ~ 'pat'       regex: { path: "content", query: "(.*)pat(.*)" }
  content ~* 'pat'      regex: { path: "content", query: "(?i)(.*)pat(.*)" }
                        ((?i) inline Lucene flag — no extra field needed)
  content !~ 'pat'      compound: { filter: [wildcard *], mustNot: [regex pat] }
  content ~ 'a|b|c'     regex: { query: "(.*)(a|b|c)(.*)" }
                        (use alternation, not separate compound/should clauses)

  Key rules:
  • lucene.keyword analyzer is required on the field so the whole value is
    one token and the regex can span the full string.
  • Wrap patterns with (.*) prefix/suffix to match anywhere in the value.
  • Use (?i) for case-insensitive matching instead of storing a lowercased copy.
  • Prefer compound.filter over compound.must for non-scoring criteria.
""")

        except Exception as exc:
            print(f"\nERROR: {exc}")
            raise


if __name__ == "__main__":
    main()
