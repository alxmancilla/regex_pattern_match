"""schema.py — Shared Atlas Search index definition and index-wait utility.

Imported by both main.py and perf_test.py so the schema is defined exactly
once.  Any change to the index mapping only needs updating here.

Index design summary
────────────────────
  content        lucene.keyword   Full field value as one token → regex can
                                  match across the entire string.
    .multi.std   lucene.standard  Word-tokenised → inverted-index term lookup
                                  for the text operator and compound pre-filters.
  content_lc     lucene.keyword   Pre-lowercased copy of content.  Atlas Search
                                  regex does NOT support (?i); querying this field
                                  with a lowercase pattern is the correct fix.
  filename       lucene.keyword   Exact-match field.
  metadata.type  stringFacet      Enables $searchMeta bucket counts (Example 9).
                                  Harmless in perf_test.py which never runs
                                  $searchMeta.
  storedSource                    Keeps listed fields inside the Lucene index
                                  (mongot).  Combined with returnStoredSource:true
                                  in queries, this skips the per-hit round-trip
                                  from mongot back to mongod.
"""

import time

SEARCH_INDEX_DEFINITION: dict = {
    "mappings": {
        "dynamic": False,
        "fields": {
            "content": {
                "type": "string",
                "analyzer": "lucene.keyword",
                "multi": {
                    # lucene.standard tokenises words → inverted-index term
                    # lookup for the text operator (compound pre-filters).
                    "std": {"type": "string", "analyzer": "lucene.standard"},
                },
            },
            # Pre-lowercased copy of content for case-insensitive regex.
            # (?i) inline flags are NOT supported by the Atlas Search regex
            # operator.  Multi-field paths (e.g. content.lc) are also not
            # reliably queryable via regex — a separate field is required.
            "content_lc": {"type": "string", "analyzer": "lucene.keyword"},
            "filename":   {"type": "string", "analyzer": "lucene.keyword"},
            # stringFacet enables $searchMeta bucket counts by document type.
            "metadata": {
                "type": "document",
                "fields": {
                    "type": {"type": "stringFacet"},
                },
            },
        },
    },
    # storedSource keeps these fields inside the Lucene index (mongot).
    # Set returnStoredSource:true in queries to serve docs directly from
    # mongot, avoiding a separate round-trip to mongod per matching document.
    "storedSource": {
        "include": ["filename", "content", "content_lc", "metadata"],
    },
}


def wait_for_index(collection, index_name: str, max_wait: int = 600) -> bool:
    """Poll until the named Atlas Search index is READY *and* actually queryable.

    Atlas can briefly report status=READY before the underlying mongot sync
    completes, causing the first real query to fail with INITIAL_SYNC.  To
    guard against this, after each READY status we fire a lightweight probe
    query ($search exists); only when that succeeds do we declare victory.

    Prints one dot per poll tick and finishes with ' ready!' or ' timed out!'.
    Used by both main.py and perf_test.py.

    Typical build times:
        2 K  docs →  ~10 s
        10 K docs →  ~20 s
        100 K docs → ~90–180 s  (default max_wait=600 gives ample headroom)

    Returns True when READY + queryable, False on timeout or terminal failure
    states (FAILED, STALE) so the caller can decide whether to abort.
    """
    _TERMINAL_FAIL = {"FAILED", "STALE", "DOES_NOT_EXIST"}
    # Probe pipeline: same regex operator the benchmark uses, on the same field
    # (lucene.keyword — no allowAnalyzedField needed).  ".*ERROR.*" matches the
    # majority of synthetic documents, so getting ≥1 result confirms that mongot
    # has actually synced documents into the index.
    #
    # Why not `exists` or bare `.*`:
    #   • `exists` succeeds even in INITIAL_SYNC (served from document store).
    #   • `.*` with allowAnalyzedField=True returns an empty list without error
    #     during the false-READY window (control-plane READY before data sync).
    #   • Checking for ≥1 result is the only reliable signal that data is indexed.
    _PROBE = [
        {"$search": {"index": index_name,
                     "regex": {"query": ".*ERROR.*", "path": "content"},
                     "returnStoredSource": True}},
        {"$limit": 1},
    ]

    print(f"  Waiting for index '{index_name}'", end="", flush=True)
    deadline = time.time() + max_wait
    while time.time() < deadline:
        status = None
        for idx in collection.list_search_indexes():
            if idx["name"] == index_name:
                status = idx["status"]
                break
        if status == "READY":
            # Status says READY — verify data is actually indexed by confirming
            # we get ≥1 hit.  Atlas briefly shows READY before mongot begins
            # the document sync; during that window queries succeed but return
            # empty results, then fail with INITIAL_SYNC once sync starts.
            try:
                if list(collection.aggregate(_PROBE)):
                    print(" ready!")
                    return True
                # Got READY status + no error, but 0 results → data not synced yet.
            except Exception:
                # OperationFailure (INITIAL_SYNC) or any other transient error.
                pass
        elif status in _TERMINAL_FAIL:
            print(f" FAILED (status={status})!")
            return False
        print(".", end="", flush=True)
        time.sleep(5)
    print(" timed out!")
    return False
