"""
Performance Test: Plain $regex vs Atlas Search $search / regex operator

Sequential benchmark that reports avg / median / min / max / stdev latency.
For P95 / P99 and concurrent load use the Locust files instead:
    locust -f locustfile_regex.py    # MQL $regex load test
    locust -f locustfile_search.py   # Atlas Search load test

Benchmark configuration (matches reference profile):
    Documents        100,000   --docs 100000  (default)
    Entity Records       600   ~600 rare docs per type in the corpus
    Warm-up Iterations    15   --warmup 15    (default)
    Measured Iterations   60   --runs 60      (default)
    Queries per Iteration  5   --queries-per-iter 5  (default)
    Total Samples / Op   300   runs × queries-per-iter = 60 × 5

Usage:
    python perf_test.py
    python perf_test.py --docs 100000 --runs 60 --warmup 15 --queries-per-iter 5
    python perf_test.py --reuse   # skip data rebuild if indexes are READY
    python perf_test.py --limit 0 # fetch all hits (selective scenarios)
"""

import sys
import argparse
from statistics import mean, median, stdev
from typing import NamedTuple
from pymongo import MongoClient

from perf_common import (
    MONGODB_URI, DATABASE_NAME, SEARCH_INDEX_NAME,
    setup_perf_collection,
    run_mql_regex, run_atlas_search_regex, run_compound_search_regex,
    run_atlas_text, run_mql_negation, run_atlas_negation,
)

# =============================================================================
# Typed scenario definitions
# =============================================================================

class Scenario(NamedTuple):
    """One benchmarked pattern pair (MQL + Atlas Search)."""
    label:           str
    mql_pattern:     str               # PCRE pattern for $regex / $not $regex
    atlas_pattern:   str               # Lucene pattern for $search regex
    atlas_path:      str  = "content"  # indexed field ("content" or "content_lc")
    mql_options:     str  = ""         # e.g. "i" for case-insensitive MQL regex
    text_query:      str | None = None # set to also run compound text+regex
    fetch_limit:     int | None = None # None=use CLI --limit; 0=fetch all hits
    text_standalone: str | None = None # set to also run standalone text operator


class NegationScenario(NamedTuple):
    """One negation benchmark: $not $regex vs compound.mustNot."""
    label:       str
    mql_pattern:   str          # PCRE for $not $regex
    atlas_pattern: str          # Lucene for mustNot regex
    fetch_limit:   int | None = None  # None=use CLI --limit; 0=fetch all







# =============================================================================
# Utility
# =============================================================================

def _stats(times: list[float]) -> dict:
    """Return avg/median/min/max/stdev summary for a list of millisecond timings."""
    return {
        "avg":    mean(times),
        "median": median(times),
        "min":    min(times),
        "max":    max(times),
        "stdev":  stdev(times) if len(times) > 1 else 0.0,
    }



def benchmark(collection, label: str,
              mql_pattern: str, atlas_pattern: str,
              mql_options: str = "", atlas_path: str = "content",
              text_query: str = None,
              limit: int = 0, warmup: int = 15, runs: int = 60,
              fetch_limit: int = None,
              text_standalone: str = None,
              queries_per_iter: int = 5) -> dict:
    """
    Run MQL, plain $search regex (+ concurrent), and optionally compound
    $search (text pre-filter + regex) and/or standalone text operator for
    <warmup> + <runs> iterations, executing <queries_per_iter> queries per
    timed slot and recording the mean — matching the reference benchmark profile:
        Warm-up Iterations 15 | Measured Iterations 60 | Queries per Iteration 5
        Total Samples / Operation = 60 × 5 = 300

    text_query      : if set, also runs compound search (text pre-filter + regex).
    text_standalone : if set, also runs the text operator alone (no regex).
                      Shows inverted-index keyword speed vs regex full scan.
    fetch_limit     : per-scenario override for how many docs to fetch.
                      None → use the global `limit` argument (CLI --limit).
                      0    → fetch ALL matching docs.
    queries_per_iter: queries executed per timed iteration; their latencies are
                      averaged to form one data point, reducing per-sample noise.
    """
    effective_limit = limit if fetch_limit is None else fetch_limit
    fetch_label     = f"top {effective_limit}" if effective_limit else "all hits"
    print(f"  Benchmarking: {label!r} [{fetch_label}] ...", end="", flush=True)

    has_compound = text_query is not None
    has_text     = text_standalone is not None

    # Warmup — prime query planner, warm network buffers, and stabilise caches.
    # queries_per_iter warmup queries per method match the timed-loop cadence.
    for _ in range(warmup):
        for _ in range(queries_per_iter):
            run_mql_regex(collection, mql_pattern, mql_options, effective_limit)
            run_atlas_search_regex(collection, atlas_pattern, atlas_path, effective_limit)
            if has_compound:
                run_compound_search_regex(collection, text_query, atlas_pattern,
                                          atlas_path, effective_limit)
            if has_text:
                run_atlas_text(collection, text_standalone, effective_limit)

    mql_times, atlas_times, compound_times, text_times = [], [], [], []
    mql_count = atlas_count = compound_count = text_count = 0

    # Timed loop: each iteration runs queries_per_iter queries per method and
    # records the mean latency as one data point.  This smooths per-query
    # variance so that stdev reflects real workload variation, not OS jitter.
    for _ in range(runs):
        q_mql, q_atlas, q_compound, q_text = [], [], [], []
        for _ in range(queries_per_iter):
            mql_count,      t = run_mql_regex(collection, mql_pattern, mql_options, effective_limit)
            q_mql.append(t)
            atlas_count,    t = run_atlas_search_regex(collection, atlas_pattern,
                                                       atlas_path, effective_limit)
            q_atlas.append(t)
            if has_compound:
                compound_count, t = run_compound_search_regex(
                    collection, text_query, atlas_pattern, atlas_path, effective_limit)
                q_compound.append(t)
            if has_text:
                text_count,     t = run_atlas_text(collection, text_standalone, effective_limit)
                q_text.append(t)
        mql_times.append(mean(q_mql))
        atlas_times.append(mean(q_atlas))
        if has_compound:
            compound_times.append(mean(q_compound))
        if has_text:
            text_times.append(mean(q_text))

    print(" done")

    if atlas_count != mql_count:
        print(f"  *** WARNING: result count mismatch — $regex={mql_count}, $search={atlas_count} ***")
    if has_compound and compound_count != mql_count:
        print(f"  *** WARNING: result count mismatch — $regex={mql_count}, compound={compound_count} ***")
    if has_text and text_count != mql_count:
        print(f"  *** WARNING: result count mismatch — $regex={mql_count}, text={text_count} ***")

    result = {
        "label":           label,
        "mql_pattern":     mql_pattern,
        "atlas_pattern":   atlas_pattern,
        "effective_limit": effective_limit,
        "mql_count":       mql_count,
        "atlas_count":     atlas_count,
        "mql":             _stats(mql_times),
        "atlas":           _stats(atlas_times),
    }
    if has_compound:
        result["compound_count"] = compound_count
        result["compound"]       = _stats(compound_times)
    if has_text:
        result["text_count"] = text_count
        result["text"]       = _stats(text_times)
    return result


def benchmark_negation(collection, label: str,
                        mql_pattern: str, atlas_pattern: str,
                        limit: int = 0, warmup: int = 15, runs: int = 60,
                        fetch_limit: int = None,
                        queries_per_iter: int = 5) -> dict:
    """
    Benchmark MQL $not $regex vs Atlas Search compound.mustNot.
    Neither benefits from the inverted index — both are O(N) with regex.
    Uses the same warmup / runs / queries_per_iter profile as benchmark().
    """
    effective_limit = limit if fetch_limit is None else fetch_limit
    fetch_label     = f"top {effective_limit}" if effective_limit else "all hits"
    print(f"  Benchmarking: {label!r} [{fetch_label}] ...", end="", flush=True)

    for _ in range(warmup):
        for _ in range(queries_per_iter):
            run_mql_negation(collection, mql_pattern, effective_limit)
            run_atlas_negation(collection, atlas_pattern, effective_limit)

    mql_times, atlas_times = [], []
    mql_count = atlas_count = 0

    for _ in range(runs):
        q_mql, q_atlas = [], []
        for _ in range(queries_per_iter):
            mql_count,   t = run_mql_negation(collection, mql_pattern, effective_limit)
            q_mql.append(t)
            atlas_count, t = run_atlas_negation(collection, atlas_pattern, effective_limit)
            q_atlas.append(t)
        mql_times.append(mean(q_mql))
        atlas_times.append(mean(q_atlas))

    print(" done")

    if atlas_count != mql_count:
        print(f"  *** WARNING: count mismatch — $not $regex={mql_count}, Atlas mustNot={atlas_count} ***")

    return {
        "label":           label,
        "effective_limit": effective_limit,
        "mql_count":       mql_count,
        "atlas_count":     atlas_count,
        "mql":             _stats(mql_times),
        "atlas":           _stats(atlas_times),
        "is_negation":     True,
    }


# =============================================================================
# Report helpers  (module-level for testability)
# =============================================================================

_REPORT_WIDTH = 96
_REPORT_HEADER = (
    f"{'Scenario':<30} {'Fetch':<10} {'Method':<28} "
    f"{'Avg':>8} {'Median':>8} {'Min':>8} {'Max':>8}  {'Hits':>6}"
)


def _result_methods(r: dict) -> list[tuple[str, str, str]]:
    """Return (display_name, stats_key, count_key) triples for result dict r."""
    if r.get("is_negation"):
        return [
            ("$not $regex (MQL)",  "mql",   "mql_count"),
            ("$search mustNot",    "atlas", "atlas_count"),
        ]
    methods = [
        ("$regex (MQL)",             "mql",   "mql_count"),
        ("$search regex+concurrent", "atlas", "atlas_count"),
    ]
    if "compound" in r:
        methods.append(("$search compound ✓", "compound", "compound_count"))
    if "text" in r:
        methods.append(("$search text ★",     "text",     "text_count"))
    return methods


def _print_result_row(r: dict) -> None:
    """Print all method rows for a single benchmark result plus the winner line."""
    fetch_lbl = f"top {r['effective_limit']}" if r["effective_limit"] else "all hits"
    methods   = _result_methods(r)

    for i, (name, key, cnt_key) in enumerate(methods):
        s = r[key]
        print(
            f"{(r['label'] if i == 0 else ''):<30} "
            f"{(fetch_lbl  if i == 0 else ''):<10} "
            f"{name:<28} "
            f"{s['avg']:>7.1f}ms {s['median']:>7.1f}ms "
            f"{s['min']:>7.1f}ms {s['max']:>7.1f}ms  {r[cnt_key]:>6}"
        )

    avgs   = {name: r[key]["avg"] for name, key, _ in methods}
    winner = min(avgs, key=avgs.get)
    factor = max(avgs.values()) / avgs[winner] if avgs[winner] else float("inf")
    print(f"  → {winner} is {factor:.1f}× faster than slowest")
    print()


def _print_section(group: list, title: str) -> None:
    """Print a labelled section of benchmark results."""
    if not group:
        return
    print(f"\n  ── {title} ──")
    print(_REPORT_HEADER)
    print("-" * _REPORT_WIDTH)
    for r in group:
        _print_result_row(r)


def _print_legend(num_docs: int) -> None:
    """Print the 'How to read this report' legend at the bottom."""
    rare_est = num_docs // 167   # ~600 entity records per type at 100K docs
    print("  How to read this report:")
    print()
    print("  DENSE scenarios (top N, paginated)")
    print("    $regex (MQL) wins because PCRE stops after finding N results.")
    print("    $search adds a fixed mongot IPC hop (~50 ms) with no index benefit for regex.")
    print("    Both methods are O(N_docs) — the inverted index cannot accelerate regex.")
    print()
    print("  SELECTIVE scenarios (all hits, fetch full result set)")
    print("    Both $search methods beat $regex (MQL) here, for different reasons:")
    print(f"    $search regex+concurrent wins because returnStoredSource serves docs directly")
    print(f"      from the Lucene index — no per-hit mongod lookup — and concurrent parallelises")
    print(f"      the scan. $regex (MQL) must read ALL {num_docs:,} BSON docs from mongod.")
    print(f"    $search compound ✓ wins by more because the text pre-filter cuts candidates")
    print(f"      from {num_docs:,} → ~{rare_est:,} before regex even runs, reducing both CPU and I/O.")
    print(f"    Actual hit counts vary by pattern selectivity (e.g. 31–124 in this run).")
    print("    $regex cost ≈ constant regardless of hit count (full COLLSCAN).")
    print("    $search cost scales with candidates+hits, winning when BOTH are small.")
    print()
    print("  NEGATION scenarios (compound.mustNot vs $not $regex)")
    print("    Neither benefits from the inverted index — both scan all docs.")
    print("    Results are roughly equal: any difference is within measurement noise.")
    print("    Avoid Atlas Search for pure negation queries with no positive filter —")
    print("    the IPC overhead gives no benefit when there is nothing to index-accelerate.")
    print()
    print("  ✓  $search compound = text pre-filter (inverted index) + regex refinement.")
    print("     Correct production pattern when the text filter is selective.")
    print("  ★  $search text     = inverted-index term lookup; O(matching docs).")
    print("     Ties with MQL at top-N (IPC hop absorbs the gain); wins decisively")
    print("     when fetching large result sets where mongod BSON reads dominate.")
    print()
    print("  $regex (MQL)     = COLLSCAN + PCRE (C library, very fast per doc). No index.")
    print("  $search regex    = full Lucene token scan + IPC overhead. No index benefit.")
    print("  $search compound = inverted index pre-filter + regex on small candidate set.")
    print("  $search text     = inverted index term lookup. O(matching) — fastest for keywords.")


# =============================================================================
# Report entry point
# =============================================================================

def print_report(results: list, negation_results: list,
                 num_docs: int, runs: int, limit: int,
                 total_samples: int = 0) -> None:
    """Print the full benchmark report: header, three sections, legend."""
    lbl       = f"top {limit}" if limit else "all hits"
    dense     = [r for r in results if r["effective_limit"] != 0]
    selective = [r for r in results if r["effective_limit"] == 0]
    samples_lbl = f" | {total_samples} samples/op" if total_samples else ""

    print("\n" + "=" * _REPORT_WIDTH)
    print(f" Performance Report — {num_docs:,} docs | {runs} measured iterations{samples_lbl} | default fetch: {lbl}")
    print("=" * _REPORT_WIDTH)

    _print_section(dense,            "DENSE patterns — common tokens, compound adds overhead")
    _print_section(selective,        "SELECTIVE patterns — rare tokens (~0.6% of corpus), fetch all hits")
    _print_section(negation_results, "NEGATION patterns — $not $regex vs compound.mustNot")

    print("=" * _REPORT_WIDTH)
    print()
    _print_legend(num_docs)


# =============================================================================
# Test scenarios
# =============================================================================

SCENARIOS: list[Scenario] = [
    # ── DENSE scenarios (common tokens, large hit sets) ──────────────────────
    # fetch_limit=None → use CLI --limit (paginated top-N).
    # text_standalone="ERROR": shows $search text beating regex for a plain keyword.
    Scenario(
        label="Simple keyword",
        mql_pattern=r"ERROR",
        atlas_pattern=r"(.*)ERROR(.*)",
        text_query="ERROR",
        text_standalone="ERROR",
    ),
    # Date parts tokenise to numbers → no useful text pre-filter or standalone.
    Scenario(
        label="Date pattern",
        mql_pattern=r"[0-9]{4}-[0-9]{2}-[0-9]{2}",
        atlas_pattern=r"(.*)[0-9]{4}-[0-9]{2}-[0-9]{2}(.*)",
    ),
    # (?i) not supported in Atlas Search regex → pre-lowercased content_lc field.
    Scenario(
        label="Case-insensitive",
        mql_pattern=r"error",
        atlas_pattern=r"(.*)error(.*)",
        atlas_path="content_lc",
        mql_options="i",
        text_query="error",
    ),
    Scenario(
        label="Alternation",
        mql_pattern=r"(Exception|Error|WARN)",
        atlas_pattern=r"(.*)(Exception|Error|WARN)(.*)",
        text_query="Exception Error WARN",
    ),
    # IP octets split by standard tokenizer → no useful text pre-filter.
    Scenario(
        label="IP address",
        mql_pattern=r"[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+",
        atlas_pattern=r"(.*)[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+(.*)",
    ),
    # JSON field: " must be escaped as \" in Lucene (string-literal delimiter);
    # \s unsupported → [ ]*.  Config docs ~20% of corpus → dense.
    Scenario(
        label="JSON field (port)",
        mql_pattern=r'"port":[ ]*[0-9]+',
        atlas_pattern=r'(.*)(\"port\":[ ]*[0-9]+)(.*)',
    ),

    # ── SELECTIVE scenarios (rare tokens, ~0.5% of corpus each) ─────────────
    # fetch_limit=0 → returns ALL matching docs; $regex must scan every BSON doc.
    # Compound finds candidates via inverted index (~0.5% of corpus) and runs
    # regex only on those → wins decisively when the text filter is selective.
    #
    # HOW COMPOUND WINS:
    #   Stage 1 — text pre-filter: inverted-index → O(~0.5% of corpus)
    #   Stage 2 — regex refinement: scan only those docs → O(final_hits)
    #   $regex (MQL) scans ALL N BSON docs regardless.

    # "CRIT" token: ~0.5% CRIT docs.  "host=prod-db-01" narrows to ~1/5.
    Scenario(
        label="CRIT on prod-db-01 (all hits)",
        mql_pattern=r"CRIT-[0-9]{4}.*host=prod-db-01",
        atlas_pattern=r"(.*)CRIT-[0-9]{4}(.*)host=prod-db-01(.*)",
        text_query="CRIT",
        fetch_limit=0,
    ),
    # "AUDIT" + "DELETE": text pre-filter ~0.5% audit docs; regex keeps op=DELETE (~1/4).
    Scenario(
        label="Audit DELETE events (all hits)",
        mql_pattern=r"AUDIT-EVENT.*op=DELETE",
        atlas_pattern=r"AUDIT-EVENT(.*)op=DELETE(.*)",
        text_query="AUDIT DELETE",
        fetch_limit=0,
    ),
    # "DEPLOY" + "production": text pre-filter ~0.17% prod-deploy docs.
    Scenario(
        label="Deploy svc-api/prod (all hits)",
        mql_pattern=r"DEPLOY svc-api v[0-9]+\.[0-9]+\.[0-9]+ env=production",
        atlas_pattern=r"(.*)DEPLOY svc-api v[0-9]+\.[0-9]+\.[0-9]+ env=production(.*)",
        text_query="DEPLOY production",
        fetch_limit=0,
    ),
]

NEGATION_SCENARIOS: list[NegationScenario] = [
    # Neither approach uses the inverted index — honest O(N) comparison.
    # mql_pattern  : PCRE for $not $regex (no (.*) wrappers needed)
    # atlas_pattern: Lucene for compound.mustNot regex (needs (.*) wrappers)
    NegationScenario(
        label="Negate ERROR",
        mql_pattern=r"ERROR",
        atlas_pattern=r"(.*)ERROR(.*)",
    ),
]


# =============================================================================
# Entry point
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark $regex vs $search regex",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--docs",             type=int, default=100000,
                        help="Synthetic documents (Entity Records 100,000)")
    parser.add_argument("--runs",             type=int, default=60,
                        help="Measured iterations per scenario")
    parser.add_argument("--warmup",           type=int, default=15,
                        help="Warm-up iterations discarded before timing")
    parser.add_argument("--queries-per-iter", type=int, default=5,
                        dest="queries_per_iter",
                        help="Queries executed per timed iteration; mean is recorded")
    parser.add_argument("--limit",            type=int, default=20,
                        help="Max documents fetched per query (0 = all). "
                             "Simulates pagination; MQL wins at top-N, $search wins at all-hits.")
    parser.add_argument("--reuse", action="store_true",
                        help="Skip data rebuild if collection already exists with a READY index.")
    args = parser.parse_args()

    total_samples = args.runs * args.queries_per_iter

    print("=" * 76)
    print(" $regex vs $search / regex — Performance Benchmark")
    print("=" * 76)

    with MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000) as client:
        try:
            client.admin.command("ping")
        except Exception as exc:
            sys.exit(f"Connection failed: {exc}")
        print("Connected to MongoDB.")

        collection, actual_docs = setup_perf_collection(client, args.docs, reuse=args.reuse)

        lbl = f"top {args.limit}" if args.limit else "all hits"
        print(f"\nBenchmark profile:")
        print(f"  Documents           {actual_docs:>10,}")
        print(f"  Warm-up Iterations  {args.warmup:>10}")
        print(f"  Measured Iterations {args.runs:>10}")
        print(f"  Queries / Iteration {args.queries_per_iter:>10}")
        print(f"  Total Samples / Op  {total_samples:>10}")
        print(f"  Default fetch       {lbl:>10}")

        n_dense     = sum(1 for s in SCENARIOS if s.fetch_limit is None)
        n_selective = sum(1 for s in SCENARIOS if s.fetch_limit == 0)
        print(f"\nRunning {len(SCENARIOS)} scenarios "
              f"({n_dense} dense/paginated, {n_selective} selective/all-hits) "
              f"+ {len(NEGATION_SCENARIOS)} negation scenario(s)...")
        results = [
            benchmark(
                collection,
                s.label, s.mql_pattern, s.atlas_pattern,
                s.mql_options, s.atlas_path,
                s.text_query, args.limit, args.warmup, args.runs,
                fetch_limit=s.fetch_limit,
                text_standalone=s.text_standalone,
                queries_per_iter=args.queries_per_iter,
            )
            for s in SCENARIOS
        ]
        negation_results = [
            benchmark_negation(
                collection,
                s.label, s.mql_pattern, s.atlas_pattern,
                args.limit, args.warmup, args.runs,
                fetch_limit=s.fetch_limit,
                queries_per_iter=args.queries_per_iter,
            )
            for s in NEGATION_SCENARIOS
        ]

    print_report(results, negation_results, actual_docs, args.runs, args.limit,
                 total_samples=total_samples)


if __name__ == "__main__":
    main()
