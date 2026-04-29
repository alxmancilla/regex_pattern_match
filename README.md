# MongoDB Atlas Search Regex Pattern Matching Demo

A Python demonstration of grep-style regex pattern matching using MongoDB Atlas Search, with PostgreSQL `~` / `~*` / `!~` operator equivalents, a text-vs-regex comparison, faceted counts, and a full performance benchmark.

## Overview

This project demonstrates how to:
- Use the Atlas Search `regex` operator for pattern matching (case-sensitive, case-insensitive, JSON fields, IP addresses, alternation, negation)
- Use the `text` operator for plain keyword search and understand when *not* to reach for regex
- Use `$searchMeta` facets to count matches by category in a single query
- Avoid Lucene regex pitfalls: `(?i)` unsupported; `"` is a string-literal delimiter (escape as `\"`); `\s`/`\d`/`\w` shorthands unsupported
- Avoid query-design pitfalls: `filter` beats `must` for non-scoring criteria; alternation beats `compound/should` for OR patterns
- Build an explicit, lean index (`dynamic: false`) with `storedSource` to eliminate the mongot→mongod round-trip per hit
- Benchmark four strategies — `$regex` MQL, `$search regex`, compound text+regex, and standalone `text` — across dense, selective, and negation workloads

## Prerequisites

- Python 3.8+
- MongoDB Atlas cluster (any tier; M0 free tier works for the demo)
- Network access to MongoDB Atlas from your machine

## Installation

1. Clone or download this project

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the project root:
```bash
MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/
DATABASE_NAME=regex_demo
COLLECTION_NAME=documents
SEARCH_INDEX_NAME=content_search
```
`DATABASE_NAME`, `COLLECTION_NAME`, and `SEARCH_INDEX_NAME` are optional — the defaults shown above are used if omitted.

Ensure your Atlas cluster's network access list includes your IP address.

## Usage

### Demo (`main.py`)

```bash
python main.py            # reuse existing collection + index (fast)
python main.py --reset    # drop and rebuild everything (~60-120 s)
```

The script detects schema changes automatically. If the collection or index is missing fields added in a later revision, it rebuilds without requiring `--reset`.

### Performance benchmark (`perf_test.py`)

```bash
python perf_test.py                        # 100K docs, 10 runs, top-20 fetch
python perf_test.py --docs 500000         # larger corpus (even more pronounced Atlas win)
python perf_test.py --reuse                # skip data rebuild if collection exists
python perf_test.py --runs 20 --warmup 5  # more iterations for tighter numbers
python perf_test.py --limit 0             # fetch all hits (forces full COLLSCAN for MQL)
```

## Project Structure

```
.
├── main.py          # 9 demo examples (regex, text, facets)
├── perf_test.py     # benchmark: $regex vs $search vs compound
├── requirements.txt # Python dependencies
├── .env             # credentials (create from the template above)
└── README.md        # this file
```

## Demo Examples

| # | What it shows | PostgreSQL equivalent |
|---|---------------|----------------------|
| 1 | Case-sensitive timestamp match | `content ~ 'ERROR [0-9]{4}-[0-9]{2}-[0-9]{2}'` |
| 2 | Case-insensitive email match via `content_lc` | `content ~* '[a-z]+@[a-z]+\.(com\|org)'` |
| 3 | JSON field pattern match | `content ~ '"port":\s*5432'` |
| 4 | Multi-pattern alternation | `content ~ '(Exception\|Error\|WARN)'` |
| 5 | Extract and count matching lines (grep -C) | *(aggregation)* |
| 6 | IP address detection and extraction | `content ~ '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+'` |
| 7 | Negation — exclude a pattern | `content !~ 'ERROR'` |
| 8 | Text operator vs regex — when to use which | `content LIKE '%ERROR%'` |
| 9 | `$searchMeta` facets — counts by document type | `SELECT type, COUNT(*) ... GROUP BY type` |

## Key Design Decisions

**`content_lc` for case-insensitive matching**
The Atlas Search `regex` operator does **not** support the `(?i)` inline Lucene flag. Store a pre-lowercased copy of each field (`content_lc = content.lower()`) indexed with `lucene.keyword`, then query it with a fully lowercase pattern.

**`text` operator for keyword presence, `regex` for patterns**
The `text` operator uses an inverted index (O(matching docs)), while `regex` must scan every token in the field (O(all docs)). Use `text` for "does this doc contain ERROR?"; use `regex` only when you need an actual pattern like timestamps, IPs, or email addresses.

**`compound.filter` instead of `compound.must` for non-scoring clauses**
`must` computes a relevance score for every clause. When a clause is always true (e.g., the match-all wildcard in negation queries), that computation is wasted. `filter` skips scoring and is faster.

**Regex alternation instead of `compound/should`**
`(.*)(Exception|Error|WARN)(.*)` is a single Lucene regex evaluation. Three separate `should` clauses each trigger independent lookups and score merges. Alternation is simpler and cheaper.

**`storedSource` + `returnStoredSource: true`**
Fields listed in `storedSource` are kept inside the Lucene index (mongot). When queries set `returnStoredSource: true`, Atlas Search serves the document body directly from the Lucene index, skipping the extra round-trip from mongot back to mongod per matching document.

**Explicit index mapping (`dynamic: false`)**
Only the fields actually queried are indexed: `content` (with a `lucene.standard` multi-field for the text operator), `content_lc`, `filename`, and `metadata.type` (as a `stringFacet` for Example 9). Dynamic mappings index every field in every document and grow the index unnecessarily.

**Automatic schema migration**
`main.py` checks whether the live collection and index match the current definition (presence of `content_lc`, `storedSource`, `content.multi`). If any are missing, it drops and rebuilds without requiring `--reset`.

## Performance Benchmark Summary

`perf_test.py` benchmarks four strategies across 9 scenarios (6 dense, 3 selective) and 1 negation scenario:

| Strategy | Dense / paginated | Selective / all hits | Negation |
|----------|-------------------|----------------------|----------|
| `$regex` MQL | **Wins** — PCRE stops after N results | Loses — full COLLSCAN regardless of hit count | **Wins** — no IPC overhead |
| `$search regex` alone | Loses — IPC overhead, no index benefit | Loses — same full Lucene token scan | Loses — IPC overhead, no index benefit |
| `$search` compound ✓ | Loses — IPC overhead without benefit | **Wins** — text pre-filter → regex on ~0.5% of corpus | Loses — IPC overhead, O(N) scan |
| `$search text` ★ | **Wins** — inverted-index O(matches), no regex scan | N/A — text alone can't enforce patterns | N/A |

**Dense scenarios** (6): Simple keyword, Date pattern, Case-insensitive, Alternation, IP address, JSON field (port) — common tokens, large hit sets, paginated top-N fetch.

**Selective scenarios** (3): CRIT on prod-db-01, Audit DELETE events, Deploy svc-api/prod — rare tokens (~0.5% of corpus), all hits fetched. The compound strategy wins here: a `text` pre-filter narrows candidates via the inverted index, then regex runs only on ~0.5% of the corpus.

**Negation scenario** (1): Negate ERROR — `$not $regex` (MQL) vs `compound.mustNot` regex (Atlas). Neither strategy benefits from the inverted index; both are O(N). MQL wins because it avoids the mongot IPC overhead.

The `text ★` strategy (standalone `text` operator) only applies to scenarios where the pattern is a plain keyword — it cannot enforce structural patterns like timestamps, IPs, or JSON field shapes.

## Atlas Search Operator Quick Reference

```
PostgreSQL            Atlas Search
--------------------  ---------------------------------------------------------
content ~ 'pat'       regex: { path: "content", query: "(.*)pat(.*)" }
content ~* 'pat'      regex: { path: "content_lc", query: "(.*)pat(.*)" }
                      (content_lc = content.lower(); (?i) is NOT supported)
content !~ 'pat'      compound: { filter: [wildcard *], mustNot: [regex pat] }
content ~ 'a|b|c'     regex: { query: "(.*)(a|b|c)(.*)" }
                      (alternation beats separate compound/should clauses)
LIKE '%keyword%'      text: { path: {"value":"content","multi":"std"}, query:"kw" }
                      (inverted index — use for keywords, not patterns)
GROUP BY type         $searchMeta + facet (stringFacet on the field)

Key rules:
• lucene.keyword on the field is required — stores the full value as one token
  so the regex can span the entire string.
• Wrap patterns with (.*) prefix/suffix to match anywhere in the value.
• Lucene regex is NOT PCRE: \s, \d, \w shorthands are unsupported.
  Use [ ]* for optional whitespace, [0-9] for digits, [a-zA-Z0-9_] for word chars.
• In Lucene regex, " is a string-literal delimiter, not a character to match.
  Escape it as \" to match a literal double-quote (in Python source, use a raw
  string: r'(.*)(\"field\":[ ]*value)(.*)' so the backslash reaches Lucene).
• Add returnStoredSource:true + storedSource in the index to skip the extra
  mongot→mongod document-fetch round-trip per hit.
• For multi-analyzer fields use {"value": "field", "multi": "name"} in queries,
  not "field.name" dot notation — dot notation is not valid for text queries.
```

## Troubleshooting

**Connection fails**
- Verify `MONGODB_URI` in `.env`
- Check the Atlas network access list for your IP

**`Field X is analyzed` error**
- The index was built before `content_lc` or `metadata.type` were added. Run `python main.py` (no flags) — the schema migration triggers automatically and rebuilds.
- If it persists, run `python main.py --reset`.

**Index build times out**
- Atlas M0/M2/M5 shared clusters build search indexes more slowly. Increase `max_wait` in `wait_for_index_ready()` or monitor progress in the Atlas console.

**Benchmark shows no speedup for compound strategy**
- Run with `--docs 500000` — at 100K the compound win is already clear; at 500K the selectivity gap is even more pronounced.
- Confirm you are using a selective scenario (`fetch_limit=0`). Dense/paginated scenarios intentionally show MQL winning.

**Benchmark prints `*** WARNING *** count mismatch`**
- The `text ★` strategy uses the `lucene.standard` tokenizer, which may match more (or fewer) documents than the regex because it tokenizes differently. This is expected for some patterns (e.g., date patterns whose tokens are just numbers). The warning is informational — the timing numbers are still valid.

## License

MIT
