# MongoDB Atlas Search Regex Pattern Matching Demo

A Python demonstration of regex pattern matching using MongoDB Atlas Search, with examples similar to PostgreSQL's `~` (case-sensitive) and `~*` (case-insensitive) operators.

## Overview

This project demonstrates how to:
- Use MongoDB Atlas Search with regex patterns for text matching
- Perform case-sensitive and case-insensitive searches using the `(?i)` inline Lucene flag — no duplicate fields needed
- Use regex alternation instead of verbose `compound/should` clauses for multi-pattern matching
- Apply `compound.filter` (instead of `must`) for non-scoring criteria to improve query performance
- Create and manage Atlas Search indexes with explicit field mappings
- Load configuration securely from a `.env` file

## Prerequisites

- Python 3.8+
- MongoDB Atlas cluster
- Network access to MongoDB Atlas

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

1. Create a `.env` file in the project root and fill in your MongoDB Atlas credentials:
```bash
MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/
DATABASE_NAME=regex_demo
COLLECTION_NAME=documents
SEARCH_INDEX_NAME=content_search
```
`DATABASE_NAME`, `COLLECTION_NAME`, and `SEARCH_INDEX_NAME` are optional — the defaults shown above are used if omitted.

2. Ensure your MongoDB Atlas cluster has network access enabled for your IP address

## Usage

Run the main demo:
```bash
python main.py
```

## Project Structure

```
.
├── main.py              # Main demo script
├── requirements.txt     # Python dependencies
├── .env                 # Configuration file (create and customize)
└── README.md           # This file
```

## Features

- **Case-Sensitive Regex Search**: Find patterns with exact case matching (like PostgreSQL `~`)
- **Case-Insensitive Regex Search**: Uses the `(?i)` inline Lucene flag — no separate lowercased field required (like PostgreSQL `~*`)
- **Multi-Pattern Alternation**: Single regex with `(a|b|c)` instead of multiple `compound/should` clauses
- **Negation Search**: `compound.mustNot` with a `filter` (not `must`) match-all for better performance (like PostgreSQL `!~`)
- **Grep-with-Context**: Extract and count matching lines using `$filter` and `$regexFindAll`
- **IP Address Extraction**: Detect and return IP addresses found in content
- **Explicit Index Mapping**: `dynamic: false` — only queried fields are indexed for a smaller, faster index
- **`.env` Configuration**: All credentials and settings loaded from environment variables via `python-dotenv`
- **Safe Connection Management**: `serverSelectionTimeoutMS` for fast failure on misconfiguration; context manager for clean pool teardown

## Example Queries

| Example | Pattern | PostgreSQL equivalent |
|---------|---------|----------------------|
| 1 | Case-sensitive timestamp match | `content ~ 'ERROR [0-9]{4}-...'` |
| 2 | Case-insensitive email match via `(?i)` | `content ~* '[a-z]+@[a-z]+\.(com\|org)'` |
| 3 | JSON field pattern match | `content ~ '"port":\s*5432'` |
| 4 | Multi-pattern alternation | `content ~ '(Exception\|Error\|WARN)'` |
| 5 | Extract and count matching lines | *(aggregation)* |
| 6 | IP address detection | `content ~ '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+'` |
| 7 | Negation — exclude pattern | `content !~ 'ERROR'` |

## Key Design Decisions

**`(?i)` instead of a `contentLower` field**
The original approach stored a lowercased copy of every document to simulate case-insensitive search. The Lucene `(?i)` inline flag achieves the same result with zero storage overhead — prefix any pattern with `(?i)` to make it case-insensitive.

**`compound.filter` instead of `compound.must` for non-scoring clauses**
`must` computes a relevance score for every clause. When a clause is always true (e.g., the match-all wildcard in negation queries), that score computation is wasted work. `filter` skips scoring and is faster.

**Regex alternation instead of `compound/should`**
`(.*)(Exception|Error|WARN)(.*)` is a single Lucene regex evaluation. Three separate `should` clauses each trigger independent index lookups and score merges. For simple OR patterns, alternation is both simpler and cheaper.

**Explicit index mapping (`dynamic: false`)**
Only `content` and `filename` are indexed. Dynamic mappings index every field in every document, growing the index unnecessarily. Explicit mappings give a smaller index and faster queries.

## Notes

- Search indexes may take a few minutes to build after creation
- Ensure you have appropriate permissions in MongoDB Atlas
- The `lucene.keyword` analyzer is required on fields used with the `regex` operator — it stores the entire value as a single token so the pattern can match across the full string

## Troubleshooting

**Connection Issues:**
- Verify MongoDB URI is correct in `.env`
- Check network access list in MongoDB Atlas
- Ensure IP address is whitelisted

**Index Not Found:**
- Wait a few minutes for the search index to build
- Check MongoDB Atlas console for index creation status

**Performance Issues:**
- Use `compound.filter` instead of `compound.must` for criteria that don't affect relevance scoring
- Prefer regex alternation `(a|b|c)` over multiple `compound/should` clauses for simple OR patterns
- Keep `dynamic: false` in the index definition and only map fields you actually query

## License

MIT
