<!---
Copyright IBM Corp.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# SAI Optimizer Options

SAI optimizer options are per-query directives that tune how the Storage-Attached Index (SAI) query
optimizer executes a specific `SELECT` statement.  They let you override cluster-wide system
properties for a single query without changing any global configuration.

The CQL syntax is:
```
SELECT ... FROM ... WHERE ...
  WITH optimizer_options = { '<key>': '<value>' [, ...] };
```

Options are expressed as a map of string key/value pairs.  Unrecognized keys are rejected at
prepare time with an `InvalidRequestException`.

---

## Available options

### `query_optimization_level`

Controls whether the SAI query optimizer is active for this query.

| Value | Meaning |
|-------|---------|
| `0`   | Optimizer disabled — the first eligible index is used without cost estimation. |
| `1`   | Optimizer enabled (default). |

The cluster-wide default is controlled by the system property
`cassandra.sai.query.optimization.level` (dynamically updatable at runtime).

**Example** — disable the optimizer for one query while leaving the cluster-wide setting unchanged:
```
SELECT * FROM orders
  WHERE status = 'pending' AND region = 'eu'
  WITH optimizer_options = {'query_optimization_level': '0'};
```

---

### `intersection_clause_limit`

Sets the maximum number of index clauses that may be intersected for this query.
Must be a positive integer (≥ 1).

The cluster-wide default is controlled by the system property
`cassandra.sai.intersection_clause_limit` (default `2`, dynamically updatable at runtime).

**Example** — allow up to 5 clauses to be intersected:
```
SELECT * FROM products
  WHERE category = 'tools' AND brand = 'acme' AND price < 50 AND rating > 4
  WITH optimizer_options = {'intersection_clause_limit': '5'};
```

---

### `use_term_statistics`

When `true`, the optimizer uses per-term posting-list statistics (available in index format `EB`
and later) to produce more accurate cost estimates.  When `false`, it falls back to per-segment
row-count estimates.

| Value   | Meaning |
|---------|---------|
| `true`  | Use per-term statistics when available. |
| `false` | Use per-segment statistics only. |

The cluster-wide default is controlled by the system property
`cassandra.sai.query_optimization.use_term_statistics` (dynamically updatable at runtime).

**Example** — force coarse-grained estimates for one query:
```
SELECT * FROM articles
  WHERE author = 'smith' AND topic = 'database'
  WITH optimizer_options = {'use_term_statistics': 'false'};
```

---

### `hybrid_sort_order`

For hybrid queries that combine predicate filtering with ordering (including `ORDER BY ... ANN`,
`ORDER BY ... BM25`, and generic `ORDER BY`), controls whether the optimizer materializes
WHERE-clause keys before scoring (`filter_then_sort`) or fetches scored results first and then
applies the predicate (`sort_then_filter`).
The default `auto` leaves the choice to the optimizer.

| Value              | Meaning |
|--------------------|---------|
| `auto`             | Let the optimizer decide (default). |
| `sort_then_filter` | Score via the ordering index first, then filter by predicates. |
| `filter_then_sort` | Evaluate predicates first, then score the surviving rows. |

**Example** — force filter-before-score for a hybrid ANN query:
```
SELECT * FROM images
  WHERE tag = 'cat' ORDER BY embedding ANN OF [0.1, 0.2, ...]  LIMIT 10
  WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'};
```

---

## Combining options

Multiple options may be specified in a single map:
```
SELECT * FROM events
  WHERE type = 'click' AND region = 'us'
  WITH optimizer_options = {
    'query_optimization_level': '1',
    'intersection_clause_limit': '4',
    'use_term_statistics': 'true'
  };
```

---

## Scope and isolation

Per-query options affect only the single query execution in which they appear.
They never mutate global system properties or affect concurrent queries.
