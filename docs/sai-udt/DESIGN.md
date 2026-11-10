# HCD-653: SAI indexes on the fields of UDT and tuple columns

## 1. Summary

SAI can index one field of a user-defined type (UDT) or tuple column, at any nesting depth, including the
fields of the elements of lists, sets and maps of UDTs or tuples, and CQL `SELECT` statements can restrict
those fields directly in the `WHERE` clause:

```
CREATE INDEX ON users (addr.city) USING 'sai';
CREATE INDEX ON users (addrs.zip) USING 'sai';                       -- list<frozen<address>>
SELECT * FROM users WHERE addr.city = 'Rome' AND p.age > 40;
SELECT * FROM users WHERE addrs.zip >= 150;                           -- some element
SELECT * FROM users WHERE addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100);   -- the same element
SELECT * FROM users WHERE addr.loc.lat IS NOT NULL;
```

The feature covers every UDT and tuple use case of DSE Search (the `TupleField` type and the `{!tuple}`
query parser), so that applications migrating from DSE can replace their `solr_query` with CQL, and goes
further where SAI makes it cheap (maps of UDTs, conditions on different nesting levels inside one element,
fields of key and static columns).

Design principles:

1. **One ordinary SAI index per field.** An index on `addr.city` is an SAI index whose terms are the values
   of that field: it has its own name, options, analyzer, memtable index and on-disk components, with the
   same formats as an index on a column. There is no SSTable or SAI on-disk format change; the `RowFilter` wire
   format gets two new expression kinds (4.4), without a messaging version change.
2. **The results never depend on the indexes.** A restriction on a field has the same result with an
   index, with `ALLOW FILTERING` and without any index, except the analyzer-dependent operators (`=`, `:`,
   `LIKE`, `CONTAINS` and `NOT CONTAINS`, as on columns) and the values over the maximum term size (see 4.6),
   and the coordinator can re-apply it when it reconciles the replicas. In particular, the presence of an
   index analyzer never changes the result of a query beyond what it already does for columns.
3. **Collections are visible, not hidden.** A restriction on a field inside a collection means "some
   element"; conditions on the same element are written explicitly with `CONTAINS ELEMENT (...)`.

The working notes of the implementation (phase by phase, with the decisions taken along the way) are in
`implementation-notes.md`, the behaviour recorded on a DSE 6.9 node in `dse-runtime.md`, and the review history
in `REVIEW-LEDGER.md`, next to this document.

## 2. Background

### 2.1 DSE Search

DSE Search maps a UDT or tuple column to a Solr field of type `TupleField` and every leaf to a field named by
its dotted path (`addr.city`, `p.home.loc.lat`); tuple components are `field1`, `field2`, ... Collections of
UDTs or tuples become multi-valued tuple fields whose elements are indexed as Lucene child documents (a
block join). Queries use `solr_query` with the tuple parser: `{!tuple}addr.city:Rome`, ranges
(`addr.zip:[0 TO 5]`), wildcards, `addr.city:*` for "has a value". Inside one `{!tuple}` query, AND-ed
conditions on a collection apply to the same element; separate `q` / `fq` clauses apply to independent
elements. DSE refuses maps of UDTs, conditions on different nesting levels inside one tuple query, `IS NULL`
on nested fields, and sorting or faceting on tuple fields. CQL has no predicate on UDT fields in DSE.

### 2.2 SAI before this change

SAI accepted a whole frozen UDT or tuple column as a literal (`=`, `IN`), refused non-frozen UDT columns,
and had no notion of a field in index targets, in the `WHERE` grammar, in `RowFilter` or in its query
planner, which groups expressions by column.

## 3. User-facing design

### 3.1 Index targets

The target is the column name followed by the path to the field:

| target | meaning |
|---|---|
| `addr.city` | a field of a frozen or non-frozen UDT |
| `p.home.loc.lat` | a nested field, any depth |
| `t.field2` | a tuple component (1-based `fieldN`, as in DSE Search) |
| `addrs.city`, `values(addrs).city` | a field of the elements of a list or set, or of the values of a map (the two forms are the same index) |
| `addr.tags` | a collection field: its values (`CONTAINS`) |
| `full(addr.tags)` | a frozen collection field as a whole (`=`) |

- A path crosses the elements of a list or set, and the values of a map, implicitly. Paths through map
  keys, and `keys()` or `entries()` on a field, are refused.
- The stored `target` option is the canonical CQL form, with names (`addrs.city`, `values(addr.tags)`,
  `"Addr"."City"`), so `DESCRIBE` round-trips. Field positions are resolved from the names when the target
  is parsed and are what the index uses at runtime and on the wire.
- Default index name: `<table>_<column>_<fields>_idx` (`users_addr_city_idx`).
- The usual SAI options apply to the field type. `index_analyzer` and `query_analyzer` are refused on the
  fields inside collections (see 4.6); `case_sensitive`, `normalize` and `ascii` are allowed everywhere.
- Refused: fields of vector or `duration` type, and field targets with any index class other than SAI (legacy
  or custom).
- `ALTER TYPE ... ADD` needs nothing: positions are stable, and the values written before the change
  have no value for the new field. `ALTER TYPE ... RENAME` of a field on the path of any index target is
  refused: drop the index first.
- There is no "index all fields" shorthand: each field has its own index, with its own cost, counted by the
  `sai_indexes_per_table` guardrails.

### 3.2 Restrictions on fields

A field is restricted like a column, with `=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`, `NOT IN`, `LIKE`,
`:` (with an analyzer), `CONTAINS` and `NOT CONTAINS` (on collection fields) and `IS NOT NULL`. Bind markers
take the type of the field. The restrictions on the fields of partition key, clustering and static columns
are supported. Field restrictions are accepted only in `SELECT`, not in `UPDATE`, `DELETE` or materialized
views; `SELECT DISTINCT` refuses them on clustering columns.

The rules are those of the same operators on columns:

- An index serves ranges on text fields only without an analyzer; `LIKE` and `:` need an index.
- A restriction that no index serves needs `ALLOW FILTERING` and gives the same rows as the index would,
  except the analyzer-dependent operators (`=`, `:`, `LIKE`, `CONTAINS` and `NOT CONTAINS`, as on columns)
  and the values over the maximum term size (see 4.6).
- `=`, `:`, `LIKE`, `CONTAINS` and `NOT CONTAINS` use the analyzer of the index serving them, as on columns;
  ranges, `!=` and `NOT IN` always compare the raw values. `IN` on a field whose index has an analyzer fails
  when the query is executed, with a server error ("IN operation does not support analyzers"), as on columns.

`IS NOT NULL` selects the rows where the field is set: present and not null in a frozen value, a live cell in
a non-frozen UDT. An empty value of a literal type (text, ascii, boolean) is a value; an empty value of another
type (an `int` from `blobAsInt(0x)`, a blob) is not (the same rule as the indexes, which skip those values).
`IS NULL` is not supported.

### 3.3 Fields inside collections

On a path through a collection, a restriction is satisfied when **some** element satisfies it, and `!=`,
`NOT IN` and `NOT CONTAINS` when **no** element does, so they include the rows without elements. AND-ed
restrictions are independent: `addrs.city = 'Rome' AND addrs.zip = 100` accepts a row with an element in
Rome and another element with zip 100. Two bounds on one path are independent too (`addrs.zip >= 150 AND
addrs.zip < 300` is not a range on one element). This matches DSE's separate `q` / `fq` clauses, CQL's
`CONTAINS a AND CONTAINS b`, and the dotted paths of document databases.

### 3.4 Conditions on the same element: `CONTAINS ELEMENT`

```
WHERE addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)
WHERE addrs CONTAINS ELEMENT (city = 'Rome' AND (zip >= 150 OR loc.lat < 2) AND zip != 100)
WHERE addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 100) ALLOW FILTERING
WHERE p.names CONTAINS ELEMENT (first = 'Bilbo' AND last = 'Baggins')      -- a collection inside a UDT
WHERE ts CONTAINS ELEMENT (field1 = 'foo' AND field2 = 2)                  -- a list of tuples
```

- `CONTAINS ELEMENT` is satisfied by the rows where some element satisfies the whole group;
  `NOT CONTAINS ELEMENT` by the rows where no element does.
- The names inside the group are relative to the element, at any depth, so conditions on different
  nesting levels combine (DSE can't). The group accepts the operators of 3.2 with `AND`, `OR` and
  parentheses; groups can't be nested; `ELEMENT` is a new unreserved keyword.
- Inside an element, an unset field satisfies no condition, `!=` included (DSE's `NOT zip:100` matches the
  elements without `zip`; see 7).
- Groups are independent from each other and from the restrictions outside them.
- A group needs no `ALLOW FILTERING` when an index serves one positive condition of its top-level `AND`
  (or all the conditions of an `OR`). `NOT CONTAINS ELEMENT` needs `ALLOW FILTERING`, except the
  single-equality group on a field whose index has no analyzer, which is exactly `addrs.city != 'Rome'`.

Alternatives considered for the syntax: implicit grouping as in DSE (AND-ed conditions on one collection
mean the same element), rejected because it makes independent conditions impossible to write and
contradicts `CONTAINS a AND CONTAINS b`; a partial literal `addrs CONTAINS {city: 'Rome', zip: 100}`,
rejected because that literal means equality with a whole element today and can't express ranges or `OR`;
`ANY addrs SATISFIES (...)` (N1QL), two new keywords for the same power; `CONTAINS (...)` without a keyword,
ambiguous with tuple literals and casts.

### 3.5 DSE Search migration

Every DSE `{!tuple}` form has a CQL counterpart; the user documentation
(`doc/modules/cassandra/pages/developing/cql/indexing/sai/udt-tuple-fields.adoc`) has the full table. In
short: `{!tuple}addr.city:Rome` is `addr.city = 'Rome'`; ranges and wildcards are ranges and `LIKE`;
`addr.city:*` is `IS NOT NULL`; `addrs.city:Rome` is "some element"; `addrs.city:Rome AND addrs.zip:100`
is `CONTAINS ELEMENT (city = 'Rome' AND zip = 100)`; `-{!tuple}(...)` is `NOT CONTAINS ELEMENT (...)` or a
`!=` on the path; `WITH COLUMNS p.age` is one `CREATE INDEX` per field. `lowerCase` field types map to
`case_sensitive: false` for `=`, `LIKE` and `:`; not for ranges, `!=` and `NOT IN`, which compare the raw values.

## 4. Architecture

### 4.1 `FieldPath`

`org.apache.cassandra.cql3.FieldPath` is an immutable list of steps from a column type to a field:
`Field(position)` (a UDT field or tuple component) and `Elements` (the elements of a list or set, or the
values of a map, inserted implicitly when a step crosses a collection of UDTs or tuples). It resolves a list
of names against a column type (`fieldN` resolves against tuples only, so it can't clash with a UDT field of
that name), gives the leaf type and its CQL specification (the receiver of bind markers), knows whether it is
multi-valued, renders the canonical CQL form, and visits the values of a path in a row or a serialized value:
`collectValues` (all the values) and `anyValue` (stop at the first value deciding a predicate). Values are
reached without allocation where possible: frozen UDTs and tuples are split into slices (a shorter value,
written before an `ALTER TYPE ... ADD`, yields trailing nulls); the first step of a non-frozen UDT reads the
cell of the field; non-frozen collections iterate their cells; frozen collections are unpacked by their
serializer. The wire form (used by `RowFilter`) is the step count, then per step a tag byte and, for a field
step, the position; a path of at most 64 steps that must match the receiver's column type.

### 4.2 Index definition and build

`IndexContext` gets a `FieldPath` (null for the existing indexes) and uses the leaf type as its validator,
so the term encoding, comparators, analyzers, memtable index, SSTable writers and readers are those of an
index on a column of the leaf type. The value extraction (`getValueOf` / `getValuesOf`) walks the path, and
`isMultiValued()` (a path through a collection, or a collection leaf) replaces the "non-frozen collection"
checks that decided between one and several terms per row, in the memtable index, the SSTable writer and the
post-filter. A null or unset field produces no term; an empty string is a term for the literal types, as on
columns.

Writes to non-frozen UDTs go through the memtable update path with the reconciled row, so `UPDATE ... SET
p.age = 41` re-extracts the field from the merged cells; elements added to, removed from or replaced in a
non-frozen collection are re-extracted the same way. Each SSTable indexes the cells it holds; stale terms in
older SSTables are removed by the usual post-filter on the merged row.

`TargetParser` returns the column, the index type and the path; `CreateIndexStatement` applies the existing
validations to the leaf (duration, `full()` only on a frozen collection, map forms only on a map);
`StorageAttachedIndex.validateOptions` validates the options and the analyzer against the leaf type, checks
duplicates on the (column, type, path) triple, and refuses the Lucene analyzers inside collections. As on columns,
one index with an analyzer and one without may coexist on a field: `=` is then ambiguous without index hints
(4.6), ranges and `!=` are served by the index without analyzer only, `:` by the analyzed one only, and
`IS NOT NULL` by either.
`Index.isOnFieldOf` lets `ALTER TYPE ... RENAME` and the error messages find the indexes on a field.
The SAI system views `system_views.indexes` and `system_views.sai_column_indexes` expose the path
(`field_path`) and the canonical `target` next to the unchanged `column_name`.

### 4.3 CQL layer

- Grammar: an optional field path after the column name in the index target and in the relations
  (`relationType`, `LIKE`, `IN` / `NOT IN`, `CONTAINS` / `NOT CONTAINS`, `IS NOT NULL`), and the
  `[NOT] CONTAINS ELEMENT ( expression )` alternative, whose syntactic predicate keeps `CONTAINS element(x)`
  a call to a function named `element`.
- `SingleColumnRelation` carries the field names next to `mapKey`; the receiver is the leaf specification.
  `ElementMatchRelation` holds the nested `WhereClause` of a group; each relation of the group is rewritten
  on its absolute path, so the group reuses the field relations for operators, receivers and messages.
- `SingleRestriction.fieldPath()` keys the restrictions: `RestrictionSet` merges AND-ed restrictions only
  when their paths are equal and single-valued, so `addr.city = ? AND addr.zip = ?` and the independent
  restrictions on a collection path coexist. A restriction on a field of a primary key column goes to the
  index/filtering restrictions, never to the key restrictions. `FieldIsNotNullRestriction` serves
  `IS NOT NULL` outside views. `ElementMatchRestriction` holds the tree of positive and negative conditions of
  a group and decides, with the same rule as the planner (4.5), whether indexes serve it.
- Index selection: `Index.supportsExpression(column, path, operator)` defaults to "no path and the column
  operator is supported", so whole-column indexes (SAI on a frozen UDT, legacy 2i, custom indexes) never
  claim a field expression; SAI matches the path exactly. `IndexRegistry.getBestIndexFor` and
  `getAnalyzerFor` get path overloads, so the `=` behaviour and the analyzers apply per field.

### 4.4 `RowFilter`

Two new expression kinds, each with a CQL form for logging and tracing:

- `FIELD`: `FieldExpression(column, path, operator, value, analyzer)`, a `SimpleExpression` on the values of
  a path (one for a single field, "some" or "none" for a multi-valued path). `IS NOT NULL` is a `FIELD`
  expression with `IS_NOT` and an empty value, without analyzer. Wire: column, operator, path, value.
- `ELEMENT_MATCH`: `ElementMatchExpression(column, prefix, negated, FilterElement)`, whose conditions are
  `FieldExpression`s with paths *relative* to the element type, evaluated on each element buffer as a
  single-cell value; `anyValue` stops at the first matching element. Wire: column, operator (`CONTAINS` or
  `NOT_CONTAINS`), prefix, then the `FilterElement` (so `AND` / `OR` nesting is that of the `WHERE` clause).
  Deserialization refuses a prefix that doesn't reach a collection of UDTs or tuples, conditions on other
  columns or of other kinds, relative paths that don't match the element type, and empty groups (also refused
  at bind time on the coordinator).

The filtering without index, the SAI post-filter and the coordinator's replica filtering protection all
evaluate these expressions with the same code, which is the correctness invariant of the feature: the
results of an index-served query are, by construction, those of the same query with `ALLOW FILTERING` on a
table without indexes (the "twin table" oracle of the tests). `isMutableIntersection` counts each field of
a non-frozen UDT and each expression on a field inside a non-frozen collection as a target of its own
(different cells), and the fields of a frozen value as one target.

### 4.5 SAI query planning and execution

- `Operation.analyzeGroup` groups and merges expressions by `(column, path)`; `plan/Expression` compares the
  path; `FilterTree` evaluates each expression with its own `IndexContext`, which extracts its own field.
- A single-valued target behaves as a column: ranges merge into one expression, `!=` becomes exclusions of
  the previous expression of the same context, `IS NOT NULL` is a `RANGE` without bounds (all the terms,
  whatever their order and type) post-filtered as "set", merged into the previous expression of the target
  when there is one so that its post-filter still rejects the unset values.
- A multi-valued target never merges bounds (each bound is an index scan, intersected): a merged range would
  require one element inside both bounds and return fewer rows than the filtering. The only merge is that of
  the AND-ed `!=` of a `NOT IN`.
- `!=` / `NOT IN` on a multi-valued path means "no element equals", which the two-half-ranges plan of the
  columns can't express (it misses the rows without values). It is served as `NOT CONTAINS` is: all the keys
  of a source minus the keys with an equal term (`getNonEqIterator`), then the post-filter. It only needs
  equality, so it works on the EQ-only literal types too, by indexes without analyzer. The subtraction is
  unsafe when an index holds a term the live row no longer has: expired (TTL) cells, rows shadowed by a
  partition deletion or a range tombstone, and the two memtables live during a flush. The guards, shared
  with `NOT CONTAINS` (fixed in the same series): the subtraction is done memtable by memtable; a source
  that may have expired terms (the memtable records the earliest expiration it indexed; an SSTable with a TTL
  whose minimum local deletion time isn't after the query time) doesn't subtract its keys; a deletion that may
  shadow terms in any source (a partition deletion or range tombstone applied to a memtable, an SSTable with
  partition deletions, an SSTable of a table with clustering columns and tombstones) disables the subtraction
  in all of them, which then fall back to a scan plus post-filter.
- `CONTAINS ELEMENT`: the planner turns a group into the AND / OR of its *positive* conditions on the
  absolute paths (`candidates()`), each "some element", a superset of the rows satisfying the group; negative
  conditions are left out of an `AND`, and an `OR` with a negative condition has no candidates. The group is
  an `Operation.ElementNode` whose plan is that of the candidates and whose filter, `FilterTree.ElementFilter`,
  evaluates the `ElementMatchExpression` itself on each candidate row, with the read command's `nowInSec`.
  No change to the postings format: the alternative, element ordinals in the postings with a positional
  intersection, needs a new on-disk version to save some false-positive reads, and can be revisited if the
  per-table metric `TotalElementFilterRejectedRows` (rows the indexes returned for a group that no element
  satisfied) shows high rates. `NOT CONTAINS ELEMENT` has no candidates (subtracting a superset would drop
  rows): it is a filter over the other restrictions or a full scan with `ALLOW FILTERING`, except the
  single-equality group, rewritten to the `!=` on the path when no analyzed index serves the `=` on the
  field (an analyzed `=` has no raw `!=` as its complement, so such a group stays a filter).

### 4.6 Analyzers

The results of a query never depend on an index analyzer, beyond what the analyzed `=`, `:`, `LIKE`,
`CONTAINS` and `NOT CONTAINS` already do on columns (`IN` on an analyzed field fails at execution with a server
error, "IN operation does not support analyzers", as on columns):

- `=` on a field follows the `equals_behaviour_when_analyzed` option of the analyzed index on the field, as on a
  column (`SingleColumnRelation.newFieldEQRestriction`, `IndexRegistry.getEqBehavior(column, path, hints)`): with
  `match` (the default) it becomes a `:`, served by the index, with the client warning of the columns naming the
  field and the index; with `unsupported` the index doesn't serve it, and with `ALLOW FILTERING` the raw values
  are compared. An analyzed and a raw index on the same field make `=` ambiguous, refused with the error of the
  columns, unless the index hints (`WITH included_indexes = {...}` / `excluded_indexes`, per index, so per field)
  select one of them; the same rules apply to the conditions of a `CONTAINS ELEMENT` group, and decide whether a
  single-equality `NOT CONTAINS ELEMENT` is reduced to the `!=` (4.5).

- An analyzed field index (any of `case_sensitive: false`, `normalize`, `ascii`, `index_analyzer`,
  `query_analyzer`) rejects ranges on text fields, as on columns; with `ALLOW FILTERING` the raw values are
  compared.
- `!=` and `NOT IN` compare the raw values, inside and outside collections and groups, and the anti-join of
  4.5 is served only by indexes without analyzer.
- `IS NOT NULL` is served by field indexes without a Lucene analyzer: the normalizing options index every
  set value, the empty string included, so the result doesn't depend on them; a Lucene analyzer may produce
  no token for a value, so those indexes don't serve it. On text fields it relies on the fix, in the same
  series, that indexes the empty strings written by updates to rows in the memtable.
- Term size: a value whose term exceeds the maximum term size of the index (8 KiB by default; for an analyzed
  index, the size of the analyzed value, so a normalizing index may skip a value that a raw index keeps) is not
  indexed: the write only logs a warning, unless the coordinator validates the term sizes
  (`cassandra.sai.validate_max_term_size_at_coordinator`, off by default). The rows with such a value are then
  missing from the results of every operator the index serves, `IS NOT NULL` included, while `ALLOW FILTERING`
  without index returns them. This is the behaviour of the indexes on columns; `IS NOT NULL` stays served by the
  indexes (a documented limitation, 7).
- `index_analyzer` and `query_analyzer` are refused on the fields inside collections: SAI would match each
  token of an operand in some element, possibly a different one per token, while the filter requires all the
  tokens in one element, so the results would depend on the consistency level. The normalizing options
  analyze each element value, consistently on both paths.

## 5. Compatibility and upgrades

- **No version change.** No messaging version, no SAI on-disk version, no SSTable format change, no
  version gate on `CREATE INDEX`. A field index is written with the SAI version of its SSTable, as any index;
  its files are ordinary literal or numeric index files whose meaning comes from the index definition.
- **Mixed versions.** Field indexes are to be created only once every node has the feature. Schema changes
  during a rolling upgrade are discouraged but not enforced between 5.0.x versions: a field index created while
  some nodes don't have the feature makes those nodes fail to load it ("Unable to parse targets for index ..."),
  until they are upgraded or the index is dropped. A `SELECT` with a field restriction reaching a
  replica without the feature (only possible with `ALLOW FILTERING`) fails cleanly: the replica refuses the
  unknown `RowFilter` kind, and the client gets a read failure, never wrong results. Before downgrading,
  drop the field indexes. Documented in `NEWS.txt`.
- **Drivers** see the target `addr.city` as a column name in their index metadata. **cqlsh** completes
  field paths, the operators and `CONTAINS ELEMENT` groups in `SELECT`.
- **Existing indexes** on whole frozen UDT or tuple columns keep serving `=` on whole values; a field index
  and a whole-column index can coexist on one column, each serving its own form.

## 6. Performance notes

- Each field index costs about as much as an index on a column of the leaf type; DSE users who indexed
  every field by default should create only the indexes their queries use.
- Extraction locates fields by offsets in serialized values; the multi-valued filters stop at the first
  deciding element; the per-row cost of a field index build is that of a collection index.
- The `!=` anti-join degrades to a scan plus post-filter on sources with partition deletions, with range
  tombstones, or with expired cells, as described in 4.5; the row estimates of the planner account for it.
- `CONTAINS ELEMENT` candidates can be a large superset when the values are common but never together in
  one element; the metric `TotalElementFilterRejectedRows` and the query trace report it.

## 7. Limitations and future work

- `IS NULL` on fields (and columns). On collections, `NOT CONTAINS ELEMENT (f IS NOT NULL)` gives "no
  element has it".
- Map-entry restrictions on fields (`addr.m['k'] = 'v'`), `CONTAINS KEY` on fields, indexes on the keys or
  entries of a map field, paths through map keys.
- `ORDER BY` (including ANN and BM25) and `GROUP BY` on fields; vector and `duration` fields.
- Nested groups; returning the matching elements rather than whole rows (DSE doesn't either).
- Inside a group, an unset field doesn't satisfy `!=` (DSE's `NOT f:v` does); `addrs CONTAINS ELEMENT
  (x != 23)` is the DSE form, `addrs.x != 23` is "no element equals".
- `IS NOT NULL` served by indexes on the values of a collection field or with a Lucene analyzer (filtering
  only); an `IN` / `NOT IN` in a single-condition `NOT CONTAINS ELEMENT` is not reduced (filtering).
- The values whose term exceeds the maximum term size of the index are not indexed (4.6): the index-served
  queries, `IS NOT NULL` included, miss those rows, as on columns.
- Exact per-SSTable guards for the anti-join (recording partition and range deletions and the earliest
  expiration in the SAI metadata) would need an on-disk change.

## 8. Testing

- Unit tests per layer: `FieldPathTest`; `UdtFieldIndexDDLTest` (accepted and refused targets, naming,
  `DESCRIBE`, `ALTER TYPE`, builds after flush and compaction); `UdtFieldQueryTest`,
  `UdtCollectionFieldQueryTest`, `UdtElementMatchQueryTest`, `UdtFieldIsNotNullTest` (frozen and non-frozen,
  memtable, flushed and compacted, key and static columns, prepared statements, paging);
  `SelectSingleColumnRelationTest` (field restrictions filtered without index); `RowFilterTest` (wire round
  trips, evaluation); `ExpressionTest`.
- The twin-table oracle: every query runs on the indexed table and, with `ALLOW FILTERING`, on a copy
  without indexes, in the memtables, after flush and after compaction; the results must be equal.
- `DseSearchUdtCompatibilityTest`: the scenarios of DSE's own UDT and tuple search tests and the behaviour
  recorded on a DSE 6.9 node, each CQL query annotated with its `solr_query`.
- `NonEqualityStaleTermsTest`: the anti-join guards across four memtable implementations and the configured
  default (TTL, partition and range deletions, re-inserts, flush in progress).
- `UdtFieldIndexDistributedTest` (in-JVM, 3 nodes): QUORUM and ALL from every coordinator, replica
  filtering protection with newer values on some replicas, paging over inconsistent replicas.
- cqlsh completion tests, offline and against a live node.
