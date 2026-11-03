# HCD-653: SAI indexes on UDT and tuple fields (design)

Status: APPROVED by the owner (2026-11-02) with these decisions:
1. Same-element syntax: `col CONTAINS ELEMENT (...)` / `NOT CONTAINS ELEMENT (...)`; plain AND-ed leaf relations on a collection path are independent ("some element" each).
2. No version change (final owner decision): no new messaging version, no new SAI on-disk version, no file-format version, and no CREATE INDEX version gate (§E).
3. `ALTER TYPE ... RENAME` of a field used by an index target is refused (drop the index first).
4. Tuple components are addressed as `field1`, `field2`, ... (1-based, as DSE Search).

 Inputs: DSE 6.9.26 source collection (issue #105, branch
`hcd-653-collection`, file `HCD-653-dse-search-udt.md`) and the local DSE run (`dse-runtime.md`).
File:line references were checked against `hcd-653-sai-udt-fields` at `63868f5d74`. §0 lists hazards
found in the code that any implementation must handle.

## 0. Hazards found while checking the code
1. **`TargetParser.parse` has no fallback for a dotted target** (`index/TargetParser.java:49-96`). `getColumn("addr.city")` returns null, `parse` returns null, and `parse(TableMetadata, IndexMetadata)` throws `ConfigurationException` (:45). That parse runs in the `StorageAttachedIndex` constructor (:251). A node without this feature that receives the schema would fail to load the index. DDL is not allowed in mixed-version clusters, so field indexes are created only once every node is upgraded (§E).
2. **`RowFilter.Expression.Kind.fromVal`** (`RowFilter.java:1036-1048`) throws `IllegalArgumentException` on an unknown kind byte. An old replica fails the request with "Unknown index expression kind": a clean error, never wrong results (§E).
3. **SAI groups expressions by `ColumnMetadata`, never by field.**
   - `Operation.analyzeGroup` (`plan/Operation.java:73-87`) keeps a `ListMultimap<ColumnMetadata,…>` and `columnIsMultiExpression` per column. Its range merge (`range = Iterables.getLast(perColumn)`, :170-178) would fold `addr.city >= 'a'` and `addr.zip <= 5` into one wrong range.
   - `FilterTree.expressions` (`plan/FilterTree.java:52,91-123`) is keyed the same way.
   - `plan/Expression.equals`/`hashCode` (:718, :736) compare only the column name.
   - All of them need a `(column, path)` key.
4. **`IndexContext.isNonFrozenCollection()`, `isCollection()` and `isFrozen()` look at `column.type`** (`IndexContext.java:597-610`). These flags drive `index()` (:303), `update()` (:444), `validateMaxTermSizeForRow` (:330), `supports()` (:800), `supportsLikeQueries` (:759), `SSTableIndexWriter.addRow` (:121) and `FilterTree` (:108). For a leaf index the column type is the wrong input.
5. **`Index.supportsExpression(RowFilter.Expression)`** defaults to `(column, operator)` (`index/Index.java:480-483`). Without a change, an existing whole-value index on `addr` (frozen UDT, EQ), or a legacy 2i index, would claim a field expression on `addr`.
6. **`RestrictionSet.Builder.addRestriction`** merges every AND-ed restriction on the same column (`RestrictionSet.java:427-440`). Without a change, `addr.city = ? AND addr.zip = ?` would fail as "more than one restriction".
7. **ALTER TYPE.** `AlterTypeStatement.RenameFields.apply` (:213-251) does not look at indexes. `ADD` appends a field, so existing positions stay valid. `ALTER field type` is refused already (:260-263).
8. **Minor.**
   - `IndexDescriptor.tmpFileFor` names temporary files `buildId_column_component` (:581). Two indexes on the same column would collide. Only vector indexes use it (`CompactionGraph:196,207`), so it is harmless if vector leaves are refused.
   - `IndexMetadata.generateDefaultIndexName(table, column)` would produce `t_addr_idx_1`, `_2`, … for several leaves of one column. Generate `t_addr_city_idx` instead.
   - SAI does not support `IS NOT NULL` today (`Expression.Op`, :62-67). `StatementRestrictions` (:454-459) refuses IS NOT outside views.

## A. DDL

**Recommendation: one SAI index per leaf, addressed by a dotted path.** It reuses all of SAI unchanged: one validator, analyzer and index writer config per index, and the existing per-index components (§E).

```
CREATE INDEX ON t (addr.city)        USING 'sai';                -- frozen or non-frozen UDT
CREATE INDEX ON t (p.home.loc.lat)   USING 'sai';                -- nested, any depth
CREATE INDEX ON t (t.field2)         USING 'sai';                -- tuple component, 1-based (DSE naming)
CREATE INDEX ON t (addrs.city)       USING 'sai';                -- list/set<frozen<udt>>, element implied
CREATE INDEX ON t (values(addrs).city) USING 'sai';              -- same index, explicit form
CREATE INDEX ON t (values(m).city)   USING 'sai';                -- map<k,frozen<udt>> values (DSE refuses this)
CREATE INDEX ON t (addr.tags)        USING 'sai';                -- leaf is set<text>: CONTAINS (=VALUES)
CREATE INDEX ON t (full(addr.tags))  USING 'sai';                -- whole frozen leaf collection: EQ
CREATE INDEX ON t (addr.city) USING 'sai' WITH OPTIONS = {'case_sensitive':'false'}; -- DSE lowerCase
```

**Path model.** Add a new immutable `FieldPath`, for example in `org.apache.cassandra.cql3` or `db/marshal`. It is a list of steps:
- `Field(int position)`: a UDT field or tuple component.
- `Elements`: list or set elements, or map values. Inserted implicitly when a step crosses a collection whose elements are UDTs or tuples.

API:
- `FieldPath.resolve(AbstractType<?> columnType, List<FieldIdentifier> names)` returns the path, or throws "Undefined field x in …".
- `leafType(columnType)`.
- `isMultiValued()`: true if there is any `Elements` step, or the leaf is a collection.
- `forEachValue(ColumnMetadata, Row | ByteBuffer, nowInSec, Consumer<ByteBuffer>)`, used in §C.
- `toCQLString(columnType)`: names, quoted when needed.
- A wire serializer of the positions.

**Tuple components.** CQL tuples have no field names, so use `fieldN`, 1-based, as DSE does (C2.2). It only resolves against `TupleType`, so it cannot clash with a UDT field that happens to be named `field1`. Alternative: `t[1]`. Owner to choose. I don't recommend it: `cident '[' term ']'` is the map-entry relation (`Parser.g:1844`), and `t[1]` would not match the DSE path names.

**Grammar.** In `indexIdent` (`Parser.g:900-906`), add an optional `('.' fident)+` path after `cident` and after `values(cident)`, plus `full(cident '.' …)`. Then:
- `IndexTarget` and `IndexTarget.Raw` get a nullable `List<FieldIdentifier> path`.
- `asCqlString()` renders `addrs.city` or `full(addr.tags)`.
- `Raw.prepare` (`IndexTarget.java` ~:112) applies the same rule it already has, "SIMPLE on a collection becomes VALUES", to the leaf.

**Stored target.** Keep it in the existing `target` option, in canonical CQL form, using names (for example `addrs.city`). `IndexMetadata.appendCqlTo` (:403, :424) writes it verbatim, so DESCRIBE round-trips.
- `TargetParser.parse` gains a variant that returns a `(column, type, FieldPath)` triple; keep the old method and its `Pair` return type for existing callers.
- Positions are resolved when the target is parsed and are the only thing used at runtime and on the wire.
- Nothing else needs to change: names are what users read, positions are stable under `ADD`.

**ALTER TYPE RENAME.** Recommendation: in `RenameFields.apply`, refuse to rename a field that appears on the path of any index target, in any table of the keyspace that uses the type, including nested use. The error says to drop the index first. This mirrors how DROP COLUMN is blocked by `dependsOn`. Alternative: rewrite the targets of affected indexes in the same `Keyspaces` transformation (`withUpdatedUserType`). That is more work and would put a schema change on a table inside an ALTER TYPE. `ADD` needs nothing: old frozen values are shorter and split as trailing nulls.

**Validation.**
- `CreateIndexStatement.validateIndexTarget` (:307-356): when a path is present, skip `CREATE_ON_NON_FROZEN_UDT` (:355) and the frozen-collection rule (:340). Apply the existing rules to the leaf: duration, FULL only on a frozen leaf collection, KEYS/ENTRIES only on a map leaf.
- Refuse: vector or duration leaves, non-SAI index classes (2i), and paths through a map key.
- `StorageAttachedIndex.validateOptions` (:301-402): replace `TypeUtil.cellValueType(target.left, target.right)` (:336) with `leafType`. The duplicate check (:309-322) compares the triple.
- Analyzer validation (`AbstractAnalyzer.java:209`) runs on the leaf type, so analyzers work on text leaves.
- No version gate (§E).

**"Index all leaves" form.** Not in v1. DSE builds all leaves by default (C2.4), but for SAI it would be N indexes hidden behind one name, each with its own options. That breaks the 1:1 model of index name to components to options. A scripting helper or documented DDL covers migration. If needed later: `CREATE INDEX … ON t (addr.*)` expanding to N named indexes. Owner to decide.

## B. CQL query surface

**Leaf relations.** Extend `relation` (`Parser.g:1824-1871`) with `fieldPath` (`cident ('.' fident)+`) on the left of these forms:
- `relationType term`
- `LIKE`
- `IN` / `NOT IN`
- `CONTAINS` / `NOT CONTAINS`, for leaves that are collections
- `IS NOT NULL`
- `[key]`: not in v1

```
WHERE addr.city = 'Rome'           WHERE p.home.loc.lat >= 1.0 AND p.home.loc.lat < 2
WHERE addr.street LIKE 'b%'        WHERE t.field2 IN (1, 2)        WHERE addr.city != 'Paris'
WHERE addr.loc.lat IS NOT NULL     WHERE addr.tags CONTAINS 'y'
```

Text range and `LIKE` work on leaf indexes with no extra code, through HCD-642 and HCD-637 (`supportsLiteralRangeQueries` / `supportsLikeQueries`, `IndexContext.java:757,836`), once the flags in §0.4 are computed from the path.

**Multi-valued paths (collections of UDTs).** A leaf relation outside a group means **some element satisfies it**. This matches DSE `{!tuple}addrs.city:Rome` and CQL `CONTAINS`. AND-ed leaf relations are **independent**: each may be satisfied by a different element. That matches DSE separate `q`/`fq` and MongoDB dotted paths.
- `!=` and `NOT IN` on a multi-valued path mean **no element equals**. This follows `NOT CONTAINS` and SAI's NOT_EQ, which is computed as "all rows minus the EQ postings". It is the CQL form of DSE `-{!tuple}…`.

**Same element: explicit group syntax. Recommended:**
```
WHERE addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)
WHERE addrs CONTAINS ELEMENT (city = 'Rome' AND (zip >= 150 OR loc.lat < 2) AND zip != 100)
WHERE addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 100)   -- no element satisfies
WHERE p.home.names CONTAINS ELEMENT (first = 'Bilbo' AND last = 'Baggins')  -- collection inside a UDT
```
- Inside the parentheses the normal `expression` rule (`Parser.g:444-452`) is reused, built into a nested `WhereClause`. Names are paths relative to the element; nested and cross-level paths are allowed, which is more than DSE (C4.2.4, runtime "mixing nesting levels").
- `ELEMENT` becomes a new **unreserved** keyword (it goes into `basic_unreserved_keyword`).
- Grammar cost: one alternative in `relation`. A syntactic predicate `(K_ELEMENT '(' fident)=>` keeps `CONTAINS element(x)` working as a call to a UDF named `element`; no current term starts with `ident relop`.
- Why this form: CQL users already read `CONTAINS` as element membership. `NOT CONTAINS` gives "none" for free. A single frozen UDT needs no group, since AND on one instance is the same element anyway.

Alternatives considered:
1. Implicit grouping, DSE-like (AND-ed leaf relations on one collection mean the same element). Rejected: it makes independent conditions impossible to write, and it contradicts `CONTAINS a AND CONTAINS b`.
2. Partial literal `addrs CONTAINS {city:'Rome', zip:100}`. Rejected: today that literal means equality with the whole element, with the other fields null. Changing it would silently break existing queries, and it cannot express ranges or OR.
3. N1QL style `ANY addrs SATISFIES (…)`. Two new keywords for the same power.
4. `CONTAINS (…)` without a keyword. Ambiguous with tuple literals (`ts CONTAINS ('foo', 1)` is valid today) and with casts.

## C. Execution

**Term extraction.** `IndexContext` gets a `FieldPath` (null for today's indexes) and `validator = leafType`. Everything goes through `getValueOf` / `getValuesOf` (`IndexContext.java:841-889`), so changing them covers the memtable, the SSTable writer and FilterTree.
- Add `isMultiValued()`. It replaces `isNonFrozenCollection()` in `index`, `update`, `validateMaxTermSizeForRow`, `SSTableIndexWriter.addRow:121` and `FilterTree:108`.
- Keep `isNonFrozenCollection()` for the operator-family logic in `supports()` (:800-809).

How a value is reached, step by step:
- **Frozen UDT or tuple.** `TupleType.split(ByteBufferAccessor, v)` (`TupleType.java:267`). Shorter values give trailing nulls; recurse for nested types. Splitting allocates nothing, since the components are slices.
- **Non-frozen UDT, first step.** `row.getCell(column, userType.cellPathForField(...))`, the same approach as `ColumnCondition.UDTFieldAccessBound.rowValue` (:661-669). Later steps split frozen values.
- **Non-frozen list/set of frozen UDTs.** Iterate `ComplexColumnData`, as `TypeUtil.collectionIterator` (:330) does: list value = `cell.buffer()`, set value = `cell.path().get(0)`.
- **Frozen list/set and frozen leaf collections.** Unpack with the collection serializer.
- **Clustering, partition-key and static columns.** Same source as today (:847-863). Frozen UDTs in keys get leaf indexes at no cost; DSE refuses them.

**Writes.**
- Memtable: `updateRow(old, new)` receives the reconciled row (`BTreePartitionUpdater:68`, `StorageAttachedIndex:920`). An `UPDATE SET p.age = 41` therefore re-extracts the leaf from the merged cells. The multi-valued branch of `update` (:444-448) handles collection paths.
- SSTables: each SSTable indexes the cells it has. Stale leaf terms in older SSTables are removed by SAI's normal post-filter on the merged row, exactly as for regular columns.
- Null or unset field: no term, as DSE does (C3.3). An empty string is indexed for text leaves only, as for top-level columns.

**Correctness for same-element groups: no change to the postings format.**
1. The index side (Operation / plan) turns an `ELEMENT` group into the AND/OR of its *positive* leaf expressions with the prefix added to their paths. That is a candidate superset (each leaf matches "some element").
   - `!=` inside a group is element-level, so it is **not** pushed to the index.
   - A group with no positive indexed leaf restricts nothing. It needs another restriction or ALLOW FILTERING, through the existing `getColumnsWithUnsupportedIndexRestrictions` path.
2. A new FilterTree node `ElementFilter(prefix, childTree)` iterates the elements at the prefix and evaluates the child expressions, which extract their relative path from each element buffer. SAI already re-reads and post-filters every candidate row, so the only extra cost is CPU plus false-positive reads.

Rejected alternative: element ordinals in the postings, with a positional intersection. It needs a new SAI on-disk version, plus memtable changes and term-dictionary payloads, to save some false-positive reads. Revisit only if benchmarks show high false-positive rates.

## D. Restrictions and RowFilter

**Relations.**
- Add `FieldRelation(ColumnIdentifier, List<FieldIdentifier>, op, value | inValues | escape)`. It is a new `Relation` subclass, or `SingleColumnRelation` with a `fieldPath` alongside `mapKey` (:54).
- The receiver is the leaf `ColumnSpecification`, built with `UserTypes.fieldSpecOf` (:42) at each step, plus a tuple equivalent. That gives correct bind-marker types for prepared statements.
- It must bypass the "Non-frozen UDT … cannot be restricted" check (`SingleColumnRelation.java:436-440`).

**Restrictions.**
- Give `SingleColumnRestriction` a nullable `FieldPath`. EQ, IN, Slice, Like, Contains and a new IsNotNull-for-select pass it through. `shouldMerge` (`SingleRestriction.java:148`) is false when the paths differ, so the `RestrictionSet` multimap holds one restriction per (column, path); BM25+MATCH is a precedent for several per column.
- `isSupportedBy(Index)` calls a new `Index.supportsExpression(ColumnMetadata, FieldPath, Operator)`, default `false`.
- `StatementRestrictions` (:454): allow `IS_NOT` when `relation.hasFieldPath()`.
- Add `ElementMatchRestriction(column, prefix, nested RestrictionSet/WhereClause tree)`.

**RowFilter.**
- Add `Kind.FIELD(5)`, a `FieldExpression(column, FieldPath, operator, value, analyzer)`. It is serialized as the column name, the operator, the path (vint count, then per step a tag byte plus a vint position) and the value.
- Add `Kind.ELEMENT_MATCH(6)`, an `ElementMatchExpression(column, prefix, FilterElement)` that reuses `FilterElement.Serializer` (:967-1016).
- Add `Expression.fieldPath()`, returning null for other kinds.
- In the default `Index.supportsExpression(RowFilter.Expression)`, return `false` when `fieldPath() != null`. This protects whole-UDT SAI indexes, legacy 2i and custom indexes.
- `IndexRegistry.getBestIndexFor` and `getAnalyzerFor` (`IndexRegistry.java:112-118, 372-391`) get path overloads, so EQ/MATCH behaviour (`SingleColumnRelation.java:224`) and analyzer normalization (the HCD-642 RowFilter change) apply per leaf.
- `isSatisfiedBy` with ALLOW FILTERING and no index: extract the leaf with `FieldPath`. Single-valued: compare with `leafType`. Multi-valued: "exists", or "none" for NEQ / NOT IN / NOT CONTAINS. The element group runs its `FilterElement` against each element. With no index, filtering gives the same results as SAI; that is a test invariant.

**SAI planner.**
- `QueryController.getContext` / `getBestIndexFor` (:252-269, :908) receive a path. The fallback IndexContext for an unindexed leaf carries the path.
- `determineIndexTargetType` (:276-300) applies to the leaf.
- `StorageAttachedIndex.supportsExpression(RowFilter.Expression)` (:738) compares the path exactly.
- Re-key on a `(ColumnMetadata, FieldPath)` record: `Operation.analyzeGroup`, `FilterTree`, `plan/Expression.equals`/`hashCode`.

**Mixed versions.** The coordinator refuses before sending (§E). Replicas never see the new kinds while an old node is in the cluster.

## E. Versioning

**No version change (final owner decision).** HCD-653 adds no messaging version, no SAI on-disk version and no file-format version, and CREATE INDEX on a field has no version gate.
- **SAI on-disk.** A field index is written with the SAI version of the SSTable, like any other index (CNDB-8756: one version per SSTable, `IndexDescriptor.versionForNewComponents`). Its files are ordinary scalar or literal index files, with the terms and comparators of the leaf type (`TypeUtil.comparator` / `useFastByteOperations`, :349-367, run on the validator); only the index definition (the target `addr.city`) says which values they hold. Several values per row already exist for collection indexes. Works on every SAI version from AA on; on AA `update()` re-indexes (:429).
- **Schema (DDL).** DDL is not allowed while the cluster has mixed versions (the Apache Cassandra rule for rolling upgrades), so field indexes are created only once every node runs a version with this feature, and a node without it never receives such an index (§0.1). There is no check in CREATE INDEX.
- **Queries (from P2).** During an upgrade, a field restriction can reach an old replica only through ALLOW FILTERING (no field index can exist yet). The old replica fails to deserialize the new `RowFilter` kind with "Unknown index expression kind": a clean error, never wrong results. To be written in `NEWS.txt` (P6).
- **Rejected alternatives**, kept for the record: a messaging version `VERSION_DS_21 = 111` with a CREATE INDEX / SELECT gate (with or without 110/111 equivalence for schema, repair and durable files); a SAI version `GB` selected per keyspace or forced per index; comparing release versions through `Gossiper.getMinVersion()`.
- **Docs.** `NEWS.txt`: say the feature can be used only once all nodes are upgraded, that no index rebuild is needed, and that drivers show the target `addr.city` as a column name in index metadata.

## F. DSE `solr_query` to CQL

| DSE (`dse-runtime.md`, C7.2) | CQL (SAI) | Status |
|---|---|---|
| `{!tuple}addr.city:Rome`, `direc.city:bcn`, `skill.name:masonry`, `name.firstname:fn_0` | `WHERE addr.city = 'Rome'` | P2 |
| `{!tuple}addr.loc.lat:1.0`, `direc.name.firstname:Bilbo`, `nestedtuple.field3.field1:6` | `addr.loc.lat = 1.0`; `nestedtuple.field3.field1 = 6` | P2 |
| `{!tuple}addr.street:b*`; graph `*x`, `/.*x.*/` | `addr.street LIKE 'b%'` / `'%x'` / `'%x%'` | P2 (HCD-637) |
| `{!tuple}addr.city:rome` gives none (StrField) | `= 'rome'` gives none by default; `case_sensitive:false` gives a match (= `lowerCase`) | P2 |
| `addr.city:[P TO S]`, `direc.zip:[0 TO 5]`, `t.field2:[2 TO *]`, graph `{23 TO *}` | `>=` / `<=` / `>` / `<` | P2 (HCD-642) |
| `addr.city:*`, `direc.zip:*`, `udtcol_list.street:*` | `addr.city IS NOT NULL`; `addrs.street IS NOT NULL` (some element) | P5 |
| `_parent_:F AND -direc.empty_field:[* TO *]`; `*:* AND NOT addr.loc.lat:*` (error in DSE) | `addr.loc.lat IS NULL`: **gap**, not in v1. `NOT CONTAINS ELEMENT (x IS NOT NULL)` for collections, P5 | gap |
| `p.age:41` after `UPDATE SET p.age` (non-frozen) | `p.age = 41` | P2 |
| `p.home.city:Rome` | `p.home.city = 'Rome'` | P2 |
| `t.field1:foo` | `t.field1 = 'foo'` | P2 |
| `direc.name.firstname:(Bilbo OR Sam)`, graph Within | `IN ('Bilbo','Sam')` | P2 |
| graph Neq `(x:* AND -x:23)` / Without | `x != 23` / `NOT IN` (single-valued; null semantics to verify, §H) | P2 |
| date `direc.time:"1970-…Z"` | `addr.time = '1970-…Z'` | P2 |
| `addrs.city:Rome`, `udtcol_set.street:*`, `tuplecollection_list.field1:3`, `names.first:foo` | `addrs.city = 'Rome'`; `ts.field1 = 3` (some element) | P3 |
| `addrs.tags:y`, `direc.listfield:listval1`, `tuplewithcollection.field3:(a OR b)` | `addrs.tags CONTAINS 'y'`; `… CONTAINS 'a' OR … CONTAINS 'b'` | P3 |
| `direc.names.firstname:Bilbo`, `direc.names.hobbies:swim1`, `tuplewithtuplecollection.field3.field1:9` | the same dotted paths (collections at any level) | P3 |
| `addrs.zip:[150 TO *]`, `addrs.loc.lat:[2 TO 6]` | ranges on some element | P3 |
| `addrs.city:Rome AND addrs.zip:100` (same element), `ts.field1:foo AND ts.field2:2` | `addrs CONTAINS ELEMENT (city='Rome' AND zip=100)` | P4 |
| `addrs.city:Rome OR addrs.city:Milan` | `addrs.city IN (…)`, or OR inside ELEMENT | P3/P4 |
| `addrs.city:Rome AND NOT addrs.zip:100` | `CONTAINS ELEMENT (city='Rome' AND zip != 100)` (nulls differ, §H) | P4 |
| separate `q` / `fq` (independent elements) | `addrs.city='Rome' AND addrs.zip=100` | P3 |
| `id:1 AND {!tuple}addrs.city:Rome`; `{!tuple}a AND {!tuple}b` across columns | ordinary AND / OR with other restrictions | P2/P3 |
| `zip:200 AND loc.lat:1.0` mixing levels (DSE: no match) | `CONTAINS ELEMENT (zip=200 AND loc.lat=1.0)` matches (better than DSE) | P4 |
| `-{!tuple}(…)` | `NOT CONTAINS ELEMENT (…)` / leaf `!=` on multi-valued path | P3/P4 |
| `direc.mapfieldkey2:dynVal2` (frozen map leaf, dynamic field) | `direc.mapfield['key2'] = 'dynVal2'`: **gap in v1**; later `entries(direc.mapfield)` | gap |
| CQL `WHERE skill={name:'masonry',type:'bricks'}` / `=('masonry','bricks')` | works today with a whole frozen-UDT/tuple SAI index (EQ) | exists |
| `WITH COLUMNS id, p.age`; `COLUMNS * {excluded}`, `tuplevalue.field2 {excluded:false}` | one `CREATE INDEX … (p.age)` per leaf | P1 |
| `{docValues}`, `copyField` | n/a (SAI terms are always searchable; no copy fields) | n/a |
| sort / facet on sub-fields (refused by DSE) | `ORDER BY` on a leaf refused (`supports(ORDER_BY…)` requires `!isMultiValued()` and a top-level column in v1) | same as DSE |
| phrase / fuzzy / regex / TextField leaf | `index_analyzer` on a leaf plus `:` (ANALYZER_MATCHES); no fuzzy/regex, as for top-level SAI | partial |
| `map<text,frozen<udt>>` (refused by DSE) | `values(m).city` | beyond DSE |

## G. Phasing (one PR, one commit per phase)

**P1: `FieldPath`, DDL, index build.**
- Files: `FieldPath` (new), `Parser.g` (`indexIdent`), `IndexTarget`, `TargetParser` (triple), `CreateIndexStatement`, `StorageAttachedIndex` (validateOptions, constructor :251, `dependsOn`), `IndexContext` (path, validator, `isMultiValued`, `getValueOf`/`getValuesOf`), `SSTableIndexWriter`, `AlterTypeStatement.RenameFields` guard, default index name.
- Tests:
  - `FieldPathTest`: resolve, quoting, tuple `fieldN`, errors, extraction from frozen / non-frozen / nested / short values.
  - `cql/UdtFieldIndexDDLTest`: accepted and refused shapes; duplicates; DESCRIBE round trip; RENAME refused; `ADD` field then build; non-frozen UDT; index building after flush and compaction, checked through the `IndexesSystemView` / `SSTableIndexesSystemView` cell counts.

**P2: leaf WHERE on single (non-collection) UDTs and tuples.**
- Files: `Parser.g` `relation`, `FieldRelation`, restrictions with a path, `RowFilter` FIELD kind and serializer, `Index` / `IndexRegistry` path overloads, `QueryController`, `Operation`, `FilterTree`, `plan/Expression` keyed by (column, path).
- Tests: `cql/UdtFieldQueryTest`, parameterized frozen / non-frozen, nested, tuple:
  - EQ, IN, NEQ, ranges (numeric and text), LIKE, analyzers.
  - Each through `beforeAndAfterFlush` (`CQLTester.java:2754`) plus `compact()`.
  - Non-frozen single-field UPDATE / DELETE / TTL in memtable and across SSTables.
  - Null and unset fields.
  - Whole-UDT index and leaf index on the same column, each serving only its own form.
  - ALLOW FILTERING without an index gives the same results.
  - Prepared statements with bind markers.
- `RowFilterTest` / new `FieldExpressionSerializationTest`: round trip and `serializedSize`.

**P2 implementation notes** (where the implementation differs from or details the plan above):
- *Relations.* No new `FieldRelation` class: `SingleColumnRelation` has a nullable list of field names beside `mapKey` (the alternative the plan allowed). The grammar adds the optional `indexFieldPath` after `cident` in the `relationType`, `LIKE`, `IS NOT NULL`, `IN` / `NOT IN` (marker and literals) and `CONTAINS` forms; the map-entry form and LWT conditions are unchanged. The receiver is `FieldPath.leafSpecification` (the leaf type, named in CQL form, `addr.loc.lat` or `"Addr"."City"`), also for tuples.
- *Restrictions.* `SingleRestriction.fieldPath()` (null for columns). Instead of overriding `shouldMerge` in every class, `RestrictionSet.Builder` merges only restrictions with equal paths, which keeps the BM25/MATCH override as is. A field restriction on a primary key column goes to the non-primary-key restriction set (index or filtering), never to the partition key or clustering restrictions; but, as for the partition key columns, a restriction on a field of a partition key column must be in the top-level AND (the filter evaluates the partition-level expressions separately from the row-level ones, so it can't apply an OR between them). `SELECT DISTINCT` refuses the restrictions on the fields of clustering columns (they select rows), and accepts those on the fields of partition key and static columns. Field restrictions are refused in UPDATE / DELETE and in materialized views, `IS NOT NULL` on a field is refused (P5), `CONTAINS KEY` on a field is refused (map-key forms), paths through collections are refused with "Restrictions on fields inside collections are not yet supported: <path>" (P3).
- *Error messages.* A missing ALLOW FILTERING names the restricted fields no index serves (`StatementRestrictions.FIELD_REQUIRES_ALLOW_FILTERING_MESSAGE`); an index on the field that doesn't support the operator is named as `addr.city` in the existing "has an index but does not support the operators" message (new `Index.isOnField(column, path)`). The messages and warnings of the restrictions (unset values, multiple indexes for CONTAINS) name the field.
- *Index API.* `Index.supportsExpression(column, path, op)` defaults to "path is null and `supportsExpression(column, op)`", and the default `supportsExpression(RowFilter.Expression)` dispatches on `fieldPath()`: an index that doesn't override the path method never serves a field expression.
- *RowFilter.* `FieldExpression` extends `SimpleExpression` (analyzer, LIKE and range handling reused), kind `FIELD(5)`. Path wire format: unsigned vint step count (1 to 64), then per step a tag byte (`Step.Kind.tag`: 0 field, 1 elements) and, for a field step, the position as an unsigned vint. A path that doesn't match the receiver's column type fails the deserialization with "Invalid field path"; a multi-valued path is refused. `isMutableIntersection` counts the fields of a non-frozen (multi-cell) UDT as different targets (different cells); the fields of a frozen column are the same target as the column. The partition-key and clustering-only checks go through the new `Expression.isSatisfiedByColumnValue`, so they extract the field; `without(column, op, value)` and `withoutFirstLevelExpression(column)` keep the expressions on the fields of the column.
- *SAI planner.* `Operation.analyzeGroup` groups and range-merges by `(column, path)` internally, but still returns a multimap keyed by column (the expressions of each target next to each other): `FilterTree` evaluates each `plan/Expression` with its own `IndexContext`, which extracts its own field, so its key only selects the static row and doesn't need the path. `plan/Expression.equals`/`hashCode` include the path.
- *Same semantics as the columns.* Text NEQ / NOT IN are not served by SAI (EQ-only literal types), so they need ALLOW FILTERING on fields too. `IN` on an analyzed text column fails with "IN operation does not support analyzers" (pre-existing, also on columns; not fixed here). A range plus `!=` on the same literal column returned no rows (HCD-642 code): fixed on the HCD-642 branch, which also normalizes the values excluded by `!=` / `NOT IN` with the normalizing index serving the ranges on the column.
- *`!=` normalization on fields.* The same applies to the fields, per field: a `!=` or `NOT IN` on a text field is normalized by the analyzer of the index serving the ranges on that exact field (`case_sensitive: false`, `normalize`, `ascii`), combined with a range (SAI: `QueryController.getAnalysisContext` probes with a `FieldExpression`) and alone with ALLOW FILTERING (`RowFilter.analyzerFor` with a path, and `FieldExpression` evaluates the shared per-row analysis on the field value). An index on the whole column never normalizes a field, and an index on a field never normalizes the column. Tokenizing analyzers serve no ranges, so `!=` on their fields compares the raw values. `Operation.analyzeGroup` merges a `!=` into the previous expression of the same `(column, path)` target only if that expression's context is the `!=`'s analysis context.
- *Open (not P2).* With the default memtable of the unit tests (`TrieMemtable`), a row whose only content is the deletion of a whole non-frozen column (`DELETE s FROM t WHERE ...`) is not written by the flush: the deleted values come back for every read, with or without SAI (`ComplexDeletionAcrossSSTablesTest`, ignored; see the review ledger). `UdtFieldQueryTest` deletes the fields of a non-frozen UDT one by one until it is fixed.

**P3: collections, "some element".** List/set frozen and non-frozen, map values, leaf collections (`CONTAINS`), NEQ on multi-valued paths meaning "none".
- Tests: `cql/UdtCollectionFieldQueryTest`, the DSE u_list / t_list data, duplicate elements, removal of a list element in the memtable (update path), set element deletion.

**P4: `CONTAINS ELEMENT` group.**
- Files: grammar, `ElementMatchRestriction`, `ELEMENT_MATCH` kind, Operation expansion, `FilterTree.ElementFilter`.
- Tests: same element vs independent elements, OR / NEQ inside, cross-level, `NOT CONTAINS ELEMENT`, a group with no indexed leaf (needs ALLOW FILTERING), equivalence with filtering, serialization.

**P5: `IS NOT NULL` on leaves.**
- Full unbounded range on the leaf index; HCD-642 already includes the empty term (`TrieMemoryIndex`). Needs `Expression` to accept a range with no bounds, and a `StatementRestrictions` exemption.

**P6: DSE compatibility test and docs.**
- `cql/DseSearchUdtCompatibilityTest`: mirrors the `Cql3TupleTest` scenarios (C6.2: `testSubFieldSearch`, `testNestedUDTSearch`, `testCollectionsInsideUDTSearch`, `testUDTCollectionInsideUDTSearch`, `testCollectionsOfUDTSSearch`, `testBlockJoins`, `testEmptySubFieldSearch`, `testDateSubFieldSearch`), `FrozenCollectionsTest` (:145, :169) and every row of `dse-runtime.md`, with a comment giving the `solr_query` beside each CQL query.
- Distributed test (`test/distributed/.../sai/UdtFieldDistributedTest`): field queries on a multi-node cluster; the mixed-version behaviour of §E (an old replica fails a filtering query with "Unknown index expression kind") if an upgrade test harness can run it.
- `NEWS.txt` and the SAI docs (`supported-query-operators-list.adoc`, FAQ).

## H. Risks and open questions for the owner

1. **Versioning.** Decided: no version change (§E).
2. **Group syntax.** Confirm `CONTAINS ELEMENT (…)` and the unreserved keyword `ELEMENT`. Also tuple naming `fieldN` vs `[n]`.
3. **NEQ and nulls.** DSE `NOT zip:100` matches elements whose zip is null. CQL / SAI `!=` presumably excludes nulls; not verified, test before documenting. Also the "none" meaning of NEQ on multi-valued paths.
4. **False positives.** The candidate superset for same-element groups may read many rows on wide collections, for example `city='Rome' AND zip=100` when both values are common but never in the same element. Add a metric or trace counter for "rows rejected by ElementFilter".
5. **RENAME.** Refusing it is a usability regression compared with no indexes; the alternative is rewriting the targets.
6. **Cost of many indexes.** Each leaf index costs about the same as a column index (memtable plus components). DSE users index all leaves by default and could end up with dozens. Document guardrails: `sai_indexes_per_table` (already a guardrail?); no indexes on blob leaves (DSE does not index them either).
7. **Drivers and cqlsh.** Index metadata parsers see `addr.city` as a column name. cqlsh completion lags the grammar.
8. **Not in v1.**
   - `IS NULL` on a leaf.
   - Map-key paths and `mapfield['k']` leaves.
   - ORDER BY / BM25 on leaves.
   - Vector leaves.
   - The "index all leaves" shorthand.
   - Returning only the matching element (DSE does not either, C4.6).
9. **`AbstractAnalyzer` frozen-collection refusal** (`AbstractAnalyzer.java:209`) must look at the leaf type, otherwise a text leaf of a frozen UDT can't get `case_sensitive:false`.

### Critical files for implementation
- /home/eolivelli/dev/cassandra4/src/java/org/apache/cassandra/index/sai/IndexContext.java
- /home/eolivelli/dev/cassandra4/src/java/org/apache/cassandra/db/filter/RowFilter.java
- /home/eolivelli/dev/cassandra4/src/java/org/apache/cassandra/index/sai/plan/Operation.java (with `plan/FilterTree.java`, `plan/QueryController.java`, `plan/Expression.java`)
- /home/eolivelli/dev/cassandra4/src/antlr/Parser.g (with `cql3/SingleColumnRelation.java`, `cql3/restrictions/SingleColumnRestriction.java`, `cql3/restrictions/RestrictionSet.java`)
- /home/eolivelli/dev/cassandra4/src/java/org/apache/cassandra/index/TargetParser.java (with `cql3/statements/schema/IndexTarget.java`, `CreateIndexStatement.java`, `AlterTypeStatement.java`, `index/sai/StorageAttachedIndex.java`, `net/MessagingService.java`)