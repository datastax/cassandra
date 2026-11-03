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
- *Restrictions.* `SingleRestriction.fieldPath()` (null for columns). Instead of overriding `shouldMerge` in every class, `RestrictionSet.Builder` merges only restrictions with equal paths, which keeps the BM25/MATCH override as is. A field restriction on a primary key column goes to the non-primary-key restriction set (index or filtering), never to the partition key or clustering restrictions; but, as for the partition key columns, a restriction on a field of a partition key column must be in the top-level AND (the filter evaluates the partition-level expressions separately from the row-level ones, so it can't apply an OR between them). `SELECT DISTINCT` refuses the restrictions on the fields of clustering columns (they select rows), and accepts those on the fields of partition key and static columns. Field restrictions are refused in UPDATE / DELETE and in materialized views, `IS NOT NULL` on a field is refused (P5), `CONTAINS KEY` on a field is refused (map-key forms), paths through collections were refused with "Restrictions on fields inside collections are not yet supported: <path>" until P3 (now supported, see the P3 notes).
- *Error messages.* A missing ALLOW FILTERING names the restricted fields no index serves (`StatementRestrictions.FIELD_REQUIRES_ALLOW_FILTERING_MESSAGE`); an index on the field that doesn't support the operator is named as `addr.city` in the existing "has an index but does not support the operators" message (new `Index.isOnField(column, path)`). The messages and warnings of the restrictions (unset values, multiple indexes for CONTAINS) name the field.
- *Index API.* `Index.supportsExpression(column, path, op)` defaults to "path is null and `supportsExpression(column, op)`", and the default `supportsExpression(RowFilter.Expression)` dispatches on `fieldPath()`: an index that doesn't override the path method never serves a field expression.
- *RowFilter.* `FieldExpression` extends `SimpleExpression` (analyzer, LIKE and range handling reused), kind `FIELD(5)`. Path wire format: unsigned vint step count (1 to 64), then per step a tag byte (`Step.Kind.tag`: 0 field, 1 elements) and, for a field step, the position as an unsigned vint. A path that doesn't match the receiver's column type fails the deserialization with "Invalid field path"; a multi-valued path is refused. `isMutableIntersection` counts the fields of a non-frozen (multi-cell) UDT as different targets (different cells); the fields of a frozen column are the same target as the column. The partition-key and clustering-only checks go through the new `Expression.isSatisfiedByColumnValue`, so they extract the field; `without(column, op, value)` and `withoutFirstLevelExpression(column)` keep the expressions on the fields of the column.
- *SAI planner.* `Operation.analyzeGroup` groups and range-merges by `(column, path)` internally, but still returns a multimap keyed by column (the expressions of each target next to each other): `FilterTree` evaluates each `plan/Expression` with its own `IndexContext`, which extracts its own field, so its key only selects the static row and doesn't need the path. `plan/Expression.equals`/`hashCode` include the path.
- *Same semantics as the columns.* Text NEQ / NOT IN are not served by SAI (EQ-only literal types), so they need ALLOW FILTERING on fields too. `IN` on an analyzed text column fails with "IN operation does not support analyzers" (pre-existing, also on columns; not fixed here). A range plus `!=` on the same literal column returned no rows (HCD-642 code): fixed on the HCD-642 branch, which also normalizes the values excluded by `!=` / `NOT IN` with the normalizing index serving the ranges on the column.
- *`!=` normalization on fields.* The same applies to the fields, per field: a `!=` or `NOT IN` on a text field is normalized by the analyzer of the index serving the ranges on that exact field (`case_sensitive: false`, `normalize`, `ascii`), combined with a range (SAI: `QueryController.getAnalysisContext` probes with a `FieldExpression`) and alone with ALLOW FILTERING (`RowFilter.analyzerFor` with a path, and `FieldExpression` evaluates the shared per-row analysis on the field value). An index on the whole column never normalizes a field, and an index on a field never normalizes the column. Tokenizing analyzers serve no ranges, so `!=` on their fields compares the raw values. `Operation.analyzeGroup` merges a `!=` into the previous expression of the same `(column, path)` target only if that expression's context is the `!=`'s analysis context.
- *Open (not P2).* With the default memtable of the unit tests (`TrieMemtable`), a row whose only content is the deletion of a whole non-frozen column (`DELETE s FROM t WHERE ...`) is not written by the flush: the deleted values come back for every read, with or without SAI (`ComplexDeletionAcrossSSTablesTest`, ignored; see the review ledger). `UdtFieldQueryTest` deletes the fields of a non-frozen UDT one by one until it is fixed.

**P3: collections, "some element".** List/set frozen and non-frozen, map values, leaf collections (`CONTAINS`), NEQ on multi-valued paths meaning "none".
- Tests: `cql/UdtCollectionFieldQueryTest`, the DSE u_list / t_list data, duplicate elements, removal of a list element in the memtable (update path), set element deletion.

**P3 implementation notes** (where the implementation differs from or details the plan above):
- *Relations.* `SingleColumnRelation` no longer refuses the paths through collections: every operator of P2 works on them (EQ, IN, NOT IN, `!=`, ranges, LIKE, `:`, `CONTAINS` / `NOT CONTAINS` on a leaf collection of the elements). The receiver of a bind marker is the field of the elements (`addrs.city`, text). Still refused: map-key paths (`FieldPath.resolve`), `CONTAINS KEY` on fields, `IS NOT NULL` on fields (P5); `CONTAINS ELEMENT` was not in the grammar yet (added by P4, see the P4 notes).
- *Semantics.* A relation on a multi-valued path is satisfied when **some** value of the path (the field of some element; the unset fields are skipped) satisfies it. `!=`, `NOT IN` (AND-ed `!=`) and `NOT CONTAINS` are satisfied when **no** value is equal to (contains) the operand; so they include the rows with an empty or null collection and the rows whose elements all have the field unset, as `NOT CONTAINS` on a collection column does. `RowFilter.FieldExpression` (filtering, coordinator re-checks) and the SAI post-filter (`plan/Expression.isSatisfiedBy(Iterator)`) implement the same any/none rule, so index-served and filtered results are identical (the twin-table invariant of the tests).
- *Independent relations, also on the same path.* `RestrictionSet.Builder` doesn't merge the AND-ed restrictions on a multi-valued path: `addrs.city = 'Rome' AND addrs.city = 'Milan'` is valid (an element in Rome and an element in Milan, as `CONTAINS a AND CONTAINS b`), and two bounds, or a bound and a `!=`, stay separate restrictions and separate `RowFilter` expressions.
- *Merged ranges: decision.* `addrs.zip >= 150 AND addrs.zip < 300` means "some element with zip >= 150, and some element (possibly another one) with zip < 300": the bounds are independent, as every other AND-ed relation, and as the filtering evaluates them (one `FieldExpression` per bound). `Operation.analyzeGroup` therefore never combines the expressions of a multi-valued target into one range (nor a `!=` into a range as an exclusion): a combined range is evaluated on each element by the post-filter, which would require a single element inside both bounds and return fewer rows than the filtering (`addrs.zip > 150 AND addrs.zip < 150` selects u_list row 1, with 200 and 100). Each bound is an index scan of its own, intersected; the same-element range is P4's `CONTAINS ELEMENT (zip >= 150 AND zip < 300)`. The only combination is that of the AND-ed `!=` (a `NOT IN`), see below.
- *SAI `!=` / `NOT IN` on a multi-valued field index.* The planner served `!=` as the union of the two half ranges around the value (`QueryController.buildInequalityPlan`), which misses the rows without any value. On a multi-valued field context (not on the indexes of whole collection columns, e.g. map entries, which keep their plan) it uses the index scan of `NOT CONTAINS`: all the keys minus the keys with an equal value (`IndexContext.getNonEqIterator` / `SSTableIndex.getNonEqIterator`), then the post-filter ("no value equals"). It only needs equality, so `IndexContext.supports(NEQ)` is true on these contexts also for the EQ-only types (text, ascii, boolean, uuid), unless the index has a tokenizing analyzer or indexes the values of a leaf collection: `addrs.city != 'Rome'` and `addrs.city >= 'a' AND addrs.city != 'rome'` are served without `ALLOW FILTERING` (as on single fields in P2), and a normalizing index normalizes the excluded values (`RowFilter.analyzerFor` finds it for NEQ directly). A `NOT IN (v1..vk)` (k AND-ed `!=`) is one `plan/Expression` NOT_EQ with exclusions (`Operation.analyzeGroup` combines the `!=` of a multi-valued target, and only them), served by a single anti-join, all the keys minus the union of the keys matching each value (`Expression.negations`), and post-filtered as "no value in the set" (`Expression.isSatisfiedBy` checks the exclusions of a NOT_EQ).
- *Anti-join and stale terms (reviews B1, F1, F2 and the split review; own commit "SAI: NOT CONTAINS and NOT CONTAINS KEY must not subtract keys whose index terms may be stale (TTL, partition and range deletions)", below the HCD-653 commits, which P3 extends to `!=` / `NOT IN` on multi-valued fields).* The subtraction "all keys minus keys with an equal term" drops a row when an index has a term that the live merged row doesn't have and no source includes the key without the term. Cases: (1) a cell that expired (TTL) after being indexed: the indexes have no expiration; (2) a partition deletion or a range tombstone: it doesn't remove the terms of the rows it shadows from the memtable index; the skiplist, legacy trie and stage 2 trie memtables keep the shadowed rows too, so a re-inserted row (or a write older than the deletion) keeps the old cells in its reconciled row and the old terms in the index; the stage 3 trie memtable deletes the covered rows without an index callback, and a re-insert goes through `index()`, adding new terms next to the stale ones; either way the terms stay in the SSTable index written by the flush (the SAI flush writes the memtable index terms for the rows written; verified: with the SSTable check disabled, the delete + re-insert test fails right after the flush), and the source of the deletion has no row for the key; (3) during a flush the old and the new memtables are both live, and subtracting the keys matching in either memtable from the keys of both drops a key the new memtable has without the term. The row deletions are updates of their rows (`updateRow`; the shadowed cells are not in the new row), so they are not marked; `removeRow` is only called by compaction and cleanup (SAI's `indexerFor` serves only the update transactions), it isn't marked either. The fix: the subtraction is done memtable by memtable; a source that may have expired terms doesn't subtract its keys (per source: an expired term only matters for the anti-join of its source; a memtable index records the earliest expiration of its indexed expiring cells while the values are extracted, `MemtableIndex.markExpiringTerm` / `mayHaveExpiredTerms`, checking the minimum before any CAS; an SSTable with a TTL whose minimum local deletion time isn't after the query time, `IndexContext.mayHaveExpiredTerms`); a deletion that may shadow the terms of any source prevents the subtraction in all of them (global: `IndexContext.hasShadowingDeletions`, decided by `KeyRangeTermIterator` with the read command's `nowInSec`, now in `QueryContext`: a partition deletion or range tombstone applied to a memtable, `markShadowingDeletion` in `StorageAttachedIndex.UpdateIndexer`; an SSTable with partition deletions; an SSTable of a table with clustering columns whose minimum local deletion time isn't after the query time, as the SSTables don't record the range tombstones apart from the other tombstones). The histogram row estimates of the planner count all the rows of the sources that don't subtract. The cost: a table with partition deletions, or with clustering columns and any tombstone, searches non-equalities as a full scan plus post-filter; an SSTable with expiring cells and any tombstone (an INSERT of a non-frozen collection writes one) scans its own keys; a TTL that hasn't expired yet keeps the anti-join. Follow-up option (an on-disk change, not now): record in the SAI per-SSTable metadata whether the SSTable has partition or range deletion markers and the earliest expiration of its indexed cells, which would make the SSTable checks exact. The map-entry `!=` doesn't use the anti-join; its index-served results equal the filtered ones (tested).
- *Analyzers.* Normalizing indexes (`case_sensitive`, `normalize`, `ascii`) analyze each element value, at write and in the post-filter; the `!=` / `NOT IN` normalization of P2 applies per element (`FieldExpression` evaluates the shared analysis on each value; `RowFilter.AnalyzedValues` is now keyed by hash, a row having one entry per element). `IN` on an analyzed field fails as on an analyzed column ("IN operation does not support analyzers", pre-existing). Tokenizing analyzers (owner decision, review M1): refused by `CREATE INDEX` on the fields inside collections (paths with elements, including the values of a leaf collection inside the elements), with "index_analyzer and query_analyzer are not supported on indexes on fields inside collections (target addrs.city): results would depend on which element holds each token; use case_sensitive, normalize or ascii" (`StorageAttachedIndex.validateOptions`: any `index_analyzer` / `query_analyzer`, also a non-tokenizing Lucene chain such as keyword + lowercase; only the options `case_sensitive`, `normalize`, `ascii` are allowed). Reason: SAI would match each token of an EQ / `:` operand in some element, possibly a different one for each token, while the `RowFilter` requires all the tokens in one element, and the coordinator re-applies the `RowFilter` when it reconciles replicas (replica filtering protection), so the results would depend on the consistency level. To revisit when P4 can match the tokens within one element. Tokenizing analyzers stay allowed on single fields (P2).
- *Filtering cost.* `FieldExpression` visits the values of a multi-valued path with `FieldPath.anyValue`, stopping at the first deciding element (a match for a positive operator, an equal value for a negative one), locating the fields by offsets in the serialized values: no list or lambda per row (the predicates are constants), one buffer per visited value. A `NOT IN` of k values is still k `FieldExpression` on the `RowFilter` side (k visits per row, each stopping at the first excluded value); the wire form is unchanged.
- *Memtable update path.* `IndexContext.update` re-extracts all the values of the old and new rows (`getValuesOf`), so an element added, removed (list index, list value, set value, map key) or replaced (list index, map key) is reflected in the memtable index; duplicates in lists are indexed once per row (postings are per primary key). Pre-existing bug found and fixed (own commit "SAI: index empty strings written by updates to rows in the memtable"): `TrieMemoryIndex.update` skipped the empty values that `index` indexes for the literal types, so an empty string written by an update of a row in the memtable (e.g. an element appended to a non-frozen collection, or a frozen collection overwritten) was missing from the index; it now indexes (and removes) an empty value when `TypeUtil.skipsEmptyValue` is false. The same on a text column: the single-value `TrieMemtableIndex.update` returned early when both values were null or empty, so `UPDATE ... SET name = ''` on a row of the memtable without `name` was not indexed; it now tells null from empty.
- *`isMutableIntersection`.* Each expression on a field inside a non-frozen collection is a mutable target of its own, even with the same path (they may be satisfied by different elements, that is, different cells); inside a frozen collection all are on one cell, as for frozen UDTs. A field inside a frozen collection field of a non-frozen UDT (`p.addrs.city`) stays keyed by `(column, path)` (conservative).
- *RowFilter wire format.* Unchanged. `FieldExpression` accepts the multi-valued paths; it refuses a path that doesn't end with a field step ("it doesn't end with a field": `FieldPath.resolve` never produces one) besides the paths that don't match the column type.
- *Key and static columns.* Fields inside the elements of frozen collections in partition key and clustering columns, and of frozen and non-frozen static collections (including `!=` and element additions), work through the same extraction.
- *Open for P4-P6.* `CONTAINS ELEMENT` / same-element ranges and `!=` inside an element (P4, done); `IS NOT NULL` on multi-valued paths (P5); tokenizing analyzers on multi-valued paths (refused, revisit with P4); the same-element false-positive metric (§H.4) is P4.

**P4: `CONTAINS ELEMENT` group.**
- Files: grammar, `ElementMatchRestriction`, `ELEMENT_MATCH` kind, Operation expansion, `FilterTree.ElementFilter`.
- Tests: same element vs independent elements, OR / NEQ inside, cross-level, `NOT CONTAINS ELEMENT`, a group with no indexed leaf (needs ALLOW FILTERING), equivalence with filtering, serialization.

**P4 implementation notes** (where the implementation differs from or details the plan above):
- *Grammar.* `ELEMENT` is an unreserved keyword (`basic_unreserved_keyword`). The `CONTAINS` alternative of `relation` became `cident indexFieldPath? ( (K_CONTAINS K_ELEMENT elementMatchGroup)=> ... | (K_NOT K_CONTAINS K_ELEMENT elementMatchGroup)=> ... | containsOperator term )`, with `elementMatchGroup = '(' expression ')'` building a nested `WhereClause`. The syntactic predicates parse the whole group (the actions are disabled while they run), not just `'(' fident`: no term is a WHERE expression, so `CONTAINS element(1)`, `element(?)`, `element(foo(1))`, `element((int) 1)` and `CONTAINS ELEMENT ()` (a call without arguments) stay calls to a function named `element`, and `element` stays a valid column, field and function name (`element CONTAINS ELEMENT (element = 1)`). As for the other unreserved keywords used as function names, an unknown `ELEMENT()` is reported as "Unknown function ELEMENT called".
- *Relation.* New `ElementMatchRelation(column, collection path names, negated, WhereClause)` (`Relation.toRestriction` is no longer final: the group needs the index hints for the EQ behaviour of its conditions). The collection is the column or a field path (`p.home.names`, also through collections: `dirs.names` reaches the names of every element of `dirs`), whose type, unwrapped, must be a list, set or map whose elements (values) are UDTs or tuples, frozen or not; otherwise "CONTAINS ELEMENT requires a collection of user-defined types or tuples, but <path> is of type <type>". Each relation of the group is rewritten on its absolute path (`SingleColumnRelation.withColumnPrefix`: `city` becomes `addrs.city`; the first name, parsed as a column name, becomes a field name) and turned into a restriction by the P2/P3 code, so the operators, error messages, receivers of the bind markers (`addrs.city`, text) and EQ behaviours are those of the relations outside groups, and names and paths can be at any depth (cross-level, more than DSE). Refused inside a group: nested groups (`IS NOT NULL` was refused until P5), token / multi-column / map-entry / geo relations and custom expressions ("Only relations on the fields of the elements are supported in CONTAINS ELEMENT groups"), `CONTAINS KEY` (as on fields); `LIKE` and `:` need an index on their field, as outside groups. In statements, a group is a field relation: refused in UPDATE / DELETE and materialized views with the P2 messages, refused by `SELECT DISTINCT` on non-static columns, and, on a partition key column, it must be in the top-level AND.
- *Restriction.* `ElementMatchRestriction` (a `SingleColumnRestriction` whose `fieldPath()` is the prefix, `FieldPath.elementsOf(collection path)`: the path ending with an `ELEMENTS` step) holds a tree of AND / OR nodes of the leaf restrictions on the absolute paths, each marked positive (`=`, `IN`, ranges, `LIKE`, `:`, `CONTAINS`) or negative (`!=`, `NOT IN`, `NOT CONTAINS`). As every restriction on a path through collections, it's never merged: two groups on one collection, and a group and a leaf relation on it, are independent. Its target name, in the messages, is the CQL form of the group.
- *Semantics.* `CONTAINS ELEMENT` is satisfied by the rows where some element satisfies all the conditions of the group; `NOT CONTAINS ELEMENT` by the rows where no element does, so it includes the rows with an empty or null collection, consistently with P3's "none". The `null` elements are skipped (they satisfy nothing). Inside an element, a condition is evaluated as on a single frozen value of the element type: an unset field satisfies no condition, `!=` included (as on single fields in P2; DSE `NOT zip:100` matches the elements without zip, §H.3), but `NOT CONTAINS`; a condition on a path through a collection inside the element (`names.firstname != 'Sam'`) is "some value" / "no value" as in P3, so it's satisfied by an element without names.
- *RowFilter.* `Kind.ELEMENT_MATCH(6)`, `RowFilter.ElementMatchExpression(column, prefix, negated, FilterElement conditions)`; the operator is `CONTAINS`, or `NOT_CONTAINS` for `NOT CONTAINS ELEMENT`, the value is empty, `fieldPath()` is the prefix (so the default `Index.supportsExpression` never serves it, and `isMutableIntersection` counts a group on a non-frozen collection as a target of its own). The conditions are `FieldExpression`s that are *relative* (`FieldExpression.isRelative()`): their path starts from the type of the elements (`city`, `loc.lat`), they are evaluated on each element buffer with `isSatisfiedByColumnValue` (the element as a single-cell value), and their analyzer is that of the index on the absolute path (prefix + relative path), computed by the coordinator (`RowFilter.Builder.nestedBuilder` / `addElementMatch`: the restrictions add their expressions on the absolute paths, `ElementMatchExpression.fromAbsolute` makes them relative) and by the replica on deserialization. The elements are visited with `FieldPath.anyValue` on the prefix (multi-cell collections, non-frozen UDTs whose field is the collection, frozen values, partition key and clustering values), stopping at the first matching element. Wire form: column name, operator, the prefix (FieldPath format), then a `FilterElement` (`FilterElement.Serializer`, so AND / OR / nesting are those of the WHERE clause) of FIELD expressions on the same column written as usual but with relative paths. The deserialization refuses a prefix that doesn't reach the elements of a collection of UDTs or tuples, a condition of another kind or on another column, a relative path that doesn't match the type of the elements or doesn't end with a field, an empty group and an operator other than CONTAINS / NOT CONTAINS. CQL form (`toCQLString`, read command logging, with redaction): `addrs CONTAINS ELEMENT (city = 'Rome' AND zip != 100 AND (zip >= 150 OR loc.lat < 2.0))` (the expressions of a node, then its children). Also fixed: `SingleColumnRelation.toCQLString` printed `x NOT IN null` for a `NOT IN` of literals.
- *Candidates and index selection.* `ElementMatchExpression.candidates()` turns a `CONTAINS ELEMENT` group into the AND / OR of its positive conditions on the absolute paths, each "some element", a superset of the rows satisfying the group: in an AND the negative conditions are left out, an OR with a negative condition (or without candidates in a child) has no candidates, and so has a group without positive conditions. The restriction side applies the same rule to decide whether indexes serve the group (`ElementMatchRestriction.hasSupportingIndex` / `needsFiltering`: one served positive condition in an AND, all the conditions positive and served in an OR), and SAI to select its indexes (`StorageAttachedIndexQueryPlan.selectedIndexes` recurses into the candidates; `StorageAttachedIndex.supportsExpression` is true for a group when the index serves one of its candidates). A served group needs no ALLOW FILTERING, even with unindexed conditions (they're evaluated on the candidate rows); a group that the indexes don't restrict needs ALLOW FILTERING, as an unindexed field, also when another restriction is served by an index (the message names the group: "No index serves the restrictions on addrs CONTAINS ELEMENT (...)").
- *NOT CONTAINS ELEMENT execution rule.* The candidates are a superset of the rows matching the group, so subtracting them would drop rows: a `NOT CONTAINS ELEMENT` group has no candidates, it's served as a filter (all the keys of the other restrictions, or a full scan with ALLOW FILTERING, plus the element filter), and it needs ALLOW FILTERING. The only exception is a group with a single positive equality, `NOT CONTAINS ELEMENT (city = 'Rome')`, which is exactly `addrs.city != 'Rome'` ("no element equals"): the restriction is replaced by that `!=` (`ElementMatchRestriction.reduced`), served by the P3 guarded anti-join (`canSubtractEqualKeys`, `hasShadowingDeletions`, per-source TTL) without ALLOW FILTERING, and normalized by the index of the field; its row filter and CQL form are those of the `!=`.
- *SAI execution.* `Operation.ElementNode`: its plan is that of the candidates (built with `buildTree`, so IN, OR, analyzers, LIKE and the non-indexed conditions follow the usual rules), or `everything` without candidates (removed from an intersection; an OR with it is a full scan, as for any unindexed expression); its filter is `FilterTree.ElementFilter`, which evaluates the `ElementMatchExpression` itself on the row (the static row for a static column): the same code as the filtering without index and the coordinator's re-filtering, so the results of the three paths are the same by construction (the twin-table invariant of the tests), instead of a second, SAI-specific element evaluator. The static row alone (the check of a partition without matching rows) never satisfies a group on a clustering or regular column, not even a `NOT CONTAINS ELEMENT`.
- *False positives (§H.4).* The rows evaluated by the element filter of a group whose candidates are the source of the keys (no OR above the group) are counted in `QueryContext` (`elementFilterCandidates`, `elementFilterRejections`); the rejected ones are added to the per-table counter `TotalElementFilterRejectedRows` (`TableQueryMetrics.PerTable`), and `QueryController.finish` traces "CONTAINS ELEMENT: <r> of <n> rows evaluated by the same-element filter rejected". It is an approximation of the false positives of the candidates, documented as "rows evaluated / rejected by the element filter": the groups under an OR, and those without candidates, are not counted; the planner may still read keys that are not the group's candidates (e.g. when it drops the candidates' index scans from an intersection with a more selective plan), and the check of the static row of a partition without matching rows is not counted. The SAI metrics log string of the traces and of the slow query logger is unchanged. The candidates' filter tree is kept in the element filter (never evaluated) for the SSTable indexes it references, so the `sai_sstable_indexes_per_query` guardrail counts the indexes read for a group (review F2).
- *Analyzers.* The conditions are normalized by the indexes on their fields exactly as outside groups (`case_sensitive`, `normalize`, `ascii`: equalities, ranges, LIKE, `:`, and the `!=` / `NOT IN` normalized by the index serving the ranges on the field), also with ALLOW FILTERING on the indexed table; without index the raw values are compared. The tokenizing analyzers stay refused on the fields inside collections (review M1): a group could match the tokens within one element, but the relations outside groups would still let each token match a different element in SAI and not in the RowFilter.
- *Review fixes (P4 review, see the ledger).* An empty group after binding (`zip NOT IN ()` or `zip NOT IN ?` bound to an empty list as its only condition) is refused at bind time (`ElementMatchRestriction.EMPTY_GROUP_MESSAGE`), as replicas refuse to deserialize an empty group; with other conditions, the empty `NOT IN` restricts nothing. `SecondaryIndexManager.hasAnalyzerOnContains` looks at the candidates of a group, not at its CONTAINS operator. The candidates are computed once per expression. The element filter evaluates with the read command's time (`QueryContext.nowInSec`), as the filtering without index and the coordinator do (the other `FilterTree` nodes still use the current time, unchanged: aligning them means passing the time to every `FilterTree`, left as a follow-up). `x CONTAINS ELEMENT ()` is a call to a function named `element` without arguments; the error says "Unknown function ELEMENT called (CONTAINS ELEMENT needs at least one condition)".
- *Not done (open for P5-P6).* `IS NOT NULL` inside groups (P5, done, see the P5 notes); nested groups (`addrs CONTAINS ELEMENT (subs CONTAINS ELEMENT (...))`, refused); an `IN` / `NOT IN` in a single-condition `NOT CONTAINS ELEMENT` is not reduced to `NOT IN` / `IN` exclusions (filtering); the element filter doesn't share the analysis of a value between the `!=` of a `NOT IN` inside a group (each analyzes it); the false-positive counter is per table, not per index; cqlsh completion doesn't know `CONTAINS ELEMENT` (to mention in P6's `NEWS.txt`); `dse-runtime.md` rows to mirror in P6's compatibility test (u_list, t_list, cross-level, negation within the element).

**P5: `IS NOT NULL` on leaves.**
- Full unbounded range on the leaf index; HCD-642 already includes the empty term (`TrieMemoryIndex`). Needs `Expression` to accept a range with no bounds, and a `StatementRestrictions` exemption.

**P5 implementation notes** (where the implementation differs from or details the plan above):
- *Semantics.* `f IS NOT NULL` on a field selects the rows where the field is set: its value is present (frozen values: the component is present and not null, so the values written before an `ALTER TYPE ... ADD` don't have the new field; non-frozen UDTs: a live cell) and, for the non-literal types, not empty (`RowFilter.FieldExpression.isSet`: an empty string or `ascii` is set, an empty `int` from `blobAsInt(0x)` or an empty `blob` is not), exactly the values the indexes on the field index (`TypeUtil.skipsEmptyValue`), so that the index and the filtering give the same rows. A frozen collection field (even empty) and a frozen UDT field are set when present. On a path through collections it's "some element has the field set"; in a `CONTAINS ELEMENT` group it's a condition on the element, positive (a candidate); `NOT CONTAINS ELEMENT (f IS NOT NULL)` is "no element has it set" (filter-only, ALLOW FILTERING, as every non-reduced `NOT CONTAINS ELEMENT`).
- *Restrictions.* `SingleColumnRelation.newIsNotRestriction` creates `SingleColumnRestriction.FieldIsNotNullRestriction` for a field (the `IsNotNullRestriction` of the views is unchanged), and `StatementRestrictions` sends the field `IS NOT NULL` to the restriction sets instead of the views' not-null columns; refused in UPDATE / DELETE / materialized views as the other field relations. It's never merged with the other restrictions on the same field (`shouldMerge` false; `RestrictionSet.Builder` now asks both restrictions), so `addr.city IS NOT NULL AND addr.city = 'Rome'` is valid. `IS NULL` is refused by the grammar with "IS NULL restrictions are not supported: only IS NOT NULL, on the fields of UDT and tuple columns (and in materialized views)" (columns and fields; it was a generic syntax error); `IS NOT NULL` on whole columns outside views keeps its "Unsupported restriction" error.
- *RowFilter.* A `FieldExpression` with `Operator.IS_NOT` and an empty value (no wire change: kind FIELD, the empty value written as usual), without analyzer (none is looked up: there is no operand, and the presence of the value doesn't depend on its analysis), CQL form `addr.city IS NOT NULL`; `isMutableIntersection` treats it as any field expression.
- *SAI.* `IndexContext.supports(IS_NOT)` only for field indexes, not on the indexes of the values of a leaf collection (an empty collection is set but has no term) nor with a tokenizing analyzer (a value may have no token); the normalizing options are fine (they index every value, the empty string included); never for whole-column indexes. `plan/Expression.add(IS_NOT)`: a `RANGE` without bounds (all the terms, whatever their order, so it works on numeric, literal, boolean, uuid, timeuuid, inet, date, decimal and varint terms and on the frozen values of `full()` indexes; in the memtable trie, which has the empty term, and through the unbounded range scans of the SSTable trie and BKD readers), post-filtered as "set" (`isNotNull`, same rule as `isSet`); `Operation.analyzeGroup` adds it without analysis, combined with the previous expression on the same single-valued target (IS_NOT has the lowest priority, `getPriority`), never on a multi-valued path: the combined expression keeps the other expression's operation and bounds and is also marked `isNotNull`, so that its post-filter still rejects the unset values (an empty `int` satisfies `!= 5`, review B1). The unindexed fields fall back to the non-indexed context, so `ALLOW FILTERING` gives the same rows (twin-table tests).
- *Tests.* `UdtFieldIsNotNullTest` (frozen / non-frozen, memtable / flush / compaction, twin table): the DSE rows (`addr.city:*` → 1, 2; `addr.loc.lat:*` → 1; `udtcol_list.street:*`), nested fields, tuple components, empty strings, the types, `ALTER TYPE ADD`, null fields and removed elements of non-frozen columns, multi-valued some / none and groups, normalizing indexes, key and static columns, prepared statements, paging, the refusals; `RowFilterTest.testFieldIsNotNull`.
- *Not done.* `IS NULL` on fields (§F gap); `IS NOT NULL` served by an index on the values of a leaf collection or with a tokenizing analyzer (filtering only).

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