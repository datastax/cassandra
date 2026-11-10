# HCD-653 review ledger

Every review finding is listed here until it is fixed or explicitly closed by the owner.

## P1 review (commit 6c9c4725ba, fixes amended into the P1 commit)

| # | sev | finding | status |
|---|---|---|---|
| 1 | major | messaging 111 breaks schema pull/push and repair with HCD 2.0 nodes at 110 | closed: no version change (owner decision) |
| 2 | major | commit log, hints, batchlog, paxos written at 111, unreadable by 110 builds | closed: no version change (owner decision) |
| 3 | major | SSTableIndexWriter addedRow overwritten per value; segment may be dropped; row split across segments | fixed in the P1 commit |
| 4 | minor | TargetParser legacy parse matches a column literally named "addr.city" | fixed in the P1 commit |
| 5 | minor | version gate before IF NOT EXISTS / duplicate check | fixed in the P1 commit (ordering); the gate itself: closed: no version change (owner decision) |
| 6 | minor | term-size messages name only the column | fixed in the P1 commit |
| 7 | minor | field index changes the error for whole-column restrictions | fixed in the P1 commit |
| 8 | nit | per-row allocations in IndexContext.getValueOf / isMultiValued | fixed in the P1 commit |
| 9 | nit | FieldPath offset + size overflow | fixed in the P1 commit |
| 10 | nit | gate is point-in-time (late 110 joiner / downgrade) | closed in P6: not applicable, there is no version change and no gate (owner decision) |
| 11 | nit | system views show only the column name for a field index | fixed in P6: new columns `field_path` (raw names, `city`, `loc.lat`; null for whole-column indexes) and `target` (canonical target, tells `full()` from `values()`) in `system_views.indexes` and `system_views.sai_column_indexes`, `column_name` unchanged; `IndexesSystemViewTest.testFieldIndexes` (quoted names) |
| 12 | nit | default current version 111 also for CNDB embedders | closed in P6: not applicable, there is no messaging version 111 (owner decision) |

Note on 3: `testMultiValuedRowsInSmallSegments` now forces the memory-limit trigger (Byteman injections on `NamedMemoryLimiter.usageExceedsLimit` and `SegmentBuilder.hasReachedMinimumFlushSize`); it fails with the old per-term flush decision (terms lost) and passes with the per-row decision.

## P1 re-review (331a520636)

| # | sev | finding | status |
|---|---|---|---|
| F1 | major | 110/111 compatibility predicate (D1) diverges from the Apache conventions | closed: no version change (owner decision) |
| F2 | major | durable files written at 110 while the node speaks 111 (D2) | closed: no version change (owner decision) |
| F3 | minor | `Batch` deserialization keeps encoded mutations of a 110 peer | closed: no version change (owner decision) |
| F4 | minor | `ByteLimitedMaterializer` analyzed-token oversize warning names only the column | fixed: `indexContext.getTargetName()` |
| F5 | minor | hints fast path with 110 files and 111 peers | closed: no version change (owner decision) |
| F6 | nit | `IndexContext` target name mixes an unquoted column with a quoted path | fixed: column and path both in CQL form (`column.name.toCQLString()` + path) |
| test gap | — | `testMultiValuedRowsInSmallSegments` passed on the old code | fixed: forces a flush after the first term of a row; fails before the fix, passes after |
| N2 | — | SAI version GB for the per-index components of field indexes | closed: no SAI version (owner decision); field indexes use the SAI version of the SSTable |

## P2 review (8bedf8f9fc)

| # | sev | finding | status |
|---|---|---|---|
| M1 | major | a partition key field restriction under OR passed, and the filter split OR(pk field, column) into partition-level AND row-level trees: wrong results | fixed: refused with `PARTITION_KEY_RESTRICTION_MUST_BE_TOP_LEVEL` (`StatementRestrictions.doBuild`); tests on the indexed and the twin table |
| M1 note | — | whole static columns under OR have the same pre-existing partition-level / row-level split problem | parked: pre-existing, not HCD-653 |
| M2 | major | `SELECT DISTINCT` accepted a restriction on a field of a clustering column | fixed: `StatementRestrictions.hasFieldRestrictionsOnClusteringColumns`, checked by `SelectStatement.validateDistinctSelection`; partition key and static fields still allowed; test |
| M3 | major | the `plan/Expression` exclusion fix (range + `!=` on literals, HCD-642) belongs to HCD-642 | done: superseded by the revised HCD-642 fix 4d212a9d57 (PR #87, normalizing `!=` / `NOT IN` with the index serving ranges); the separate commit was dropped and HCD-653 rebased onto 4d212a9d57 |
| m1 | minor | `isMutableIntersection` counted the fields of frozen columns as mutable targets | fixed: only the fields of multi-cell columns; RowFilterTest + guardrail test at QUORUM / RF 3 |
| m2 | minor | `without(column, op, value)` / `withoutFirstLevelExpression(column)` removed the field expressions | fixed; RowFilterTest |
| m3 | minor | bind-marker name mixed a raw column name and a quoted path | fixed: `column.name.toCQLString()`; quoted-names test |
| m4 | minor | unset/null value messages, IN marker messages and the multiple-indexes CONTAINS warning (text and key) named the column | fixed: `targetName()` / `targetIdentifier()`; test (one warning per field) |
| m5 | minor | `FieldPath.deserialize` presized from an unchecked count | fixed: 1..64 steps, negative positions refused; RowFilterTest (oversized, empty, unknown tag, multi-valued, out of type) |
| m6 | minor | wire format used `Kind.ordinal()` | fixed: explicit `Kind.tag` and `fromTag` |
| n1 | nit | unreachable field checks for IS NOT NULL / ANN / BM25 in `SingleColumnRelation` | fixed: asserts |
| n2 | nit | `compareFieldPaths` built strings | fixed: step by step |
| n3 | nit | whole-column and field CONTAINS counted together for `maskHasMultipleContains` | comment (conservative) |
| gaps | — | static UDT field (frozen and non-frozen), paging (page sizes 1/2/5, LIMIT) with key-column fields, whole + field restriction on the same column both indexed | tests added (`testStaticUdtColumn`, `testPaging`; the last one is `testWholeColumnAndFieldIndexes`) |
| D1 | major | deletion of a whole non-frozen column flushed into its own SSTable comes back | open, not HCD-653: the default `TrieMemtable` of the tests doesn't write a row whose only content is the complex deletion (the flushed SSTable is empty and discarded, 1 live SSTable instead of 2); also with ALLOW FILTERING on a table without index and on primary key reads; `SkipListMemtable`, `TrieMemtableStage1/2/3` are fine; reproduced on 63868f5d74; tracked by issue eolivelli/cassandra#106 (fix proposed in PR #107); the ignored `ComplexDeletionAcrossSSTablesTest` is no longer in the branch (dropped from the P2 commit; the reproduction is in the issue and the PR) |

## Rebase onto 4d212a9d57 (HCD-642 revised)

| # | sev | item | status |
|---|---|---|---|
| R1 | — | conflict in `Operation.analyzeGroup` (analysis context vs `(column, path)` grouping) | resolved: both kept, the `!=` merge rule uses the per-target group and the same-analysis-context condition |
| R2 | minor | `!=` / `NOT IN` normalization of HCD-642 didn't apply to fields: `QueryController.getAnalysisContext` probed with a column expression (a field index was never found, a whole-column index could be), `RowFilter.analyzerFor` had no path, `FieldExpression` didn't override the shared-analysis evaluation (it would have read the whole column value) | fixed: field probe with `FieldExpression`, `RowFilter.analyzerFor(registry, column, path, ...)`, `FieldExpression.isSatisfiedBy(..., AnalyzedValues)`; `UdtFieldQueryTest.testNormalizedFieldExclusions` (range, ALLOW FILTERING, after another index, raw twin baseline, column not normalized by a field index, LIMIT and paging) |
| R3 | — | `UdtFieldQueryTest.testRanges` range + `!=` assertion from the dropped commit | re-added |


## Delta review of the rebased P2 (6b719a236a)

Clean: no defects. Test notes, all applied:

| # | note | status |
|---|---|---|
| T1 | the `name != 'ROME'` check under "doesn't normalize other targets" tested another column | fixed: the comment says what it checks (a plain column), plus a real cross-target check on another field of the same column (`addr.street`, same values, no index: raw comparison) |
| T2 | two normalizing fields of one column in the same query | added `testTwoNormalizedFields`: case-insensitive indexes on `addr.city` and `addr.street`, `!=` / `NOT IN` on both with a range on either, expected rows computed from the twin table's values |
| T3 | bound `NOT IN ?` on a field | added to `testNormalizedFieldExclusions`, with a range and alone with ALLOW FILTERING (and a bound `!=`) |

## P3 review (20f3ef8566, fixes amended into the P3 commit)

| # | sev | finding | status |
|---|---|---|---|
| B1 | blocking | the anti-join plan of `!=` / `NOT IN` / `NOT CONTAINS` ("all keys minus keys with an equal term") missed the rows whose indexed cell expired (TTL): the memtable index has no expiration, an SSTable index written before the expiration keeps the term | fixed: per-source subtraction only without expiring cells (`SSTableIndex.getNonEqIterator`: `getMaxTTL() > 0` → all keys; `IndexContext.getNonEqIterator`: per memtable, `MemtableIndex.mayHaveExpiringValues`, marked by `IndexContext.index` / `update` for multi-valued targets); also fixes the shared, pre-existing `NOT CONTAINS` on whole collection columns; `testExpiringElements` (memtable, flush before expiry, compaction; frozen / non-frozen; `nums NOT CONTAINS 5`), fails with either guard removed |
| M1 | major | tokenizing analyzers on multi-valued field indexes: SAI lets each token match a different element, the RowFilter (coordinator re-filter, RFP) requires one element | fixed: refused by DDL (owner decision): `CREATE INDEX` with `index_analyzer` / `query_analyzer` on a field inside collections fails with `StorageAttachedIndex.ANALYZER_INSIDE_COLLECTIONS_MESSAGE` naming the target; non-tokenizing options allowed; `UdtFieldIndexDDLTest.testTokenizingAnalyzerInsideCollections`; to revisit with P4 |
| m1 | minor | EQ_ONLY_TYPES refused NOT_EQ on multi-valued field contexts, though the anti-join only needs EQ | fixed: `IndexContext.supports(NEQ)` true for multi-valued field contexts without tokenizing analyzer and not unpacking a leaf collection; the range + `!=` behaviour change of P3 is gone; tests: `!=`, `NOT IN`, range + `!=` served (also normalized); the unsupported-operator message for `!=` on a leaf collection whose index is on its values, alone and with a served restriction |
| m2 | minor | `NOT IN (v1..vk)` planned as k anti-joins intersected | fixed: one NOT_EQ with exclusions (`Operation.analyzeGroup`), one anti-join minus the union of the EQ searches (`Expression.negations`), exclusions checked by the NOT_EQ post-filter; `testNotInPlan` (3 values, int and text; range + 2 `!=` → 2 expressions) and results with 3-value NOT IN |
| m3 | minor | per-row ArrayList + lambda in `FieldExpression`, AnalyzedValues linear lookup | fixed: `FieldPath.anyValue` short-circuiting visitor by offsets, constant predicates, `AnalyzedValues` by hash; FieldPathTest checks the visit equals `collectValues` and stops at the first accepted value. Not done: a `NOT IN` is still k RowFilter expressions (k visits per row), changing it would change the wire form |
| m4 | minor | DESIGN notes: P2 note on refused collection paths, "never missing rows", tokenizing note without the coordinator re-filter | fixed |
| gaps | — | non-frozen static collections, `!=` on a static field, map value replaced by key, OR with a multi-valued `!=`, IN on an analyzed field | tests added (`testKeyAndStaticColumns` runs in both modes, `testElementUpdates`, `testCollectionOfUDTs`, `testAnalyzedField`) |
| pre-existing | — | `TrieMemoryIndex.update` skipped empty values that `index` indexes (an empty string written by an update in the memtable was missing) | verified (`testEmptyStringsInUpdates` failed before the fix: row 2 missing for `addrs.street = ''`, frozen and non-frozen), fixed in `TrieMemoryIndex.update` (both overloads: index an empty value when `TypeUtil.skipsEmptyValue` is false); also on a text column: `UPDATE SET name = ''` from null in the memtable was missing (row 1 of the test, fails without the fix), fixed in the single-value `TrieMemtableIndex.update` early return (null vs empty) |

## P3 re-review (5d7a7a6025)

| # | sev | finding | status |
|---|---|---|---|
| F1 | blocking | partition deletions and range tombstones in a memtable leave stale terms (rows not removed, a re-inserted row keeps the old terms), also in the SSTable of the flush: the anti-join subtracted re-inserted rows | fixed (revised in the split review): `UpdateIndexer.partitionDelete` / `rangeTombstone` mark the memtable index; global decision for the deletions (a deletion in one source can shadow another source's terms), SSTables with partition deletions or possible range tombstones excluded; flush path checked: the SSTable index carries the stale terms (delete + re-insert fails after the flush with the SSTable check disabled); tests on plain columns (`NonEqualityStaleTermsTest`: delete + re-insert, out-of-order writes under a deletion in the memtable and flushed, range tombstone + re-insert, `NOT CONTAINS KEY` on a map) and on fields (`testDeletionAndReinsert`) |
| F2 | major | the TTL guards disabled the anti-join for any TTL'd write | fixed: memtable records the earliest expiration of its indexed expiring cells, the anti-join is used until it expires; SSTables: maximum TTL 0 or minimum local deletion time after the query time; the query time is the read command's `nowInSec` (new `QueryContext.nowInSec`); tests: not yet expired → `canSubtractEqualKeys` true and results right (memtable and SSTable), expired → false and results right |
| F3 | minor | `allKeys` leaked if a search threw | fixed: one try/catch closing `allKeys` and the matched iterators (`SSTableIndex.getNonEqIterator`, `IndexContext.getNonEqIterator`) |
| F4 | minor | DESIGN claimed the fix covers map-entry `!=` | fixed: it uses the half-range plan |
| F5 | minor | M1 message said "Tokenizing" for any analyzer | fixed: `ANALYZER_INSIDE_COLLECTIONS_MESSAGE` names `index_analyzer` / `query_analyzer` and the allowed options |
| F6 | nit | the expiring-cell walk ran on every write | fixed: skipped once the memtable index may have stale terms |
| split | — | own commits for the pre-existing SAI fixes | done: "SAI: NOT CONTAINS and != anti-join must not subtract keys whose index terms may be stale (TTL, partition and range deletions)", "SAI: index empty strings written by updates to rows in the memtable", then the P3 commit |

## P3 split review (466ddb946b / a2d98bb986 / b6b18c2953)

| # | sev | finding | status |
|---|---|---|---|
| F1 | major | the memtable anti-join subtracted the keys matching in any live memtable from the keys of all of them: during a flush, a key of the new memtable without the term was dropped (pre-existing) | fixed: memtable by memtable (`IndexContext.getNonEqIterator`); `NonEqualityStaleTermsTest.testTwoLiveMemtables` (two memtable indexes, the old one with the term), fails with the merged version |
| F2 | major | commit 1 depended on P1 (`isMultiValued`, imports) and its title said `!=` | fixed: commits 1 and 2 moved below the design notes, directly on the HCD-642 head, self-contained (`isNonFrozenCollection`, `getValuesOf(row, now)`), commit 1 retitled "NOT CONTAINS and NOT CONTAINS KEY"; each verified alone on the HCD-642 head (build + its test); P3's body says it extends the guarded anti-join to `!=` / `NOT IN` on multi-valued fields |
| F3 | doc | `removeRow` is compaction / cleanup only; stage 3 trie deletes covered rows | fixed: `removeRow` not marked; row deletions are updates; DESIGN wording per memtable implementation |
| F4 | perf | CAS per expiring cell; extra walk over the cells | fixed: the minimum is checked before the CAS; the expirations are taken while the values are extracted (`TypeUtil.collectionIterator`, `FieldPath.collectValues` with a `LongConsumer`), not taken once the memtable index may have expired terms |
| F5 | planner | histogram estimates assumed the subtraction | fixed: the sources that don't subtract count all their rows (`QueryController.estimateMatchingRowCountUsingHistograms`); not tested (needs V6+ histograms and term statistics) |
| refinement | — | TTL staleness per source | done: an SSTable or memtable with expired terms only stops its own subtraction, the deletions stay global; `testExpiredTermsPerSource` (3 SSTables, one with an expired TTL), `testTombstonesWithoutClusteringColumns` |
| gaps | — | flushed range tombstone shadowing a memtable write; memtable implementations; static collections | `testFlushedRangeTombstoneShadowingMemtableWrite`, `testStaticCollection`; `NonEqualityStaleTermsTest` parameterized over default, trie, trie_stage2, trie_stage3, skiplist |
| parked | — | map-entry `m['k'] != v` and the rows without the key | verified, not a bug: index-served results equal the filtered ones (`testMapEntryNotEqual`) |
| rebase | — | onto the HCD-642 head 5676709fbe | done; the HCD-642 coordinator-side `withoutUnsatisfiable` (empty ranges) compared field bounds with the column type: P2 now requires the same field path and compares with the leaf type, P3 never prunes the bounds on a multi-valued path (independent elements) |

## P4 review (5500f948ef, fixes amended into the P4 commit)

| # | sev | finding | status |
|---|---|---|---|
| F1 | major | an empty group (`zip NOT IN ()`, or `NOT IN ?` bound to `[]`, as its only condition) passed on the coordinator (true for every element locally) and was refused by the deserialization of the replicas: results depended on the cluster layout | fixed: refused at bind time (`ElementMatchRestriction.addToRowFilter`, `EMPTY_GROUP_MESSAGE`), deserialization kept strict; `UdtElementMatchQueryTest.testEmptyGroups` (literal and bound, CONTAINS and NOT CONTAINS, with another restriction; empty NOT IN with other conditions), `RowFilterTest.testEmptyElementMatchRoundTrip` |
| F1 note | — | at top level (and inside groups), `(city = 'a' OR zip NOT IN ())` silently reduces to `city = 'a'` (the empty NOT IN adds no expression to the OR) | parked: pre-existing, not HCD-653 |
| F2 | major | the `sai_sstable_indexes_per_query` guardrail didn't count the indexes read for a group (the element filter had no expressions, the candidates' filter tree was dropped) | fixed: `ElementFilter` keeps the candidates' `FilterTree` (not evaluated) and delegates `sstableIndexes`; `testSSTableIndexesGuardrail` (`numSSTableIndexes` 4 / 2 / 2, failing and passing thresholds) |
| F3 | minor | `TotalElementFilterRejectedRows` and the trace counted rows that weren't the group's candidates (OR, planner dropping candidate scans, static re-check) | fixed: documented as rows evaluated / rejected by the element filter (metric, `QueryContext`, `FilterTree` Javadoc, DESIGN §H.4 note); not counted under an OR (`ElementNode.candidatesAreKeySource`) nor for the static row check; `testFalsePositivesMetric` OR and AND-with-OR cases (the latter planner-dependent, 2 or 3) |
| F4 | minor | `SecondaryIndexManager.hasAnalyzerOnContains` saw a group's CONTAINS operator | fixed: it looks at the CONTAINS candidates of the group; not tested (needs two index groups with an analyzed CONTAINS candidate, no cheap setup) |
| F5 | minor (perf) | `candidates()` / `candidateExpressions()` rebuilt the expressions on every call | fixed: computed once in the constructor |
| F6 | minor | the element filter used `FBUtilities.nowInSeconds()`, the filtering and the coordinator the command's `nowInSec` | fixed: `QueryContext.nowInSec()`; `FilterTree.localSatisfiedBy` has the same pattern (pre-existing), not trivial to align (the time would be passed to every `FilterTree`): noted, follow-up |
| note | — | `x CONTAINS ELEMENT ()` reported "Unknown function ELEMENT called" | fixed: hint "(CONTAINS ELEMENT needs at least one condition)" for an unknown `element()` without arguments (`FunctionCall.Raw.prepare`) |
| note | — | cqlsh completion doesn't know `CONTAINS ELEMENT` | fixed in P6: `cql3handling.py` completes field paths (index targets, relations), `IS NOT NULL` on fields and `[NOT] CONTAINS ELEMENT (...)`; offline test `test_cql_parsing.test_complete_udt_field_paths`; the remaining lag (LIKE, OR, `!=` on columns, pre-existing) is in `NEWS.txt` |

## P5 review (7603691a21, fixes amended into the P5 commit; the P4 delta was clean)

| # | sev | finding | status |
|---|---|---|---|
| B1 | major | an `IS NOT NULL` combined with another expression on the same single-valued field (`k.i != 5 AND k.i IS NOT NULL`, `k.i < 5 AND k.i IS NOT NULL` with another index driving) lost the "not empty" check: the SAI post-filter accepted an empty `int`, the RowFilter (twin, coordinator re-filter) rejected it | fixed: `Expression.add(IS_NOT)` always marks the expression, `isSatisfiedBy` rejects the unset values and goes on with the bounds and exclusions; `ExpressionTest.testIsNotNull` (fails without the fix: `!=` accepted the empty int), CQL checks in `UdtFieldIsNotNullTest.testTypes` (indexed, both forms) and `testCombinedWithOtherRestrictions` (unindexed field post-filtered next to an index, int / double / text); the CQL checks don't reach the combined expression with an empty value on the current plans (the scans of the field don't return unindexed values), the unit test does |
| m1 | minor | `IS_NOT` got the lowest priority from the default branch of `getPriority`, and the early return of `isSatisfiedBy` skipped the bounds | fixed: explicit `case IS_NOT: return 0` with a comment; the early return is gone (B1) |
| m2 | minor | dead `ElementMatchExpression` branch in `Operation.Node.buildExpression` (with the local `isDisjunction` only) | fixed: an assertion, the groups are built by `buildTree` |
| m3 | cosmetic (P4) | the CONTAINS ELEMENT hint of an unknown `element()` showed outside WHERE clauses too | fixed: softened wording "(in a WHERE clause, CONTAINS ELEMENT needs at least one condition)"; no cheap signal of the WHERE context in `FunctionCall.Raw.prepare` |
| gaps | — | unindexed IS NOT NULL post-filtered next to an indexed restriction; the leaf-collection `IS NOT NULL ALLOW FILTERING` check ran on an empty table; the P2-P4 refusal replacements only asserted acceptance; the P4 metric assertion was a range (2 or 3) | tests added / fixed: `addr.city = 'Rome' AND addr.loc IS NOT NULL ALLOW FILTERING` and others vs the twin; `testInvalidQueries` with rows and a twin; the three replacements assert rows; the metric case uses an OR whose every scan selects the same rows, so the count is exactly 2 whatever the plan |

## P6 (DSE Search compatibility tests and docs)

| # | item | status |
|---|---|---|
| 10, 12 | version-gate items | closed: not applicable (no version change) |
| 11 | system views | fixed: `field_path` column |
| D1 | `ComplexDeletionAcrossSSTablesTest` | removed from P2 (where it was added, history rewritten); issue eolivelli/cassandra#106, PR #107 |
| pre-existing | `GROUP BY addr.city` (a field selector in GROUP BY) failed with a `ClassCastException` (server error) in `SelectStatement.getAggregationSpecFactory` | fixed: "Group by is currently only supported on the columns of the PRIMARY KEY, got addr.city"; `DseSearchUdtCompatibilityTest` (the DSE facet refusal) |
| mixed versions | "an older replica rejects the new kinds" | not testable in-JVM without a second code version; documented in `NEWS.txt` (Upgrading) |

## P6 review (f2a4b20cc7, fixes amended into the P6 commit)

| # | sev | finding | status |
|---|---|---|---|
| F1 | blocking | the field-path / CONTAINS ELEMENT alternatives were added to the shared cqlsh `<relation>`, used by UPDATE and DELETE: their completions changed (`.`, `NOT`) and the live completion tests broke | fixed: SELECT-only `selectWhereClause` / `selectRelation`, `relation` restored; offline test checks UPDATE / DELETE choices unchanged; live `test_cqlsh_completion` (43 tests, with the new `test_complete_udt_fields_in_select`) passes against a local node |
| F2 | blocking | the docs mapped DSE `(x:* AND -x:23)` on fields inside collections to `addrs.x != 23` (no element equal), but DSE evaluates it per element | fixed: the row is for single fields; new row `addrs CONTAINS ELEMENT (x != 23)` / `(x NOT IN (23, 45))`; `testNegationsOnFieldsInsideCollections` (`[23, 45]` matches the group and not the `!=`, the rows without elements the reverse) |
| F3 | — | runtime of `DseSearchUdtCompatibilityTest` | measured: 81 s locally (52 runs, 8 skipped; about 170 s at the CI factor of 2.1, under the 480 s fork limit): not split |
| F4 | — | replica filtering only asserted at ALL; class Javadoc | fixed: `testReplicaFilteringProtectionAtQuorum` (newer values on nodes 2 and 3), paged queries (page sizes 1, 2) over inconsistent replicas at ALL and QUORUM; Javadoc lists what is asserted |
| F5 | — | upgrade notes: the client doesn't see "Unknown index expression kind"; DDL in mixed clusters isn't enforced; downgrade | fixed (NEWS, docs page): ERROR log on the replica, read failure `UNKNOWN` to the client, "Unable to parse targets for index ..." on old nodes for an index created too early, drop the field indexes before a downgrade |
| F6 | — | docs examples without the indexes they need, `p.home.names` not in the page schema | fixed: the page lists the extra indexes; `person` has `names`, example `p.names CONTAINS ELEMENT (...)` |
| F7 | — | DESCRIBE shows the canonical target; ranges wording | fixed |
| F8 | — | `direc.mapfield CONTAINS 'dynVal2'` matches any key | fixed: says it returns more rows than DSE and the application checks the key |
| F9 | — | cqlsh: `!=`, `NOT IN`, `IS NOT NULL`, OR inside groups; dotted targets quoted as one name; table columns inside groups | fixed: group grammar with the field operators, AND / OR / parentheses; target columns split from the path (`_index_target_column`); element fields offered inside groups and after `col.`; the rest listed in NEWS |
| F10 | — | `field_path` quoted while `column_name` isn't; `full()` vs `values()` indistinguishable | fixed: raw names in `field_path`, new `target` column; quoted-names test; virtual tables doc and sample output |
| F11 | — | GROUP BY guard cases and message for fields of clustering columns | fixed: "Group by is not supported on the fields of columns, got ..." for fields (also of primary key columns); `SelectGroupByTest.testGroupByOnSelectorsThatAreNotColumns` (UDT and tuple fields, a clustering column field, `m['k']`, CAST) |
| F12 | — | missing cheap DSE scenarios | added: unparenthesized `{!tuple}a AND {!tuple}b`, `addr.city:Rome` without the parser (comment), sort on `addrs.zip`, SELECT JSON, quoted mixed-case projection and aliases, null fields, UDT added then changed, row delete, TTL (frozen value and non-frozen field), whole-value equality with collections and nesting (more than DSE), LIKE `'%x'` / `'%x%'`; "every" dropped |
| F13 | — | schema deviations incomplete | fixed: body / date / title and `tuplewithtuplecollectionwithcollection` listed |
| F14 | — | the twin oracle was always read from SSTables | fixed: a first pass with both tables in the memtables, then twin flushed, then both, then compacted; Javadoc |
| F15 | — | NEWS placement; ELEMENT keyword table | fixed: drivers / cqlsh notes in New features, the empty-string fix in CHANGES.txt (with the other HCD-653 entries), `ELEMENT` in `appendices.adoc` |
| F16 | — | `ComplexDeletionAcrossSSTablesTest` added in P2 and removed in P6 | fixed: removed from the P2 commit (history rewritten, tree identical), DESIGN name of the distributed test fixed |


## HCD-642 direction change (2026-11-06)

After Piotr's review of HCD-642, the owner's rule: the results of a query must never depend on an index analyzer. HCD-642 (squashed into 73706bbaa2) removed its analyzer-dependent machinery; HCD-653 was rebased onto it and its field counterparts were removed or changed the same way.

| # | item | was | now |
|---|---|---|---|
| X1 | `RowFilter.analyzerFor(registry, column, path, op, value, hints)` and its NEQ→GTE fallback | normalized a field `!=` with the index serving the ranges on the field | removed; `RowFilter.Builder.add(column, path, ...)` and the FIELD deserializer (also the relative conditions of a group) use `IndexRegistry.getAnalyzerFor(column, path, op, value, hints)`, as the base does for columns |
| X2 | `RowFilter.AnalyzedValues` (keyed by analysis identity and value, one entry per element) | shared the analysis of a row's values among the `!=` of a `NOT IN` | removed (base) with its per-element keying (HCD-653) |
| X3 | `FieldExpression.isSatisfiedBy(..., AnalyzedValues)` override and `AnalyzedEvaluation` | analyzed exclusion on single and multi-valued paths | removed; `isSatisfiedByFields` is no longer generic, it decides with a `FieldExpression` predicate |
| X4 | `RowFilter.FilterElement.withoutUnsatisfiable` on fields | compared the normalized bounds in the field type | compares the raw bounds in the field type (`FieldExpression.leafType`) |
| X5 | `QueryController.getAnalysisContext` (field probe with a `FieldExpression`) and its javadoc | the analysis context of a `!=` no index serves | removed (base); no HCD-653 edit left |
| X6 | `Operation.analyzeGroup`: `analysisContext`, the IS_NOT combination and the range/NEQ merge rule | compared the previous expression's context with the analysis context | both use `indexContext` (base rule: a `!=` joins an expression only if it has the same context), per `(column, path)` target; `new Expression(indexContext)` everywhere |
| X7 | `Index.Analyzer.indexedTokensIdentity` | used by `AnalyzedValues` | removed (base); no HCD-653 use |
| X8 | `IndexContext.supports(NEQ)` anti-join on multi-valued field paths | `!hasCustomAnalyzer()`: normalizing indexes served it with normalized exclusions | `!isAnalyzed`: only an index without analyzer serves it; with an analyzed index `!=` / `NOT IN` need ALLOW FILTERING and compare the raw values, also for the single-equality `NOT CONTAINS ELEMENT` reduction |
| X9 | ranges on text fields with an analyzed index | served, bounds normalized | rejected as on columns (`supportsLiteralRangeQueries` is `!isAnalyzed && ...` in the base, shared by the field contexts): "has an index but does not support the operators specified in the query"; with ALLOW FILTERING the raw values are compared |
| X10 | `IS NOT NULL` on fields | served by the field indexes without a tokenizing analyzer | unchanged: the normalizing options index every set value (empty string included), so the result doesn't depend on them (`UdtFieldIsNotNullTest.testAnalyzedFields`). A value whose term exceeds the maximum term size of the index (8 KiB by default; the analyzed value for an analyzed index, so a normalizing index may skip a value a raw index keeps) is skipped at index time, so the index-served `IS NOT NULL` misses its row, as every index-served operator does (rework review R1: documented limitation, `UdtFieldIsNotNullTest.testOversizedValue`). **For the owner / Piotr to confirm.** |
| X11 | `addrs NOT CONTAINS ELEMENT (city = 'ROME')` on an analyzed index | reduced to the normalized anti-join `!=`, no ALLOW FILTERING | not reduced: the group's `=` is analyzed by the index, the raw `!=` wouldn't be its complement (it would exclude only `'ROME'`, while `CONTAINS ELEMENT (city = 'ROME')` matches `'Rome'` too). `ElementMatchRelation.toRestriction` reduces only when no analyzed index serves the `=` on the field; with an analyzer the group is filtered (ALLOW FILTERING) as the complement of the analyzed `CONTAINS ELEMENT` (`UdtElementMatchQueryTest.testAnalyzedConditions`). |
| X12 | tests | asserted normalized `!=` / `NOT IN` and normalized ranges | `UdtFieldQueryTest.testAnalyzedField` (ranges rejected, raw with ALLOW FILTERING), `testFieldExclusionsCompareRawValues` (was `testNormalizedFieldExclusions`; twin invariant for every `!=` / `NOT IN`, also after an analyzed `:`, bound values, paging), `testTwoAnalyzedFieldsExclusions` (was `testTwoNormalizedFields`; raw expectations, checked on both tables); `UdtCollectionFieldQueryTest.testAnalyzedField`; `UdtElementMatchQueryTest.testAnalyzedConditions` (was `testNormalization`); `DseSearchUdtCompatibilityTest.testCaseInsensitiveField` |
| X13 | docs | "ranges only without a tokenizing analyzer", normalized `!=` | NEWS.txt (HCD-653 entry; the HCD-642 wording of the base untouched), `udt-tuple-fields.adoc` (operators paragraph, `NOT CONTAINS ELEMENT` reduction, the lowerCase and `!=` rows of the migration table), `sai-faq.adoc`, implementation-notes.md (D, P2, P3, P4, §F, §H 9) |

## Re-cut into smaller commits (2026-11-06)

Piotr asked for smaller commits. The six phase commits P1-P6 were re-cut into 15 HCD-653 commits on top of the two
standalone SAI fixes, each compiling on its own (clean `ant jar build-test`) with its own tests passing on it; the
final tree is the one of the rebased series, but for this section and the note in implementation-notes.md §G.

| # | commit | from | notes |
|---|---|---|---|
| 3 | design notes | design commit | DESIGN.md (the design document), implementation-notes.md (the working notes, phase by phase) and dse-runtime.md in their final form; the ledger moved to the last commit |
| 4 | FieldPath | P1-P4 | the whole class and its test, as at the end of the series |
| 5 | index targets on fields | P1 | grammar, IndexTarget, TargetParser, IndexMetadata, CreateIndexStatement, AlterTypeStatement, Index.isOnFieldOf, StatementRestrictions; no test of its own: only SAI indexes fields, so the DDL tests need commit 6 |
| 6 | SAI indexes on field values | P1, P3 | IndexContext, TypeUtil, SSTableIndexWriter, StorageAttachedIndex (validateOptions, with the P3 refusal of index_analyzer / query_analyzer inside collections), FilterTree, plan/Expression; SSTableImporter (it builds the contexts with the field path); the whole UdtFieldIndexDDLTest (DDL and build / flush / compaction) |
| 7 | RowFilter expressions on fields | P2, P3 | FieldExpression with the multi-valued evaluation; the Index / IndexRegistry path overloads come with it (listed under commit 8 in the plan): the expressions find their index and analyzer through them |
| 8 | WHERE restrictions on fields | P2, P6 | CQL relations and restrictions; the GROUP BY guard and its SelectGroupByTest part from P6; `SelectSingleColumnRelationTest.testFieldRestrictionsWithFiltering` (filtering without index; the field inside a list is added in commit 10, which accepts those paths); the indexed queries are tested from commit 9 on (`UdtFieldQueryTest`) |
| 9 | SAI query planning on fields | P2 | StorageAttachedIndex.isOnField / supportsExpression with a path, Operation, QueryController, IndexContext; UdtFieldQueryTest |
| 10 | restrictions on fields inside collections | P3 | UdtCollectionFieldQueryTest |
| 11-13 | CONTAINS ELEMENT | P4 | RowFilter expression / CQL relation and restriction (the FunctionCall hint for an empty group) / SAI execution (StorageAttachedIndexQueryPlan too) |
| 14 | IS NOT NULL on fields | P5 | as is |
| 15 | virtual tables and cqlsh | P6 | |
| 16 | DSE Search compatibility tests | P6 | |
| 17 | documentation | P6 | docs, NEWS.txt, CHANGES.txt, this ledger; the CHANGES.txt entries of the two standalone fixes, and the NEWS.txt upgrading note of the first one, are in their own commits (rework review R11) |

Gate (the list of the rework brief: 23 unit classes and UdtFieldIndexDistributedTest): green on the rebased series
(859 tests, 0 failures, 13 skipped) and on the head of the re-cut series. Per commit: `ant jar build-test` from a
clean build directory, and the tests of the commit (FieldPathTest; UdtFieldIndexDDLTest; RowFilterTest;
SelectGroupByTest, SelectTest, SelectSingleColumnRelationTest; UdtFieldQueryTest; UdtCollectionFieldQueryTest;
RowFilterTest; CqlParserTest; UdtElementMatchQueryTest; UdtFieldIsNotNullTest, ExpressionTest and the earlier field
tests; IndexesSystemViewTest) pass on it.

## Rework review (2026-11-06)

Review of the re-cut series (head ca2d960b60). The fixes were made as fixups of the commits named in the table and
squashed into them; the 17-commit structure is unchanged.

| # | sev | finding | resolution |
|---|---|---|---|
| R1 | major | an index-served `IS NOT NULL` misses the rows whose field value exceeds the SAI max term size (skipped at index time, in the memtable and SSTable writers; the coordinator validation is off by default); the analyzed limit applies to the normalized value | accepted with reason: the same skip makes every index-served operator miss oversized values (`= <9000 chars>` too), so `IS NOT NULL` stays served by the indexes. Fixed: the comment of `IndexContext.supports` (commit 14); documented as a limitation in DESIGN.md 4.6 and 7 (commit 3), `udt-tuple-fields.adoc` and NEWS.txt (commit 17); `UdtFieldIsNotNullTest.testOversizedValue` (commit 14: the indexed `IS NOT NULL` and `=` don't return the row, ALLOW FILTERING on the twin table does; no client warning is relied on). Row X10 updated. For the owner / Piotr |
| R2 | minor | `RowFilter.FilterElement.withoutUnsatisfiable`: the inet guard checks the column type (the UDT for a field expression), not the compared leaf type | fixed (commit 7): the guard runs on the type used for the comparison; `UdtFieldQueryTest.testInetFieldRange` (commit 9: `a.ip > '239.0.0.1' AND a.ip <= '7ea::1'` returns rows 1 and 2 as the same range on an inet column, served by the index, also with ALLOW FILTERING on the indexed table; the twin table without index returns no row for both, as the filter compares the raw bytes: only the index-served assertions test the fix) |
| R3 | docs | implementation-notes.md §H 9 states the old X11 rule | fixed (commit 3): reduced to the `!=` only when no analyzed index serves the `=` on the field, otherwise filtered as the complement of the analyzed `CONTAINS ELEMENT` |
| R4 | docs | implementation-notes.md: "the element filter doesn't share the analysis of a value between the `!=` of a NOT IN inside a group" | fixed (commit 3): removed, a `!=` is never analyzed |
| R5 | test | X11 tests missing: `unsupported` equals behaviour, analyzed + raw index with hints, prepared group, raw filter difference | fixed (commit 13): `UdtElementMatchQueryTest.testAnalyzedConditionsWithUnsupportedEquals` (the group is reduced, the `!=` isn't served by the analyzed index: ALLOW FILTERING, raw results, message), `testAnalyzedAndRawIndexes` (ambiguous `=` without hints, with the error of the columns; `included_indexes` on the raw index: reduced and served; the analyzed index: filtered; an included index that can't serve the filtered group is refused with the base "not possible to use all the specified included indexes" error, so the analyzed index is selected with `excluded_indexes` there), and in `testAnalyzedConditions` a prepared `NOT CONTAINS ELEMENT (city = ?)` with ALLOW FILTERING and the intended difference from the raw filter (`assertFilteredIds(..., 1, 3)` next to the indexed 3, with a comment) |
| R6 | test | no test of the field `=` with analyzers and of index hints on fields | fixed (commit 9): `UdtFieldQueryTest.testFieldEqualityWithAnalyzers` (MATCH with the client warning, UNSUPPORTED, AMBIGUOUS, `included_indexes` / `excluded_indexes` selecting each index) |
| R7 | test | no distributed scenario with an analyzed index inside a collection | fixed (commit 16): `UdtFieldIndexDistributedTest.testAnalyzedFieldInsideCollectionAtQuorum` (analyzed `=` outside and inside a group, non-reduced `NOT CONTAINS ELEMENT ... ALLOW FILTERING`, at QUORUM with newer values on two replicas) |
| R8 | minor | the FunctionCall.java message change of commit 14 belongs to commit 12 | fixed: the final message ("in a WHERE clause, CONTAINS ELEMENT needs at least one condition") is in commit 12 and its test assertion in commit 13 (`UdtElementMatchQueryTest`), commit 14 no longer touches either |
| R9 | docs | commit 12 message: the single-equality reduction has an exception | fixed: "unless an index with an analyzer serves the = on the field (the != compares the raw values)" |
| R10 | test | commit 8 has no test of the field restrictions themselves | fixed: `SelectSingleColumnRelationTest.testFieldRestrictionsWithFiltering` (commit 8; SelectTest isn't touched by the series, SelectSingleColumnRelationTest is the home of the single-column relations): `=`, `!=`, ranges, IN, NOT IN on a frozen and a non-frozen UDT field, a tuple component, a nested field, the UPDATE / DELETE refusal and the ALLOW FILTERING error; the field inside a list is added by commit 10, as commit 8 still refuses the paths through collections. The deferral of the indexed tests to commit 9 is noted in row 8 of the re-cut table |
| R11 | minor | the CHANGES.txt entries and the NEWS.txt upgrading note of the two standalone fixes arrive only in commit 17 | fixed: each CHANGES.txt entry is in its fix commit (1 and 2), and so is the NEWS.txt note of the first one (commit 17 extends it to the `!=`, NOT IN and NOT CONTAINS on fields inside collections). Decision: the entries keep the "(HCD-653)" tag, as the fixes ship under this ticket, while the subjects of the two fix commits stay without the HCD-653 prefix |
| R12 | docs | DESIGN.md says IN uses the analyzer; IN on an analyzed field fails, as on columns | fixed (commit 3): 3.2 and 4.6 |
| R13 | minor | an empty boolean is set (BooleanType is literal and allows empty) | fixed: DESIGN.md 3.2 (commit 3), `udt-tuple-fields.adoc` (commit 17) and the message of commit 14 say "literal types (text, ascii, boolean)"; `UdtFieldIsNotNullTest.testTypes` has an empty boolean (`blobAsBoolean(0x)`), set for the index and the filter (commit 14) |
| R14 | docs | DESIGN.md 5 over-promises on schema changes during upgrades | fixed (commit 3): aligned with NEWS.txt (discouraged, not enforced between 5.0.x versions; the nodes without the feature fail to load the index until upgraded or the index is dropped) |
| R15 | docs | DESIGN.md 4.2 duplicate rule incomplete | fixed (commit 3): one analyzed and one raw index may coexist on a field: `=` ambiguous without hints, ranges and `!=` by the raw one, `:` by the analyzed one, `IS NOT NULL` by either |
| R16 | docs | DESIGN.md 4.6 lacks decisions | fixed (commit 3): `=` with `equals_behaviour_when_analyzed` (MATCH with the client warning, UNSUPPORTED), the ambiguity error and the index hints on fields, the term-size limit, `IS NOT NULL` on text fields relying on the empty-string fix |
| R17 | docs | "the results don't depend on the analyzers" over-claims | fixed: NEWS.txt and `udt-tuple-fields.adoc` (commit 17), the javadoc of `DseSearchUdtCompatibilityTest.testCaseInsensitiveField` (commit 16): ranges, `!=` and NOT IN don't depend on the analyzers; `=`, `:`, LIKE, CONTAINS and NOT CONTAINS do, as on columns |
| R18 | docs | DESIGN.md 8 lists OperationTest, not touched by the series | fixed (commit 3): removed (SelectSingleColumnRelationTest added) |
| R19 | docs | `lowerCase` maps to `case_sensitive:false` "for = only" | fixed: DESIGN.md 3.5 (commit 3) and the migration table of `udt-tuple-fields.adoc` (commit 17): for `=`, LIKE and `:`; not for ranges, `!=` and NOT IN |
| R20 | docs | `udt-tuple-fields.adoc`: the positive conditions served by the indexes miss `:` | fixed (commit 17) |
| R21 | minor | `Operation` priority: IS_NOT ties with the default operators (`:`, IN, BM25, ...), so "IS_NOT last" is wrong | fixed (commit 14): IS_NOT has priority -1, strictly below every other operator (the sort is by descending priority), so it merges into the previous expression of its target whatever its operator; comments in Operation and Expression fixed; `OperationTest.testFieldIsNotNullCombinedWithAnyOperator` (`Operation.analyzeGroup` on a case-insensitive tuple field index: `IS NOT NULL` before or after `:` or `=` gives one expression, with the operation of the other one and `isNotNull`; `IS NOT NULL` before `:` gave two expressions before the fix) |
| R22 | nit | IndexContext comment on EQ_ONLY_TYPES is stale | fixed (commit 10): NOT_EQ is served on the multi-valued field paths |
| R23 | nit | RowFilter uses `java.util.Objects.equals` fully qualified | fixed (commit 7): Guava's `Objects.equal`, as in the rest of the file |
| R24 | minor | `ElementMatchRelation.toRestriction` builds the `!=` (registering its bind marker again) before the analyzer check | fixed (commit 12): the check uses the path of the `=` leaf, and the `!=` is built only when the group is reduced |
| R25 | nit | over-long javadoc line of `ElementMatchRelation.isPositive` | fixed: rewrapped in commit 14, which introduced the long line (the IS NOT NULL wording; the commit 12 line was within the limit) |
| R26 | docs | DESIGN.md 8 "five memtable implementations" | fixed (commit 3): four implementations and the configured default |
| R27 | docs | DESIGN.md wording: 1 "on the wire format of the SSTables"; 3.1 redundant refusal | fixed (commit 3): no SSTable or SAI on-disk change, two new `RowFilter` expression kinds without a messaging version change; the refused index classes said once |
| R28 | docs | commit messages | fixed: a body for commit 3 (what the three documents are), the body of commit 9 (ranges, `!=` and NOT IN compare the raw values; `=`, `:`, LIKE and CONTAINS use the analyzer of the index, as on columns), commit 16 subject "HCD-653: DSE Search compatibility tests and distributed test", commit 1 subject shortened (the details moved to the body). Accepted: the reference from DESIGN.md (commit 3) to this ledger (commit 17) |
| R29 | question | implementation-notes.md and this ledger name the reviewer and carry dates | left as they are: for the owner to decide |
| R30 | question | a non-reduced NOT CONTAINS ELEMENT gets "No index serves the restrictions on ..." although an index serves its `=` | fixed (commit 12): the non-reduced `NOT CONTAINS ELEMENT` groups (`ElementMatchRestriction.isFilteredOnly`) get "addrs NOT CONTAINS ELEMENT (...) is not served by the indexes: a NOT CONTAINS ELEMENT group is filtered, except a single equality on a field whose index has no analyzer. ... use ALLOW FILTERING" (`StatementRestrictions.NOT_CONTAINS_ELEMENT_REQUIRES_ALLOW_FILTERING_MESSAGE`); next to other restricted fields that no index serves, the same sentence followed by "No index serves the restrictions on <the other fields>." (`NOT_CONTAINS_ELEMENT_AND_FIELDS_REQUIRE_ALLOW_FILTERING_MESSAGE`); the reduced single-equality group keeps the field message (its `!=` is served by an index without analyzer). Tests updated in commits 13 and 14 |

Also moved while checking the series: the `UdtCollectionFieldQueryTest` change that turns the "CONTAINS ELEMENT is not
yet valid syntax" assertion of commit 10 into a query moved from commit 13 to commit 12, which adds the grammar (the
test failed on commit 12 alone).

Verification: every commit compiles from a clean build directory (`ant jar build-test`); the tests of the commits
whose tests changed pass on them (7: RowFilterTest; 8: SelectGroupByTest, SelectTest, SelectSingleColumnRelationTest;
9: UdtFieldQueryTest; 10: UdtCollectionFieldQueryTest, UdtFieldQueryTest, SelectSingleColumnRelationTest; 12:
CqlParserTest, UdtCollectionFieldQueryTest; 13: UdtElementMatchQueryTest, UdtCollectionFieldQueryTest; 14:
UdtFieldIsNotNullTest, ExpressionTest, OperationTest, RowFilterTest and the earlier field tests; 16:
DseSearchUdtCompatibilityTest, UdtFieldIndexDistributedTest); the gate on the head (23 unit classes and
UdtFieldIndexDistributedTest): 873 tests, 0 failures, 13 skipped.

## Rework review, round 2 (2026-11-06)

Delta review of the fix round (`git diff hcd-653-pre-fixes-2 HEAD`, head before this round cfc5131830, tagged
hcd-653-pre-fixes-3): no blocker. The fixes were squashed into the commits named below; 17 commits.

| # | finding | resolution |
|---|---|---|
| D1 | `testInetFieldRange`: only the index-served assertion tests the R2 fix; the ALLOW FILTERING results are empty for column and field | fixed (commit 9): explicit ids, rows 1 and 2 on the indexed table, also with ALLOW FILTERING; no row on the twin table (the filter compares the raw bytes, SAI compares IPv4 as IPv4-mapped IPv6); R2 row corrected |
| D2 | the R21 test used reflection and `toString()` | fixed (commit 14): moved to `OperationTest.testFieldIsNotNullCombinedWithAnyOperator`, which calls `Operation.analyzeGroup` on a table with a case-insensitive index on a tuple field and asserts one expression, its `Op` and `isNotNull` (package-private `Expression.isNotNull()`); MATCH for `:`. Deviation: an `=` given directly to the planner keeps `Op.EQ` (the CQL layer is what turns an analyzed `=` into `:`, so through CQL it is MATCH); the test asserts EQ for it and says so. The discriminating case (`IS NOT NULL` before `:`, two expressions with priority 0) is kept; R21 row updated |
| D3 | wording of `NOT_CONTAINS_ELEMENT_REQUIRES_ALLOW_FILTERING_MESSAGE` | fixed (commit 12): "%s is not served by the indexes: a NOT CONTAINS ELEMENT group is filtered, except a single equality on a field whose index has no analyzer.", with the ALLOW_FILTERING_MESSAGE prefix and the usual ALLOW FILTERING sentence; implementation-notes.md, R30 row and the tests (commits 13, 14) updated |
| D4 | a filtered-only group hides the other unindexed fields from the message | fixed (commit 12): `NOT_CONTAINS_ELEMENT_AND_FIELDS_REQUIRE_ALLOW_FILTERING_MESSAGE` names the groups, then "No index serves the restrictions on <the other unindexed fields>."; mixed case in `UdtElementMatchQueryTest.testGroupsWithoutIndexedConditions` (commit 13); R30 row says what is printed |
| D5 | principle 2 and DESIGN 3.2, adoc: same result with and without index, unqualified | fixed (commits 3, 17): except the analyzer-dependent operators and the values over the maximum term size |
| D6 | `testAnalyzedConditions` javadoc states the old X11 rule | fixed (commit 13) |
| D7 | IN on an analyzed field said "refused" | checked: `Operator.isSatisfiedByAnalyzed` throws `UnsupportedOperationException` when the query is executed (the row filter evaluation, for fields as for columns); the client gets a server error ("An unexpected error occurred server side ... IN operation does not support analyzers"), not an InvalidRequest. Docs worded by what happens (DESIGN 3.2 and 4.6, commit 3; adoc, commit 17). Pre-existing column behaviour, not changed; for the owner if it should become a validation error |
| D8 | `testFieldExclusionsCompareRawValues` javadoc over-claims | fixed (commit 9): the results of `!=` and NOT IN don't depend on the analyzers |
| D9 | the prepared NOT CONTAINS ELEMENT case asserted only "use ALLOW FILTERING" | fixed (commit 13): the full message, with the group as printed, `addrs NOT CONTAINS ELEMENT (city = ?)` |
| D10 | expected message chosen by a text pattern of the WHERE | fixed (commit 13): each unserved group is paired with its message |
| D11 | `rawMatch` names the warning of the analyzed index | fixed (commit 9): `analyzedWarning` |
| D12 | the MATCH block of `testFieldEqualityWithAnalyzers` ran in one state only | fixed (commit 9): in `inAllStates`, indexed table only, with a comment (the twin differs by design) |
| D13 | system property set and restored by hand in `testOversizedValue` | fixed (commit 14): `WithProperties` |
| D14 | distributed test comment "without index, the raw values" | fixed (commit 16): "on both tables: the analyzed and raw results agree here" |
| D15 | IndexContext comment wording on the analyzed term size | fixed (commit 14) |
| D16 | the analyzer-dependent operators listed differently | fixed: `=`, `:`, LIKE, CONTAINS and NOT CONTAINS in DESIGN 1, 3.2, 4.6 (commit 3), the adoc and NEWS.txt (commit 17), the `testCaseInsensitiveField` javadoc (commit 16), the commit 9 body and the R17 row |
| D17 | implementation-notes.md P5 semantics: empty boolean | fixed (commit 3): "an empty string, `ascii` or `boolean` is set" |
| D18 | commit 1 message repeats NOT CONTAINS KEY | fixed: the first paragraph folded into the next one |
| D19 | index hints on a filtered NOT CONTAINS ELEMENT group undocumented | fixed (commit 17): one sentence in the index-hints paragraph of `udt-tuple-fields.adoc` |

Verification: every commit compiles from a clean build directory; the tests of the commits whose tests changed pass
on them (9: UdtFieldQueryTest; 12: UdtCollectionFieldQueryTest; 13: UdtElementMatchQueryTest; 14:
UdtFieldIsNotNullTest, OperationTest, ExpressionTest, UdtElementMatchQueryTest; 16: DseSearchUdtCompatibilityTest,
UdtFieldIndexDistributedTest); the gate on the head (23 unit classes and UdtFieldIndexDistributedTest): 872 tests
(the two parameterized runs of the moved R21 test are now one OperationTest case), 0 failures, 13 skipped.
