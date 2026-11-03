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
| 10 | nit | gate is point-in-time (late 110 joiner / downgrade) | closed: no version change (owner decision) — no gate |
| 11 | nit | system views show only the column name for a field index | parked → decide in P6 |
| 12 | nit | default current version 111 also for CNDB embedders | closed: no version change (owner decision) — no 111 |

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
| D1 | major | deletion of a whole non-frozen column flushed into its own SSTable comes back | open, not HCD-653: the default `TrieMemtable` of the tests doesn't write a row whose only content is the complex deletion (the flushed SSTable is empty and discarded, 1 live SSTable instead of 2); also with ALLOW FILTERING on a table without index and on primary key reads; `SkipListMemtable`, `TrieMemtableStage1/2/3` are fine; reproduced on 63868f5d74; `ComplexDeletionAcrossSSTablesTest` (ignored) |

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
| note | — | cqlsh completion doesn't know `CONTAINS ELEMENT` | parked → P6 `NEWS.txt` |
