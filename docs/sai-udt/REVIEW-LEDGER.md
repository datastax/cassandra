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
