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
