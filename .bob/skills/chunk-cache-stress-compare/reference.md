<!--
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

# Reference: reading the results

## What the chunk cache and its pool are

**Which files go through the cache:** with `file_cache_enabled: true`, reads of compressed (and encrypted) data files go through `ChunkCache`. So do reads of any file that is not memory-mapped.
- Under the default `disk_access_mode` (`mmap_index_only`), the uncompressed BTI index files are memory-mapped and don't use the cache.

**Who reads through the cache:**
- client reads;
- compaction, cleanup and scrub;
- repair validation;
- the SAI index build;
- the `SizeEstimatesRecorder` (OptionalTasks).

Streaming does **not**: it uses the data channel directly.

**The pool:** cached chunks are allocated from a dedicated `BufferPool` named `chunk-cache`, whose size is `file_cache_size`. Each thread takes buffers through a local pool, so some of the pool can be held by threads at any moment.

**Two behaviours to compare when the pool is full:**
- **Overflow** (the behaviour of `main-5.0` before CNDB-19222): the allocation falls back to `allocateDirect` outside the pool. JMX `BufferPool … OverflowSize` grows. The only hard limit is `-XX:MaxDirectMemorySize`, which ends in an OOM.
- **Bounded** (CNDB-19222): reclaim, retry, park up to 5 ms, then throw `BufferPoolExhaustedException`. The operation that needed the chunk fails, whatever it was: a client read, compaction, the SAI build, cqlsh's own system-table reads.
  - Part of the pool can be withheld from the cache as headroom: `-Dcassandra.chunk_cache_reserved_pool_space_in_mb` (32 in `0a296021ceb`, 64 in `d38e61306eb`).
  - The cache capacity is then `file_cache_size - reserved`. Check the JMX `ChunkCache Size` peak.

**The key number for the base build is the peak overflow:** it is how much memory the base used beyond the pool, and so how much the bounded build has to absorb from its headroom.

## JMX columns (`samples.csv`)

| column | MBean | meaning |
|---|---|---|
| `bp_used` | `type=BufferPool,scope=chunk-cache,name=UsedSize` | bytes in use in the pool |
| `bp_overflow` | `…name=OverflowSize` | bytes allocated outside the pool (base build only) |
| `bp_size` | `…name=Size` | pool size |
| `cc_size` | `type=Cache,scope=ChunkCache,name=Size` | cache weight (capacity once it is full) |
| `cc_misses` | `…name=Misses` | cache misses |
| `cc_sync_reclaims`, `cc_reclaim_retry_ok`, `cc_blocked`, `cc_wait_ok`, `cc_pool_exhausted` | `…name=SyncReclaims`, `ReclaimRetrySuccesses`, `BlockedOnPoolAllocation`, `PoolWaitSuccesses`, `PoolExhausted` | the bounded-pool path; empty on builds that don't have it |
| `direct_used` | `java.nio:type=BufferPool,name=direct` | all direct memory of the JVM |

**`PoolExhausted` counts the failed allocations.** Compare it with `BlockedOnPoolAllocation`: when the two are close, the wait almost never helps.

**`bp_used` is sampled every 2 s**, so a pool can be exhausted between samples even though the peak looks below the size.

## Known failure modes seen with this harness

1. **Client reads fail:**
   - client side: `Cassandra failure during read query at consistency ONE (… 1 failed)`;
   - server side: `ReadStage`, `RuntimeException` ← `BufferPoolExhaustedException`.
2. **Failed reads reported as corruption:** `CorruptSSTableException` ← `IOException: Error building row …` ← `BufferPoolExhaustedException`.
   - The sstable is **not** corrupt. The base build reads the same golden files without error.
   - `d38e61306eb` stops this wrapping. Its implementation uses `Throwables.propagateIfPossible(e, BufferPoolExhaustedException.class)`, which rethrows every `RuntimeException` and `Error` unchanged, so real corruption detection there is lost too. Check the candidate's code if this area changed.
3. **Major compaction fails:** `CompactionExecutor`, `SortedStringTableCursor.advance` → `ChunkCache.load` → `BufferPoolExhaustedException`. `nodetool compact` prints the same error, and the sstable count stays at the original 7.
4. **SAI initial build fails:**
   - `SecondaryIndexExecutor` with `BufferPoolExhaustedException`;
   - the index is left non-queryable ("Please run full index rebuild").
5. **cqlsh can't connect:** `Unable to connect to any servers … ReadFailure code=1300`. Its system-table reads fail, so `CREATE INDEX` can't even be sent (`create-index.out` shows 30 failed attempts).
6. **Size estimates fail:** `OptionalTasks`, `SizeEstimatesRecorder` → `BufferPoolExhaustedException` (every 5 minutes).
7. **Log flood:** millions of stack traces, about 19 GiB of `system.log` in 16 minutes, and hundreds of rotations.

## Failures not caused by the chunk cache

**SAI initial build during the major compaction:** it fails with `IllegalArgumentException: new position should not be negative` in `StorageAttachedIndexBuilder.indexSSTable`, preceded by `Couldn't acquire reference to the SSTable` and `… has no TOC file` warnings. It happens on **both** builds, mostly at 256 client threads.
- It is https://github.com/riptano/cndb/issues/19876 (a race with early-opened sstables of a running major compaction).
- Report it as such, and don't count it as a difference between the builds.
- Tell it apart from failure mode 4 by the root cause in the stack trace.

## Pitfalls

- **Python for cqlsh:** the bundled cqlsh doesn't support Python 3.14. Set `CQLSH_PYTHON`.
- **Java for nb5:** NoSQLBench 5.25 jars are compiled for Java 25 (class version 69).
- **nb5 bindings:** use `HashRange(0L,N)` for the random keys; `Uniform(0L,N)->long` doesn't exist.
- **cqlsh during a collapse:** it fails, so the harness retries `CREATE INDEX` and never aborts the run on a cqlsh failure.
- **Rotated logs:** count errors over all of them; `summarize` and `error_catalogue.py` already do.
- **Don't edit `ccblast.sh` while a round is running.** Bash reads the script while it executes, and the running round breaks.
- **Don't kill processes with `pkill -f <pattern>`** when the pattern also matches your own shell. Kill by PID (`$CCB_WORKDIR/node.pid`).
- **Smaller cache:** a smaller effective cache (a larger reserve) changes the hit rate. With uniform random reads almost every read misses anyway, so also compare `cc_misses`.
- **Background load on the host** (VMs, builds) slows both builds. Interleaving the builds keeps the comparison fair; still record the host load.
