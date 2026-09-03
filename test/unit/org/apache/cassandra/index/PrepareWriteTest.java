/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.CassandraWriteContext;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.WriteOptions;
import org.apache.cassandra.db.WriteOrigin;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link Index#prepareWrite}: an index that {@linkplain Index#preparesWrites prepares writes} sees each partition
 * update BEFORE the mutation is appended to the commit log, takes the handle it returned back from the
 * {@link CassandraWriteContext} in {@code indexerFor} when the update reaches the memtable, and gets
 * {@link Index#abortWrite} for every handle that is not taken.
 */
public class PrepareWriteTest extends CQLTester
{
    @After
    public void resetIndexBehaviour()
    {
        PreparingIndex.failAfter.set(-1);
        PreparingIndex.takeHandles = true;
        PreparingIndex.prepareNothing = false;
        ReadOnlyOnFailureIndex.failInitialization = false;
    }

    @Test
    public void prepareWriteRunsBeforeTheCommitLogAppend() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        PreparingIndex index = createPreparingIndex("prep_idx");
        assertTrue(getCurrentColumnFamilyStore().indexManager.preparesWrites());

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 2, 3);

        assertEquals(1, index.prepared.size());
        assertEquals(1, index.applied.size());
        Prepared prepared = index.prepared.get(0);
        Applied applied = index.applied.get(0);

        // The handle returned by prepareWrite is the very object indexerFor takes back for the same update.
        assertSame(prepared, applied.handle);
        assertEquals(1, prepared.rows);
        assertTrue("a taken handle is not aborted", index.aborted.isEmpty());

        // The ordering proof: the commit log's allocation point observed in prepareWrite lies strictly
        // before the position this mutation was appended at, which is only possible if prepareWrite ran
        // before the append (a mutation's position is the END of its own allocation).
        assertNotNull("the write went through the commit log", applied.position);
        assertTrue("prepareWrite observed " + prepared.commitLogPositionBefore + ", the mutation landed at " + applied.position,
                   prepared.commitLogPositionBefore.compareTo(applied.position) < 0);

        // A client write this node coordinated, applied with the default options.
        assertEquals(WriteOptions.DEFAULT, prepared.options);
        assertSame(WriteOrigin.LOCAL, prepared.origin);
    }

    @Test
    public void everyUpdateOfAMultiTableMutationIsPrepared() throws Throwable
    {
        String t1 = createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a))");
        PreparingIndex index1 = createPreparingIndex("prep_idx_t1");
        String t2 = createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a))");
        PreparingIndex index2 = createPreparingIndex("prep_idx_t2");

        // Same partition key in both tables: one Mutation, two PartitionUpdates.
        execute(String.format("BEGIN UNLOGGED BATCH " +
                              "INSERT INTO %s.%s (a, b) VALUES (7, 1); " +
                              "INSERT INTO %s.%s (a, b) VALUES (7, 2); " +
                              "APPLY BATCH", KEYSPACE, t1, KEYSPACE, t2));

        assertEquals(1, index1.prepared.size());
        assertEquals(1, index2.prepared.size());
        assertSame(index1.prepared.get(0), index1.applied.get(0).handle);
        assertSame(index2.prepared.get(0), index2.applied.get(0).handle);

        // Both updates were prepared before the (single) commit log append of the mutation.
        CommitLogPosition position = index1.applied.get(0).position;
        assertEquals(position, index2.applied.get(0).position);
        assertTrue(index1.prepared.get(0).commitLogPositionBefore.compareTo(position) < 0);
        assertTrue(index2.prepared.get(0).commitLogPositionBefore.compareTo(position) < 0);
    }

    @Test
    public void writesThatSkipIndexingAreNotPrepared() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a))");
        PreparingIndex index = createPreparingIndex("prep_idx_skip");

        Keyspace.open(KEYSPACE).apply(mutation(1), WriteOptions.SKIP_INDEXES_AND_COMMITLOG);

        assertTrue(index.prepared.isEmpty());
        assertTrue(index.applied.isEmpty());
        assertTrue(index.aborted.isEmpty());
    }

    @Test
    public void commitLogReplayIsPreparedWithItsOwnOptions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a))");
        PreparingIndex index = createPreparingIndex("prep_idx_replay");

        // A fresh commit log, so that the replay below contains this test's write and nothing older.
        CommitLog.instance.resetUnsafe(true);
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 2, 2);
        assertEquals(1, index.prepared.size());
        CommitLogPosition appendedAt = index.applied.get(0).position;
        assertNotNull(appendedAt);

        // The real thing: stop the commit log without discarding its segments and start it again, which
        // replays them the way a restart does (CommitLogReplayer, then a STARTUP flush of what it replayed).
        // Nothing was flushed, so the mutation above is re-applied through the normal write path, and
        // Cassandra does not second-guess which applies an index wants to know about: the replayed mutation
        // is prepared like any other, labelled with the replay options so the index can recognise one it has
        // already recorded when it was first written.
        CommitLog.instance.resetUnsafe(false);

        assertEquals(2, index.prepared.size());
        assertEquals(2, index.applied.size());
        Prepared replayed = index.prepared.get(1);
        assertEquals(WriteOptions.FOR_COMMITLOG_REPLAY, replayed.options);
        assertSame("a replayed mutation did not arrive over the wire", WriteOrigin.LOCAL, replayed.origin);
        assertEquals(1, replayed.rows);
        assertSame(replayed, index.applied.get(1).handle);
        assertNull("no commit log append on replay", index.applied.get(1).position);
        assertTrue("the replayed mutation was written before the replay started",
                   appendedAt.compareTo(replayed.commitLogPositionBefore) < 0);
        assertTrue(index.aborted.isEmpty());
    }

    @Test
    public void aFailingPrepareAbortsTheOthersAndFailsTheWriteBeforeTheCommitLog() throws Throwable
    {
        String t1 = createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a))");
        PreparingIndex index1 = createPreparingIndex("prep_idx_fail_t1");
        String t2 = createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a))");
        PreparingIndex index2 = createPreparingIndex("prep_idx_fail_t2");

        // One mutation, two tables, two indexes: the first prepareWrite returns a handle, the second throws.
        // Which index goes first is not specified, hence the shared countdown rather than a per-index switch.
        PreparingIndex.failAfter.set(1);
        Mutation mutation = Mutation.merge(Arrays.asList(mutation(t1, 1), mutation(t2, 1)));
        CommitLogPosition before = CommitLog.instance.getCurrentPosition();
        try
        {
            Keyspace.open(KEYSPACE).apply(mutation, WriteOptions.DEFAULT);
            fail("the write should have failed");
        }
        catch (IllegalStateException expected)
        {
            assertEquals(PreparingIndex.FAILURE_MESSAGE, expected.getMessage());
        }

        // Exactly one handle was returned, and it was aborted: for the index that got it, the write never
        // happened. Nobody was asked for an indexer.
        List<Prepared> prepared = concat(index1.prepared, index2.prepared);
        List<Prepared> aborted = concat(index1.aborted, index2.aborted);
        assertEquals(1, prepared.size());
        assertEquals(1, aborted.size());
        assertSame(prepared.get(0), aborted.get(0));
        assertTrue(index1.applied.isEmpty());
        assertTrue(index2.applied.isEmpty());

        // Nothing was persisted: no commit log append, no row in either memtable.
        assertEquals(before, CommitLog.instance.getCurrentPosition());
        assertEmpty(execute(String.format("SELECT * FROM %s.%s", KEYSPACE, t1)));
        assertEmpty(execute(String.format("SELECT * FROM %s.%s", KEYSPACE, t2)));
    }

    @Test
    public void aHandleTheIndexDoesNotTakeIsAbortedWhenTheWriteEnds() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a))");
        PreparingIndex index = createPreparingIndex("prep_idx_untaken");

        PreparingIndex.takeHandles = false;
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);

        // The write itself went through: the index got its indexer, just without taking the handle...
        assertEquals(1, index.prepared.size());
        assertEquals(1, index.applied.size());
        assertNull(index.applied.get(0).handle);
        assertRows(execute("SELECT a, b FROM %s"), row(1, 1));
        // ... so the handle was aborted when the write context closed.
        assertEquals(1, index.aborted.size());
        assertSame(index.prepared.get(0), index.aborted.get(0));

        // A handle is taken at most once: taking it again yields nothing, and does not abort it again.
        PreparingIndex.takeHandles = true;
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 2, 2);
        assertEquals(2, index.applied.size());
        assertSame(index.prepared.get(1), index.applied.get(1).handle);
        assertNull(index.applied.get(1).secondTake);
        assertEquals(1, index.aborted.size());
    }

    @Test
    public void aPreparingIndexMayDeclineAWriteAndStillGetsItsIndexer() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a))");
        PreparingIndex index = createPreparingIndex("prep_idx_null");

        // The index is asked and returns null: no handle to carry, nothing to abort, and the
        // indexer is still asked for, taking null from the context, like every other write.
        PreparingIndex.prepareNothing = true;
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);

        assertEquals(1, index.declined.get());
        assertTrue(index.prepared.isEmpty());
        assertEquals(1, index.applied.size());
        assertNull(index.applied.get(0).handle);
        assertTrue(index.aborted.isEmpty());
        assertRows(execute("SELECT a, b FROM %s"), row(1, 1));

        // Deciding per write: the next one is prepared and its handle taken as usual.
        PreparingIndex.prepareNothing = false;
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 2, 2);
        assertEquals(1, index.prepared.size());
        assertSame(index.prepared.get(0), index.applied.get(1).handle);
        assertTrue(index.aborted.isEmpty());
    }

    @Test
    public void aNonWritableIndexIsNeitherPreparedNorAborted() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a))");
        ReadOnlyOnFailureIndex.failInitialization = true;
        String name = createIndexAsync(String.format("CREATE CUSTOM INDEX prep_idx_ro ON %%s(b) USING '%s'",
                                                     ReadOnlyOnFailureIndex.class.getName()));
        waitForIndexBuilds(name);
        SecondaryIndexManager manager = getCurrentColumnFamilyStore().indexManager;
        ReadOnlyOnFailureIndex index = (ReadOnlyOnFailureIndex) manager.getIndexByName(name);
        assertFalse("premise: the failed initialization left the index non-writable", manager.isIndexWritable(index));
        assertTrue("registered, so the table still checks its indexes", manager.preparesWrites());

        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);

        // Not writable means not asked for an indexer, hence not prepared either -- and nothing to abort.
        assertTrue(index.prepared.isEmpty());
        assertTrue(index.applied.isEmpty());
        assertTrue(index.aborted.isEmpty());
    }

    @Test
    public void anIndexThatDoesNotPrepareCostsNothingAndTakesNull() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a))");
        String name = "plain_idx";
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(b) USING '%s'", name, PlainIndex.class.getName()));
        SecondaryIndexManager manager = getCurrentColumnFamilyStore().indexManager;
        PlainIndex index = (PlainIndex) manager.getIndexByName(name);
        assertFalse("no index of this table prepares writes: the write path skips the hook", manager.preparesWrites());

        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);

        assertEquals(1, index.taken.size());
        assertNull(index.taken.get(0));

        // Adding a preparing index flips the switch; dropping it flips it back.
        createPreparingIndex("prep_idx_plain");
        assertTrue(manager.preparesWrites());
        dropIndex("DROP INDEX %s.prep_idx_plain");
        assertFalse(manager.preparesWrites());
    }

    private PreparingIndex createPreparingIndex(String name)
    {
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(b) USING '%s'", name, PreparingIndex.class.getName()));
        return (PreparingIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(name);
    }

    private Mutation mutation(int key)
    {
        return mutation(currentTable(), key);
    }

    private Mutation mutation(String table, int key)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        return new RowUpdateBuilder(cfs.metadata(), 0, key).add("b", 1).build();
    }

    private static List<Prepared> concat(List<Prepared> a, List<Prepared> b)
    {
        List<Prepared> all = new CopyOnWriteArrayList<>(a);
        all.addAll(b);
        return all;
    }

    /** What one prepareWrite call saw; doubles as the handle taken back in indexerFor. */
    static final class Prepared implements Index.PreparedWrite
    {
        final CommitLogPosition commitLogPositionBefore;
        final WriteOptions options;
        final WriteOrigin origin;
        final int rows;

        Prepared(CommitLogPosition commitLogPositionBefore, WriteOptions options, WriteOrigin origin, int rows)
        {
            this.commitLogPositionBefore = commitLogPositionBefore;
            this.options = options;
            this.origin = origin;
            this.rows = rows;
        }
    }

    /** What one indexerFor call took from the write context on the write path. */
    static final class Applied
    {
        final Index.PreparedWrite handle;
        final Index.PreparedWrite secondTake;
        final CommitLogPosition position;

        Applied(Index.PreparedWrite handle, Index.PreparedWrite secondTake, CommitLogPosition position)
        {
            this.handle = handle;
            this.secondTake = secondTake;
            this.position = position;
        }
    }

    public static class PreparingIndex extends StubIndex
    {
        static final String FAILURE_MESSAGE = "prepareWrite is configured to fail";
        /** Shared by all instances: when non-negative, the prepareWrite call that reaches zero throws. */
        static final AtomicInteger failAfter = new AtomicInteger(-1);
        /** Whether indexerFor takes the handle back from the write context. */
        static volatile boolean takeHandles = true;
        /** When set, prepareWrite declines every write (returns null) instead of preparing it. */
        static volatile boolean prepareNothing = false;

        final List<Prepared> prepared = new CopyOnWriteArrayList<>();
        final List<Applied> applied = new CopyOnWriteArrayList<>();
        final List<Prepared> aborted = new CopyOnWriteArrayList<>();
        /** Writes prepareWrite was asked about and declined. */
        final AtomicInteger declined = new AtomicInteger();

        public PreparingIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        @Override
        public boolean preparesWrites()
        {
            return true;
        }

        @Override
        public PreparedWrite prepareWrite(PartitionUpdate update, WriteOptions options, WriteOrigin origin)
        {
            if (failAfter.get() >= 0 && failAfter.getAndDecrement() == 0)
                throw new IllegalStateException(FAILURE_MESSAGE);
            if (prepareNothing)
            {
                declined.incrementAndGet();
                return null;
            }
            int rows = 0;
            for (@SuppressWarnings("unused") Object row : update.rows())
                rows++;
            Prepared p = new Prepared(CommitLog.instance.getCurrentPosition(), options, origin, rows);
            prepared.add(p);
            return p;
        }

        @Override
        public void abortWrite(PreparedWrite prepared)
        {
            aborted.add((Prepared) prepared);
        }

        @Override
        public Indexer indexerFor(DecoratedKey key,
                                  RegularAndStaticColumns columns,
                                  long nowInSec,
                                  WriteContext ctx,
                                  IndexTransaction.Type transactionType,
                                  Memtable memtable)
        {
            if (memtable != null) // the write path; the build/compaction/cleanup paths carry no handle
            {
                PreparedWrite handle = takeHandles ? ctx.takePreparedWrite(this) : null;
                PreparedWrite secondTake = takeHandles ? ctx.takePreparedWrite(this) : null;
                applied.add(new Applied(handle, secondTake, CassandraWriteContext.fromContext(ctx).getPosition()));
            }
            return super.indexerFor(key, columns, nowInSec, ctx, transactionType, memtable);
        }
    }

    /** A preparing index whose initialization can be made to fail, leaving it readable but not writable. */
    public static class ReadOnlyOnFailureIndex extends PreparingIndex
    {
        static volatile boolean failInitialization = false;

        public ReadOnlyOnFailureIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        @Override
        public Callable<?> getInitializationTask()
        {
            return () ->
            {
                if (failInitialization)
                    throw new IllegalStateException("initialization is configured to fail");
                return null;
            };
        }

        @Override
        public LoadType getSupportedLoadTypeOnFailure(boolean isInitialBuild)
        {
            return LoadType.READ;
        }
    }

    /** Does not prepare writes; only records what taking a handle yields on the write path. */
    public static class PlainIndex extends StubIndex
    {
        final List<Index.PreparedWrite> taken = new CopyOnWriteArrayList<>();

        public PlainIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        @Override
        public Indexer indexerFor(DecoratedKey key,
                                  RegularAndStaticColumns columns,
                                  long nowInSec,
                                  WriteContext ctx,
                                  IndexTransaction.Type transactionType,
                                  Memtable memtable)
        {
            if (memtable != null)
                taken.add(ctx.takePreparedWrite(this));
            return super.indexerFor(key, columns, nowInSec, ctx, transactionType, memtable);
        }
    }
}
