/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.memtable;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.partitions.BTreePartitionUpdate;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdateSerializer;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.tries.OnDiskDeletionAwareTrie;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * A trie partition update keys each column by its index among the static or regular columns of the metadata it was
 * built or read with, and a trie memtable keys its contents by those of the metadata it was created with. A schema
 * change between building or reading an update and applying it can leave the two numbering the columns differently,
 * and the update must still land in the columns it was written for.
 * <p>
 * Each test makes updates under one schema, changes the schema, which switches the memtable, applies the updates to the
 * new memtable, and reads them back from it and from the sstable it flushes.
 * <p>
 * An update made under a schema newer than the memtable's, which has a column the memtable cannot key, fails instead,
 * and is applied once more if the table's schema is newer than the memtable's as well. An update whose schema numbers
 * the columns as the memtable's does is merged as it is.
 */
@RunWith(BMUnitRunner.class)
@BMRules(rules = { @BMRule(name = "Run a test's action when a schema change reaches a table, before the table switches its memtable",
                           targetClass = "ColumnFamilyStore",
                           targetMethod = "reload",
                           action = "org.apache.cassandra.db.memtable.TrieMemtableSchemaChangeTest.reloading($0)"),
                   @BMRule(name = "Count the updates rebuilt with another column numbering",
                           targetClass = "TriePartitionUpdate",
                           targetMethod = "withColumnIndexes",
                           action = "org.apache.cassandra.db.memtable.TrieMemtableSchemaChangeTest.rebuilding($2)"),
                   @BMRule(name = "Count the writes failed for holding a column newer than the memtable",
                           targetClass = "ColumnNewerThanMemtableException",
                           targetMethod = "<init>",
                           action = "org.apache.cassandra.db.memtable.TrieMemtableSchemaChangeTest.failing()"),
                   @BMRule(name = "Count the column lists of updates a trie memtable compares column by column with its own",
                           targetClass = "RegularAndStaticColumns",
                           targetMethod = "equals",
                           condition = "callerEquals(\"keyedAsThisMemtable\")",
                           action = "org.apache.cassandra.db.memtable.TrieMemtableSchemaChangeTest.comparingColumns($0)") })
public class TrieMemtableSchemaChangeTest extends CQLTester
{
    /** The table whose updates {@link #rebuilding} counts. */
    private static volatile TableId rebuildsOf;
    private static final AtomicInteger rebuilds = new AtomicInteger();
    private static final AtomicInteger failedWrites = new AtomicInteger();
    /** The column lists whose comparisons {@link #comparingColumns} counts. */
    private static volatile RegularAndStaticColumns comparisonsOf;
    private static final AtomicInteger columnComparisons = new AtomicInteger();

    @BeforeClass
    public static void setUpNetwork()
    {
        // For the writes made through the coordinator, which needs the node up as a replica.
        requireNetworkWithoutDriver();
    }

    /** Called on every rebuild of an update with another column numbering, given the metadata of the result. */
    public static void rebuilding(TableMetadata metadata)
    {
        if (metadata.id.equals(rebuildsOf))
            rebuilds.incrementAndGet();
    }

    /** Called on every write a memtable fails for holding a column newer than the memtable. */
    public static void failing()
    {
        failedWrites.incrementAndGet();
    }

    /** Called on every comparison of an update's column lists with a memtable's, column by column. */
    public static void comparingColumns(RegularAndStaticColumns columns)
    {
        if (columns == comparisonsOf)
            columnComparisons.incrementAndGet();
    }

    /** See {@link #reloading}. */
    private static volatile Consumer<ColumnFamilyStore> beforeMemtableSwitch;

    /**
     * Called at the start of {@link ColumnFamilyStore#reload}, which a schema change calls after it has published the
     * table's new metadata, and which switches the memtable if the change requires it.
     */
    public static void reloading(ColumnFamilyStore cfs)
    {
        Consumer<ColumnFamilyStore> action = beforeMemtableSwitch;
        if (action != null)
            action.accept(cfs);
    }

    /** See {@link #switchingMemtable}. */
    private static volatile HeldSwitch heldSwitch;

    /**
     * The next switch of a table's memtable, which {@link #switchingMemtable} holds after the switch has taken the
     * table's lock, and before it has created the new memtable and issued the write barrier that sends the write groups
     * started after it there.
     */
    private static class HeldSwitch
    {
        final String table;
        final AtomicBoolean held = new AtomicBoolean();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch released = new CountDownLatch(1);
        final AtomicInteger requests = new AtomicInteger();
        /**
         * Counted down by the second request to switch the table's memtable (see {@link #requestingSwitch}), and by a
         * test when the write that should make it has ended without making it.
         */
        final CountDownLatch requestedAgain = new CountDownLatch(1);

        HeldSwitch(String table)
        {
            this.table = table;
        }
    }

    /** Called at the start of {@link ColumnFamilyStore#switchMemtableIfCurrent}, before it takes the table's lock. */
    public static void requestingSwitch(ColumnFamilyStore cfs)
    {
        HeldSwitch hold = heldSwitch;
        if (hold != null && cfs.name.equals(hold.table) && hold.requests.incrementAndGet() == 2)
            hold.requestedAgain.countDown();
    }

    /** Called at the start of {@link ColumnFamilyStore#switchMemtable}, which runs with the table's lock taken. */
    public static void switchingMemtable(ColumnFamilyStore cfs)
    {
        HeldSwitch hold = heldSwitch;
        if (hold == null || !cfs.name.equals(hold.table) || !hold.held.compareAndSet(false, true))
            return;
        hold.started.countDown();
        Uninterruptibles.awaitUninterruptibly(hold.released, 1, TimeUnit.MINUTES);
    }

    /** A column that sorts first moves every regular column up by one, the complex ones as well. */
    @Test
    public void testRegularColumnsMoved() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b int, d int, m map<text, text>, PRIMARY KEY(k, c)) WITH memtable = 'trie'");

        int rebuilt = applyAfterSchemaChange(builder -> {
                                                 builder.row(1).add("b", 1).add("d", 2).add("m", ImmutableMap.of("x", "1"));
                                                 builder.row(2).add("b", 3).delete("m");
                                             },
                                             "ALTER TABLE %s ADD a int");

        assertRowsOfEachPartition("SELECT c, a, b, d, m FROM %s WHERE k = ?",
                                  row(1, null, 1, 2, map("x", "1")),
                                  row(2, null, 3, null, null));
        assertEquals(3, rebuilt);
    }

    /** A static column that sorts first moves every static column up by one, the complex ones as well. */
    @Test
    public void testStaticColumnsMoved() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, sb int static, sd int static, ss set<text> static, v int, PRIMARY KEY(k, c)) WITH memtable = 'trie'");

        int rebuilt = applyAfterSchemaChange(builder -> {
                                                 builder.row().add("sb", 1).add("sd", 2).add("ss", ImmutableSet.of("x"));
                                                 builder.row(1).add("v", 3);
                                             },
                                             "ALTER TABLE %s ADD sa int static");

        assertRowsOfEachPartition("SELECT c, sa, sb, sd, ss, v FROM %s WHERE k = ?",
                                  row(1, null, 1, 2, set("x"), 3));
        assertEquals(3, rebuilt);
    }

    /**
     * Partition, range and row deletions, a collection deletion, and cells and a row with a TTL, in updates whose
     * regular and static columns move, over what the table held before the schema change.
     */
    @Test
    public void testDeletionsAndTtlsMoved() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b int, d int, m map<text, text>, s int static, PRIMARY KEY(k, c)) WITH memtable = 'trie'");
        // For the partition, collection, row and range deletions to delete, and a row none of them covers.
        for (int key = 0; key < 3; ++key)
        {
            execute("INSERT INTO %s (k, c, b) VALUES (?, 0, 0) USING TIMESTAMP 500", key);
            execute("INSERT INTO %s (k, c, b, m) VALUES (?, 2, 20, {'y': '2'}) USING TIMESTAMP 1500", key);
            execute("INSERT INTO %s (k, c, b) VALUES (?, 3, 30) USING TIMESTAMP 1500", key);
            execute("INSERT INTO %s (k, c, b) VALUES (?, 6, 60) USING TIMESTAMP 1500", key);
            execute("INSERT INTO %s (k, c, b) VALUES (?, 10, 100) USING TIMESTAMP 1500", key);
        }
        long now = FBUtilities.nowInSeconds();

        int rebuilt = applyAfterSchemaChange(builder -> {
                                                 builder.timestamp(1000).delete();
                                                 builder.timestamp(2000);
                                                 builder.row().add("s", 7);
                                                 builder.row(1).add("b", 1).add("d", 2).add("m", ImmutableMap.of("x", "1"));
                                                 builder.row(2).delete("m");
                                                 builder.row(3).delete();
                                                 builder.addRangeTombstone().start(5).end(9).inclStart().exclEnd();
                                                 builder.nowInSec(now).row(4).ttl(3600).add("b", 4).add("d", 44);
                                             },
                                             "ALTER TABLE %s ADD a int",
                                             "ALTER TABLE %s ADD a_s int static");

        assertRowsOfEachPartition("SELECT c, a, a_s, b, d, m, s FROM %s WHERE k = ?",
                                  row(1, null, null, 1, 2, map("x", "1"), 7),
                                  row(2, null, null, 20, null, null, 7),
                                  row(4, null, null, 4, 44, null, 7),
                                  row(10, null, null, 100, null, null, 7));
        beforeAndAfterFlush(() -> {
            for (int key = 0; key < 3; ++key)
            {
                int ttl = execute("SELECT ttl(d) FROM %s WHERE k = ? AND c = 4", key).one().getInt("ttl(d)");
                assertTrue(String.valueOf(ttl), ttl > 0 && ttl <= 3600);
            }
        });
        assertEquals(3, rebuilt);
    }

    /**
     * A counter column that sorts first moves the counter columns. The counter leader collects the counter cells of an
     * update and rewrites them with their new values, which it reads from the table (CounterMutation), under the
     * update's numbering, before the memtable rebuilds the update with its own.
     */
    @Test
    public void testCounterColumnsMoved() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b counter, d counter, PRIMARY KEY(k, c)) WITH memtable = 'trie'");
        for (int key = 0; key < 3; ++key)
            execute("UPDATE %s SET b = b + 10, d = d + 20 WHERE k = ? AND c = 1", key);

        int rebuilt = applyAfterSchemaChange(builder -> builder.row(1)
                                                               .add("b", CounterContext.instance().createUpdate(1L))
                                                               .add("d", CounterContext.instance().createUpdate(2L)),
                                             update -> new CounterMutation(new Mutation(update), ConsistencyLevel.ONE).applyCounterMutation(),
                                             "ALTER TABLE %s ADD a counter");

        assertRowsOfEachPartition("SELECT c, a, b, d FROM %s WHERE k = ?", row(1, null, 11L, 22L));
        assertEquals(3, rebuilt);
    }

    /**
     * An indexed column that moves, which a legacy secondary index finds in the rows the memtable hands it as it merges
     * them, keyed by the memtable's numbering.
     */
    @Test
    public void testIndexedColumnMoved() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b int, d int, PRIMARY KEY(k, c)) WITH memtable = 'trie'");
        createIndex("CREATE INDEX ON %s(d) USING 'legacy_local_table'");

        int rebuilt = applyAfterSchemaChange(builder -> builder.row(1).add("b", 1).add("d", 2),
                                             "ALTER TABLE %s ADD a int");

        beforeAndAfterFlush(() -> assertRowsIgnoringOrder(execute("SELECT k, c, a, b, d FROM %s WHERE d = 2"),
                                                          row(0, 1, null, 1, 2),
                                                          row(1, 1, null, 1, 2),
                                                          row(2, 1, null, 1, 2)));
        assertEquals(3, rebuilt);
    }

    /**
     * A column dropped and added again after an update was built, with another column added in between that moves it.
     * What the update holds for the column is kept if it is newer than the drop and left out if it is not. The rebuild
     * leaves it out of the memtable: reads and flushes would leave it out as well, as they do the data of dropped
     * columns, so they cannot tell.
     */
    @Test
    public void testDroppedAndReAddedColumnMoved() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b int, v int, PRIMARY KEY(k, c)) WITH memtable = 'trie'");

        int rebuilt = applyAfterSchemaChange(builder -> {
                                                 builder.timestamp(1000).row(1).add("b", 1).add("v", 2);
                                                 builder.timestamp(3000).row(2).add("b", 3).add("v", 4);
                                             },
                                             "ALTER TABLE %s DROP v USING TIMESTAMP 2000",
                                             "ALTER TABLE %s ADD a int",
                                             "ALTER TABLE %s ADD v int");

        for (int key = 0; key < 3; ++key)
        {
            assertNull(cellInMemtable(key, 1, "v"));
            assertNotNull(cellInMemtable(key, 1, "b"));
            assertNotNull(cellInMemtable(key, 2, "v"));
        }
        assertRowsOfEachPartition("SELECT c, a, b, v FROM %s WHERE k = ?",
                                  row(1, null, 1, null),
                                  row(2, null, 3, 4));
        assertEquals(3, rebuilt);
    }

    /**
     * A regular column dropped, and a clustering column renamed to its name since. The memtable's column of that name
     * is not the update's, which the memtable lists as dropped, and leaves out.
     */
    @Test
    public void testDroppedColumnNameTakenByClusteringColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ck int, b int, c int, PRIMARY KEY(k, ck)) WITH memtable = 'trie'");

        int rebuilt = applyAfterSchemaChange(builder -> builder.row(1).add("b", 1).add("c", 2),
                                             "ALTER TABLE %s DROP c",
                                             "ALTER TABLE %s RENAME ck TO c");

        assertRowsOfEachPartition("SELECT c, b FROM %s WHERE k = ?", row(1, 1));
        assertEquals(3, rebuilt);
    }

    /**
     * A column dropped, added again, and dropped again after the update was made. The update's column was added again
     * after the first drop, which is not the one the memtable's metadata lists, so the column is not newer than the
     * memtable, which leaves it out, as it does a column dropped since.
     */
    @Test
    public void testUpdateBetweenDropsOfColumnIsRebuilt() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b int, v int, PRIMARY KEY(k, c)) WITH memtable = 'trie'");
        alterTable("ALTER TABLE %s DROP v USING TIMESTAMP 1000");
        alterTable("ALTER TABLE %s ADD v int");

        failedWrites.set(0);
        int rebuilt = applyAfterSchemaChange(builder -> builder.row(1).add("b", 1).add("v", 2),
                                             "ALTER TABLE %s DROP v USING TIMESTAMP 2000");

        assertEquals(0, failedWrites.get());
        assertRowsOfEachPartition("SELECT c, b FROM %s WHERE k = ?", row(1, 1));
        assertEquals(3, rebuilt);
    }

    /**
     * A column dropped and added again in the same place leaves the column lists equal, and an update made before the
     * drop is merged as it is. What it holds of the column that is not newer than the drop, which the rebuild would
     * leave out, stays in the memtable, and reads and flushes leave it out, as they do the data of dropped columns.
     */
    @Test
    public void testDroppedAndReAddedColumnInPlace() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b int, v int, PRIMARY KEY(k, c)) WITH memtable = 'trie'");

        int rebuilt = applyAfterSchemaChange(builder -> {
                                                 builder.timestamp(1000).row(1).add("b", 1).add("v", 2);
                                                 builder.timestamp(3000).row(2).add("b", 3).add("v", 4);
                                             },
                                             "ALTER TABLE %s DROP v USING TIMESTAMP 2000",
                                             "ALTER TABLE %s ADD v int");

        for (int key = 0; key < 3; ++key)
            assertNotNull(cellInMemtable(key, 1, "v"));
        assertRowsOfEachPartition("SELECT c, b, v FROM %s WHERE k = ?",
                                  row(1, 1, null),
                                  row(2, 3, 4));
        assertEquals(0, rebuilt);
    }

    /**
     * A schema change that keeps the columns gives the table's metadata new column lists, equal to the old ones, and
     * updates made before the change are merged as they are into the memtable created after it. (Every schema change
     * switches the memtable, as the new metadata has a new comparator; see AbstractAllocatorMemtable.shouldSwitch.)
     */
    @Test
    public void testUpdateBeforeSchemaChangeKeepingColumnsIsNotRebuilt() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b int, m map<text, text>, s int static, PRIMARY KEY(k, c)) WITH memtable = 'trie'");
        RegularAndStaticColumns before = currentTableMetadata().regularAndStaticColumns();

        int rebuilt = applyAfterSchemaChange(builder -> {
                                                 builder.row().add("s", 1);
                                                 builder.row(1).add("b", 2).add("m", ImmutableMap.of("x", "3"));
                                             },
                                             "ALTER TABLE %s WITH comment = 'keeps the columns'");

        assertNotSame(before, currentTableMetadata().regularAndStaticColumns());
        assertRowsOfEachPartition("SELECT c, s, b, m FROM %s WHERE k = ?", row(1, 1, 2, map("x", "3")));
        assertEquals(0, rebuilt);
    }

    /**
     * Updates made with metadata from before a schema change that kept the columns, as a statement prepared before the
     * change goes on making, have column lists equal to the memtable's, other instances. The memtable compares the
     * first update's column by column, and the others' by identity with the first's.
     */
    @Test
    public void testEqualColumnListsAreComparedColumnByColumnOnce() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b int, PRIMARY KEY(k, c)) WITH memtable = 'trie'");
        TableMetadata before = currentTableMetadata();
        alterTable("ALTER TABLE %s WITH comment = 'keeps the columns'");
        assertMemtableHasCurrentColumns();
        assertNotSame(before.regularAndStaticColumns(), currentTableMetadata().regularAndStaticColumns());

        comparisonsOf = before.regularAndStaticColumns();
        columnComparisons.set(0);
        for (int key = 0; key < 10; ++key)
        {
            int value = key;
            new Mutation(build(before, key, builder -> builder.row(1).add("b", value))).apply();
        }
        assertEquals(1, columnComparisons.get());
        assertRows(execute("SELECT count(*) FROM %s"), row(10L));
    }

    /**
     * The memtable lists the columns of the rebuilt update, which are those of the memtable's metadata, and not those
     * of the update made before the schema change: not a column dropped since, which the rebuild leaves out, and not
     * the definition of a column dropped and added again, which is a new one.
     */
    @Test
    public void testMemtableListsColumnsOfRebuiltUpdate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b int, v int, w int, PRIMARY KEY(k, c)) WITH memtable = 'trie'");

        assertEquals(3, applyAfterSchemaChange(builder -> builder.row(1).add("b", 1).add("v", 2).add("w", 3),
                                               "ALTER TABLE %s DROP v",
                                               "ALTER TABLE %s DROP w USING TIMESTAMP 1000",
                                               "ALTER TABLE %s ADD w int"));

        Memtable memtable = getCurrentColumnFamilyStore().getCurrentMemtable();
        for (int key = 0; key < 3; ++key)
        {
            DecoratedKey partitionKey = memtable.metadata().partitioner.decorateKey(Int32Type.instance.decompose(key));
            RegularAndStaticColumns columns = memtable.getPartition(partitionKey).columns();
            assertEquals(2, columns.size());
            for (ColumnMetadata column : columns)
                assertSame(memtable.metadata().getColumn(column.name), column);
        }
        assertRowsOfEachPartition("SELECT c, b, w FROM %s WHERE k = ?", row(1, 1, 3));
    }

    /**
     * An update made with metadata that has a column the table's does not, which no memtable of the table can take.
     * The write fails, and is not applied once more. Once the schema has the column, the same update applies.
     */
    @Test
    public void testUpdateNewerThanMemtableFails() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b int, PRIMARY KEY(k, c)) WITH memtable = 'trie'");
        TableMetadata newer = currentTableMetadata().unbuild().addRegularColumn("a", Int32Type.instance).build();
        PartitionUpdate update = build(newer, 0, builder -> builder.row(1).add("a", 1).add("b", 2));

        assertFailsWithoutSwitchingMemtable(update);

        alterTable("ALTER TABLE %s ADD a int");
        new Mutation(update).apply();
        beforeAndAfterFlush(() -> assertRows(execute("SELECT c, a, b FROM %s WHERE k = 0"),
                                             row(0, null, 0),
                                             row(1, 1, 2)));
    }

    /**
     * As {@link #testUpdateNewerThanMemtableFails}, with a column the memtable's metadata lists as dropped, and the
     * update's metadata has again after that drop.
     */
    @Test
    public void testUpdateWithReAddedColumnNewerThanMemtableFails() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b int, v int, PRIMARY KEY(k, c)) WITH memtable = 'trie'");
        alterTable("ALTER TABLE %s DROP v USING TIMESTAMP 1000");
        TableMetadata newer = currentTableMetadata().unbuild().addRegularColumn("v", Int32Type.instance).build();
        PartitionUpdate update = build(newer, 0, builder -> builder.row(1).add("b", 1).add("v", 2));

        assertFailsWithoutSwitchingMemtable(update);

        alterTable("ALTER TABLE %s ADD v int");
        new Mutation(update).apply();
        beforeAndAfterFlush(() -> assertRows(execute("SELECT c, b, v FROM %s WHERE k = 0"),
                                             row(0, 0, null),
                                             row(1, 1, 2)));
    }

    /**
     * As {@link #testUpdateNewerThanMemtableFails}, with a regular column of a name the memtable's metadata gives to a
     * clustering column, which was renamed to it after a regular column of that name was dropped. The update's column
     * is not the memtable's column of that name, and was added again after that drop.
     */
    @Test
    public void testUpdateWithColumnNamedAsClusteringColumnOfMemtableFails() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ck int, b int, c int, PRIMARY KEY(k, ck)) WITH memtable = 'trie'");
        // Older than the update, which would otherwise hold no data of the column added again.
        alterTable("ALTER TABLE %s DROP c USING TIMESTAMP 1000");
        alterTable("ALTER TABLE %s RENAME ck TO c");
        TableMetadata newer = currentTableMetadata().unbuild()
                                                    .renamePrimaryKeyColumn(ColumnIdentifier.getInterned("c", false),
                                                                            ColumnIdentifier.getInterned("ck", false))
                                                    .addRegularColumn("c", Int32Type.instance)
                                                    .build();
        PartitionUpdate update = build(newer, 0, builder -> builder.row(1).add("b", 1).add("c", 2));

        assertFailsWithoutSwitchingMemtable(update);
    }

    /**
     * An update made with metadata that has a column the memtable's does not, and that lists the column without holding
     * any data of it, as a read repair lists the columns its read fetched. It is rebuilt without the column rather than
     * failed.
     */
    @Test
    public void testUpdateListingNewerColumnWithoutDataIsRebuilt() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b int, s int static, PRIMARY KEY(k, c)) WITH memtable = 'trie'");
        TableMetadata newer = currentTableMetadata().unbuild()
                                                    .addRegularColumn("a", Int32Type.instance)
                                                    .addStaticColumn("sa", Int32Type.instance)
                                                    .build();
        PartitionUpdate built = build(newer, 0, builder -> {
            builder.row().add("s", 1);
            builder.row(1).add("b", 2);
        });
        PartitionUpdate.Builder listingAll = TriePartitionUpdate.FACTORY.builder(newer, built.partitionKey(), newer.regularAndStaticColumns(), 1);
        listingAll.add(built.staticRow());
        listingAll.add(built.getRow(newer.comparator.make(1)));
        PartitionUpdate update = listingAll.build();
        assertEquals(newer.regularAndStaticColumns(), update.columns());

        failedWrites.set(0);
        rebuildsOf = newer.id;
        rebuilds.set(0);
        new Mutation(update).apply();
        assertEquals(0, failedWrites.get());
        assertEquals(1, rebuilds.get());
        beforeAndAfterFlush(() -> assertRows(execute("SELECT c, s, b FROM %s WHERE k = 0"), row(1, 1, 2)));
    }

    /**
     * As {@link #testUpdateNewerThanMemtableFails}, with an update whose only data of the newer column is the deletion
     * of the collection.
     */
    @Test
    public void testUpdateDeletingNewerCollectionFails() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b int, PRIMARY KEY(k, c)) WITH memtable = 'trie'");
        TableMetadata newer = currentTableMetadata().unbuild().addRegularColumn("m", MapType.getInstance(Int32Type.instance, Int32Type.instance, true)).build();
        PartitionUpdate update = build(newer, 0, builder -> builder.row(1).noPrimaryKeyLivenessInfo().delete("m"));

        assertFailsWithoutSwitchingMemtable(update);
    }

    /**
     * The write fails before the memtable takes any of it, and is not applied once more, as the memtable is not
     * switched: the next one, created with the table's metadata, could not take the write either.
     */
    private void assertFailsWithoutSwitchingMemtable(PartitionUpdate update) throws Throwable
    {
        execute("INSERT INTO %s (k, c, b) VALUES (0, 0, 0)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Memtable memtable = cfs.getCurrentMemtable();
        long operations = memtable.operationCount();
        long liveDataSize = memtable.getLiveDataSize();

        failedWrites.set(0);
        assertNewerThanMemtable(assertThrows(RuntimeException.class, () -> new Mutation(update).apply()));
        assertEquals(1, failedWrites.get());
        assertSame(memtable, cfs.getCurrentMemtable());
        assertEquals(operations, memtable.operationCount());
        assertEquals(liveDataSize, memtable.getLiveDataSize());
    }

    /**
     * A write naming a column a schema change adds, made through the coordinator after the change has published the
     * table's new metadata and before it has switched the memtable, as a statement prepared with the new metadata in
     * that window is. The memtable fails the write and switches, and the write is applied once more, to the new
     * memtable, and succeeds.
     */
    @Test
    public void testWriteBetweenAddingColumnAndSwitchingMemtable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b int, PRIMARY KEY(k, c)) WITH memtable = 'trie'");

        WriteInWindow write = writeBetweenSchemaChangeAndMemtableSwitch("ALTER TABLE %s ADD a int",
                                                                        "INSERT INTO %s (k, c, a, b) VALUES (0, 1, 2, 3)");

        write.assertAppliedAgainToSwitchedMemtable();
        beforeAndAfterFlush(() -> assertRows(execute("SELECT c, a, b FROM %s WHERE k = 0"), row(1, 2, 3)));
    }

    /** As {@link #testWriteBetweenAddingColumnAndSwitchingMemtable}, with a column dropped and added again. */
    @Test
    public void testWriteBetweenReAddingColumnAndSwitchingMemtable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b int, v int, PRIMARY KEY(k, c)) WITH memtable = 'trie'");
        alterTable("ALTER TABLE %s DROP v");

        WriteInWindow write = writeBetweenSchemaChangeAndMemtableSwitch("ALTER TABLE %s ADD v int",
                                                                        "INSERT INTO %s (k, c, b, v) VALUES (0, 1, 2, 3)");

        write.assertAppliedAgainToSwitchedMemtable();
        beforeAndAfterFlush(() -> assertRows(execute("SELECT c, b, v FROM %s WHERE k = 0"), row(1, 2, 3)));
    }

    /**
     * A write made in the same window after a schema change dropping a column has no column the memtable cannot key,
     * and the memtable takes it, rebuilt with its numbering. It still asks for the switch, as the memtable is older
     * than the table's metadata, which a concurrent switch that created it with the old metadata can leave it
     * (ColumnFamilyStore.switchMemtableIfCurrent).
     */
    @Test
    public void testWriteBetweenDroppingColumnAndSwitchingMemtable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b int, v int, PRIMARY KEY(k, c)) WITH memtable = 'trie'");

        WriteInWindow write = writeBetweenSchemaChangeAndMemtableSwitch("ALTER TABLE %s DROP v",
                                                                        "INSERT INTO %s (k, c, b) VALUES (0, 1, 2)");

        assertNull(write.failure);
        assertEquals(0, write.failedWrites);
        assertEquals(1, write.rebuilds);
        assertNotSame(write.memtableBefore, write.memtableAfter);
        assertEquals(1, write.operationsBefore);
        assertEquals(0, write.operationsAfter);
        assertSame(write.memtableAfter, getCurrentColumnFamilyStore().getCurrentMemtable());
        beforeAndAfterFlush(() -> assertRows(execute("SELECT c, b FROM %s WHERE k = 0"), row(1, 2)));
    }

    /**
     * Two writes as in {@link #testWriteBetweenAddingColumnAndSwitchingMemtable}, made at once. The first fails and
     * asks for the switch, which is held after it has taken the table's lock and before the new memtable takes writes.
     * The second fails on the old memtable as well, after the switch has been asked for, and waits for the switch before
     * it is applied once more, so that it lands in the new memtable rather than in the old one again.
     */
    @Test
    @BMRules(rules = { @BMRule(name = "Count the requests to switch a table's memtable",
                               targetClass = "ColumnFamilyStore",
                               targetMethod = "switchMemtableIfCurrent",
                               action = "org.apache.cassandra.db.memtable.TrieMemtableSchemaChangeTest.requestingSwitch($0)"),
                       @BMRule(name = "Hold a switch of a table's memtable after it has taken the table's lock",
                               targetClass = "ColumnFamilyStore",
                               targetMethod = "switchMemtable",
                               action = "org.apache.cassandra.db.memtable.TrieMemtableSchemaChangeTest.switchingMemtable($0)") })
    public void testConcurrentWritesBetweenAddingColumnAndSwitchingMemtable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b int, PRIMARY KEY(k, c)) WITH memtable = 'trie'");
        String table = currentTable();
        HeldSwitch hold = new HeldSwitch(table);
        ConcurrentWritesInWindow made = new ConcurrentWritesInWindow();
        beforeMemtableSwitch = cfs -> {
            if (!cfs.name.equals(table))
                return;
            beforeMemtableSwitch = null;
            made.memtableBefore = cfs.getCurrentMemtable();
            failedWrites.set(0);
            heldSwitch = hold;
            try
            {
                FutureTask<Throwable> first = applyInThread(build(cfs.metadata(), 0, builder -> builder.row(1).add("a", 1).add("b", 2)),
                                                            () -> {});
                assertTrue("The first write did not ask for the switch", hold.started.await(1, TimeUnit.MINUTES));
                // Let the switch go once the second write has asked for it as well, or has given up.
                FutureTask<Throwable> second = applyInThread(build(cfs.metadata(), 0, builder -> builder.row(2).add("a", 3).add("b", 4)),
                                                             hold.requestedAgain::countDown);
                assertTrue("The second write did not end", hold.requestedAgain.await(1, TimeUnit.MINUTES));
                hold.released.countDown();
                made.firstFailure = first.get(1, TimeUnit.MINUTES);
                made.secondFailure = second.get(1, TimeUnit.MINUTES);
                made.failedWrites = failedWrites.get();
                made.memtableAfter = cfs.getCurrentMemtable();
            }
            catch (Throwable t)
            {
                made.error = t;
            }
            finally
            {
                heldSwitch = null;
                hold.released.countDown();
            }
        };
        try
        {
            alterTable("ALTER TABLE %s ADD a int");
        }
        finally
        {
            beforeMemtableSwitch = null;
        }
        if (made.error != null)
            throw made.error;

        assertNull(String.valueOf(made.firstFailure), made.firstFailure);
        assertNull(String.valueOf(made.secondFailure), made.secondFailure);
        assertEquals(2, made.failedWrites);
        assertNotSame(made.memtableBefore, made.memtableAfter);
        assertEquals(0, made.memtableBefore.operationCount());
        assertEquals(2, made.memtableAfter.operationCount());
        beforeAndAfterFlush(() -> assertRows(execute("SELECT c, a, b FROM %s WHERE k = 0"),
                                             row(1, 1, 2),
                                             row(2, 3, 4)));
    }

    /** What the writes of {@link #testConcurrentWritesBetweenAddingColumnAndSwitchingMemtable} did. */
    private static class ConcurrentWritesInWindow
    {
        Throwable error;
        Throwable firstFailure;
        Throwable secondFailure;
        int failedWrites;
        Memtable memtableBefore;
        Memtable memtableAfter;
    }

    /** Applies the update in a thread of its own, which runs {@code then} at its end. */
    private static FutureTask<Throwable> applyInThread(PartitionUpdate update, Runnable then)
    {
        FutureTask<Throwable> write = new FutureTask<>(() -> {
            try
            {
                return failureOf(() -> new Mutation(update).apply());
            }
            finally
            {
                then.run();
            }
        });
        new Thread(write).start();
        return write;
    }

    /** What a write made between a schema change publishing the table's metadata and switching its memtable did. */
    private static class WriteInWindow
    {
        Throwable failure;
        int failedWrites;
        int rebuilds;
        Memtable memtableBefore;
        Memtable memtableAfter;
        long operationsBefore;
        long operationsAfter;

        /**
         * The memtable failed the write and switched, and the write was applied once more, to the new memtable, which
         * was created with the new metadata, and which the schema change keeps.
         */
        void assertAppliedAgainToSwitchedMemtable()
        {
            assertNull(failure);
            assertEquals(1, failedWrites);
            assertNotSame(memtableBefore, memtableAfter);
            assertEquals(0, operationsBefore);
            assertEquals(1, operationsAfter);
            assertSame(memtableAfter, Keyspace.open(memtableAfter.metadata().keyspace).getColumnFamilyStore(memtableAfter.metadata().id).getCurrentMemtable());
        }
    }

    /**
     * Make the write through the coordinator when the schema change reaches the table, before the table switches its
     * memtable.
     */
    private WriteInWindow writeBetweenSchemaChangeAndMemtableSwitch(String schemaChange, String write) throws Throwable
    {
        String table = currentTable();
        String query = formatQuery(write);
        WriteInWindow made = new WriteInWindow();
        beforeMemtableSwitch = cfs -> {
            if (!cfs.name.equals(table))
                return;
            beforeMemtableSwitch = null;
            made.memtableBefore = cfs.getCurrentMemtable();
            failedWrites.set(0);
            rebuildsOf = cfs.metadata.id;
            rebuilds.set(0);
            made.failure = failureOf(() -> QueryProcessor.process(query, ConsistencyLevel.ONE));
            made.failedWrites = failedWrites.get();
            made.rebuilds = rebuilds.get();
            made.memtableAfter = cfs.getCurrentMemtable();
            made.operationsBefore = made.memtableBefore.operationCount();
            made.operationsAfter = made.memtableAfter.operationCount();
        };
        try
        {
            alterTable(schemaChange);
        }
        finally
        {
            beforeMemtableSwitch = null;
        }
        return made;
    }

    private static void assertNewerThanMemtable(Throwable failure)
    {
        assertNotNull("The write did not fail", failure);
        assertTrue(String.valueOf(failure), failure instanceof ColumnNewerThanMemtableException);
        assertTrue(failure.getMessage(), failure.getMessage().contains("newer than this memtable"));
    }

    private static Throwable failureOf(Runnable action)
    {
        try
        {
            action.run();
            return null;
        }
        catch (Throwable t)
        {
            return t;
        }
    }

    /**
     * Make the content under the current schema three times: as a trie update of partition 0; as a BTree update of
     * partition 1, which the memtable converts to a trie update keyed by the update's schema; and as a trie update of
     * partition 2 written and read back, which the reader reads in place, as the schema it reads under numbers the
     * columns as the writer did, unless the table is a counter table, whose updates the reader copies to memory for the
     * counter leader to rewrite. Then make the schema changes, apply the three updates, and return how many of them
     * the memtable rebuilt with its column numbering.
     */
    private int applyAfterSchemaChange(Consumer<PartitionUpdate.SimpleBuilder> content, String... schemaChanges) throws Throwable
    {
        return applyAfterSchemaChange(content, update -> new Mutation(update).apply(), schemaChanges);
    }

    /** As above, applying each update with {@code apply}. */
    private int applyAfterSchemaChange(Consumer<PartitionUpdate.SimpleBuilder> content,
                                       Consumer<PartitionUpdate> apply,
                                       String... schemaChanges) throws Throwable
    {
        TableMetadata before = currentTableMetadata();
        PartitionUpdate trieUpdate = TriePartitionUpdate.asTrieUpdate(build(before, 0, content));
        PartitionUpdate btreeUpdate = BTreePartitionUpdate.asBTreeUpdate(build(before, 1, content));
        TriePartitionUpdate readUpdate = writeAndRead(TriePartitionUpdate.asTrieUpdate(build(before, 2, content)), before);
        assertTrue(before.isCounter() || TriePartitionUpdate.asMergableTrieWithoutPartitionPrefix(readUpdate) instanceof OnDiskDeletionAwareTrie);

        for (String schemaChange : schemaChanges)
            alterTable(schemaChange);
        assertMemtableHasCurrentColumns();

        rebuildsOf = before.id;
        rebuilds.set(0);
        apply.accept(trieUpdate);
        apply.accept(btreeUpdate);
        apply.accept(readUpdate);
        return rebuilds.get();
    }

    private static TriePartitionUpdate writeAndRead(TriePartitionUpdate update, TableMetadata metadata) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            TriePartitionUpdateSerializer.serialize(update, out, MessagingService.VERSION_DS_21);
            try (DataInputBuffer in = new DataInputBuffer(out.toByteArray()))
            {
                return TriePartitionUpdateSerializer.deserialize(in, MessagingService.VERSION_DS_21, DeserializationHelper.Flag.FROM_REMOTE, metadata);
            }
        }
    }

    private static PartitionUpdate build(TableMetadata metadata, int key, Consumer<PartitionUpdate.SimpleBuilder> content)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, key);
        builder.timestamp(2000).nowInSec(1500);
        content.accept(builder);
        return builder.build();
    }

    /** The memtable the updates go to was created with the current schema. */
    private void assertMemtableHasCurrentColumns()
    {
        Memtable memtable = getCurrentColumnFamilyStore().getTracker().getView().getCurrentMemtable();
        assertTrue(memtable instanceof TrieMemtable);
        assertEquals(currentTableMetadata().regularAndStaticColumns(), memtable.metadata().regularAndStaticColumns());
    }

    /** The cell of the row as the memtable holds it, which reads and flushes filter by the table's dropped columns. */
    private Cell<?> cellInMemtable(int key, int clustering, String column)
    {
        Memtable memtable = getCurrentColumnFamilyStore().getCurrentMemtable();
        TableMetadata metadata = memtable.metadata();
        DecoratedKey partitionKey = metadata.partitioner.decorateKey(Int32Type.instance.decompose(key));
        Row row = memtable.getPartition(partitionKey).getRow(metadata.comparator.make(clustering));
        return row.getCell(metadata.getColumn(ColumnIdentifier.getInterned(column, false)));
    }

    private void assertRowsOfEachPartition(String query, Object[]... rows) throws Throwable
    {
        beforeAndAfterFlush(() -> {
            for (int key = 0; key < 3; ++key)
                assertRows(execute(query, key), rows);
        });
    }
}
