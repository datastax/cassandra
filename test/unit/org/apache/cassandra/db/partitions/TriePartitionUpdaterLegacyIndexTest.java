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

package org.apache.cassandra.db.partitions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.memtable.SkipListMemtable;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.index.StubIndex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/// Tests the index notifications issued by [TriePartitionUpdaterLegacyIndex] for deletions that overlap existing
/// deletions in the trie memtable.
///
/// Each scenario applies two deletion steps to its own partition, with rows written before the first and between the
/// two steps, once with the second step newer than the first and once with it older. The rows overwrite a collection,
/// which also places column deletions inside the deleted ranges. The indexed table uses the trie memtable; its index
/// queries must return the same rows as a filtering query on an unindexed skip-list table that received the same
/// writes, both before and after a flush. The range tombstones reported to an index on a trie table must also be the
/// ones reported on a skip-list table.
public class TriePartitionUpdaterLegacyIndexTest extends CQLTester
{
    /// Table definitions, to be completed with the memtable option.
    private static final String TABLE = "CREATE TABLE %s (pk bigint, ck bigint, s bigint static, v bigint, c set<bigint>, PRIMARY KEY (pk, ck)) WITH ";
    private static final String REVERSED_TABLE = TABLE + "CLUSTERING ORDER BY (ck DESC) AND ";
    /// Rows of this table are written with `ck2` 0 and 1 for each `ck`.
    private static final String MULTI_COLUMN_TABLE = "CREATE TABLE %s (pk bigint, ck bigint, ck2 bigint, s bigint static, v bigint, c set<bigint>, PRIMARY KEY (pk, ck, ck2)) WITH ";

    /// Pairs of deletion steps. A step is a list of clustering restrictions, applied as a single update (a batch if
    /// there is more than one); an empty restriction deletes the whole partition. Entries made with [#deleteAt] and
    /// [#updateAt] are complete statements with their own timestamp, which do not change with the order of the steps.
    private static final List<List<List<String>>> SCENARIOS = ImmutableList.of(
        // the second range starts inside the first
        steps(ImmutableList.of("ck > 10 AND ck <= 30"), ImmutableList.of("ck >= 20 AND ck <= 40")),
        // the second range ends inside the first
        steps(ImmutableList.of("ck >= 20 AND ck <= 40"), ImmutableList.of("ck > 10 AND ck <= 30")),
        // the second range contains the first
        steps(ImmutableList.of("ck >= 20 AND ck <= 30"), ImmutableList.of("ck > 10 AND ck <= 40")),
        // the second range is contained in the first
        steps(ImmutableList.of("ck > 10 AND ck <= 40"), ImmutableList.of("ck >= 20 AND ck < 30")),
        // identical bounds
        steps(ImmutableList.of("ck > 10 AND ck <= 30"), ImmutableList.of("ck > 10 AND ck <= 30")),
        // adjacent ranges
        steps(ImmutableList.of("ck > 10 AND ck <= 30"), ImmutableList.of("ck > 30 AND ck <= 40")),
        steps(ImmutableList.of("ck > 30 AND ck <= 40"), ImmutableList.of("ck > 10 AND ck <= 30")),
        // ranges sharing a single clustering
        steps(ImmutableList.of("ck > 10 AND ck <= 30"), ImmutableList.of("ck >= 30 AND ck <= 40")),
        // ranges open at the partition end
        steps(ImmutableList.of("ck > 10"), ImmutableList.of("ck >= 20 AND ck <= 40")),
        steps(ImmutableList.of("ck >= 20 AND ck <= 40"), ImmutableList.of("ck < 30")),
        steps(ImmutableList.of("ck > 20"), ImmutableList.of("ck <= 30")),
        steps(ImmutableList.of("ck <= 30"), ImmutableList.of("ck > 20")),
        steps(ImmutableList.of("ck > 20"), ImmutableList.of("ck > 10")),
        steps(ImmutableList.of("ck <= 30"), ImmutableList.of("ck <= 20")),
        // ranges with the same timestamp, in one of the two orders
        steps(ImmutableList.of("ck > 10 AND ck <= 30"), ImmutableList.of(deleteAt(100, "ck >= 20 AND ck <= 40"))),
        steps(ImmutableList.of(deleteAt(100, "ck >= 20 AND ck <= 40")), ImmutableList.of("ck > 10 AND ck <= 30")),
        steps(ImmutableList.of(deleteAt(100, "ck > 10 AND ck <= 30")), ImmutableList.of("ck > 10 AND ck <= 30")),
        // several ranges in one update overlapping an existing range
        steps(ImmutableList.of("ck > 10 AND ck <= 40"), ImmutableList.of("ck >= 5 AND ck <= 15", "ck >= 25 AND ck < 30", "ck >= 35 AND ck <= 45")),
        steps(ImmutableList.of("ck >= 5 AND ck <= 15", "ck >= 35 AND ck <= 45"), ImmutableList.of("ck > 10 AND ck <= 40")),
        // touching ranges with different timestamps in one update, i.e. a boundary that closes one and opens the other
        steps(ImmutableList.of("ck > 15 AND ck <= 25"), ImmutableList.of(deleteAt(140, "ck >= 10 AND ck <= 20"), deleteAt(160, "ck > 20 AND ck <= 30"))),
        steps(ImmutableList.of("ck > 15 AND ck <= 25"), ImmutableList.of(deleteAt(160, "ck >= 10 AND ck < 20"), deleteAt(140, "ck >= 20 AND ck <= 30"))),
        // range and partition deletions
        steps(ImmutableList.of("ck > 10 AND ck <= 30"), ImmutableList.of("")),
        steps(ImmutableList.of(""), ImmutableList.of("ck > 10 AND ck <= 30")),
        steps(ImmutableList.of("ck > 10 AND ck <= 30"), ImmutableList.of("", deleteAt(250, "ck >= 5 AND ck <= 15"), deleteAt(260, "ck >= 25 AND ck < 35"))),
        // row deletions inside a deleted range
        steps(ImmutableList.of("ck > 10 AND ck <= 30"), ImmutableList.of("ck = 20")),
        steps(ImmutableList.of("ck > 10 AND ck <= 30"), ImmutableList.of("ck = 30")),
        steps(ImmutableList.of("ck = 20"), ImmutableList.of("ck > 10 AND ck <= 30")),
        // a range in one update with a collection overwrite inside it, i.e. a column deletion inside the range
        steps(ImmutableList.of("ck >= 15 AND ck <= 40"), ImmutableList.of("ck > 10 AND ck <= 30", updateAt(160, "c = {7}", "ck = 20"))),
        steps(ImmutableList.of("ck >= 15 AND ck <= 40"), ImmutableList.of("ck > 10 AND ck <= 30", updateAt(250, "c = {7}", "ck = 20"))),
        steps(ImmutableList.of("ck >= 15 AND ck <= 40"), ImmutableList.of("ck > 10 AND ck <= 30", deleteAt(250, "ck = 20"), updateAt(300, "c = {7}", "ck = 20"))),
        // a row deletion and a collection overwrite of the same row in one update, with the overwrite older or newer
        // than the row deletion, and the existing deletion older than both, newer than both or between them
        steps(ImmutableList.of(), ImmutableList.of("ck = 20", updateAt(160, "c = {7}", "ck = 20"))),
        steps(ImmutableList.of(""), ImmutableList.of("ck = 20", updateAt(160, "c = {7}", "ck = 20"))),
        steps(ImmutableList.of(""), ImmutableList.of("ck = 20", updateAt(250, "c = {7}", "ck = 20"))),
        steps(ImmutableList.of("ck > 10 AND ck <= 30"), ImmutableList.of("ck = 20", updateAt(160, "c = {7}", "ck = 20"))),
        steps(ImmutableList.of("ck > 10 AND ck <= 30"), ImmutableList.of("ck = 20", updateAt(250, "c = {7}", "ck = 20")))
    );

    /// Scenarios for [#MULTI_COLUMN_TABLE], with deletions bounded by clustering prefixes.
    private static final List<List<List<String>>> MULTI_COLUMN_SCENARIOS = ImmutableList.of(
        steps(ImmutableList.of("ck > 10 AND ck <= 30"), ImmutableList.of("ck = 20")),
        steps(ImmutableList.of("ck = 20"), ImmutableList.of("ck >= 20 AND ck < 30")),
        steps(ImmutableList.of("ck >= 20 AND ck < 30"), ImmutableList.of("ck = 20 AND ck2 > 0")),
        steps(ImmutableList.of("ck = 20 AND ck2 >= 1"), ImmutableList.of("ck >= 10 AND ck <= 20")),
        steps(ImmutableList.of("(ck, ck2) > (10, 0) AND (ck, ck2) <= (30, 0)"), ImmutableList.of("ck >= 20 AND ck < 40")),
        steps(ImmutableList.of("ck > 10 AND ck <= 30"), ImmutableList.of("(ck, ck2) >= (20, 1) AND (ck, ck2) < (40, 1)")),
        steps(ImmutableList.of("ck > 10 AND ck <= 30"), ImmutableList.of("ck = 20 AND ck2 = 1")),
        steps(ImmutableList.of("ck >= 15 AND ck <= 40"), ImmutableList.of("ck = 20", updateAt(250, "c = {7}", "ck = 20 AND ck2 = 1"))),
        steps(ImmutableList.of("ck = 20"), ImmutableList.of("ck = 20 AND ck2 = 1", updateAt(250, "c = {7}", "ck = 20 AND ck2 = 1")))
    );

    private static final long BEFORE_TIMESTAMP = 10;
    private static final long BETWEEN_TIMESTAMP = 150;
    private static final long[][] DELETION_TIMESTAMPS = { { 100, 200 }, { 200, 100 } };

    /// The statements from the original report, which failed an assertion on the second deletion.
    @Test
    public void testRangeDeletionStartingInsideExistingRange()
    {
        createTable(TABLE + "memtable = 'trie'");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        assertTrue(getCurrentColumnFamilyStore().getCurrentMemtable() instanceof TrieMemtable);

        execute("DELETE FROM %s USING TIMESTAMP 1 WHERE pk = 1 AND ck > 10 AND ck <= 30");
        execute("DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck >= 20 AND ck <= 40");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 35, 35) USING TIMESTAMP 3");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 25, 25) USING TIMESTAMP 1");
        assertRows(execute("SELECT pk, ck, v FROM %s WHERE v >= 0"), row(1L, 35L, 35L));
    }

    /// A row deletion and a newer collection overwrite of the same row in one update, under a newer partition
    /// deletion, which failed an assertion.
    @Test
    public void testRowAndComplexDeletionUnderNewerPartitionDeletion()
    {
        createTable(TABLE + "memtable = 'trie'");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        assertTrue(getCurrentColumnFamilyStore().getCurrentMemtable() instanceof TrieMemtable);

        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 1, {1}) USING TIMESTAMP 1");
        execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 1");
        execute("BEGIN UNLOGGED BATCH " +
                "DELETE FROM %1$s USING TIMESTAMP 5 WHERE pk = 1 AND ck = 1; " +
                "UPDATE %1$s USING TIMESTAMP 10 SET c = {2}, v = 2 WHERE pk = 1 AND ck = 1; " +
                "APPLY BATCH");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 3) USING TIMESTAMP 30");
        assertRows(execute("SELECT pk, ck, v FROM %s WHERE v >= 0"), row(1L, 2L, 3L));
    }

    @Test
    public void testOverlappingDeletionsWithSAI()
    {
        testOverlappingDeletions(TABLE, SCENARIOS, "CREATE INDEX ON %s(v) USING 'sai'", "v = ?");
    }

    @Test
    public void testOverlappingDeletionsWithSAIOnCollection()
    {
        testOverlappingDeletions(TABLE, SCENARIOS, "CREATE INDEX ON %s(c) USING 'sai'", "c CONTAINS ?");
    }

    @Test
    public void testOverlappingDeletionsWithSAIReversed()
    {
        testOverlappingDeletions(REVERSED_TABLE, SCENARIOS, "CREATE INDEX ON %s(v) USING 'sai'", "v = ?");
    }

    @Test
    public void testOverlappingDeletionsWithSAIMultiColumn()
    {
        testOverlappingDeletions(MULTI_COLUMN_TABLE, MULTI_COLUMN_SCENARIOS, "CREATE INDEX ON %s(v) USING 'sai'", "v = ?");
    }

    /// Legacy indexes on regular columns only take part in updates that write the indexed column, i.e. here in the row
    /// writes that land inside deleted ranges, but not in the deletions themselves.
    @Test
    public void testOverlappingDeletionsWithLegacyIndexOnRegularColumn()
    {
        testOverlappingDeletions(TABLE, SCENARIOS, "CREATE INDEX ON %s(v)", "v = ?");
    }

    @Test
    public void testOverlappingDeletionsWithLegacyIndexOnCollection()
    {
        testOverlappingDeletions(TABLE, SCENARIOS, "CREATE INDEX ON %s(c)", "c CONTAINS ?");
    }

    /// Legacy indexes on primary key columns take part in all updates, including deletions.
    @Test
    public void testOverlappingDeletionsWithLegacyIndexOnClusteringColumn()
    {
        testOverlappingDeletions(TABLE, SCENARIOS, "CREATE INDEX ON %s(ck)", "ck = ?");
    }

    private void testOverlappingDeletions(String table, List<List<List<String>>> scenarios, String indexDefinition, String indexedPredicate)
    {
        String reference = createTable(table + "memtable = 'skiplist'");
        String indexed = createTable(table + "memtable = 'trie'");
        createIndex(indexDefinition);
        assertTrue(getColumnFamilyStore(KEYSPACE, indexed).getCurrentMemtable() instanceof TrieMemtable);

        applyScenarios(scenarios, reference, indexed);

        assertIndexedReadsMatch(scenarios, reference, indexed, indexedPredicate);
        flush(KEYSPACE, indexed);
        assertIndexedReadsMatch(scenarios, reference, indexed, indexedPredicate);
    }

    /// The trie memtable must report the range tombstones of the incoming update, like the legacy memtables do,
    /// regardless of how they overlap with existing deletions.
    @Test
    public void testReportedRangeTombstonesMatchLegacyMemtable()
    {
        testReportedRangeTombstonesMatchLegacyMemtable(TABLE, SCENARIOS);
    }

    @Test
    public void testReportedRangeTombstonesMatchLegacyMemtableReversed()
    {
        testReportedRangeTombstonesMatchLegacyMemtable(REVERSED_TABLE, SCENARIOS);
    }

    @Test
    public void testReportedRangeTombstonesMatchLegacyMemtableMultiColumn()
    {
        testReportedRangeTombstonesMatchLegacyMemtable(MULTI_COLUMN_TABLE, MULTI_COLUMN_SCENARIOS);
    }

    private void testReportedRangeTombstonesMatchLegacyMemtable(String table, List<List<List<String>>> scenarios)
    {
        String legacy = createTable(table + "memtable = 'skiplist'");
        String legacyIndex = createIndex("CREATE CUSTOM INDEX ON %s(v) USING '" + StubIndex.class.getName() + "'");
        String trie = createTable(table + "memtable = 'trie'");
        String trieIndex = createIndex("CREATE CUSTOM INDEX ON %s(v) USING '" + StubIndex.class.getName() + "'");

        ColumnFamilyStore legacyCfs = getColumnFamilyStore(KEYSPACE, legacy);
        ColumnFamilyStore trieCfs = getColumnFamilyStore(KEYSPACE, trie);
        assertTrue(legacyCfs.getCurrentMemtable() instanceof SkipListMemtable);
        assertTrue(trieCfs.getCurrentMemtable() instanceof TrieMemtable);
        StubIndex legacyStub = (StubIndex) legacyCfs.indexManager.getIndexByName(legacyIndex);
        StubIndex trieStub = (StubIndex) trieCfs.indexManager.getIndexByName(trieIndex);
        legacyStub.reset();
        trieStub.reset();

        applyScenarios(scenarios, legacy, trie);

        List<String> expected = describe(legacyCfs, legacyStub.rangeTombstones);
        assertTrue(expected.size() > scenarios.size());
        // This is not an invariant for every update: ranges with the same timestamp that touch or overlap within one
        // update are reported as one range by the trie memtable and separately by the legacy ones (e.g. [10, 30]@5
        // against [10, 20]@5 and (20, 30]@5). That difference is harmless; the scenarios avoid it.
        assertEquals(expected, describe(trieCfs, trieStub.rangeTombstones));
    }

    private void applyScenarios(List<List<List<String>>> scenarios, String... tables)
    {
        for (int scenario = 0; scenario < scenarios.size(); ++scenario)
        {
            for (int order = 0; order < DELETION_TIMESTAMPS.length; ++order)
            {
                long pk = scenario * DELETION_TIMESTAMPS.length + order;
                List<List<String>> steps = scenarios.get(scenario);
                long[] timestamps = DELETION_TIMESTAMPS[order];
                for (String table : tables)
                {
                    for (long ck = 0; ck <= 50; ck += 5)
                        insert(table, BEFORE_TIMESTAMP, "s, v, c", pk, ck, pk, ck, "{" + ck + '}');

                    applyStep(table, pk, steps.get(0), timestamps[0]);

                    for (long ck = 2; ck <= 50; ck += 5)
                        insert(table, BETWEEN_TIMESTAMP, "v, c", pk, ck, ck, "{" + ck + '}');
                    for (long ck = 10; ck <= 40; ck += 10)
                        insert(table, BETWEEN_TIMESTAMP, "v, c", pk, ck, 1000 + ck, "{" + (1000 + ck) + '}');

                    applyStep(table, pk, steps.get(1), timestamps[1]);
                }
            }
        }
    }

    /// Inserts the given values into the row with the given partition and clustering key, or into the rows with `ck2`
    /// 0 and 1 if the table has a second clustering column.
    private void insert(String table, long timestamp, String columns, long pk, long ck, Object... values)
    {
        String valueList = Arrays.stream(values).map(String::valueOf).collect(Collectors.joining(", "));
        if (getColumnFamilyStore(KEYSPACE, table).metadata().clusteringColumns().size() == 1)
            write(table, "INSERT INTO %s (pk, ck, " + columns + ") VALUES (" + pk + ", " + ck + ", " + valueList + ") USING TIMESTAMP " + timestamp);
        else
            for (long ck2 = 0; ck2 <= 1; ++ck2)
                write(table, "INSERT INTO %s (pk, ck, ck2, " + columns + ") VALUES (" + pk + ", " + ck + ", " + ck2 + ", " + valueList + ") USING TIMESTAMP " + timestamp);
    }

    private void applyStep(String table, long pk, List<String> step, long timestamp)
    {
        if (step.isEmpty())
            return;

        List<String> statements = new ArrayList<>();
        for (String entry : step)
            statements.add(String.format(entry.contains("%1$s") ? entry : deleteAt(timestamp, entry), "%1$s", pk));

        if (statements.size() == 1)
            write(table, statements.get(0));
        else
            write(table, "BEGIN UNLOGGED BATCH " + String.join("; ", statements) + "; APPLY BATCH");
    }

    private void assertIndexedReadsMatch(List<List<List<String>>> scenarios, String reference, String indexed, String indexedPredicate)
    {
        List<Long> values = new ArrayList<>();
        for (long v = 0; v <= 50; ++v)
            values.add(v);
        for (long ck = 10; ck <= 40; ck += 10)
            values.add(1000 + ck);

        for (long value : values)
            assertEquals(indexedPredicate + " with " + value,
                         read(reference, "SELECT * FROM %s WHERE " + indexedPredicate + " ALLOW FILTERING", value),
                         read(indexed, "SELECT * FROM %s WHERE " + indexedPredicate, value));

        for (long pk = 0; pk < scenarios.size() * DELETION_TIMESTAMPS.length; ++pk)
            assertEquals("pk = " + pk,
                         read(reference, "SELECT * FROM %s WHERE pk = ?", pk),
                         read(indexed, "SELECT * FROM %s WHERE pk = ?", pk));
    }

    private void write(String table, String query)
    {
        executeFormattedQuery(String.format(query, KEYSPACE + '.' + table));
    }

    private List<String> read(String table, String query, Object... values)
    {
        UntypedResultSet result = executeFormattedQuery(String.format(query, KEYSPACE + '.' + table), values);
        List<String> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : result)
            rows.add(row.getLong("pk") + ":" + row.getLong("ck") + ":" +
                     (row.has("ck2") ? row.getLong("ck2") + ":" : "") +
                     (row.has("s") ? row.getLong("s") : null) + ":" +
                     (row.has("v") ? row.getLong("v") : null) + ":" +
                     (row.has("c") ? row.getSet("c", LongType.instance) : null));
        rows.sort(null);
        return rows;
    }

    private static List<String> describe(ColumnFamilyStore cfs, List<RangeTombstone> rangeTombstones)
    {
        return rangeTombstones.stream()
                              .map(rt -> rt.deletedSlice().toString(cfs.metadata().comparator) +
                                         '@' + rt.deletionTime().markedForDeleteAt())
                              .collect(Collectors.toList());
    }

    private static List<List<String>> steps(List<String> first, List<String> second)
    {
        return ImmutableList.of(first, second);
    }

    /// A deletion with the given clustering restriction, or of the whole partition if it is empty, with its own
    /// timestamp.
    private static String deleteAt(long timestamp, String restriction)
    {
        return "DELETE FROM %1$s USING TIMESTAMP " + timestamp + " WHERE pk = %2$s" +
               (restriction.isEmpty() ? "" : " AND " + restriction);
    }

    /// An update of the row with the given clustering restriction, with its own timestamp.
    private static String updateAt(long timestamp, String assignments, String restriction)
    {
        return "UPDATE %1$s USING TIMESTAMP " + timestamp + " SET " + assignments + " WHERE pk = %2$s AND " + restriction;
    }
}
