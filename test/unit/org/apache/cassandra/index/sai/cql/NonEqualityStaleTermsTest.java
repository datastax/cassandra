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
package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.TrieMemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The non-equalities on collection columns ({@code NOT CONTAINS}, {@code NOT CONTAINS KEY}) are searched as "all the
 * keys minus the keys with an equal term", source by source. That's only correct if the indexed terms are those of
 * the live rows: the terms of cells that expired (TTL) after being indexed, and those of rows shadowed by a partition
 * deletion or a range tombstone, are not removed from the indexes, so the subtraction would drop rows that satisfy the
 * restriction. A source with expired terms doesn't subtract its keys; a deletion that may shadow the terms of any
 * source prevents the subtraction everywhere. The post-filter decides on the candidates.
 * <p>
 * Run with the memtable implementations, which apply the deletions differently.
 */
@RunWith(Parameterized.class)
public class NonEqualityStaleTermsTest extends SAITester
{
    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameters(name = "memtable={0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[]{ "default" },
                             new Object[]{ "trie" },
                             new Object[]{ "trie_stage2" },
                             new Object[]{ "trie_stage3" },
                             new Object[]{ "skiplist" });
    }

    private IndexContext numsIndex;

    private String table(String schema)
    {
        return createTable(memtable.equals("default") ? schema : schema + " WITH memtable = '" + memtable + '\'');
    }

    private void createTableAndIndexes()
    {
        table("CREATE TABLE %s (id int PRIMARY KEY, v int, nums list<int>, m map<text, int>)");
        numsIndex = getIndexContext(createIndex("CREATE INDEX ON %s(nums) USING 'sai'"));
        createIndex("CREATE INDEX ON %s(keys(m)) USING 'sai'");
    }

    private long now()
    {
        return FBUtilities.nowInSeconds();
    }

    private void assertNotContains(Object... ids)
    {
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE nums NOT CONTAINS 5"), rows(ids));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE m NOT CONTAINS KEY 'k'"), rows(ids));
    }

    private static Object[][] rows(Object... ids)
    {
        Object[][] rows = new Object[ids.length][];
        for (int i = 0; i < ids.length; i++)
            rows[i] = row(ids[i]);
        return rows;
    }

    /**
     * @return for each SSTable of the index, whether it may have expired terms
     */
    private List<Boolean> sstablesWithExpiredTerms()
    {
        List<Boolean> expired = new ArrayList<>();
        for (SSTableIndex index : numsIndex.getView().getIndexes())
            expired.add(IndexContext.mayHaveExpiredTerms(index.getSSTable(), now()));
        return expired;
    }

    private boolean memtablesWithExpiredTerms()
    {
        for (MemtableIndex index : numsIndex.getLiveMemtableIndexes())
            if (index.mayHaveExpiredTerms(now()))
                return true;
        return false;
    }

    /**
     * Cells with a TTL: a memtable subtracts the keys until its first indexed cell expires, an SSTable until its
     * first expiring cell (or tombstone) time; an SSTable flushed before the expiration keeps the terms.
     */
    @Test
    public void testExpiringCells() throws Throwable
    {
        createTableAndIndexes();
        execute("UPDATE %s SET v = 0, nums = nums + [1], m['a'] = 1 WHERE id = 1");

        // not expired yet: the keys are still subtracted, and the results are right
        execute("UPDATE %s USING TTL 1000 SET nums = nums + [5], m['k'] = 1 WHERE id = 2");
        assertThat(memtablesWithExpiredTerms()).isFalse();
        assertThat(numsIndex.hasShadowingDeletions(now())).isFalse();
        assertNotContains(1);

        // expiring soon, in the memtable
        execute("UPDATE %s SET v = 0 WHERE id = 3");
        execute("UPDATE %s USING TTL 1 SET nums = nums + [5], m['k'] = 1 WHERE id = 3");
        assertNotContains(1);
        Thread.sleep(2000);
        assertThat(memtablesWithExpiredTerms()).isTrue();
        assertNotContains(1, 3);

        // in an SSTable flushed before the expiration
        execute("UPDATE %s SET v = 0 WHERE id = 4");
        execute("UPDATE %s USING TTL 2 SET nums = nums + [5], m['k'] = 1 WHERE id = 4");
        flush();
        assertNotContains(1, 3);
        Thread.sleep(3000);
        assertThat(sstablesWithExpiredTerms()).containsExactly(true);
        assertNotContains(1, 3, 4);
        compact();
        assertNotContains(1, 3, 4);
    }

    /**
     * The expired terms of an SSTable only prevent its own subtraction: another SSTable without expiring cells, and
     * one whose expiring cells haven't expired yet, still subtract their keys.
     */
    @Test
    public void testExpiredTermsPerSource() throws Throwable
    {
        createTableAndIndexes();
        disableCompaction();
        execute("UPDATE %s SET v = 0 WHERE id = 1");
        execute("UPDATE %s USING TTL 1 SET nums = nums + [5], m['k'] = 1 WHERE id = 1");
        flush();
        execute("UPDATE %s SET v = 0, nums = nums + [5], m['k'] = 1 WHERE id = 2");
        execute("UPDATE %s SET v = 0, nums = nums + [1], m['a'] = 1 WHERE id = 3");
        flush();
        execute("UPDATE %s USING TTL 1000 SET v = 0, nums = nums + [5], m['k'] = 1 WHERE id = 4");
        flush();
        Thread.sleep(2000);

        // in flush order
        assertThat(numsIndex.hasShadowingDeletions(now())).isFalse();
        List<Boolean> expired = sstablesWithExpiredTerms();
        assertThat(expired).hasSize(3);
        assertThat(expired.stream().filter(e -> e).count()).isEqualTo(1);
        assertNotContains(1, 3);
    }

    /**
     * A tombstone (here, that of the collections overwritten by an INSERT) in an SSTable without expiring cells, of a
     * table without clustering columns, doesn't prevent the subtraction; a tombstone in an SSTable with expiring cells
     * prevents the subtraction of that SSTable only: the SSTables don't record when their first expiring cell expires
     * apart from their tombstones.
     */
    @Test
    public void testTombstonesWithoutClusteringColumns() throws Throwable
    {
        createTableAndIndexes();
        disableCompaction();
        execute("INSERT INTO %s (id, v, nums, m) VALUES (1, 0, [1], {'a': 1})");
        flush();
        assertThat(numsIndex.hasShadowingDeletions(now())).isFalse();
        assertThat(sstablesWithExpiredTerms()).containsExactly(false);
        assertNotContains(1);

        execute("INSERT INTO %s (id, v, nums, m) VALUES (2, 0, [1], {'a': 1})");
        execute("UPDATE %s USING TTL 1000 SET nums = nums + [5], m['k'] = 1 WHERE id = 2");
        flush();
        assertThat(numsIndex.hasShadowingDeletions(now())).isFalse();
        assertThat(sstablesWithExpiredTerms()).containsExactlyInAnyOrder(false, true);
        assertNotContains(1);
    }

    /**
     * A partition deletion in the memtable: a re-inserted row has the old element in the reconciled row (the memtable
     * index keeps its term), or the old term stays next to the new ones, but the live data doesn't have it.
     */
    @Test
    public void testPartitionDeletionAndReinsert() throws Throwable
    {
        createTableAndIndexes();
        execute("INSERT INTO %s (id, v, nums, m) VALUES (1, 0, [5], {'k': 1})");
        execute("INSERT INTO %s (id, v, nums, m) VALUES (2, 0, [5], {'k': 1})");
        execute("DELETE FROM %s WHERE id = 1");
        execute("INSERT INTO %s (id, v) VALUES (1, 0)");
        assertThat(numsIndex.hasShadowingDeletions(now())).isTrue();
        assertNotContains(1);
        flush();
        assertNotContains(1);
        compact();
        assertNotContains(1);
    }

    /**
     * Writes older than a partition deletion, in the same memtable and after the deletion was flushed.
     */
    @Test
    public void testOutOfOrderWritesUnderDeletion() throws Throwable
    {
        createTableAndIndexes();
        execute("DELETE FROM %s USING TIMESTAMP 10 WHERE id = 1");
        execute("INSERT INTO %s (id, v, nums, m) VALUES (1, 0, [5], {'k': 1}) USING TIMESTAMP 5");
        execute("INSERT INTO %s (id, v) VALUES (1, 0) USING TIMESTAMP 20");
        assertNotContains(1);

        execute("DELETE FROM %s USING TIMESTAMP 10 WHERE id = 2");
        flush();
        execute("INSERT INTO %s (id, v, nums, m) VALUES (2, 0, [5], {'k': 1}) USING TIMESTAMP 5");
        execute("INSERT INTO %s (id, v) VALUES (2, 0) USING TIMESTAMP 20");
        assertThat(numsIndex.hasShadowingDeletions(now())).isTrue();
        assertNotContains(1, 2);
        flush();
        assertNotContains(1, 2);
        compact();
        assertNotContains(1, 2);
    }

    /**
     * A range tombstone in the memtable, then a re-inserted row.
     */
    @Test
    public void testRangeTombstoneAndReinsert() throws Throwable
    {
        table("CREATE TABLE %s (k int, c int, v int, nums list<int>, PRIMARY KEY (k, c))");
        numsIndex = getIndexContext(createIndex("CREATE INDEX ON %s(nums) USING 'sai'"));
        execute("INSERT INTO %s (k, c, v, nums) VALUES (0, 1, 0, [5])");
        execute("INSERT INTO %s (k, c, v, nums) VALUES (0, 2, 0, [5])");
        execute("DELETE FROM %s WHERE k = 0 AND c >= 0 AND c < 2");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 0)");
        assertThat(numsIndex.hasShadowingDeletions(now())).isTrue();

        CheckedFunction check = () -> assertRows(execute("SELECT c FROM %s WHERE nums NOT CONTAINS 5"), row(1));
        check.apply();
        flush();
        check.apply();
        compact();
        check.apply();
    }

    /**
     * A range tombstone flushed in an SSTable shadows a write older than it in the memtable: only the SSTable knows
     * about the deletion (a table with clustering columns, whose SSTables may have range tombstones).
     */
    @Test
    public void testFlushedRangeTombstoneShadowingMemtableWrite() throws Throwable
    {
        table("CREATE TABLE %s (k int, c int, v int, nums list<int>, PRIMARY KEY (k, c))");
        numsIndex = getIndexContext(createIndex("CREATE INDEX ON %s(nums) USING 'sai'"));
        execute("DELETE FROM %s USING TIMESTAMP 10 WHERE k = 0 AND c >= 0 AND c < 5");
        flush();
        execute("INSERT INTO %s (k, c, v, nums) VALUES (0, 1, 0, [5]) USING TIMESTAMP 5");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 0) USING TIMESTAMP 20");
        execute("INSERT INTO %s (k, c, v, nums) VALUES (0, 7, 0, [5])");
        assertThat(numsIndex.hasShadowingDeletions(now())).isTrue();

        CheckedFunction check = () -> assertRows(execute("SELECT c FROM %s WHERE nums NOT CONTAINS 5"), row(1));
        check.apply();
        flush();
        check.apply();
        compact();
        check.apply();
    }

    /**
     * A static collection column, after a partition deletion and a re-insertion of a row of the partition.
     */
    @Test
    public void testStaticCollection() throws Throwable
    {
        table("CREATE TABLE %s (k int, c int, v int, s list<int> static, PRIMARY KEY (k, c))");
        numsIndex = getIndexContext(createIndex("CREATE INDEX ON %s(s) USING 'sai'"));
        execute("INSERT INTO %s (k, c, v, s) VALUES (0, 1, 0, [5])");
        execute("INSERT INTO %s (k, c, v, s) VALUES (1, 1, 0, [5])");
        execute("DELETE FROM %s WHERE k = 0");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 0)");
        execute("INSERT INTO %s (k, c, v, s) VALUES (2, 1, 0, [1])");

        CheckedFunction check = () -> assertRowsIgnoringOrder(execute("SELECT k, c FROM %s WHERE s NOT CONTAINS 5"), row(0, 1), row(2, 1));
        check.apply();
        flush();
        check.apply();
        compact();
        check.apply();
    }

    /**
     * A map-entry {@code !=} ({@code m['k'] != v}) doesn't use the anti-join. The filtering accepts the rows without
     * an entry for the key (here row 2): the index-served query returns the same rows.
     */
    @Test
    public void testMapEntryNotEqual() throws Throwable
    {
        String plain = table("CREATE TABLE %s (id int PRIMARY KEY, m map<text, int>)");
        table("CREATE TABLE %s (id int PRIMARY KEY, m map<text, int>)");
        createIndex("CREATE INDEX ON %s(entries(m)) USING 'sai'");
        for (String t : new String[]{ plain, currentTable() })
        {
            execute("INSERT INTO " + KEYSPACE + '.' + t + " (id, m) VALUES (1, {'k': 1})");
            execute("INSERT INTO " + KEYSPACE + '.' + t + " (id, m) VALUES (2, {'j': 2})");
            execute("INSERT INTO " + KEYSPACE + '.' + t + " (id, m) VALUES (3, {'k': 3})");
        }
        assertRowsIgnoringOrder(execute("SELECT id FROM " + KEYSPACE + '.' + plain + " WHERE m['k'] != 1 ALLOW FILTERING"), row(2), row(3));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE m['k'] != 1"), row(2), row(3));
    }

    /**
     * During a flush, the old and the new memtables are both live: the new one may have a key without the term that
     * the old one has (e.g. an element removed by an update in the new memtable). The keys are subtracted memtable by
     * memtable, so the key matching in the old memtable is still a candidate from the new one.
     */
    @Test
    public void testTwoLiveMemtables() throws Throwable
    {
        createTableAndIndexes();
        execute("INSERT INTO %s (id, v, nums) VALUES (1, 0, [1])");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Memtable memtable = cfs.getCurrentMemtable();
        DecoratedKey key = cfs.decorateKey(Int32Type.instance.decompose(1));
        // the old memtable index has the old element, the new one the current element, for the same row
        MemtableIndex old = new TrieMemtableIndex(numsIndex, memtable);
        MemtableIndex current = new TrieMemtableIndex(numsIndex, memtable);
        old.index(key, Clustering.EMPTY, Int32Type.instance.decompose(5), memtable, new OpOrder().start());
        current.index(key, Clustering.EMPTY, Int32Type.instance.decompose(1), memtable, new OpOrder().start());

        Expression notContains = new Expression(numsIndex).add(Operator.NOT_CONTAINS, Int32Type.instance.decompose(5));
        List<DecoratedKey> keys = new ArrayList<>();
        try (KeyRangeIterator iterator = numsIndex.searchMemtable(new QueryContext(), List.of(old, current), notContains,
                                                                  DataRange.allData(cfs.getPartitioner()).keyRange(), true))
        {
            while (iterator.hasNext())
                keys.add(iterator.next().partitionKey());
        }
        assertThat(keys).contains(key);

        // each memtable alone subtracts its own matching keys
        try (KeyRangeIterator iterator = numsIndex.searchMemtable(new QueryContext(), List.of(old), notContains,
                                                                  DataRange.allData(cfs.getPartitioner()).keyRange(), true))
        {
            assertThat(iterator.hasNext()).isFalse();
        }
    }
}
