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

package org.apache.cassandra.io.sstable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.OutputHandler;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scrub must keep the partitions that hold range tombstones: their live rows and their deletions.
 */
@RunWith(Parameterized.class)
public class ScrubRangeTombstonesTest extends CQLTester
{
    private static final int PARTITIONS = 6;
    private static final int ROWS = 10;

    @Parameterized.Parameter
    public String formatName;

    private SSTableFormat<?, ?> savedFormat;

    /** pk -> ck -> v, the expected content of the table. */
    private final Map<Integer, TreeMap<Integer, Integer>> model = new TreeMap<>();

    @Parameterized.Parameters(name = "format={0}")
    public static Collection<Object[]> parameters()
    {
        DatabaseDescriptor.daemonInitialization();
        return DatabaseDescriptor.getSSTableFormats()
                                 .keySet()
                                 .stream()
                                 .sorted()
                                 .map(name -> new Object[]{ name })
                                 .collect(Collectors.toList());
    }

    @Before
    public void selectFormat()
    {
        savedFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(DatabaseDescriptor.getSSTableFormats().get(formatName));
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(savedFormat);
    }

    @Test
    public void testScrubKeepsRangeTombstones() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        disableCompaction();

        // first sstable: plain rows, some of them deleted by the range tombstones of the second sstable
        for (int pk = 1; pk < PARTITIONS; pk++)
            for (int ck = 0; ck < ROWS; ck++)
                insert(pk, ck, pk * 100 + ck);
        // an expiring row in a partition without range tombstones
        execute("INSERT INTO %s (pk, ck, v) VALUES (5, 30, 530) USING TTL 3600");
        model.get(5).put(30, 530);
        flush();

        // second sstable: range tombstones, with live rows around and inside the deleted ranges
        deleteRange(1, 3, 5);                // live rows before and after the deleted range
        deleteRange(2, 0, 100);              // only a range tombstone in this sstable
        deleteRange(3, Integer.MIN_VALUE, 1); // deleted range at the start of the partition
        deleteRange(3, 8, Integer.MAX_VALUE); // deleted range at the end of the partition
        execute("INSERT INTO %s (pk, ck, v) VALUES (3, 20, 320) USING TTL 3600");
        model.get(3).put(20, 320);
        deleteRange(4, 2, 4);
        insert(4, 3, 999);                   // live row inside the deleted range, between the two markers
        deleteRange(6, 0, 5);                // a partition that only holds a range tombstone
        flush();

        assertThat(cfs.getLiveSSTables()).hasSize(2);
        assertContent();

        int goodPartitions = 0;
        for (SSTableReader sstable : new ArrayList<>(cfs.getLiveSSTables()))
        {
            IScrubber.ScrubResult result;
            try (LifecycleTransaction txn = cfs.getTracker().tryModify(Collections.singletonList(sstable), OperationType.SCRUB);
                 IScrubber scrubber = sstable.descriptor.getFormat().getScrubber(cfs, txn, new OutputHandler.LogOutput(), IScrubber.options().checkData().build()))
            {
                result = scrubber.scrubWithResult();
            }
            assertThat(result.badPartitions).as("partitions skipped while scrubbing %s", sstable).isZero();
            assertThat(result.emptyPartitions).as("empty partitions dropped while scrubbing %s", sstable).isZero();
            goodPartitions += result.goodPartitions;
        }

        // 5 partitions in the first sstable, 5 partitions (1, 2, 3, 4 and 6) in the second one
        assertThat(goodPartitions).isEqualTo(10);
        assertThat(cfs.getLiveSSTables()).hasSize(2);
        assertContent();
        assertThat(execute("SELECT ttl(v) FROM %s WHERE pk = 3 AND ck = 20").one().getInt("ttl(v)")).isPositive();
        assertThat(execute("SELECT ttl(v) FROM %s WHERE pk = 5 AND ck = 30").one().getInt("ttl(v)")).isPositive();

        // and the data is still the same once all the sstables are compacted together
        compact();
        assertContent();
    }

    private void insert(int pk, int ck, int v) throws Throwable
    {
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, v);
        model.computeIfAbsent(pk, k -> new TreeMap<>()).put(ck, v);
    }

    private void deleteRange(int pk, int from, int to) throws Throwable
    {
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck <= ?", pk, from, to);
        if (model.containsKey(pk))
            model.get(pk).subMap(from, true, to, true).clear();
    }

    private void assertContent() throws Throwable
    {
        List<Object[]> expected = new ArrayList<>();
        model.forEach((pk, rows) -> rows.forEach((ck, v) -> expected.add(row(pk, ck, v))));

        for (int pk = 1; pk <= PARTITIONS; pk++)
        {
            Integer key = pk;
            List<Object[]> partition = expected.stream().filter(r -> r[0].equals(key)).collect(Collectors.toList());
            UntypedResultSet result = execute("SELECT pk, ck, v FROM %s WHERE pk = ?", pk);
            assertRows(result, partition.toArray(new Object[0][]));
        }
        assertRowsIgnoringOrder(execute("SELECT pk, ck, v FROM %s"), expected.toArray(new Object[0][]));
    }
}
