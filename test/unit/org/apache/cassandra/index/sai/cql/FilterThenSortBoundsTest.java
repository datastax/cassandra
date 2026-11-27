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

package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.exceptions.ReadTimeoutException;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.memory.TrieMemtableIndex;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithByteComparable;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that an ORDER BY query executed as filter-then-sort is bounded: it does not materialize more keys than
 * {@link CassandraRelevantProperties#SAI_MAX_FILTER_THEN_SORT_KEYS} and it stops when the query times out.
 */
public class FilterThenSortBoundsTest extends SAITester
{
    private static final int ROWS = 3000;
    private static final int LIMIT = 10;
    private static final String FILTER_THEN_SORT = " WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'}";
    private static final String SORT_THEN_FILTER = " WITH optimizer_options = {'hybrid_sort_order': 'sort_then_filter'}";

    private static final int TIMEOUT = 2000;
    private static final int DELAY = TIMEOUT + (TIMEOUT / 2);

    private int defaultMaxKeys;
    private long defaultRangeTimeout;
    private long defaultReadTimeout;

    @Before
    public void setup()
    {
        defaultMaxKeys = CassandraRelevantProperties.SAI_MAX_FILTER_THEN_SORT_KEYS.getInt();
        defaultRangeTimeout = DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS);
        defaultReadTimeout = DatabaseDescriptor.getReadRpcTimeout(TimeUnit.MILLISECONDS);

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val int, str_val text)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
    }

    @After
    public void restoreDefaults()
    {
        Injections.deleteAll();
        CassandraRelevantProperties.SAI_MAX_FILTER_THEN_SORT_KEYS.setInt(defaultMaxKeys);
        DatabaseDescriptor.setRangeRpcTimeout(defaultRangeTimeout);
        DatabaseDescriptor.setReadRpcTimeout(defaultReadTimeout);
    }

    /** Half of the rows match {@code val = 0}; the first half of the table is flushed, the rest is in the memtable. */
    private void insertRows(boolean flushAll)
    {
        for (int i = 0; i < ROWS; i++)
        {
            execute("INSERT INTO %s (pk, val, str_val) VALUES (?, ?, ?)", i, i % 2, String.format("%06d", ROWS - i));
            if (i == ROWS / 2)
                flush();
        }
        if (flushAll)
            flush();
    }

    private Object[][] expectedRows(boolean ascending)
    {
        // str_val decreases with pk, and only even pks match
        List<Object[]> rows = new ArrayList<>();
        int lastEven = (ROWS - 1) & ~1;
        for (int i = 0; i < LIMIT; i++)
            rows.add(row(ascending ? lastEven - 2 * i : 2 * i));
        return rows.toArray(new Object[0][]);
    }

    @Test
    public void testSwitchToSortThenFilterAboveTheCap() throws Throwable
    {
        insertRows(false);

        Injections.Counter switches = Injections.newCounter("switch_to_sort_then_filter")
                                                .add(newInvokePoint().onClass(QueryController.class)
                                                                     .onMethod("switchToSortThenFilter"))
                                                .build();
        Injections.inject(switches);

        String asc = "SELECT pk FROM %s WHERE val = 0 ORDER BY str_val ASC LIMIT " + LIMIT;
        String desc = "SELECT pk FROM %s WHERE val = 0 ORDER BY str_val DESC LIMIT " + LIMIT;

        // under the cap: the keys are materialized and sorted
        assertRows(execute(asc + FILTER_THEN_SORT), expectedRows(true));
        assertRows(execute(desc + FILTER_THEN_SORT), expectedRows(false));
        assertThat(switches.get()).isEqualTo(0);

        // exactly at the cap
        CassandraRelevantProperties.SAI_MAX_FILTER_THEN_SORT_KEYS.setInt(ROWS / 2);
        assertRows(execute(asc + FILTER_THEN_SORT), expectedRows(true));
        assertThat(switches.get()).isEqualTo(0);

        // above the cap: same results as sort-then-filter, through the ordered scan
        for (int cap : new int[]{ ROWS / 2 - 1, 100, 1 })
        {
            switches.reset();
            CassandraRelevantProperties.SAI_MAX_FILTER_THEN_SORT_KEYS.setInt(cap);
            assertRows(execute(asc + FILTER_THEN_SORT), expectedRows(true));
            assertRows(execute(desc + FILTER_THEN_SORT), expectedRows(false));
            assertThat(switches.get()).isEqualTo(2);
        }

        // a filter under the cap is not affected
        switches.reset();
        assertRows(execute("SELECT pk FROM %s WHERE val = 0 AND pk = 4 ORDER BY str_val ASC LIMIT " + LIMIT + FILTER_THEN_SORT),
                   row(4));
        assertThat(switches.get()).isEqualTo(0);

        // sort-then-filter never materializes the keys
        assertRows(execute(asc + SORT_THEN_FILTER), expectedRows(true));
        assertThat(switches.get()).isEqualTo(0);

        // no cap
        CassandraRelevantProperties.SAI_MAX_FILTER_THEN_SORT_KEYS.setInt(0);
        assertRows(execute(asc + FILTER_THEN_SORT), expectedRows(true));
        assertThat(switches.get()).isEqualTo(0);
    }

    @Test
    public void testTimeoutWhileMaterializingKeys() throws Throwable
    {
        insertRows(false);
        setTimeouts();

        Injection delay = Injections.newPause("materialize_keys_delay", DELAY)
                                    .add(newInvokePoint().onClass(QueryController.class)
                                                         .onMethod("materializeKeys")
                                                         .at("INVOKE QueryContext.checkpoint"))
                                    .build();
        Injections.inject(delay);

        assertThatThrownBy(() -> executeNet("SELECT pk FROM %s WHERE val = 0 ORDER BY str_val ASC LIMIT " + LIMIT + FILTER_THEN_SORT))
        .isInstanceOf(ReadTimeoutException.class);
    }

    @Test
    public void testTimeoutWhileSortingMemtableKeys() throws Throwable
    {
        for (int i = 0; i < ROWS; i++)
            execute("INSERT INTO %s (pk, val, str_val) VALUES (?, ?, ?)", i, i % 2, String.format("%06d", i));

        testTimeoutWhileSortingKeys(TrieMemtableIndex.class, "getCellForKey");
    }

    @Test
    public void testTimeoutWhileSortingSSTableKeys() throws Throwable
    {
        insertRows(true);
        testTimeoutWhileSortingKeys(IndexSearcher.class, "encode");
    }

    /**
     * Delays the lookup of the sort value of each key for longer than the timeout. The query must stop at the next
     * key instead of looking up all of them.
     */
    private void testTimeoutWhileSortingKeys(Class<?> clazz, String perKeyMethod) throws Throwable
    {
        setTimeouts();

        Injection delay = Injections.newPause("sort_keys_delay", DELAY)
                                    .add(newInvokePoint().onClass(clazz).onMethod(perKeyMethod))
                                    .build();
        // A sort key is created for each key that has been looked up. Its public constructors delegate to a private
        // one, so each key counts twice.
        Injections.Counter lookups = Injections.newCounter("sort_keys_lookups")
                                               .add(newInvokePoint().onClass(PrimaryKeyWithByteComparable.class)
                                                                    .onMethod("<init>"))
                                               .build();
        Injections.inject(delay, lookups);

        assertThatThrownBy(() -> executeNet("SELECT pk FROM %s WHERE val = 0 ORDER BY str_val ASC LIMIT " + LIMIT + FILTER_THEN_SORT))
        .isInstanceOf(ReadTimeoutException.class);
        // The client gets the timeout while the first lookup is still delayed. Without a timeout check per key
        // the second lookup would complete one more delay after the first.
        Util.spinAssertEquals(null, 2L, lookups::get, 2 * DELAY, TimeUnit.MILLISECONDS);
        Thread.sleep(DELAY + 1000);
        assertThat(lookups.get()).isEqualTo(2);
    }

    private void setTimeouts()
    {
        requireNetwork();
        DatabaseDescriptor.setRangeRpcTimeout(TIMEOUT);
        DatabaseDescriptor.setReadRpcTimeout(TIMEOUT);
    }
}
