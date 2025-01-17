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
package org.apache.cassandra.cql3;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.memory.BufferPool;

import static org.junit.Assert.assertEquals;


/**
 * Tests randomly causing re-read with NotInCacheException.
 */
public class BasicReadTest extends CQLTester
{
    final int BASE_COUNT = 700;
    final int REPS = 150;
    final int DELETIONS = 55;

    Random rand;

    @BeforeClass
    public static void setupBasicReadTest()
    {
        // Make sure to go through the multiple page path for chunk cache entries
        CassandraRelevantProperties.BUFFERPOOL_DISABLE_COMBINED_ALLOCATION.setBoolean(true);
    }

    @Before
    public void setUp()
    {
        rand = new Random();
    }

    @Test
    public void testWideIndexingForward() throws Throwable
    {
        DatabaseDescriptor.setColumnIndexCacheSizeInKiB(16);  // 16 to allow exception within block
        createTable("CREATE TABLE %s (k int, c int, v int, d text, PRIMARY KEY (k, c))");
        int COUNT = rand.nextInt(BASE_COUNT / 10) + BASE_COUNT;

        addDeletions(1, COUNT, rand.nextInt());

        for (int i = 0; i < COUNT; i++)
            execute("INSERT INTO %s (k, c, v, d) VALUES (?, ?, ?, ?)", 1, i, i, generateString(10 << (i % 12)));
        flush();

        interceptCache();
        for (int rep = 0; rep < REPS; ++rep)
        {
            int i = rand.nextInt(COUNT);
            int j = i + rand.nextInt(BASE_COUNT / 10);
            if (j > COUNT)
                j = COUNT;
            Object[][] rows = getRows(execute("SELECT v FROM %s WHERE k = 1 and c >= ? and c < ?", i, j));
            String message = String.format("%d<=c<%d rows returned %s", i, j, Arrays.deepToString(rows));
            assertNoDeletions(message, rows);
            assertEquals(message, j - i, rows.length);
        }
    }

    @Test
    public void testWideIndexingReversed() throws Throwable
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(16);
        createTable("CREATE TABLE %s (k int, c int, v int, d text, PRIMARY KEY (k, c))");
        int COUNT = rand.nextInt(BASE_COUNT / 10) + BASE_COUNT;

        addDeletions(1, COUNT, rand.nextInt());

        for (int i = 0; i < COUNT; i++)
            execute("INSERT INTO %s (k, c, v, d) VALUES (?, ?, ?, ?)", 1, i, i, generateString(10 << (i % 12)));
        flush();

        interceptCache();
        for (int rep = 0; rep < REPS; ++rep)
        {
            int i = rand.nextInt(COUNT);
            int j = i + rand.nextInt(BASE_COUNT / 10);
            if (j > COUNT)
                j = COUNT;
            Object[][] rows = getRows(execute("SELECT v FROM %s WHERE k = 1 and c >= ? and c < ? ORDER BY c DESC", i, j));
            String message = String.format("%d<=c<%d rows returned %s", i, j, Arrays.deepToString(rows));
            assertNoDeletions(message, rows);
            assertEquals("Lookup between " + i + " and " + j + " count " + COUNT, j - i, rows.length);
        }
    }

    @Test
    public void testWideIndexForwardIn() throws Throwable
    {
        testWideIndexIn(false, true);
    }

    @Test
    public void testWideIndexReversedIn() throws Throwable
    {
        testWideIndexIn(true, true);
    }

    @Test
    public void testWideIndexForwardAllIn() throws Throwable
    {
        testWideIndexIn(false, false);
    }

    @Test
    public void testWideIndexReversedAllIn() throws Throwable
    {
        testWideIndexIn(true, false);
    }

    private void testWideIndexIn(boolean reversed, boolean readSubset) throws Throwable
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(4);
        createTable("CREATE TABLE %s (k int, c int, v int, d text, PRIMARY KEY (k, c, v))");
        int COUNT = rand.nextInt(BASE_COUNT / 10) + BASE_COUNT;
        int MULT = 5;

        addDeletions(2, COUNT, rand.nextInt());

        for (int i = 0; i < COUNT; i++)
            for (int j = 0; j < MULT; ++j)
                execute("INSERT INTO %s (k, c, v, d) VALUES (?, ?, ?, ?)", i % 3, i, j, generateString(100 << j));

        flush();

        interceptCache();
        for (int rep = 0; rep < REPS; ++rep)
        {
            int[] arr;
            if (readSubset)
            {
                Set<Integer> vals = new HashSet<>();
                int sz = Math.max(5, rand.nextInt(BASE_COUNT / 50));
                while (vals.size() < sz)
                    vals.add(rand.nextInt(COUNT));
                arr = vals.stream().mapToInt(i -> i).toArray();
            }
            else
            {
                arr = IntStream.range(0, COUNT).toArray();
            }

            String s = Arrays.stream(arr).mapToObj(Integer::toString).collect(Collectors.joining(","));
            for (int i = 0; i < 3; ++i)
            {
                int ii = i;
                Object[][] rows = getRows(execute(String.format("SELECT c,v FROM %%s WHERE k = ? and c IN (%s)%s", s, reversed ? " ORDER BY c DESC" : ""), i));
                String message = String.format("c %s rows returned %s", s, Arrays.deepToString(rows));
                assertNoDeletions(message, rows);
                assertEquals("k = " + i + " IN " + s + " count " + COUNT + ": " +
                        Arrays.stream(rows)
                              .map(Arrays::toString)
                              .collect(Collectors.joining("\n  ", "\n {", "\n }")),
                        MULT * Arrays.stream(arr).filter(x -> x % 3 == ii).count(),
                        rows.length);
            }
        }
    }

    @Test
    public void testForward() throws Throwable
    {
        testForward("");
    }

    @Test
    public void testForward_ChunkDirectlyAllocated() throws Throwable
    {
        testForward(String.format(" WITH compression = {'chunk_length_in_kb': '%d', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}",
                                  BufferPool.NORMAL_CHUNK_SIZE / 1024 * 2));  // Note, this is currently 256k
    }

    @Test
    public void testForward_128k() throws Throwable
    {
        testForward(" WITH compression = {'chunk_length_in_kb': '128', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    @Test
    public void testForward_64k() throws Throwable
    {
        testForward(" WITH compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    @Test
    public void testForward_32k() throws Throwable
    {
        testForward(" WITH compression = {'chunk_length_in_kb': '32', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    @Test
    public void testForward_16k() throws Throwable
    {
        testForward(" WITH compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    @Test
    public void testForward_8k() throws Throwable
    {
        testForward(" WITH compression = {'chunk_length_in_kb': '8', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    @Test
    public void testForward_4k() throws Throwable
    {
        testForward(" WITH compression = {'chunk_length_in_kb': '4', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    @Test
    public void testForward_2k() throws Throwable
    {
        testForward(" WITH compression = {'chunk_length_in_kb': '2', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    @Test
    public void testForward_1k() throws Throwable
    {
        testForward(" WITH compression = {'chunk_length_in_kb': '1', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    private void testForward(String compression) throws Throwable
    {
        int STEP = 32;
        DatabaseDescriptor.setColumnIndexSizeInKiB(1000);   // make sure rows fit to test only non-indexed code
        createTable("CREATE TABLE %s (k int, c int, v int, d text, PRIMARY KEY (k, c))" + compression);
        int COUNT = rand.nextInt(BASE_COUNT / 10) + BASE_COUNT;

        addDeletions(COUNT / STEP, STEP, rand.nextInt());
        for (int i = 0; i < COUNT; i++)
            execute("INSERT INTO %s (k, c, v, d) VALUES (?, ?, ?, ?)", i / STEP, i % STEP, i, generateString(10 << (i % 12)));
        flush();

        interceptCache();
        for (int rep = 0; rep < REPS; ++rep)
        {
            int i = rand.nextInt(COUNT);
            Object[][] rows = getRows(execute("SELECT v FROM %s WHERE k = ? and c >= ?", i / STEP, i % STEP));
            int max = STEP;
            if (i / STEP == COUNT / STEP)
                max = COUNT % STEP;
            String message = String.format("k %d c %d rows returned %s", i / STEP, i % STEP, Arrays.deepToString(rows));
            assertNoDeletions(message, rows);
            assertEquals(message, max - (i % STEP), rows.length);
        }
    }

    @Test
    public void testReversed() throws Throwable
    {
        testReversed("");
    }

    @Test
    public void testReversed_ChunkDirectlyAllocated() throws Throwable
    {
        testReversed(String.format(" WITH compression = {'chunk_length_in_kb': '%d', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}",
                                   BufferPool.NORMAL_CHUNK_SIZE / 1024 * 2)); // Note, this is currently 256k
    }

    @Test
    public void testReversed_128k() throws Throwable
    {
        testReversed(" WITH compression = {'chunk_length_in_kb': '128', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    @Test
    public void testReversed_64k() throws Throwable
    {
        testReversed(" WITH compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    @Test
    public void testReversed_32k() throws Throwable
    {
        testReversed(" WITH compression = {'chunk_length_in_kb': '32', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    @Test
    public void testReversed_16k() throws Throwable
    {
        testReversed(" WITH compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    @Test
    public void testReversed_8k() throws Throwable
    {
        testReversed(" WITH compression = {'chunk_length_in_kb': '8', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    @Test
    public void testReversed_4k() throws Throwable
    {
        testReversed(" WITH compression = {'chunk_length_in_kb': '4', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    @Test
    public void testReversed_2k() throws Throwable
    {
        testReversed(" WITH compression = {'chunk_length_in_kb': '2', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    @Test
    public void testReversed_1k() throws Throwable
    {
        testReversed(" WITH compression = {'chunk_length_in_kb': '1', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    private void testReversed(String compression) throws Throwable
    {
        int STEP = 32;
        DatabaseDescriptor.setColumnIndexSizeInKiB(1000);   // make sure rows fit to test only non-indexed code
        createTable("CREATE TABLE %s (k int, c int, v int, d text, PRIMARY KEY (k, c))" + compression);
        int COUNT = rand.nextInt(BASE_COUNT / 10) + BASE_COUNT;

        addDeletions(COUNT / STEP, STEP, rand.nextInt());
        for (int i = 0; i < COUNT; i++)
            execute("INSERT INTO %s (k, c, v, d) VALUES (?, ?, ?, ?)", i / STEP, i % STEP, i, generateString(10 << (i % 12)));
        flush();

        interceptCache();
        for (int rep = 0; rep < REPS; ++rep)
        {
            int i = rand.nextInt(COUNT);
            Object[][] rows = getRows(execute("SELECT v FROM %s WHERE k = ? and c < ? ORDER BY c DESC", i / STEP, i % STEP));
            String message = String.format("k %d c %d rows returned %s", i / STEP, i % STEP, Arrays.deepToString(rows));
            assertNoDeletions(message, rows);
            assertEquals(i % STEP, rows.length);
        }
    }

    public void addDeletions(int krange, int crange, int seed) throws Throwable
    {
        addDeletedDataTable(krange, crange, seed);
        // Note: The loops here and in addDeletionsData need to fully match in their usage of rand
        Random rand = new Random(seed);
        for (int i = 0; i < DELETIONS; ++i)
        {
            int partition = rand.nextInt(krange) + 1;   // Note: partition 0 will not have tombstones intentionally
            int left = rand.nextInt(crange + 1) - 1;
            int right = left + rand.nextInt(crange + 1 - left);
            boolean leftInclusive = left == right ? true : rand.nextBoolean();
            boolean rightInclusive = left == right ? true : rand.nextBoolean();

            int start = left + 1;
            int range = right - left - 1;
            int v = range > 0 ? start + rand.nextInt(range) : -1;
            int c = rand.nextInt(3);

            execute(String.format("DELETE FROM %%s WHERE k = ? AND c %s ? AND c %s ?",
                                  leftInclusive ? ">=" : ">",
                                  rightInclusive ? "<=" : "<"),
                    partition,
                    left,
                    right);

        }
    }

    public void addDeletedDataTable(int krange, int crange, int seed) throws Throwable
    {
        Random rand = new Random(seed);
        for (int i = 0; i < DELETIONS; ++i)
        {
            int partition = rand.nextInt(krange) + 1;   // Note: partition 0 will not have tombstones intentionally
            int left = rand.nextInt(crange + 1) - 1;
            int right = left + rand.nextInt(crange + 1 - left);
            boolean leftInclusive = left == right ? true : rand.nextBoolean();
            boolean rightInclusive = left == right ? true : rand.nextBoolean();

            int start = left + 1;
            int range = right - left - 1;
            int v = range > 0 ? start + rand.nextInt(range) : -1;
            int c = rand.nextInt(3);
            if (v == -1)
            {
                if (leftInclusive && rightInclusive)
                    v = c < 1 ? left : right;
                else if (leftInclusive)
                    v = left;
                else if (rightInclusive)
                    v = right;
                else    // nothing is covered
                    continue;
            }
            else
            {
                if (leftInclusive && c == 0)
                    v = left;
                else if (rightInclusive && c == 2)
                    v = right;
            }

            if (range > 0) // else right = left + 1, nothing inclusive
                execute("INSERT INTO %s (k, c, v, d) VALUES (?, ?, ?, ?)", partition, v, v == 0 ? -11111  : -v, "DELETED");
        }
        flush();
    }

    public void assertNoDeletions(String message, Object[][] rows)
    {
        for (Object[] row : rows)
        {
            Assert.assertTrue("Deleted data resurfaced " + message, ((Number) row[0]).intValue() >= 0);
        }
    }

    @Test
    public void testRangeQueries() throws Throwable
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(16);
        interceptCache();

        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        int PARTITIONS = 20;
        int ROWS = 10;
        for (int i = 0; i < PARTITIONS; i++)
            for (int j = 0; j < ROWS; j++)
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", i, j, i * j);

        flush();

        for (int rep = 0; rep < REPS; ++rep)
        {
            Object[][] rows = getRows(execute("SELECT * FROM %s"));
            assertEquals(PARTITIONS * ROWS, rows.length);
        }

        for (int rep = 0; rep < REPS; ++rep)
        {
            int from = rand.nextInt(PARTITIONS - 2);
            int to = 2 + from + rand.nextInt(PARTITIONS - from - 2);

            Object[][] rows = getRows(execute("SELECT k, c, v FROM %s WHERE k <= ? and k >= ? ALLOW FILTERING", to, from));
            assertEquals((to - from + 1) * ROWS, rows.length);

            rows = getRows(execute("SELECT k, c, v FROM %s WHERE k < ? and k >= ? ALLOW FILTERING", to, from));
            assertEquals((to - from) * ROWS, rows.length);

            rows = getRows(execute("SELECT k, c, v FROM %s WHERE k <= ? and k > ? ALLOW FILTERING", to, from));
            assertEquals((to - from) * ROWS, rows.length);

            rows = getRows(execute("SELECT k, c, v FROM %s WHERE k < ? and k > ? ALLOW FILTERING", to, from));
            assertEquals((to - from - 1) * ROWS, rows.length);
        }
    }

    String generateString(int length)
    {
        String s = "";
        for (int i = 0; i < length; ++i)
            s += (char) ('a' + (i % 26));
        return s;
    }

    public void interceptCache()
    {
        // no additional cache modification
    }

    @After
    public void clearIntercept()
    {
        // no additional cache modification
    }
}
