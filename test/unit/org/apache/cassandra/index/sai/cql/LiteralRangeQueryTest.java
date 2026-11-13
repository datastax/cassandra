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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import com.google.common.base.Strings;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

/**
 * Tests range ({@code <}, {@code <=}, {@code >}, {@code >=}) queries on {@code text}/{@code ascii} columns with a
 * SAI index without analyzer, the CQL counterpart of the Solr term range queries ({@code field:[a TO b]},
 * {@code field:{a TO b}}, {@code field:[a TO *]}...), across the memtable-only / flushed / compacted matrix and
 * the index versions.
 * The order is the binary (unsigned byte) order of the UTF-8 values.
 */
public class LiteralRangeQueryTest extends SAITester.Versioned
{
    /** The seed of the randomized tests, which can be fixed with the {@link CassandraRelevantProperties#TEST_RANDOM_SEED} property. */
    private static final long SEED = CassandraRelevantProperties.TEST_RANDOM_SEED.getLong(System.nanoTime());
    private static final String[] ALPHABET = { "a", "b", "ab", "abc", "z", "A", "B", "Ab", " ", "0", "é", "É", "ß", "日", "😀" };

    private static final int RANDOM_ROWS = 2000;
    private static final int RANDOM_QUERIES = 200;

    private final Random random = new Random(SEED);

    /** The paged queries need the native protocol. */
    @BeforeClass
    public static void setUpNetwork()
    {
        requireNetwork();
    }

    /** Reports the seed with any failure of a test, including unexpected exceptions, not only assertion messages. */
    @Rule
    public final TestRule reportSeed = (base, description) -> new Statement()
    {
        @Override
        public void evaluate() throws Throwable
        {
            try
            {
                base.evaluate();
            }
            catch (Throwable t)
            {
                t.addSuppressed(new RuntimeException("Random seed: -D" + CassandraRelevantProperties.TEST_RANDOM_SEED.getKey() + '=' + SEED));
                throw t;
            }
        }
    };

    @Test
    public void testFixedRanges()
    {
        testFixedRanges("text");
    }

    @Test
    public void testFixedRangesAscii()
    {
        testFixedRanges("ascii");
    }

    private void testFixedRanges(String type)
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v " + type + ')');
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "a");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "ab");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "abc");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 4, "abd");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 5, "b");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 6, "ABC");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 7, "c");

        for (int i = 0; i < 3; i++)
        {
            // [ab TO b], {ab TO b}, [ab TO b}, {ab TO b]
            assertEquals(List.of(2, 3, 4, 5), keys(execute("SELECT pk FROM %s WHERE v >= 'ab' AND v <= 'b'")));
            assertEquals(List.of(3, 4), keys(execute("SELECT pk FROM %s WHERE v > 'ab' AND v < 'b'")));
            assertEquals(List.of(2, 3, 4), keys(execute("SELECT pk FROM %s WHERE v >= 'ab' AND v < 'b'")));
            assertEquals(List.of(3, 4, 5), keys(execute("SELECT pk FROM %s WHERE v > 'ab' AND v <= 'b'")));
            // open-ended: [abc TO *], {abc TO *}, [* TO ab], [* TO ab}
            assertEquals(List.of(3, 4, 5, 7), keys(execute("SELECT pk FROM %s WHERE v >= 'abc'")));
            assertEquals(List.of(4, 5, 7), keys(execute("SELECT pk FROM %s WHERE v > 'abc'")));
            assertEquals(List.of(1, 2, 6), keys(execute("SELECT pk FROM %s WHERE v <= 'ab'")));
            assertEquals(List.of(1, 6), keys(execute("SELECT pk FROM %s WHERE v < 'ab'")));
            // bounds that are not indexed terms
            assertEquals(List.of(2, 3, 4), keys(execute("SELECT pk FROM %s WHERE v > 'aa' AND v < 'abz'")));
            // the binary order puts the upper case letters before the lower case ones
            assertEquals(List.of(6), keys(execute("SELECT pk FROM %s WHERE v >= 'A' AND v < 'a'")));
            // empty ranges
            assertEquals(List.of(), keys(execute("SELECT pk FROM %s WHERE v > 'b' AND v < 'c'")));
            assertEquals(List.of(), keys(execute("SELECT pk FROM %s WHERE v > 'c'")));
            assertEquals(List.of(5), keys(execute("SELECT pk FROM %s WHERE v >= 'b' AND v <= 'b'")));

            if (i == 0)
                flush();
            else if (i == 1)
                compact();
        }
    }

    @Test
    public void testMultiByteCharacters()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "z");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "é");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "日本");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 4, "😀");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 5, "￿");

        for (int i = 0; i < 3; i++)
        {
            // the UTF-8 byte order is the code point order, so the supplementary characters sort last
            assertEquals(List.of(2, 3, 4, 5), keys(execute("SELECT pk FROM %s WHERE v > 'z'")));
            assertEquals(List.of(3, 5), keys(execute("SELECT pk FROM %s WHERE v > 'é' AND v < '😀'")));
            assertEquals(List.of(4), keys(execute("SELECT pk FROM %s WHERE v > '￿'")));
            assertEquals(List.of(1, 2), keys(execute("SELECT pk FROM %s WHERE v <= 'é'")));

            if (i == 0)
                flush();
            else if (i == 1)
                compact();
        }
    }

    @Test
    public void testEmptyStringValue()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "a");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "b");

        for (int i = 0; i < 3; i++)
        {
            assertEquals(List.of(1, 2, 3), keys(execute("SELECT pk FROM %s WHERE v >= ''")));
            assertEquals(List.of(2, 3), keys(execute("SELECT pk FROM %s WHERE v > ''")));
            assertEquals(List.of(1, 2), keys(execute("SELECT pk FROM %s WHERE v < 'b'")));
            assertEquals(List.of(1), keys(execute("SELECT pk FROM %s WHERE v <= ''")));
            assertEquals(List.of(), keys(execute("SELECT pk FROM %s WHERE v < ''")));

            if (i == 0)
                flush();
            else if (i == 1)
                compact();
        }
    }

    /**
     * A range and exclusions ({@code !=}, {@code NOT IN}) on the same literal column: the exclusions remove only the
     * excluded values, not every value in the range.
     */
    @Test
    public void testRangeWithExclusions()
    {
        for (String type : new String[]{ "text", "ascii" })
        {
            createTable("CREATE TABLE %s (pk int PRIMARY KEY, v " + type + ')');
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            insertCities("pk", "v");

            for (int i = 0; i < 3; i++)
            {
                assertEquals(List.of(2, 3), keys(execute("SELECT pk FROM %s WHERE v >= 'P' AND v != 'Rome'")));
                assertEquals(List.of(2), keys(execute("SELECT pk FROM %s WHERE v >= 'P' AND v NOT IN ('Rome', 'Turin')")));
                assertEquals(List.of(1, 2, 3), keys(execute("SELECT pk FROM %s WHERE v > 'N' AND v != 'Nowhere'")));
                assertEquals(List.of(4), keys(execute("SELECT pk FROM %s WHERE v < 'R' AND v != 'Paris'")));
                flushOrCompact(i);
            }
        }
    }

    /**
     * {@code !=} and {@code NOT IN} alone: the index serves them as the union of the ranges of terms before and
     * after each excluded value, so they don't need {@code ALLOW FILTERING}.
     */
    @Test
    public void testInequalities()
    {
        for (String type : new String[]{ "text", "ascii" })
        {
            createTable("CREATE TABLE %s (pk int PRIMARY KEY, v " + type + ')');
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            insertCities("pk", "v");
            execute("INSERT INTO %s (pk, v) VALUES (5, 'Rom')");
            execute("INSERT INTO %s (pk, v) VALUES (6, 'Rome2')");
            execute("INSERT INTO %s (pk, v) VALUES (7, '')");

            for (int i = 0; i < 3; i++)
            {
                // the values the excluded one is a prefix of, and its prefixes, are not excluded
                assertSelected(List.of(2, 3, 4, 5, 6, 7), "v != 'Rome'");
                assertSelected(List.of(1, 2, 3, 4, 5, 6, 7), "v != 'Nowhere'");
                assertSelected(List.of(1, 2, 3, 4, 5, 6), "v != ''");
                assertSelected(List.of(2, 3, 4, 6, 7), "v != 'Rome' AND v != 'Rom'");
                assertSelected(List.of(2, 4, 5, 6, 7), "v NOT IN ('Rome', 'Turin', 'Nowhere')");
                flushOrCompact(i);
            }
        }
    }

    @Test
    public void testInequalitiesBooleanAndUuid()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, b boolean, u uuid)");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(u) USING 'StorageAttachedIndex'");
        UUID[] uuids = new UUID[4];
        for (int i = 0; i < uuids.length; i++)
        {
            uuids[i] = UUID.fromString(String.format("%08x-0000-4000-8000-000000000000", i + 1));
            execute("INSERT INTO %s (pk, b, u) VALUES (?, ?, ?)", i + 1, i % 2 == 0, uuids[i]);
        }

        for (int i = 0; i < 3; i++)
        {
            assertSelected(List.of(1, 3), "b != false");
            assertSelected(List.of(2, 4), "b != true");
            assertSelected(List.of(), "b NOT IN (true, false)");
            assertSelected(List.of(1, 3, 4), "u != " + uuids[1]);
            assertSelected(List.of(1, 2, 3, 4), "u != " + UUID.fromString("00000009-0000-4000-8000-000000000000"));
            assertSelected(List.of(2, 3), "u NOT IN (" + uuids[0] + ", " + uuids[3] + ')');
            flushOrCompact(i);
        }
    }

    /**
     * A uuid range and {@code !=} on the same column are a single expression of the index, the range with the
     * excluded values, with the same results as without the index.
     */
    @Test
    public void testRangeWithExclusionsUuid()
    {
        UUID[] uuids = new UUID[6];
        for (int i = 0; i < uuids.length; i++)
            uuids[i] = UUID.fromString(String.format("%08x-0000-4000-8000-000000000000", i + 1));

        String raw = null;
        for (boolean indexed : new boolean[]{ false, true })
        {
            String table = createTable("CREATE TABLE %s (pk int PRIMARY KEY, v uuid)");
            if (indexed)
                createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            else
                raw = table;
            for (int pk = 1; pk <= uuids.length; pk++)
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, uuids[pk - 1]);
        }

        for (int i = 0; i < 3; i++)
        {
            assertSelected(List.of(3, 5, 6), "v > " + uuids[1] + " AND v != " + uuids[3]);
            assertSelected(List.of(2, 5), "v >= " + uuids[1] + " AND v < " + uuids[5] + " AND v NOT IN (" + uuids[2] + ", " + uuids[3] + ')');
            assertRawRows(List.of(3, 5, 6), raw, "v > " + uuids[1] + " AND v != " + uuids[3]);
            assertRawRows(List.of(2, 5), raw, "v >= " + uuids[1] + " AND v < " + uuids[5] + " AND v NOT IN (" + uuids[2] + ", " + uuids[3] + ')');
            flushOrCompact(i);
        }
    }

    @Test
    public void testRangeWithExclusionsBoolean()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v boolean)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, true);
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, false);
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, true);

        for (int i = 0; i < 3; i++)
        {
            assertEquals(List.of(1, 3), keys(execute("SELECT pk FROM %s WHERE v >= false AND v != false")));
            assertEquals(List.of(2), keys(execute("SELECT pk FROM %s WHERE v <= true AND v != true")));
            flushOrCompact(i);
        }
    }

    @Test
    public void testRangeWithExclusionsReversedClusteringColumn()
    {
        createTable("CREATE TABLE %s (k int, pk int, v text, PRIMARY KEY (k, v)) WITH CLUSTERING ORDER BY (v DESC)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, pk, v) VALUES (?, ?, ?)", 1, 1, "Rome");
        execute("INSERT INTO %s (k, pk, v) VALUES (?, ?, ?)", 1, 2, "Paris");
        execute("INSERT INTO %s (k, pk, v) VALUES (?, ?, ?)", 2, 3, "Turin");
        execute("INSERT INTO %s (k, pk, v) VALUES (?, ?, ?)", 2, 4, "Milan");

        for (int i = 0; i < 3; i++)
        {
            assertEquals(List.of(2, 3), keys(execute("SELECT pk FROM %s WHERE v >= 'P' AND v != 'Rome'")));
            assertEquals(List.of(4), keys(execute("SELECT pk FROM %s WHERE v < 'R' AND v != 'Paris'")));
            flushOrCompact(i);
        }
    }

    /**
     * The range and the exclusion on a non-indexed literal column, filtered with the index on another column: how
     * the issue was reachable before the indexes served literal ranges.
     */
    @Test
    public void testRangeWithExclusionsFiltered()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, idx int, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(idx) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, idx, v) VALUES (?, ?, ?)", 1, 1, "m");
        execute("INSERT INTO %s (pk, idx, v) VALUES (?, ?, ?)", 2, 1, "n");
        execute("INSERT INTO %s (pk, idx, v) VALUES (?, ?, ?)", 3, 1, "a");
        execute("INSERT INTO %s (pk, idx, v) VALUES (?, ?, ?)", 4, 2, "n");

        for (int i = 0; i < 3; i++)
        {
            assertEquals(List.of(2), keys(execute("SELECT pk FROM %s WHERE idx = 1 AND v > 'a' AND v != 'm' ALLOW FILTERING")));
            assertEquals(List.of(1), keys(execute("SELECT pk FROM %s WHERE idx = 1 AND v > 'a' AND v NOT IN ('n', 'x') ALLOW FILTERING")));
            flushOrCompact(i);
        }
    }

    private void assertRawRows(List<Integer> expected, String table, String where)
    {
        String query = "SELECT pk FROM " + KEYSPACE + '.' + table + " WHERE " + where + " ALLOW FILTERING";
        assertEquals(query, expected, keys(executeFormattedQuery(query)));
    }

    /**
     * Asserts the keys of the rows selected by the given restrictions, also paging the results with small pages.
     */
    private void assertSelected(List<Integer> expected, String where)
    {
        String query = "SELECT pk FROM %s WHERE " + where;
        assertEquals(where, expected, keys(execute(query)));
        for (int pageSize = 1; pageSize <= 2; pageSize++)
        {
            List<Integer> keys = new ArrayList<>();
            for (com.datastax.driver.core.Row row : executeNetWithPaging(query, pageSize))
                keys.add(row.getInt("pk"));
            keys.sort(Integer::compareTo);
            assertEquals(where + ", page size " + pageSize, expected, keys);
        }
    }

    private void insertCities(String key, String value)
    {
        String insert = "INSERT INTO %s (" + key + ", " + value + ") VALUES (?, ?)";
        execute(insert, 1, "Rome");
        execute(insert, 2, "Paris");
        execute(insert, 3, "Turin");
        execute(insert, 4, "Milan");
    }

    private void flushOrCompact(int i)
    {
        if (i == 0)
            flush();
        else if (i == 1)
            compact();
    }

    @Test
    public void testRandomizedRanges()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        Map<Integer, String> rows = new HashMap<>();

        // memtable-only
        insertRandomRows(rows, 0, RANDOM_ROWS / 2);
        verifyRandomizedRanges(rows);

        // flushed sstable + memtable
        flush();
        insertRandomRows(rows, RANDOM_ROWS / 2, RANDOM_ROWS);
        verifyRandomizedRanges(rows);

        // two sstables
        flush();
        verifyRandomizedRanges(rows);

        // compacted
        compact();
        verifyRandomizedRanges(rows);
    }

    /**
     * An exclusive lower bound starts the scan of the on-disk terms at the bound followed by a zero byte: values
     * with zero bytes, among which that one, are in their place.
     */
    @Test
    public void testZeroByteValues()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        String[] values = { "", "\0", "a", "a\0", "a\0\0", "a\0b", "a\u0001", "ab" };
        for (int pk = 0; pk < values.length; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, values[pk]);

        for (int i = 0; i < 3; i++)
        {
            assertEquals(List.of(3, 4, 5, 6, 7), keys(execute("SELECT pk FROM %s WHERE v > ?", "a")));
            assertEquals(List.of(2, 3, 4, 5, 6, 7), keys(execute("SELECT pk FROM %s WHERE v >= ?", "a")));
            assertEquals(List.of(4, 5, 6, 7), keys(execute("SELECT pk FROM %s WHERE v > ?", "a\0")));
            assertEquals(List.of(3, 4, 5, 6, 7), keys(execute("SELECT pk FROM %s WHERE v >= ?", "a\0")));
            assertEquals(List.of(3), keys(execute("SELECT pk FROM %s WHERE v > ? AND v < ?", "a", "a\0\0")));
            assertEquals(List.of(3, 4), keys(execute("SELECT pk FROM %s WHERE v > ? AND v <= ?", "a", "a\0\0")));
            assertEquals(List.of(0, 1, 2), keys(execute("SELECT pk FROM %s WHERE v < ?", "a\0")));
            assertEquals(List.of(0, 1, 2, 3), keys(execute("SELECT pk FROM %s WHERE v <= ?", "a\0")));
            assertEquals(List.of(1, 2, 3, 4, 5, 6, 7), keys(execute("SELECT pk FROM %s WHERE v > ?", "")));
            assertEquals(List.of(2, 3, 4, 5, 6, 7), keys(execute("SELECT pk FROM %s WHERE v > ?", "\0")));
            assertEquals(List.of(0), keys(execute("SELECT pk FROM %s WHERE v < ?", "\0")));
            assertEquals(List.of(), keys(execute("SELECT pk FROM %s WHERE v < ?", "")));
            flushOrCompact(i);
        }
    }

    /**
     * The index serves the ranges on the ascending clustering columns and on the components of a composite
     * partition key.
     */
    @Test
    public void testPrimaryKeyColumns()
    {
        createTable("CREATE TABLE %s (p1 int, p2 text, ck text, v int, PRIMARY KEY ((p1, p2), ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(p2) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");

        String[] values = { "a", "ab", "abc", "b", "c" };
        for (int i = 0; i < values.length; i++)
            for (int j = 0; j < values.length; j++)
                execute("INSERT INTO %s (p1, p2, ck, v) VALUES (?, ?, ?, ?)", i % 2, values[i], values[j], i * 10 + j);

        for (int i = 0; i < 3; i++)
        {
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE ck > 'abc'"),
                                    row(3), row(4), row(13), row(14), row(23), row(24), row(33), row(34), row(43), row(44));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE ck >= 'ab' AND ck < 'abc'"),
                                    row(1), row(11), row(21), row(31), row(41));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE p2 > 'ab' AND p2 <= 'b'"),
                                    row(20), row(21), row(22), row(23), row(24), row(30), row(31), row(32), row(33), row(34));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE p2 < 'ab' AND ck > 'b'"), row(4));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE p2 >= 'c' AND ck <= 'a'"), row(40));
            flushOrCompact(i);
        }
    }

    @Test
    public void testReversedClusteringColumn()
    {
        // Reversed (DESC clustering) string columns store their terms as the raw, un-reversed value bytes
        createTable("CREATE TABLE %s (pk int, ck text, v int, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)");
        createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1, "abc", 1);
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1, "abd", 2);
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1, "ac", 3);
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 2, "b", 4);

        for (int i = 0; i < 3; i++)
        {
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE ck > 'abc'"), row(2), row(3), row(4));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE ck >= 'abc' AND ck < 'ac'"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE ck <= 'ac'"), row(1), row(2), row(3));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE ck > 'abc' AND ck <= 'b'"), row(2), row(3), row(4));

            if (i == 0)
                flush();
            else if (i == 1)
                compact();
        }
    }

    @Test
    public void testReversedClusteringColumnSSTablePruning()
    {
        // the sstable indexes are selected by the range of their terms, which are ordered by the reversed comparator
        createTable("CREATE TABLE %s (pk int, ck text, v int, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)");
        createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1, "a", 1);
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 2, "b", 2);
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 3, "x", 3);
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 4, "y", 4);
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 5, "m", 5);
        flush();

        assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE ck > 'c'"), row(3), row(4), row(5));
        assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE ck < 'c'"), row(1), row(2));
        assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE ck >= 'b' AND ck <= 'x'"), row(2), row(3), row(5));
        assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE ck > 'b' AND ck < 'x'"), row(5));
        assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE ck >= 'y'"), row(4));
        assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE ck <= 'a'"), row(1));
    }

    @Test
    public void testStaticColumn()
    {
        createTable("CREATE TABLE %s (pk int, ck int, s text static, PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, ck, s) VALUES (?, ?, ?)", 1, 1, "a");
        execute("INSERT INTO %s (pk, ck) VALUES (?, ?)", 1, 2);
        execute("INSERT INTO %s (pk, ck, s) VALUES (?, ?, ?)", 2, 1, "b");
        execute("INSERT INTO %s (pk, ck, s) VALUES (?, ?, ?)", 3, 1, "c");

        for (int i = 0; i < 3; i++)
        {
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE s < 'b'"), row(1, 1), row(1, 2));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE s >= 'b'"), row(2, 1), row(3, 1));

            if (i == 0)
                flush();
            else if (i == 1)
                compact();
        }
    }

    @Test
    public void testOtherRestrictions()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text, n int, w text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, v, n, w) VALUES (?, ?, ?, ?)", 1, "a", 1, "x1");
        execute("INSERT INTO %s (pk, v, n, w) VALUES (?, ?, ?, ?)", 2, "b", 1, "x2");
        execute("INSERT INTO %s (pk, v, n, w) VALUES (?, ?, ?, ?)", 3, "c", 2, "y1");
        execute("INSERT INTO %s (pk, v, n, w) VALUES (?, ?, ?, ?)", 4, "d", 2, "y2");
        execute("INSERT INTO %s (pk, v, n, w) VALUES (?, ?, ?, ?)", 5, "e", 3, "z1");

        for (int i = 0; i < 3; i++)
        {
            // AND with other indexes
            assertEquals(List.of(2), keys(execute("SELECT pk FROM %s WHERE v >= 'b' AND n = 1")));
            assertEquals(List.of(3, 4), keys(execute("SELECT pk FROM %s WHERE v > 'a' AND n >= 2 AND v < 'e'")));
            assertEquals(List.of(3, 4), keys(execute("SELECT pk FROM %s WHERE v > 'a' AND w LIKE 'y%%'")));
            assertEquals(List.of(1, 3), keys(execute("SELECT pk FROM %s WHERE v IN ('a', 'c', 'e') AND w < 'z'")));
            // two ranges on different text columns
            assertEquals(List.of(2, 3), keys(execute("SELECT pk FROM %s WHERE v >= 'b' AND w < 'y2'")));
            // OR of ranges: v:[* TO b} OR v:{d TO *]
            assertEquals(List.of(1, 5), keys(execute("SELECT pk FROM %s WHERE v < 'b' OR v > 'd'")));
            assertEquals(List.of(1, 2, 5), keys(execute("SELECT pk FROM %s WHERE (v >= 'a' AND v <= 'b') OR (v > 'd' AND v <= 'e')")));
            assertEquals(List.of(1, 3, 5), keys(execute("SELECT pk FROM %s WHERE v < 'b' OR n >= 2 AND w < 'y2' OR w = 'z1'")));
            // with partition key restrictions
            assertEquals(List.of(2), keys(execute("SELECT pk FROM %s WHERE pk = 2 AND v > 'a'")));
            assertEquals(List.of(), keys(execute("SELECT pk FROM %s WHERE pk = 1 AND v > 'a'")));

            if (i == 0)
                flush();
            else if (i == 1)
                compact();
        }
    }

    @Test
    public void testOrderBy()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text, n int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");

        String[] values = { "a", "b", "c", "d", "e", "f", "g", "h", "" };
        for (int pk = 0; pk < values.length; pk++)
            execute("INSERT INTO %s (pk, v, n) VALUES (?, ?, ?)", pk, values[pk], pk % 2);

        for (int i = 0; i < 3; i++)
        {
            assertRows(execute("SELECT v FROM %s WHERE v > 'b' AND v <= 'f' ORDER BY v ASC LIMIT 3"), row("c"), row("d"), row("e"));
            assertRows(execute("SELECT v FROM %s WHERE v > 'b' AND v <= 'f' ORDER BY v DESC LIMIT 3"), row("f"), row("e"), row("d"));
            assertRows(execute("SELECT v FROM %s WHERE v >= 'b' AND v < 'f' ORDER BY v DESC LIMIT 10"), row("e"), row("d"), row("c"), row("b"));
            assertRows(execute("SELECT v FROM %s WHERE v < 'e' AND n = 1 ORDER BY v DESC LIMIT 10"), row("d"), row("b"));
            assertRows(execute("SELECT v FROM %s WHERE v > 'b' ORDER BY n ASC LIMIT 1"), row("c"));
            assertRows(execute("SELECT v FROM %s WHERE v >= 'g' ORDER BY v ASC LIMIT 10"), row("g"), row("h"));
            // descending without upper bound, with the empty term out of and in the range
            assertRows(execute("SELECT v FROM %s WHERE v > 'f' ORDER BY v DESC LIMIT 10"), row("h"), row("g"));
            assertRows(execute("SELECT v FROM %s WHERE v >= 'g' ORDER BY v DESC LIMIT 1"), row("h"));
            assertRows(execute("SELECT v FROM %s WHERE v >= '' ORDER BY v DESC LIMIT 10"),
                       row("h"), row("g"), row("f"), row("e"), row("d"), row("c"), row("b"), row("a"), row(""));
            assertRows(execute("SELECT v FROM %s WHERE v >= '' ORDER BY v ASC LIMIT 2"), row(""), row("a"));
            assertRows(execute("SELECT v FROM %s WHERE v < 'b' ORDER BY v DESC LIMIT 10"), row("a"), row(""));
            assertRows(execute("SELECT v FROM %s WHERE v > '' AND v < 'b' ORDER BY v DESC LIMIT 10"), row("a"));
            assertRows(execute("SELECT v FROM %s WHERE v <= '' ORDER BY v ASC LIMIT 10"), row(""));
            assertRowCount(execute("SELECT v FROM %s WHERE v < '' ORDER BY v ASC LIMIT 10"), 0);
            assertRows(execute("SELECT v FROM %s WHERE v > 'aa' AND v < 'bb' ORDER BY v DESC LIMIT 10"), row("b"));

            if (i == 0)
                flush();
            else if (i == 1)
                compact();
        }
    }

    @Test
    public void testPaging()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        Map<Integer, String> expected = new HashMap<>();
        for (int pk = 0; pk < 20; pk++)
        {
            for (int ck = 0; ck < 10; ck++)
            {
                String value = String.format("v%03d", pk * 10 + ck);
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, value);
                if (value.compareTo("v050") >= 0 && value.compareTo("v150") < 0)
                    expected.put(pk * 10 + ck, value);
            }
            if (pk == 9)
                flush();
        }

        for (int pageSize : new int[]{ 1, 7, 33, 1000 })
        {
            List<String> actual = new ArrayList<>();
            for (com.datastax.driver.core.Row row : executeNetWithPaging("SELECT v FROM %s WHERE v >= 'v050' AND v < 'v150'", pageSize))
                actual.add(row.getString("v"));
            actual.sort(String::compareTo);
            List<String> expectedValues = new ArrayList<>(expected.values());
            expectedValues.sort(String::compareTo);
            assertEquals("Page size " + pageSize, expectedValues, actual);
        }
    }

    @Test
    public void testBoundLargerThanMaxTermSize()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "a");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "b");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "c");

        // a bound doesn't need to be stored, so it may be longer than the longest indexable term (8KiB by default)
        String large = 'b' + Strings.repeat("x", 16 * 1024);
        for (int i = 0; i < 2; i++)
        {
            assertEquals(List.of(1, 2), keys(execute("SELECT pk FROM %s WHERE v < ?", large)));
            assertEquals(List.of(3), keys(execute("SELECT pk FROM %s WHERE v > ?", large)));
            assertEquals(List.of(2), keys(execute("SELECT pk FROM %s WHERE v >= 'b' AND v <= ?", large)));
            flush();
        }
    }

    /**
     * With a plain and a normalizing index on the same column, only the plain one serves the ranges, whatever the
     * creation order and the names of the indexes.
     */
    @Test
    public void testPlainAndNormalizingIndexes()
    {
        String plain = "WITH OPTIONS = { 'case_sensitive' : 'true' }";
        String normalizing = "WITH OPTIONS = { 'case_sensitive' : 'false', 'equals_behaviour_when_analyzed': 'unsupported' }";
        testPlainAndNormalizingIndexes("a1_plain", plain, "b1_normalizing", normalizing);
        testPlainAndNormalizingIndexes("b2_plain", plain, "a2_normalizing", normalizing);
        testPlainAndNormalizingIndexes("a3_normalizing", normalizing, "b3_plain", plain);
        testPlainAndNormalizingIndexes("b4_normalizing", normalizing, "a4_plain", plain);
    }

    private void testPlainAndNormalizingIndexes(String firstName, String firstOptions, String secondName, String secondOptions)
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX " + firstName + " ON %s(v) USING 'StorageAttachedIndex' " + firstOptions);
        createIndex("CREATE CUSTOM INDEX " + secondName + " ON %s(v) USING 'StorageAttachedIndex' " + secondOptions);
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "Paris");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "paris");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "Rome");

        for (int i = 0; i < 3; i++)
        {
            assertEquals(List.of(2), keys(execute("SELECT pk FROM %s WHERE v >= 'p'")));
            // served by the plain index, on the raw values
            assertEquals(List.of(2, 3), keys(execute("SELECT pk FROM %s WHERE v != 'Paris'")));
            assertEquals(List.of(1, 2), keys(execute("SELECT pk FROM %s WHERE v : 'PARIS'")));
            flushOrCompact(i);
        }
    }

    /**
     * Indexes with an analyzer don't serve ranges and inequalities, as the results of a query must not depend on the
     * indexes on the column: without one of those, they compare the raw values, as without any index.
     */
    @Test
    public void testAnalyzedIndexes()
    {
        for (String options : new String[]{ "{ 'case_sensitive' : 'false' }",
                                            "{ 'normalize' : 'true' }",
                                            "{ 'ascii' : 'true' }",
                                            "{ 'index_analyzer' : 'standard' }" })
        {
            createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = " + options);
            execute("INSERT INTO %s (pk, v) VALUES (1, 'Paris')");
            execute("INSERT INTO %s (pk, v) VALUES (2, 'paris')");

            for (int i = 0; i < 3; i++)
            {
                for (String range : new String[]{ "v > 'p'", "v >= 'p'", "v < 'p'", "v <= 'p'", "v >= 'a' AND v < 'z'" })
                    assertInvalidMessage("has an index but does not support the operators specified in the query",
                                         "SELECT pk FROM %s WHERE " + range);

                // nor the inequalities, which exclude the raw value and not the rows with one of its terms
                for (String inequality : new String[]{ "v != 'paris'", "v NOT IN ('paris', 'rome')" })
                    assertInvalidMessage("has an index but does not support the operators specified in the query",
                                         "SELECT pk FROM %s WHERE " + inequality);

                assertEquals(options, List.of(2), keys(execute("SELECT pk FROM %s WHERE v >= 'p' ALLOW FILTERING")));
                assertEquals(options, List.of(1), keys(execute("SELECT pk FROM %s WHERE v < 'p' ALLOW FILTERING")));
                assertEquals(options, List.of(1), keys(execute("SELECT pk FROM %s WHERE v != 'paris' ALLOW FILTERING")));
                flushOrCompact(i);
            }
        }
    }

    /**
     * The clustering restrictions of a single-partition query are clustering slices on the raw values, also with an
     * analyzed index on the column, which doesn't serve the ranges.
     */
    @Test
    public void testClusteringColumnAnalyzedIndex()
    {
        for (String order : new String[]{ "ASC", "DESC" })
        {
            createTable("CREATE TABLE %s (k int, pk int, v text, PRIMARY KEY (k, v)) WITH CLUSTERING ORDER BY (v " + order + ')');
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : 'false' }");
            execute("INSERT INTO %s (k, pk, v) VALUES (1, 1, 'Paris')");
            execute("INSERT INTO %s (k, pk, v) VALUES (1, 2, 'paris')");
            execute("INSERT INTO %s (k, pk, v) VALUES (2, 3, 'rome')");

            for (int i = 0; i < 3; i++)
            {
                assertEquals(order, List.of(2), keys(execute("SELECT pk FROM %s WHERE k = 1 AND v >= 'p'")));
                assertEquals(order, List.of(1), keys(execute("SELECT pk FROM %s WHERE k = 1 AND v < 'p'")));
                assertEquals(order, List.of(2, 3), keys(execute("SELECT pk FROM %s WHERE v >= 'p' ALLOW FILTERING")));
                assertInvalidThrow(InvalidRequestException.class, "SELECT pk FROM %s WHERE v >= 'p'");
                flushOrCompact(i);
            }
        }
    }

    @Test
    public void testUnsupportedIndexes()
    {
        // tokenizing analyzers break the 1:1 mapping between values and terms
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "a b");
        assertInvalidMessage("has an index but does not support the operators specified in the query",
                             "SELECT pk FROM %s WHERE v > 'a'");

        // frozen collections are indexed as their serialized bytes, which aren't in the CQL order
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, l frozen<list<text>>)");
        createIndex("CREATE CUSTOM INDEX ON %s(FULL(l)) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, l) VALUES (?, ?)", 1, list("a"));
        assertInvalidMessage("has an index but does not support the operators specified in the query",
                             "SELECT pk FROM %s WHERE l > ['a']");
    }

    private void insertRandomRows(Map<Integer, String> rows, int from, int to)
    {
        for (int pk = from; pk < to; pk++)
        {
            String value = randomString();
            rows.put(pk, value);
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, value);
        }
    }

    private void verifyRandomizedRanges(Map<Integer, String> rows)
    {
        List<String> values = new ArrayList<>(rows.values());
        for (int i = 0; i < RANDOM_QUERIES; i++)
        {
            // draw bounds both from the indexed values and from random strings
            String lower = random.nextBoolean() ? values.get(random.nextInt(values.size())) : randomString();
            String upper = random.nextBoolean() ? values.get(random.nextInt(values.size())) : randomString();
            // ordered bounds: the ranges with a lower bound above the upper one are not specific to these types
            if (ByteBufferUtil.compareUnsigned(UTF8Type.instance.decompose(lower), UTF8Type.instance.decompose(upper)) > 0)
            {
                String swap = lower;
                lower = upper;
                upper = swap;
            }
            // equal bounds are both inclusive: otherwise the range is empty, as with unordered bounds
            boolean equalBounds = lower.equals(upper);
            String lowerOp = equalBounds || random.nextBoolean() ? ">=" : ">";
            String upperOp = equalBounds || random.nextBoolean() ? "<=" : "<";
            int kind = random.nextInt(3);

            List<Integer> expected = new ArrayList<>();
            for (Map.Entry<Integer, String> entry : rows.entrySet())
            {
                ByteBuffer term = UTF8Type.instance.decompose(entry.getValue());
                boolean matches = (kind == 2 || satisfies(term, lowerOp, UTF8Type.instance.decompose(lower)))
                                  && (kind == 1 || satisfies(term, upperOp, UTF8Type.instance.decompose(upper)));
                if (matches)
                    expected.add(entry.getKey());
            }

            UntypedResultSet result;
            String query;
            if (kind == 0)
                result = execute(query = "SELECT pk FROM %s WHERE v " + lowerOp + " ? AND v " + upperOp + " ?", lower, upper);
            else if (kind == 1)
                result = execute(query = "SELECT pk FROM %s WHERE v " + lowerOp + " ?", lower);
            else
                result = execute(query = "SELECT pk FROM %s WHERE v " + upperOp + " ?", upper);

            expected.sort(Integer::compareTo);
            assertEquals("Seed: " + SEED + ", query: " + query + ", lower: " + lower + ", upper: " + upper, expected, keys(result));
        }
    }

    /**
     * Range check with the same semantics as the index, i.e. on the unsigned bytes of the values.
     */
    private static boolean satisfies(ByteBuffer term, String operator, ByteBuffer bound)
    {
        int cmp = ByteBufferUtil.compareUnsigned(term, bound);
        switch (operator)
        {
            case ">": return cmp > 0;
            case ">=": return cmp >= 0;
            case "<": return cmp < 0;
            case "<=": return cmp <= 0;
            default: throw new AssertionError(operator);
        }
    }

    private static List<Integer> keys(UntypedResultSet result)
    {
        List<Integer> keys = new ArrayList<>();
        for (UntypedResultSet.Row row : result)
            keys.add(row.getInt("pk"));
        keys.sort(Integer::compareTo);
        return keys;
    }

    private String randomString()
    {
        int length = 1 + random.nextInt(6);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
            sb.append(ALPHABET[random.nextInt(ALPHABET.length)]);
        return sb.toString();
    }
}
