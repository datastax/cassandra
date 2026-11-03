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
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.analyzer.AnalyzerEqOperatorSupport;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

/**
 * Tests range ({@code <}, {@code <=}, {@code >}, {@code >=}) queries on {@code text}/{@code ascii} columns with a
 * non-tokenized SAI index, the CQL counterpart of the Solr term range queries ({@code field:[a TO b]},
 * {@code field:{a TO b}}, {@code field:[a TO *]}...), across the memtable-only / flushed / compacted matrix.
 * The order is the binary (unsigned byte) order of the, possibly normalized, UTF-8 values.
 */
public class LiteralRangeQueryTest extends SAITester
{
    /** The seed of the randomized tests, which can be fixed with the {@link CassandraRelevantProperties#TEST_RANDOM_SEED} property. */
    private static final long SEED = CassandraRelevantProperties.TEST_RANDOM_SEED.getLong(System.nanoTime());
    private static final String[] ALPHABET = { "a", "b", "ab", "abc", "z", "A", "B", "Ab", " ", "0", "é", "É", "ß", "日", "😀" };

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
            assertEquals(List.of(), keys(execute("SELECT pk FROM %s WHERE v > 'b' AND v < 'b'")));
            assertEquals(List.of(), keys(execute("SELECT pk FROM %s WHERE v >= 'c' AND v <= 'b'")));
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

    @Test
    public void testEmptyStringValueCaseInsensitive()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : 'false' }");

        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "A");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "b");

        for (int i = 0; i < 3; i++)
        {
            assertEquals(List.of(1, 2, 3), keys(execute("SELECT pk FROM %s WHERE v >= ''")));
            assertEquals(List.of(2, 3), keys(execute("SELECT pk FROM %s WHERE v > ''")));
            assertEquals(List.of(1, 2), keys(execute("SELECT pk FROM %s WHERE v < 'B'")));
            assertEquals(List.of(1), keys(execute("SELECT pk FROM %s WHERE v <= ''")));

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
     * The values excluded by {@code !=} and {@code NOT IN} on a column with a case-insensitive index are normalized as
     * the range bounds are, whether the exclusions are combined with a range the index serves, or alone, filtering
     * without any index search or post-filtering the rows of a search on another column: all the forms agree.
     */
    @Test
    public void testExclusionsCaseInsensitive()
    {
        for (String type : new String[]{ "text", "ascii" })
        {
            String raw = createExclusionTables(type, "{ 'case_sensitive' : 'false' }", "Rome", "Paris", "Turin", "Milan");

            for (int i = 0; i < 3; i++)
            {
                // combined with a range served by the index
                assertSelected(List.of(2, 3), "v >= 'p' AND v != 'ROME'");
                assertSelected(List.of(2, 3), "v >= 'p' AND v != 'rome'");
                assertSelected(List.of(2, 3), "v >= 'p' AND v != 'Rome'");
                assertSelected(List.of(2), "v >= 'P' AND v NOT IN ('ROME', 'turin')");
                assertSelected(List.of(4), "v < 'R' AND v != 'PARIS'");
                assertSelected(List.of(3, 4), "v > 'a' AND v < 'z' AND v NOT IN ('ROME', 'paris')");
                assertSelected(List.of(2), "v >= 'p' AND v != 'ROME' AND v != 'TURIN'");
                assertEquals(List.of(2, 3), keys(execute("SELECT pk FROM %s WHERE v >= ? AND v != ?", "P", "ROME")));
                assertEquals(List.of(2), keys(execute("SELECT pk FROM %s WHERE v >= 'p' AND v NOT IN ?", list("ROME", "turin"))));
                // the rows the index search returns are already filtered, so that a limit doesn't count excluded rows
                for (int limit = 1; limit <= 3; limit++)
                {
                    assertEquals(Math.min(limit, 2), execute("SELECT pk FROM %s WHERE v >= 'p' AND v != 'ROME' LIMIT " + limit).size());
                    assertEquals(Math.min(limit, 3), execute("SELECT pk FROM %s WHERE idx = 1 AND v != 'ROME' LIMIT " + limit + " ALLOW FILTERING").size());
                }

                // alone, without any index search, and post-filtering the search on another column
                assertExclusionAlone(List.of(2, 3, 4), "v != 'ROME'");
                assertExclusionAlone(List.of(2, 3, 4), "v != 'rome'");
                assertExclusionAlone(List.of(3, 4), "v NOT IN ('ROME', 'paris')");
                assertExclusionAlone(List.of(3, 4), "v != 'ROME' AND v != 'paris'");
                assertEquals(List.of(2, 3, 4), keys(execute("SELECT pk FROM %s WHERE v != ? ALLOW FILTERING", "ROME")));
                assertEquals(List.of(3, 4), keys(execute("SELECT pk FROM %s WHERE v NOT IN ? ALLOW FILTERING", list("ROME", "paris"))));
                assertEquals(List.of(3, 4), keys(execute("SELECT pk FROM %s WHERE idx = 1 AND v NOT IN ? ALLOW FILTERING", list("ROME", "paris"))));

                // without the index, the raw values
                assertRawRows(List.of(1, 2, 3, 4), raw, "v != 'ROME'");
                assertRawRows(List.of(2, 3, 4), raw, "v != 'Rome'");
                assertRawRows(List.of(1, 3, 4), raw, "v NOT IN ('ROME', 'Paris')");
                assertRawRows(List.of(1, 3, 4), raw, "idx = 1 AND v NOT IN ('ROME', 'Paris')");
                assertRawRows(List.of(1, 2, 3), raw, "v >= 'P' AND v != 'ROME'");
                flushOrCompact(i);
            }
        }
    }

    /**
     * As {@link #testExclusionsCaseInsensitive()} with an index normalizing the values to the Unicode NFC form.
     */
    @Test
    public void testExclusionsNormalized()
    {
        // composed and decomposed forms of the same string
        String composed = "caf\u00e9", decomposed = "cafe\u0301";
        String raw = createExclusionTables("text", "{ 'normalize' : 'true' }", composed, decomposed, "tea", "Caf\u00e9");

        for (int i = 0; i < 3; i++)
        {
            assertSelected(List.of(3), "v >= 'c' AND v != '" + composed + '\'');
            assertSelected(List.of(3), "v >= 'c' AND v NOT IN ('" + decomposed + "')");
            assertSelected(List.of(1, 2), "v >= 'c' AND v != 'tea'");

            assertExclusionAlone(List.of(3, 4), "v != '" + composed + '\'');
            assertExclusionAlone(List.of(3, 4), "v != '" + decomposed + '\'');
            assertExclusionAlone(List.of(3), "v NOT IN ('" + decomposed + "', 'Caf\u00e9')");

            assertRawRows(List.of(2, 3, 4), raw, "v != '" + composed + '\'');
            assertRawRows(List.of(1, 3, 4), raw, "idx = 1 AND v != '" + decomposed + '\'');
            flushOrCompact(i);
        }
    }

    /**
     * As {@link #testExclusionsCaseInsensitive()} with an index folding the values to ASCII.
     */
    @Test
    public void testExclusionsAscii()
    {
        String raw = createExclusionTables("text", "{ 'ascii' : 'true' }", "caf\u00e9", "cafe", "tea");

        for (int i = 0; i < 3; i++)
        {
            assertSelected(List.of(3), "v >= 'c' AND v != 'cafe'");
            assertSelected(List.of(3), "v >= 'c' AND v NOT IN ('caf\u00e9')");

            assertExclusionAlone(List.of(3), "v != 'caf\u00e9'");
            assertExclusionAlone(List.of(3), "v NOT IN ('cafe', 'x')");

            assertRawRows(List.of(1, 3), raw, "v != 'cafe'");
            assertRawRows(List.of(2, 3), raw, "idx = 1 AND v != 'caf\u00e9'");
            flushOrCompact(i);
        }
    }

    /**
     * Tokenizing indexes serve no ranges, and the values excluded on their columns are not analyzed: {@code !=} keeps
     * comparing the raw values, as excluding the values having one of the tokens of the excluded value would exclude
     * more than the excluded value.
     */
    @Test
    public void testExclusionsTokenizingAnalyzer()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, idx int, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(idx) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        execute("INSERT INTO %s (pk, idx, v) VALUES (?, ?, ?)", 1, 1, "Rome Paris");
        execute("INSERT INTO %s (pk, idx, v) VALUES (?, ?, ?)", 2, 1, "Rome");
        execute("INSERT INTO %s (pk, idx, v) VALUES (?, ?, ?)", 3, 1, "Milan");

        for (int i = 0; i < 3; i++)
        {
            assertExclusionAlone(List.of(1, 3), "v != 'Rome'");
            assertExclusionAlone(List.of(1, 2, 3), "v != 'rome'");
            assertExclusionAlone(List.of(3), "v NOT IN ('Rome', 'Rome Paris')");
            flushOrCompact(i);
        }
    }

    /**
     * Creates two tables {@code (pk, idx, v)} with the given values of {@code v}, the keys from 1, {@code idx = 1} and
     * an index on {@code idx}: one also with an index on {@code v} with the given options, which becomes the current
     * table, and one without, whose name is returned.
     */
    private String createExclusionTables(String type, String options, String... values)
    {
        String raw = null;
        for (boolean indexed : new boolean[]{ false, true })
        {
            String table = createTable("CREATE TABLE %s (pk int PRIMARY KEY, idx int, v " + type + ')');
            createIndex("CREATE CUSTOM INDEX ON %s(idx) USING 'StorageAttachedIndex'");
            if (indexed)
                createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = " + options);
            else
                raw = table;
            for (int pk = 1; pk <= values.length; pk++)
                execute("INSERT INTO %s (pk, idx, v) VALUES (?, 1, ?)", pk, values[pk - 1]);
        }
        return raw;
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

    /**
     * Asserts the rows selected by the given exclusions on {@code v} filtering without any index search, and
     * post-filtering the search of {@code idx = 1}.
     */
    private void assertExclusionAlone(List<Integer> expected, String where)
    {
        assertSelected(expected, where + " ALLOW FILTERING");
        assertSelected(expected, "idx = 1 AND " + where + " ALLOW FILTERING");
    }

    private void assertRawRows(List<Integer> expected, String table, String where)
    {
        String query = "SELECT pk FROM " + KEYSPACE + '.' + table + " WHERE " + where + " ALLOW FILTERING";
        assertEquals(query, expected, keys(executeFormattedQuery(query)));
    }

    /**
     * The conditions of the lightweight transactions don't use the index analyzers, and warn about it: also those
     * with {@code !=} on a column with a normalizing index, which still compare the raw values.
     */
    @Test
    public void testExclusionConditionsWarning()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, idx int, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : 'false' }");
        execute("INSERT INTO %s (pk, idx, v) VALUES (1, 1, 'Rome')");

        String warning = String.format(AnalyzerEqOperatorSupport.LWT_CONDITION_ON_ANALYZED_WARNING, "v");
        assertEquals(List.of(warning), lwtWarnings("UPDATE %s SET idx = 2 WHERE pk = 1 IF v != 'ROME'", true));
        assertEquals(List.of(warning), lwtWarnings("UPDATE %s SET idx = 3 WHERE pk = 1 IF v != 'Rome'", false));

        // tokenizing indexes don't analyze the excluded values
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, idx int, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        execute("INSERT INTO %s (pk, idx, v) VALUES (1, 1, 'Rome')");
        assertEquals(List.of(), lwtWarnings("UPDATE %s SET idx = 2 WHERE pk = 1 IF v != 'ROME'", true));
    }

    /**
     * Executes the given conditional update, asserting whether it's applied, and returns its client warnings.
     */
    private List<String> lwtWarnings(String query, boolean applied)
    {
        ClientWarn.instance.captureWarnings();
        try
        {
            assertEquals(query, applied, execute(query).one().getBoolean("[applied]"));
            List<String> warnings = ClientWarn.instance.getWarnings();
            return warnings == null ? List.of() : warnings;
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
        }
    }

    /**
     * A uuid range and {@code !=}: the index serves the range, not the {@code !=}, which is evaluated as a separate
     * expression, with the same results as without the index.
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
            assertEquals(List.of(1, 3), keys(execute("SELECT pk FROM %s WHERE v != false ALLOW FILTERING")));
            assertEquals(List.of(), keys(execute("SELECT pk FROM %s WHERE v NOT IN (true, false) ALLOW FILTERING")));
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

    @Test
    public void testExclusionsClusteringColumnCaseInsensitive()
    {
        testExclusionsClusteringColumnCaseInsensitive("ASC");
    }

    /**
     * As {@link #testExclusionsClusteringColumnCaseInsensitive()}, on a reversed column (whose index currently serves
     * the {@code !=}, unlike that of the ascending column).
     */
    @Test
    public void testExclusionsReversedClusteringColumnCaseInsensitive()
    {
        testExclusionsClusteringColumnCaseInsensitive("DESC");
    }

    private void testExclusionsClusteringColumnCaseInsensitive(String order)
    {
        createTable("CREATE TABLE %s (k int, pk int, v text, PRIMARY KEY (k, v)) WITH CLUSTERING ORDER BY (v " + order + ')');
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : 'false' }");
        execute("INSERT INTO %s (k, pk, v) VALUES (?, ?, ?)", 1, 1, "Rome");
        execute("INSERT INTO %s (k, pk, v) VALUES (?, ?, ?)", 1, 2, "Paris");
        execute("INSERT INTO %s (k, pk, v) VALUES (?, ?, ?)", 2, 3, "Turin");
        execute("INSERT INTO %s (k, pk, v) VALUES (?, ?, ?)", 2, 4, "Milan");

        for (int i = 0; i < 3; i++)
        {
            // The restrictions on clustering columns are also applied as clustering slices, which compare the raw
            // values (a known limitation): the bounds select the same values with and without normalization, and
            // the raw exclusions remove no more than the normalized ones. Hence no exclusion alone either, which
            // only the slices would evaluate if the index doesn't serve it.
            assertSelected(List.of(2, 3), "v >= 'P' AND v != 'ROME'");
            assertSelected(List.of(2, 3), "v >= 'P' AND v != 'rome'");
            assertSelected(List.of(4), "v < 'R' AND v NOT IN ('PARIS')");
            assertSelected(List.of(3), "v > 'A' AND v < 'Z' AND v NOT IN ('paris', 'ROME', 'milan')");
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
    public void testCaseInsensitive()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : 'false' }");

        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "Apple");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "apricot");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "Banana");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 4, "banana");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 5, "CHERRY");

        for (int i = 0; i < 3; i++)
        {
            // both the indexed terms and the bounds are lowercased
            assertEquals(List.of(3, 4), keys(execute("SELECT pk FROM %s WHERE v >= 'B' AND v < 'C'")));
            assertEquals(List.of(3, 4), keys(execute("SELECT pk FROM %s WHERE v >= 'b' AND v < 'c'")));
            assertEquals(List.of(1, 2), keys(execute("SELECT pk FROM %s WHERE v < 'BANANA'")));
            assertEquals(List.of(1, 2, 3, 4), keys(execute("SELECT pk FROM %s WHERE v <= 'BANANA'")));
            assertEquals(List.of(5), keys(execute("SELECT pk FROM %s WHERE v > 'banana'")));
            assertEquals(List.of(2, 3, 4), keys(execute("SELECT pk FROM %s WHERE v > 'APPLE' AND v <= 'Banana'")));

            if (i == 0)
                flush();
            else if (i == 1)
                compact();
        }
    }

    @Test
    public void testRandomizedRanges()
    {
        testRandomizedRanges("{}");
    }

    @Test
    public void testRandomizedRangesCaseInsensitive()
    {
        testRandomizedRanges("{ 'case_sensitive' : 'false' }");
    }

    @Test
    public void testRandomizedRangesNormalized()
    {
        testRandomizedRanges("{ 'normalize' : 'true', 'case_sensitive' : 'false', 'ascii' : 'true' }");
    }

    private void testRandomizedRanges(String options)
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = " + options);
        AbstractAnalyzer.AnalyzerFactory analyzer = getIndexContext(index).getQueryAnalyzerFactory();

        Map<Integer, String> rows = new HashMap<>();

        // memtable-only
        insertRandomRows(rows, 0, 100);
        verifyRandomizedRanges(rows, analyzer);

        // flushed sstable + memtable
        flush();
        insertRandomRows(rows, 100, 200);
        verifyRandomizedRanges(rows, analyzer);

        // two sstables
        flush();
        verifyRandomizedRanges(rows, analyzer);

        // compacted
        compact();
        verifyRandomizedRanges(rows, analyzer);
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
            assertRows(execute("SELECT v FROM %s WHERE v < 'b' ORDER BY v DESC LIMIT 10"), row("a"), row(""));
            assertRows(execute("SELECT v FROM %s WHERE v > '' AND v < 'b' ORDER BY v DESC LIMIT 10"), row("a"));
            assertRows(execute("SELECT v FROM %s WHERE v <= '' ORDER BY v ASC LIMIT 10"), row(""));
            assertRowCount(execute("SELECT v FROM %s WHERE v < '' ORDER BY v ASC LIMIT 10"), 0);
            assertRows(execute("SELECT v FROM %s WHERE v > 'aa' AND v < 'bb' ORDER BY v DESC LIMIT 10"), row("b"));
            assertRowCount(execute("SELECT v FROM %s WHERE v > 'f' AND v < 'b' ORDER BY v ASC LIMIT 10"), 0);
            assertRowCount(execute("SELECT v FROM %s WHERE v > 'f' AND v < 'f' ORDER BY v DESC LIMIT 10"), 0);

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

    @Test
    public void testAcrossIndexVersions()
    {
        try
        {
            for (Version version : Version.ALL)
            {
                SAIUtil.setCurrentVersion(version);
                createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
                createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "a");
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "ab");
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "abc");
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 4, "ac");
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 5, "b");
                flush();
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 6, "abd");
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 7, "ac");

                String message = "Version " + version;
                assertEquals(message, List.of(2, 3, 4, 6, 7), keys(execute("SELECT pk FROM %s WHERE v >= 'ab' AND v <= 'ac'")));
                assertEquals(message, List.of(3, 6), keys(execute("SELECT pk FROM %s WHERE v > 'ab' AND v < 'ac'")));
                assertEquals(message, List.of(1, 2), keys(execute("SELECT pk FROM %s WHERE v < 'abc'")));
                assertEquals(message, List.of(4, 5, 7), keys(execute("SELECT pk FROM %s WHERE v > 'abd'")));
            }
        }
        finally
        {
            SAIUtil.resetCurrentVersion();
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

    private void verifyRandomizedRanges(Map<Integer, String> rows, AbstractAnalyzer.AnalyzerFactory analyzer)
    {
        List<String> values = new ArrayList<>(rows.values());
        for (int i = 0; i < 40; i++)
        {
            // draw bounds both from the indexed values and from random strings
            String lower = random.nextBoolean() ? values.get(random.nextInt(values.size())) : randomString();
            String upper = random.nextBoolean() ? values.get(random.nextInt(values.size())) : randomString();
            String lowerOp = random.nextBoolean() ? ">" : ">=";
            String upperOp = random.nextBoolean() ? "<" : "<=";
            int kind = random.nextInt(3);

            List<Integer> expected = new ArrayList<>();
            for (Map.Entry<Integer, String> entry : rows.entrySet())
            {
                ByteBuffer term = analyze(analyzer, entry.getValue());
                boolean matches = (kind == 2 || satisfies(term, lowerOp, analyze(analyzer, lower)))
                                  && (kind == 1 || satisfies(term, upperOp, analyze(analyzer, upper)));
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
     * Range check with the same semantics as the index, i.e. on the unsigned bytes of the normalized terms.
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

    private static ByteBuffer analyze(AbstractAnalyzer.AnalyzerFactory factory, String value)
    {
        AbstractAnalyzer analyzer = factory.create();
        try
        {
            analyzer.reset(UTF8Type.instance.decompose(value));
            return analyzer.hasNext() ? analyzer.next() : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        }
        finally
        {
            analyzer.end();
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
        int length = 1 + random.nextInt(4);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
            sb.append(ALPHABET[random.nextInt(ALPHABET.length)]);
        return sb.toString();
    }
}
