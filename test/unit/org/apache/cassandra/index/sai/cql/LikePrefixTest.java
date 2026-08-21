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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

/**
 * Tests {@code LIKE '<prefix>%'} queries on string columns with a non-analyzed SAI index across the
 * memtable-only / flushed / compacted matrix, and verifies that the unsupported LIKE variants are rejected
 * with a clear error message.
 */
public class LikePrefixTest extends SAITester
{
    private static final long SEED = System.nanoTime();
    private static final String[] ALPHABET = { "a", "b", "ab", "abc", "z", " ", "0", "é", "ß", "日", "😀" };

    private final Random random = new Random(SEED);

    @Test
    public void testFixedPrefixes()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "a");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "ab");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "abc");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 4, "abd");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 5, "b");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 6, "ABC");

        for (int i = 0; i < 3; i++)
        {
            // the term equal to the prefix matches, and 'abd' (the exclusive upper bound of the scan) does not
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'ab%%'"), row(2), row(3), row(4));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'abc%%'"), row(3));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'a%%'"), row(1), row(2), row(3), row(4));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'b%%'"), row(5));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'c%%'"));
            // default indexes are case-sensitive
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'AB%%'"), row(6));
            // CQL only allows a single LIKE relation per column
            assertInvalidMessage("cannot be restricted by more than one relation if it includes a",
                                 "SELECT pk FROM %s WHERE v LIKE 'ab%%' AND v LIKE 'abc%%'");

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
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "ab");

        for (int i = 0; i < 3; i++)
        {
            // an indexed empty term never matches a non-empty prefix
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'a%%'"), row(2), row(3));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'ab%%'"), row(3));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'b%%'"));
            // the empty prefix (LIKE '%') remains rejected by the CQL layer even with an indexed empty term
            assertInvalidMessage("LIKE value can't be empty", "SELECT pk FROM %s WHERE v LIKE '%%'");
            assertInvalidMessage("LIKE value can't be empty", "SELECT pk FROM %s WHERE v LIKE ''");

            if (i == 0)
                flush();
            else if (i == 1)
                compact();
        }
    }

    @Test
    public void testUnicodePrefixes()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "école");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "étude");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "日本語");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 4, "日本");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 5, "😀smile");

        for (int i = 0; i < 3; i++)
        {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'é%%'"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'éc%%'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '日本%%'"), row(3), row(4));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '日本語%%'"), row(3));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '😀%%'"), row(5));

            if (i == 0)
                flush();
            else if (i == 1)
                compact();
        }
    }

    @Test
    public void testRandomizedPrefixes()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        Map<Integer, String> rows = new HashMap<>();

        // memtable-only
        insertRandomRows(rows, 0, 100);
        verifyRandomizedPrefixes(rows);

        // flushed sstable + memtable
        flush();
        insertRandomRows(rows, 100, 200);
        verifyRandomizedPrefixes(rows);

        // two sstables
        flush();
        verifyRandomizedPrefixes(rows);

        // compacted
        compact();
        verifyRandomizedPrefixes(rows);
    }

    @Test
    public void testCaseInsensitivePrefix()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : 'false' }");

        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "Apple");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "apricot");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "APPLE PIE");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 4, "banana");

        for (int i = 0; i < 2; i++)
        {
            // both the indexed terms and the prefix are lowercased
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'AP%%'"), row(1), row(2), row(3));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'apple%%'"), row(1), row(3));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'Apple P%%'"), row(3));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'B%%'"), row(4));

            if (i == 0)
                flush();
        }
    }

    @Test
    public void testNonPrefixLikeVariantsAreServed()
    {
        // The non-prefix LIKE variants are served by automaton intersection with the terms dictionary;
        // see AutomatonQueryTest for exhaustive coverage.
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "abc");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "aXc");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "x");

        // suffix
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%c'"), row(1), row(2));
        // contains
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%x%%'"), row(3));
        // matches (no wildcard = exact match; '%' in the middle is an any-string wildcard)
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'x'"), row(3));
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'a%%c'"), row(1), row(2));

        // an empty prefix (LIKE '%%') is still rejected by the CQL layer
        assertInvalidMessage("LIKE value can't be empty", "SELECT pk FROM %s WHERE v LIKE '%%'");
        assertInvalidMessage("LIKE value can't be empty", "SELECT pk FROM %s WHERE v LIKE ''");
    }

    @Test
    public void testAnalyzedIndexRejectsLike()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = " +
                    "{ 'index_analyzer' : '{ \"tokenizer\" : { \"name\" : \"standard\" } }' }");

        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "some text");

        // analyzed indexes don't support LIKE at all...
        assertInvalidMessage(String.format(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_LIKE_MESSAGE, 'v'),
                             "SELECT pk FROM %s WHERE v LIKE 'some%%'");
        assertInvalidMessage(String.format(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_LIKE_MESSAGE, 'v'),
                             "SELECT pk FROM %s WHERE v LIKE '%%text'");
    }

    @Test
    public void testReversedClusteringColumnRejectsLike()
    {
        // Reversed (DESC clustering) literal columns store their kd-tree bounds byte-inverted, which does not
        // match the raw bytes literal terms are stored with, so LIKE stays unsupported on them (see
        // IndexContext#supportsPrefixQueries) and keeps the pre-existing filtering path.
        createTable("CREATE TABLE %s (pk int, ck text, v int, PRIMARY KEY (pk, ck)) " +
                    "WITH CLUSTERING ORDER BY (ck DESC)");
        createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1, "abc", 1);

        assertInvalidMessage(String.format(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_LIKE_MESSAGE, "ck"),
                             "SELECT pk FROM %s WHERE ck LIKE 'ab%%' ALLOW FILTERING");
        assertInvalidMessage(String.format(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_LIKE_MESSAGE, "ck"),
                             "SELECT pk FROM %s WHERE ck LIKE '%%bc' ALLOW FILTERING");
    }

    @Test
    public void testCollectionIndexRejectsLike()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, l list<text>)");
        createIndex("CREATE CUSTOM INDEX ON %s(l) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, l) VALUES (?, ?)", 1, list("abc"));

        // LIKE on collections is rejected by the CQL layer before even considering the index
        assertInvalidMessage("cannot be restricted by a 'LIKE' relation",
                             "SELECT pk FROM %s WHERE l LIKE 'a%%'");
        // collections keep working with their supported operators
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE l CONTAINS 'abc'"), row(1));
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

    private void verifyRandomizedPrefixes(Map<Integer, String> rows)
    {
        List<String> values = new ArrayList<>(rows.values());
        for (int i = 0; i < 25; i++)
        {
            // draw prefixes both from the indexed values and from random strings
            String source = random.nextBoolean() ? values.get(random.nextInt(values.size())) : randomString();
            String prefix = source.substring(0, source.offsetByCodePoints(0, 1 + random.nextInt(source.codePointCount(0, source.length()))));

            List<Integer> expected = new ArrayList<>();
            for (Map.Entry<Integer, String> entry : rows.entrySet())
                if (startsWith(entry.getValue(), prefix))
                    expected.add(entry.getKey());

            UntypedResultSet result = execute("SELECT pk FROM %s WHERE v LIKE ?", prefix + '%');
            List<Integer> actual = new ArrayList<>();
            for (UntypedResultSet.Row row : result)
                actual.add(row.getInt("pk"));

            expected.sort(Integer::compareTo);
            actual.sort(Integer::compareTo);
            assertEquals("Seed: " + SEED + ", prefix: " + prefix, expected, actual);
        }
    }

    /**
     * Prefix check with the same semantics as the index, i.e. on the UTF-8 encoded bytes.
     */
    private static boolean startsWith(String value, String prefix)
    {
        return ByteBufferUtil.startsWith(UTF8Type.instance.decompose(value), UTF8Type.instance.decompose(prefix));
    }

    private String randomString()
    {
        int length = 1 + random.nextInt(5);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
            sb.append(ALPHABET[random.nextInt(ALPHABET.length)]);
        return sb.toString();
    }
}
