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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;

/**
 * Tests {@code LIKE} restrictions on the column a query is ordered by ({@code ORDER BY}), whose ordering scans are
 * restricted to the terms the pattern can match, against a client-side model: every LIKE variant, ascending and
 * descending, multibyte UTF-8 values, data spread over several sstables and the memtable, overwrites and deletions,
 * with and without paging.
 */
public class LikeOrderByTest extends SAITester
{
    private static final Logger logger = LoggerFactory.getLogger(LikeOrderByTest.class);

    /** The seed of the randomized tests, which can be fixed with the {@link CassandraRelevantProperties#TEST_RANDOM_SEED} property. */
    private static final long SEED = CassandraRelevantProperties.TEST_RANDOM_SEED.getLong(System.nanoTime());

    /** Reports the seed with any failure of a test, including unexpected exceptions, not only assertion messages. */
    @Rule
    public final TestRule reportSeed = (base, description) -> new Statement()
    {
        @Override
        public void evaluate() throws Throwable
        {
            logger.info("{} random seed: -D{}={}", description.getMethodName(), CassandraRelevantProperties.TEST_RANDOM_SEED.getKey(), SEED);
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

    /** Value alphabet: ascii, 2-byte and 4-byte UTF-8 characters, and the wildcards and escapes as literals. */
    private static final String[] ALPHABET = { "a", "b", "c", "é", "😀", "%", "_", "\\", "!" };

    /** The escape characters of the patterns, {@code null} for no {@code ESCAPE} clause. */
    private static final String[] ESCAPES = { null, "\\", "!", "é" };

    private static final Comparator<String> UNSIGNED_BYTES = (x, y) -> Arrays.compareUnsigned(x.getBytes(StandardCharsets.UTF_8),
                                                                                             y.getBytes(StandardCharsets.UTF_8));

    @BeforeClass
    public static void setUpNetwork()
    {
        requireNetwork();
    }

    /**
     * With the query optimizer disabled, the plan keeps the LIKE restriction on the ordering index scan, so the
     * ordering scans are restricted by the pattern.
     */
    @Test
    public void testRandomizedWithoutQueryOptimization()
    {
        disableQueryOptimization();
        testRandomized();
    }

    /**
     * With the query optimizer, the plan may also order the whole index and post-filter the rows.
     */
    @Test
    public void testRandomizedWithQueryOptimization()
    {
        testRandomized();
    }

    private void testRandomized()
    {
        Random random = new Random(SEED);
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        Map<Integer, String> model = new HashMap<>();
        // the first two rounds end with a flush: the data ends up on two sstables and the memtable, with rows
        // overwritten or deleted across them
        for (int round = 0; round < 3; round++)
        {
            for (int i = 0; i < 120; i++)
            {
                int pk = random.nextInt(250);
                if (random.nextInt(10) == 0)
                {
                    execute("DELETE FROM %s WHERE pk = ?", pk);
                    model.remove(pk);
                }
                else
                {
                    String v = randomString(random, ALPHABET, 5);
                    execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, v);
                    model.put(pk, v);
                }
            }
            if (round < 2)
                flush();

            for (int q = 0; q < 40; q++)
            {
                String escape = ESCAPES[random.nextInt(ESCAPES.length)];
                LikePattern pattern = randomPattern(random, escape);
                boolean descending = random.nextBoolean();
                int limit = 1 + random.nextInt(30);

                List<String> expected = new ArrayList<>();
                for (String v : model.values())
                    if (pattern.regex.matcher(v).matches())
                        expected.add(v);
                expected.sort(descending ? UNSIGNED_BYTES.reversed() : UNSIGNED_BYTES);
                if (expected.size() > limit)
                    expected = expected.subList(0, limit);

                String query = "SELECT v FROM %s WHERE v LIKE ?" + (escape == null ? "" : " ESCAPE ?")
                               + " ORDER BY v " + (descending ? "DESC" : "ASC") + " LIMIT " + limit;
                Object[] values = escape == null ? new Object[]{ pattern.text.toString() } : new Object[]{ pattern.text.toString(), escape };
                String message = "round " + round + ": " + query + " with " + Arrays.toString(values);

                List<String> unpaged = new ArrayList<>();
                for (UntypedResultSet.Row row : execute(query, values))
                    unpaged.add(row.getString("v"));
                assertEquals(message, expected, unpaged);

                List<String> paged = new ArrayList<>();
                for (com.datastax.driver.core.Row row : executeNetWithPaging(query, 3, values))
                    paged.add(row.getString("v"));
                assertEquals(message + " (paged)", expected, paged);
            }
        }
    }

    /**
     * A wildcard-free pattern matches a single term, like an equality: the ordered query returns the rows of that
     * term only, however many terms start with it, on memtables and sstables alike.
     */
    @Test
    public void testExactPatternMatch() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        for (int pk = 0; pk < 200; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, String.format("a%03d", pk));
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1000, "a");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1001, "a");

        beforeAndAfterFlush(() -> {
            for (String order : new String[]{ "ASC", "DESC" })
            {
                assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'a' ORDER BY v " + order + " LIMIT 10"),
                                        row(1000), row(1001));
                assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'a050' ORDER BY v " + order + " LIMIT 10"),
                                        row(50));
                assertEquals(1, execute("SELECT pk FROM %s WHERE v LIKE 'a' ORDER BY v " + order + " LIMIT 1").size());
                assertEquals(0, execute("SELECT pk FROM %s WHERE v LIKE 'a0' ORDER BY v " + order + " LIMIT 10").size());
            }
        });
    }

    /** A LIKE pattern, as written in a query, and the regular expression matching the same values. */
    private static final class LikePattern
    {
        final StringBuilder text = new StringBuilder();
        final StringBuilder regexText = new StringBuilder();
        Pattern regex;

        void anyString()
        {
            text.append('%');
            regexText.append(".*");
        }
    }

    /**
     * Returns a random pattern of one of the LIKE variants, whose segments mix literals (wildcards and the escape
     * character escaped when there is an escape character) and '_' wildcards.
     */
    private static LikePattern randomPattern(Random random, String escape)
    {
        LikePattern pattern = new LikePattern();
        switch (random.nextInt(6))
        {
            case 0: segment(random, escape, pattern); pattern.anyString(); break;
            case 1: pattern.anyString(); segment(random, escape, pattern); break;
            case 2: pattern.anyString(); segment(random, escape, pattern); pattern.anyString(); break;
            case 3: segment(random, escape, pattern); pattern.anyString(); segment(random, escape, pattern); break;
            case 4: pattern.anyString(); segment(random, escape, pattern); pattern.anyString(); segment(random, escape, pattern); break;
            default: segment(random, escape, pattern);
        }
        pattern.regex = Pattern.compile(pattern.regexText.toString(), Pattern.DOTALL);
        return pattern;
    }

    private static void segment(Random random, String escape, LikePattern pattern)
    {
        int length = 1 + random.nextInt(2);
        for (int i = 0; i < length; i++)
        {
            if (random.nextInt(4) == 0)
            {
                pattern.text.append('_');
                pattern.regexText.append('.');
                continue;
            }
            String literal = ALPHABET[random.nextInt(ALPHABET.length)];
            boolean special = literal.equals("%") || literal.equals("_") || literal.equals(escape);
            if (special && escape == null)
                literal = "a";
            else if (special)
                pattern.text.append(escape);
            pattern.text.append(literal);
            pattern.regexText.append(Pattern.quote(literal));
        }
    }

    private static String randomString(Random random, String[] alphabet, int maxLength)
    {
        int length = 1 + random.nextInt(maxLength);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
            sb.append(alphabet[random.nextInt(alphabet.length)]);
        return sb.toString();
    }
}
