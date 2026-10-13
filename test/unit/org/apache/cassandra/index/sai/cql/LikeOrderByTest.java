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

    /** Value alphabet: ascii, 2-byte and 4-byte UTF-8 characters, and '%' and '_' as literal value characters. */
    private static final String[] ALPHABET = { "a", "b", "c", "é", "😀", "%", "_" };

    /** Pattern segment alphabet: no '%'. */
    private static final String[] SEGMENT_ALPHABET = { "a", "b", "c", "é", "😀", "_" };

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
                String pattern = randomPattern(random);
                boolean descending = random.nextBoolean();
                int limit = 1 + random.nextInt(30);

                List<String> expected = new ArrayList<>();
                for (String v : model.values())
                    if (likeMatches(v, pattern))
                        expected.add(v);
                expected.sort(descending ? UNSIGNED_BYTES.reversed() : UNSIGNED_BYTES);
                if (expected.size() > limit)
                    expected = expected.subList(0, limit);

                String query = "SELECT v FROM %s WHERE v LIKE ? ORDER BY v " + (descending ? "DESC" : "ASC") + " LIMIT " + limit;
                String message = "round " + round + ": " + query.replace("?", '\'' + pattern + '\'');

                List<String> unpaged = new ArrayList<>();
                for (UntypedResultSet.Row row : execute(query, pattern))
                    unpaged.add(row.getString("v"));
                assertEquals(message, expected, unpaged);

                List<String> paged = new ArrayList<>();
                for (com.datastax.driver.core.Row row : executeNetWithPaging(query, 3, pattern))
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

    private static String randomPattern(Random random)
    {
        String a = randomString(random, SEGMENT_ALPHABET, 2);
        String b = randomString(random, SEGMENT_ALPHABET, 2);
        switch (random.nextInt(6))
        {
            case 0: return a + '%';
            case 1: return '%' + a;
            case 2: return '%' + a + '%';
            case 3: return a + '%' + b;
            case 4: return '%' + a + '%' + b;
            default: return a;
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

    /** Reference implementation of CQL LIKE semantics: every '%' is an any-string wildcard, no escapes, '_' literal. */
    private static boolean likeMatches(String value, String pattern)
    {
        String[] segments = pattern.split("%", -1);
        StringBuilder regex = new StringBuilder();
        for (int i = 0; i < segments.length; i++)
        {
            if (i > 0)
                regex.append(".*");
            regex.append(Pattern.quote(segments[i]));
        }
        return Pattern.compile(regex.toString(), Pattern.DOTALL).matcher(value).matches();
    }
}
