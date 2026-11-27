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

import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

/**
 * Randomized range queries on a {@code text} column with an SAI index, checked against the values. The tests are
 * split in several classes to keep the run time of each class short.
 */
public abstract class LiteralRangeRandomizedQueryTester extends SAITester.Versioned
{
    /** The seed of the randomized tests, which can be fixed with the {@link CassandraRelevantProperties#TEST_RANDOM_SEED} property. */
    private static final long SEED = CassandraRelevantProperties.TEST_RANDOM_SEED.getLong(System.nanoTime());
    private static final String[] ALPHABET = { "a", "b", "ab", "abc", "z", "A", "B", "Ab", " ", "0", "é", "É", "ß", "日", "😀" };

    private static final int RANDOM_ROWS = 2000;
    private static final int RANDOM_QUERIES = 200;

    protected final Random random = new Random(SEED);

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

    protected void randomizedRanges()
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

    private String randomString()
    {
        int length = 1 + random.nextInt(6);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
            sb.append(ALPHABET[random.nextInt(ALPHABET.length)]);
        return sb.toString();
    }

    protected static List<Integer> keys(UntypedResultSet result)
    {
        List<Integer> keys = new ArrayList<>();
        for (UntypedResultSet.Row row : result)
            keys.add(row.getInt("pk"));
        keys.sort(Integer::compareTo);
        return keys;
    }
}
