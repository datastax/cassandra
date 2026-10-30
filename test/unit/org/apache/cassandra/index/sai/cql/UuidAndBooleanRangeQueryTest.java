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
import java.util.UUID;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;

/**
 * Tests range ({@code <}, {@code <=}, {@code >}, {@code >=}) queries on {@code uuid} and {@code boolean} columns
 * with an SAI index, across the memtable-only / flushed / compacted matrix. The order is the CQL order of the
 * types: {@link UUIDType} sorts by version first, then by timestamp for time-based UUIDs, and {@code false} sorts
 * before {@code true}.
 */
public class UuidAndBooleanRangeQueryTest extends SAITester
{
    /** The seed of the randomized tests, which can be fixed with the {@link CassandraRelevantProperties#TEST_RANDOM_SEED} property. */
    private static final long SEED = CassandraRelevantProperties.TEST_RANDOM_SEED.getLong(System.nanoTime());

    private final Random random = new Random(SEED);

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
    public void testBooleanRanges()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, b boolean)");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, b) VALUES (?, ?)", 1, false);
        execute("INSERT INTO %s (pk, b) VALUES (?, ?)", 2, true);
        execute("INSERT INTO %s (pk, b) VALUES (?, ?)", 3, false);

        for (int i = 0; i < 3; i++)
        {
            assertEquals(List.of(2), keys(execute("SELECT pk FROM %s WHERE b > false")));
            assertEquals(List.of(1, 2, 3), keys(execute("SELECT pk FROM %s WHERE b >= false")));
            assertEquals(List.of(1, 3), keys(execute("SELECT pk FROM %s WHERE b < true")));
            assertEquals(List.of(1, 2, 3), keys(execute("SELECT pk FROM %s WHERE b <= true")));
            assertEquals(List.of(), keys(execute("SELECT pk FROM %s WHERE b > true")));
            assertEquals(List.of(), keys(execute("SELECT pk FROM %s WHERE b < false")));
            assertEquals(List.of(), keys(execute("SELECT pk FROM %s WHERE b > false AND b < true")));
            assertEquals(List.of(2), keys(execute("SELECT pk FROM %s WHERE b >= true AND b <= true")));

            if (i == 0)
                flush();
            else if (i == 1)
                compact();
        }
    }

    @Test
    public void testFixedUuidRanges()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, u uuid)");
        createIndex("CREATE CUSTOM INDEX ON %s(u) USING 'StorageAttachedIndex'");

        UUID u1 = UUID.fromString("00000000-0000-4000-8000-000000000001");
        UUID u2 = UUID.fromString("00000000-0000-4000-8000-000000000002");
        UUID u3 = UUID.fromString("10000000-0000-4000-8000-000000000000");
        UUID u4 = UUID.fromString("ffffffff-ffff-4fff-bfff-ffffffffffff");
        execute("INSERT INTO %s (pk, u) VALUES (?, ?)", 1, u1);
        execute("INSERT INTO %s (pk, u) VALUES (?, ?)", 2, u2);
        execute("INSERT INTO %s (pk, u) VALUES (?, ?)", 3, u3);
        execute("INSERT INTO %s (pk, u) VALUES (?, ?)", 4, u4);

        for (int i = 0; i < 3; i++)
        {
            assertEquals(List.of(2, 3), keys(execute("SELECT pk FROM %s WHERE u > ? AND u < ?", u1, u4)));
            assertEquals(List.of(1, 2, 3, 4), keys(execute("SELECT pk FROM %s WHERE u >= ? AND u <= ?", u1, u4)));
            assertEquals(List.of(3, 4), keys(execute("SELECT pk FROM %s WHERE u > ?", u2)));
            assertEquals(List.of(1, 2), keys(execute("SELECT pk FROM %s WHERE u <= ?", u2)));
            assertEquals(List.of(), keys(execute("SELECT pk FROM %s WHERE u > ?", u4)));

            if (i == 0)
                flush();
            else if (i == 1)
                compact();
        }
    }

    @Test
    public void testRandomizedUuidRanges()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, u uuid)");
        createIndex("CREATE CUSTOM INDEX ON %s(u) USING 'StorageAttachedIndex'");

        Map<Integer, UUID> rows = new HashMap<>();

        // memtable-only
        insertRandomRows(rows, 0, 100);
        verifyRandomizedRanges(rows);

        // flushed sstable + memtable
        flush();
        insertRandomRows(rows, 100, 200);
        verifyRandomizedRanges(rows);

        // two sstables
        flush();
        verifyRandomizedRanges(rows);

        // compacted
        compact();
        verifyRandomizedRanges(rows);
    }

    private void insertRandomRows(Map<Integer, UUID> rows, int from, int to)
    {
        for (int pk = from; pk < to; pk++)
        {
            UUID value = randomUuid();
            rows.put(pk, value);
            execute("INSERT INTO %s (pk, u) VALUES (?, ?)", pk, value);
        }
    }

    private void verifyRandomizedRanges(Map<Integer, UUID> rows)
    {
        List<UUID> values = new ArrayList<>(rows.values());
        for (int i = 0; i < 40; i++)
        {
            UUID lower = random.nextBoolean() ? values.get(random.nextInt(values.size())) : randomUuid();
            UUID upper = random.nextBoolean() ? values.get(random.nextInt(values.size())) : randomUuid();
            String lowerOp = random.nextBoolean() ? ">" : ">=";
            String upperOp = random.nextBoolean() ? "<" : "<=";
            int kind = random.nextInt(3);

            List<Integer> expected = new ArrayList<>();
            for (Map.Entry<Integer, UUID> entry : rows.entrySet())
            {
                UUID value = entry.getValue();
                boolean matches = (kind == 2 || satisfies(value, lowerOp, lower))
                                  && (kind == 1 || satisfies(value, upperOp, upper));
                if (matches)
                    expected.add(entry.getKey());
            }

            UntypedResultSet result;
            String query;
            if (kind == 0)
                result = execute(query = "SELECT pk FROM %s WHERE u " + lowerOp + " ? AND u " + upperOp + " ?", lower, upper);
            else if (kind == 1)
                result = execute(query = "SELECT pk FROM %s WHERE u " + lowerOp + " ?", lower);
            else
                result = execute(query = "SELECT pk FROM %s WHERE u " + upperOp + " ?", upper);

            expected.sort(Integer::compareTo);
            assertEquals("Seed: " + SEED + ", query: " + query + ", lower: " + lower + ", upper: " + upper, expected, keys(result));
        }
    }

    /**
     * Range check in the CQL order of {@link UUIDType}.
     */
    private static boolean satisfies(UUID value, String operator, UUID bound)
    {
        int cmp = UUIDType.instance.compare(UUIDType.instance.decompose(value), UUIDType.instance.decompose(bound));
        switch (operator)
        {
            case ">": return cmp > 0;
            case ">=": return cmp >= 0;
            case "<": return cmp < 0;
            case "<=": return cmp <= 0;
            default: throw new AssertionError(operator);
        }
    }

    /**
     * Returns a random time-based (version 1) or random (version 4) UUID, so that the version-first order of
     * {@link UUIDType} is exercised.
     */
    private UUID randomUuid()
    {
        if (random.nextBoolean())
            return TimeUUID.Generator.atUnixMillis(random.nextInt(1_000_000), random.nextInt(10)).asUUID();
        return new UUID(random.nextLong() & 0xffffffffffff0fffL | 0x0000000000004000L,
                        random.nextLong() & 0x3fffffffffffffffL | 0x8000000000000000L);
    }

    private static List<Integer> keys(UntypedResultSet result)
    {
        List<Integer> keys = new ArrayList<>();
        for (UntypedResultSet.Row row : result)
            keys.add(row.getInt("pk"));
        keys.sort(Integer::compareTo);
        return keys;
    }
}
