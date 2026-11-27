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
package org.apache.cassandra.index.sai.memory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.LikePatterns;
import org.apache.cassandra.cql3.LikeReference;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Memtable-level tests for the LIKE pattern matching of the memtable index, which selects the matching terms while
 * walking its terms trie with the pattern ({@link Expression#getTriePattern}). Mirrors the disk-level
 * {@code TermsReaderPatternTest} property-based comparison against a brute-force reference, which evaluates the
 * pattern on every indexed value with {@link LikeReference}.
 */
public class TrieMemoryIndexPatternTest
{
    private static final String KEYSPACE = "test_keyspace";
    private static final String TABLE = "test_table";
    private static final String PART_KEY_COL = "key";
    private static final String REG_COL = "col";

    private TableMetadata table;
    private IndexContext indexContext;

    @Before
    public void setup()
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void shouldMatchLikePatterns()
    {
        TrieMemoryIndex index = newIndex(UTF8Type.instance);
        Map<Integer, String> rows = indexRows(index, "ab", "abc", "abcd", "aabc", "b", "ba", "bab",
                                              "xabx", "cab", "a", "ax", "axxb", "zab", "a%b", "a_c",
                                              "aéc", "a€c", "a😀c", "aééc", "a*c", "a?c", "a\\c", "é", "");

        for (String pattern : new String[]{ "abc%", "%abc", "%abc%", "a%c", "%", "abc", "a_c", "a%b",
                                            "ab%", "%ab", "%ab%", "a%b%c", "nomatch%",
                                            // single-character wildcards over multi-byte characters
                                            "_", "__", "_%", "%_", "a__c", "a_%c", "a%_c", "%_c", "a_%_c",
                                            // escapes, and characters that are wildcards in other syntaxes
                                            "a\\_c", "a\\%b", "a\\\\c", "%\\_%", "a*c", "a?c", "a*%", "%?%" })
            assertMatchesBruteForce(index, rows, Operator.LIKE_MATCHES, pattern);

        for (String prefix : new String[]{ "ab", "a", "a%", "a_", "é", "nomatch" })
            assertMatchesBruteForce(index, rows, Operator.LIKE_PREFIX, prefix);

        for (String suffix : new String[]{ "ab", "b", "a%b", "a_c", "é", "nomatch" })
            assertMatchesBruteForce(index, rows, Operator.LIKE_SUFFIX, suffix);

        for (String substring : new String[]{ "ab", "a%b", "x", "_", "é", "nomatch" })
            assertMatchesBruteForce(index, rows, Operator.LIKE_CONTAINS, substring);
    }

    @Test
    public void shouldMatchPatternsWithLiteralPrefix()
    {
        TrieMemoryIndex index = newIndex(UTF8Type.instance);
        Map<Integer, String> rows = indexRows(index, "aa", "aa￿", "ab", "ab ", "ab￿￿", "abz", "ac", "aca", "b");

        assertEquals(keysOf(rows, "ab", "ab ", "ab￿￿", "abz"), search(index, Operator.LIKE_MATCHES, "ab%"));
        assertEquals(keysOf(rows, "ab ", "abz"), search(index, Operator.LIKE_MATCHES, "ab_"));
        assertEquals(keysOf(rows, "ab ", "ab￿￿", "abz"), search(index, Operator.LIKE_MATCHES, "ab_%"));
    }

    @Test
    public void shouldServeWildcardFreeAndMatchAllPatterns()
    {
        TrieMemoryIndex index = newIndex(UTF8Type.instance);
        Map<Integer, String> rows = indexRows(index, "alpha", "beta", "gamma");

        // a pattern without wildcards is served by an exact lookup
        assertEquals(Expression.Op.EQ, expression(Operator.LIKE_MATCHES, "beta").getOp());
        assertEquals(keysOf(rows, "beta"), search(index, Operator.LIKE_MATCHES, "beta"));
        assertEquals(new TreeSet<>(), search(index, Operator.LIKE_MATCHES, "delta"));

        // an all-wildcard pattern accepts every term
        assertEquals(rows.keySet(), search(index, Operator.LIKE_MATCHES, "%"));
        assertEquals(rows.keySet(), search(index, Operator.LIKE_MATCHES, "%%"));
    }

    @Test
    public void shouldRejectNonLiteralIndexes()
    {
        TrieMemoryIndex index = newIndex(Int32Type.instance);
        index.add(makeKey(table, "0"), Clustering.EMPTY, Int32Type.instance.decompose(42), b -> {}, b -> {});
        Expression expression = expression(Operator.LIKE_MATCHES, "4%");
        assertThrows(IllegalStateException.class,
                     () -> index.search(expression, AbstractBounds.unbounded(Murmur3Partitioner.instance)));
    }

    @Test
    public void shouldMatchRandomizedTermsVsBruteForce()
    {
        TrieMemoryIndex index = newIndex(UTF8Type.instance);
        Random random = new Random(42);

        String[] values = new String[10_000];
        for (int i = 0; i < values.length; i++)
            values[i] = randomTerm(random);
        Map<Integer, String> rows = indexRows(index, values);

        for (int i = 0; i < 100; i++)
        {
            String seed = values[random.nextInt(values.length)];
            Pair<Operator, String> pattern = randomPattern(random, seed);
            assertMatchesBruteForce(index, rows, pattern.left, pattern.right);
        }
    }

    private void assertMatchesBruteForce(TrieMemoryIndex index, Map<Integer, String> rows, Operator operator, String value)
    {
        TreeSet<Integer> expected = new TreeSet<>();
        for (Map.Entry<Integer, String> row : rows.entrySet())
            if (LikeReference.matches(operator, value, row.getValue()))
                expected.add(row.getKey());

        // make sure the patterns with wildcards go through the pattern matching path
        if (operator != Operator.LIKE_MATCHES || LikePatterns.hasWildcard(UTF8Type.instance.decompose(value)))
            assertEquals(Expression.Op.PATTERN, expression(operator, value).getOp());

        assertEquals("index and brute-force results differ for " + operator + " '" + value + "'",
                     expected,
                     search(index, operator, value));
    }

    /** Searches the index and collects the partition key values of the matching rows. */
    private TreeSet<Integer> search(TrieMemoryIndex index, Operator operator, String value)
    {
        AbstractBounds<PartitionPosition> keyRange = AbstractBounds.unbounded(Murmur3Partitioner.instance);
        TreeSet<Integer> results = new TreeSet<>();
        KeyRangeIterator iterator = index.search(expression(operator, value), keyRange);
        while (iterator.hasNext())
            results.add(Int32Type.instance.compose(iterator.next().partitionKey().getKey()));
        return results;
    }

    /**
     * Prefix and pattern estimates count the rows of the matching terms when there are few of them, and otherwise
     * fall back to an interpolation that doesn't collapse to zero for long prefixes nor for reversed columns.
     */
    @Test
    public void shouldEstimatePrefixesAndPatterns()
    {
        String common = "a-common-prefix-longer-than-20-bytes-";
        String[] values = new String[700];
        for (int i = 0; i < 300; i++)
            values[i] = String.format("%sx%03d", common, i);
        for (int i = 300; i < 600; i++)
            values[i] = String.format("%sy%03d", common, i);
        for (int i = 600; i < 700; i++)
            values[i] = String.format("b%03d", i);

        TrieMemoryIndex index = newIndex(UTF8Type.instance);
        indexRows(index, values);
        // few matching terms: counted exactly
        assertEquals(100, index.estimateMatchingRowsCount(expression(Operator.LIKE_PREFIX, "b")));
        assertEquals(10, index.estimateMatchingRowsCount(expression(Operator.LIKE_PREFIX, common + "x01")));
        // (patterns: the terms starting with their common prefix)
        assertEquals(100, index.estimateMatchingRowsCount(expression(Operator.LIKE_MATCHES, "b%5")));
        assertEquals(0, index.estimateMatchingRowsCount(expression(Operator.LIKE_PREFIX, "c")));
        // many matching terms behind a long common prefix: at least the counted ones
        assertTrue(index.estimateMatchingRowsCount(expression(Operator.LIKE_PREFIX, common + "x")) >= 256);
        assertTrue(index.estimateMatchingRowsCount(expression(Operator.LIKE_MATCHES, common + "x%5")) >= 256);

        // a reversed (DESC clustering) column, whose min/max terms are tracked in the reversed order
        values = new String[610];
        for (int i = 0; i < 600; i++)
            values[i] = String.format("m%03d", i);
        for (int i = 600; i < 610; i++)
            values[i] = String.format("n%03d", i);
        index = newIndex(ReversedType.getInstance(UTF8Type.instance));
        indexRows(index, values);
        long estimate = index.estimateMatchingRowsCount(expression(Operator.LIKE_PREFIX, "m"));
        assertTrue("estimate: " + estimate, estimate >= 300 && estimate <= 610);
    }

    private Expression expression(Operator operator, String value)
    {
        return new Expression(indexContext).add(operator, UTF8Type.instance.decompose(value));
    }

    /** Indexes each value under partition key equal to its position and returns the key-to-value mapping. */
    private Map<Integer, String> indexRows(TrieMemoryIndex index, String... values)
    {
        Map<Integer, String> rows = new HashMap<>();
        for (int i = 0; i < values.length; i++)
        {
            rows.put(i, values[i]);
            index.add(makeKey(table, Integer.toString(i)), Clustering.EMPTY, UTF8Type.instance.decompose(values[i]), b -> {}, b -> {});
        }
        return rows;
    }

    private TreeSet<Integer> keysOf(Map<Integer, String> rows, String... values)
    {
        TreeSet<Integer> keys = new TreeSet<>();
        for (String value : values)
            for (Map.Entry<Integer, String> row : rows.entrySet())
                if (row.getValue().equals(value))
                    keys.add(row.getKey());
        return keys;
    }

    private static String randomTerm(Random random)
    {
        StringBuilder sb = new StringBuilder();
        int length = 1 + random.nextInt(12);
        for (int i = 0; i < length; i++)
        {
            switch (random.nextInt(10))
            {
                case 0:
                    sb.append("àéîöü".charAt(random.nextInt(5)));
                    break;
                case 1:
                    sb.append("日本語中文".charAt(random.nextInt(5)));
                    break;
                case 2:
                    sb.appendCodePoint(0x1F600 + random.nextInt(16));
                    break;
                default:
                    sb.append((char) ('a' + random.nextInt(6)));
            }
        }
        return sb.toString();
    }

    /**
     * A random LIKE pattern built from a substring of the seed: prefix, contains and suffix patterns, generic patterns
     * where some characters of the seed are replaced by single-character or any-string wildcards, and the dedicated
     * suffix and contains operators.
     */
    private static Pair<Operator, String> randomPattern(Random random, String seed)
    {
        int cpCount = seed.codePointCount(0, seed.length());
        int fromCp = random.nextInt(cpCount);
        int toCp = fromCp + 1 + random.nextInt(cpCount - fromCp);
        String substring = seed.substring(seed.offsetByCodePoints(0, fromCp), seed.offsetByCodePoints(0, toCp));
        String suffix = seed.substring(seed.offsetByCodePoints(0, fromCp));
        switch (random.nextInt(6))
        {
            case 0:
            {
                String prefix = seed.substring(0, seed.offsetByCodePoints(0, toCp));
                return random.nextBoolean() ? Pair.create(Operator.LIKE_MATCHES, LikePatterns.escape(prefix) + '%')
                                            : Pair.create(Operator.LIKE_PREFIX, prefix);
            }
            case 1:
                return Pair.create(Operator.LIKE_MATCHES, '%' + LikePatterns.escape(substring) + '%');
            case 2:
                return Pair.create(Operator.LIKE_SUFFIX, suffix);
            case 3:
                return Pair.create(Operator.LIKE_CONTAINS, substring);
            default:
            {
                StringBuilder pattern = new StringBuilder();
                if (random.nextBoolean())
                    pattern.append('%');
                for (int i = 0; i < seed.length(); i = seed.offsetByCodePoints(i, 1))
                {
                    int r = random.nextInt(10);
                    if (r < 3)
                        pattern.append('_');
                    else if (r < 4)
                        pattern.append('%');
                    else
                        pattern.append(LikePatterns.escape(new String(Character.toChars(seed.codePointAt(i)))));
                }
                if (random.nextBoolean())
                    pattern.append('%');
                return Pair.create(Operator.LIKE_MATCHES, pattern.toString());
            }
        }
    }

    private TrieMemoryIndex newIndex(AbstractType<?> columnType)
    {
        table = TableMetadata.builder(KEYSPACE, TABLE)
                             .addPartitionKeyColumn(PART_KEY_COL, Int32Type.instance)
                             .addRegularColumn(REG_COL, columnType)
                             .partitioner(Murmur3Partitioner.instance)
                             .caching(CachingParams.CACHE_NOTHING)
                             .build();

        Map<String, String> options = new HashMap<>();
        options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getCanonicalName());
        options.put("target", REG_COL);

        IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata("col_index", IndexMetadata.Kind.CUSTOM, options);
        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(table, indexMetadata);
        indexContext = new IndexContext(table.keyspace,
                                        table.name,
                                        table.id,
                                        table.partitionKeyType,
                                        table.comparator,
                                        target.left,
                                        target.right,
                                        indexMetadata,
                                        MockSchema.newCFS(table));
        return new TrieMemoryIndex(indexContext);
    }

    private DecoratedKey makeKey(TableMetadata table, String partitionKey)
    {
        ByteBuffer key = table.partitionKeyType.fromString(partitionKey);
        return table.partitioner.decorateKey(key);
    }
}
