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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.utils.AutomatonQueries;
import org.apache.cassandra.index.sai.utils.AutomatonTermsExceededException;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Memtable-level tests for the automaton term-matching engine ({@link TrieMemoryIndex#automatonMatch}): mirrors
 * the disk-level {@code AutomatonIntersectionTest} property-based comparison against a brute-force reference that
 * runs the same automaton over every indexed value.
 */
public class TrieMemoryIndexAutomatonTest
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
                                              "xabx", "cab", "a", "ax", "axxb", "zab", "a%b", "a_c");

        for (String pattern : new String[]{ "abc%", "%abc", "%abc%", "a%c", "%", "abc", "a_c", "a%b",
                                            "ab%", "%ab", "%ab%", "a%b%c", "nomatch%" })
            assertMatchesBruteForce(index, rows, AutomatonQueries.fromLikePattern(pattern), "LIKE " + pattern);

        for (String suffix : new String[]{ "ab", "b", "a%b", "nomatch" })
            assertMatchesBruteForce(index, rows, AutomatonQueries.suffixAutomaton(suffix), "suffix " + suffix);

        for (String substring : new String[]{ "ab", "a%b", "x", "nomatch" })
            assertMatchesBruteForce(index, rows, AutomatonQueries.containsAutomaton(substring), "contains " + substring);
    }

    @Test
    public void shouldBoundScanByCommonPrefix()
    {
        TrieMemoryIndex index = newIndex(UTF8Type.instance);
        Map<Integer, String> rows = indexRows(index, "aa", "aa￿", "ab", "ab ", "ab￿￿", "abz", "ac", "aca", "b");

        CompiledAutomaton automaton = AutomatonQueries.fromLikePattern("ab%");
        TreeSet<Integer> results = search(index, automaton, Integer.MAX_VALUE);
        assertEquals(keysOf(rows, "ab", "ab ", "ab￿￿", "abz"), results);

        // a prefix-bounded scan visits only the terms under the prefix, so a tight cap that would trip on a full
        // scan passes here
        assertEquals(results, search(index, automaton, 4));
    }

    @Test
    public void shouldTriggerVisitedTermCap()
    {
        TrieMemoryIndex index = newIndex(UTF8Type.instance);
        String[] values = new String[100];
        for (int i = 0; i < values.length; i++)
            values[i] = String.format("a%03d", i);
        Map<Integer, String> rows = indexRows(index, values);

        CompiledAutomaton broad = AutomatonQueries.fromLikePattern("a%");
        assertEquals(rows.keySet(), search(index, broad, 100));
        assertThrows(AutomatonTermsExceededException.class, () -> search(index, broad, 99));

        // the cap counts visited terms, not matches
        CompiledAutomaton selective = AutomatonQueries.fromLikePattern("a%zzz");
        assertEquals(new TreeSet<>(), search(index, selective, Integer.MAX_VALUE));
        assertThrows(AutomatonTermsExceededException.class, () -> search(index, selective, 10));
    }

    @Test
    public void shouldConsumeSharedBudgetAcrossScans()
    {
        TrieMemoryIndex index = newIndex(UTF8Type.instance);
        String[] values = new String[10];
        for (int i = 0; i < values.length; i++)
            values[i] = String.format("a%02d", i);
        Map<Integer, String> rows = indexRows(index, values);

        CompiledAutomaton broad = AutomatonQueries.fromLikePattern("a%");
        AbstractBounds<PartitionPosition> keyRange = AbstractBounds.unbounded(Murmur3Partitioner.instance);

        // The query path shares one budget per query (QueryContext#automatonExpansionsBudget) across every
        // memtable shard scan and sstable segment intersection: a budget covering one 10-term scan but not two
        // is exhausted by the second scan, and the failure carries the budget's total limit.
        AutomatonQueries.ExpansionsBudget budget = new AutomatonQueries.ExpansionsBudget(15);
        index.automatonMatch(broad, budget, keyRange);
        AutomatonTermsExceededException e =
            assertThrows(AutomatonTermsExceededException.class, () -> index.automatonMatch(broad, budget, keyRange));
        assertEquals(15, e.maxVisitedTerms());

        // fresh per-call budgets (the engine-level int-cap overload) are unaffected by earlier scans
        assertEquals(rows.keySet(), search(index, broad, 10));
        assertEquals(rows.keySet(), search(index, broad, 10));
    }

    @Test
    public void shouldServeSingleAndNoneAutomatons()
    {
        TrieMemoryIndex index = newIndex(UTF8Type.instance);
        Map<Integer, String> rows = indexRows(index, "alpha", "beta", "gamma");

        // a pattern without wildcards simplifies to a SINGLE automaton served by an exact lookup
        CompiledAutomaton single = AutomatonQueries.fromLikePattern("beta");
        assertEquals(CompiledAutomaton.AUTOMATON_TYPE.SINGLE, single.type);
        assertEquals(keysOf(rows, "beta"), search(index, single, Integer.MAX_VALUE));
        assertEquals(new TreeSet<>(), search(index, AutomatonQueries.fromLikePattern("delta"), Integer.MAX_VALUE));

        // the empty language matches nothing
        CompiledAutomaton none = AutomatonQueries.compile(org.apache.lucene.util.automaton.Automata.makeEmpty());
        assertEquals(CompiledAutomaton.AUTOMATON_TYPE.NONE, none.type);
        assertEquals(new TreeSet<>(), search(index, none, Integer.MAX_VALUE));

        // an all-wildcard pattern simplifies to a match-all (ALL) automaton accepting every term
        CompiledAutomaton all = AutomatonQueries.fromLikePattern("%");
        assertEquals(CompiledAutomaton.AUTOMATON_TYPE.ALL, all.type);
        assertEquals(rows.keySet(), search(index, all, Integer.MAX_VALUE));
    }

    @Test
    public void shouldRejectNonLiteralIndexes()
    {
        TrieMemoryIndex index = newIndex(Int32Type.instance);
        index.add(makeKey(table, "0"), Clustering.EMPTY, Int32Type.instance.decompose(42), b -> {}, b -> {});
        assertThrows(IllegalStateException.class,
                     () -> search(index, AutomatonQueries.fromLikePattern("4%"), Integer.MAX_VALUE));
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

        for (int i = 0; i < 30; i++)
        {
            String seed = values[random.nextInt(values.length)];
            CompiledAutomaton automaton = randomAutomaton(random, seed);
            assertMatchesBruteForce(index, rows, automaton, "randomized " + i);
        }
    }

    private void assertMatchesBruteForce(TrieMemoryIndex index, Map<Integer, String> rows, CompiledAutomaton automaton, String description)
    {
        TreeSet<Integer> expected = new TreeSet<>();
        for (Map.Entry<Integer, String> row : rows.entrySet())
            if (AutomatonQueries.accepts(automaton, row.getValue().getBytes(StandardCharsets.UTF_8)))
                expected.add(row.getKey());

        assertEquals("engine and brute-force results differ for " + description,
                     expected,
                     search(index, automaton, Integer.MAX_VALUE));
    }

    /** Runs the engine and collects the partition key values of the matching rows. */
    private TreeSet<Integer> search(TrieMemoryIndex index, CompiledAutomaton automaton, int maxVisitedTerms)
    {
        AbstractBounds<PartitionPosition> keyRange = AbstractBounds.unbounded(Murmur3Partitioner.instance);
        TreeSet<Integer> results = new TreeSet<>();
        KeyRangeIterator iterator = index.automatonMatch(automaton, maxVisitedTerms, keyRange);
        while (iterator.hasNext())
            results.add(Int32Type.instance.compose(iterator.next().partitionKey().getKey()));
        return results;
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

    private static CompiledAutomaton randomAutomaton(Random random, String seed)
    {
        switch (random.nextInt(4))
        {
            case 0:
            {
                int cpCount = seed.codePointCount(0, seed.length());
                int prefixCp = 1 + random.nextInt(Math.max(1, cpCount - 1));
                return AutomatonQueries.fromLikePattern(seed.substring(0, seed.offsetByCodePoints(0, prefixCp)) + '%');
            }
            case 1:
            {
                int cpCount = seed.codePointCount(0, seed.length());
                int fromCp = random.nextInt(cpCount);
                int toCp = fromCp + 1 + random.nextInt(cpCount - fromCp);
                String substring = seed.substring(seed.offsetByCodePoints(0, fromCp), seed.offsetByCodePoints(0, toCp));
                return AutomatonQueries.fromLikePattern('%' + substring + '%');
            }
            case 2:
                return AutomatonQueries.suffixAutomaton(seed.substring(seed.offsetByCodePoints(0, random.nextInt(seed.codePointCount(0, seed.length())))));
            default:
                return AutomatonQueries.fromLikePattern('%' + seed.substring(seed.offsetByCodePoints(0, random.nextInt(seed.codePointCount(0, seed.length())))));
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
