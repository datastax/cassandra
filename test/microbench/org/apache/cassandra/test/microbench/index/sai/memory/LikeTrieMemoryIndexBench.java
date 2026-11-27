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

package org.apache.cassandra.test.microbench.index.sai.memory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.LikePatterns;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.tries.TriePattern;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.analyzer.NonTokenizingOptions;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.memory.TrieMemoryIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/// Measures `LIKE` searches on a memtable string index ([TrieMemoryIndex#search]), which walk the terms trie with the
/// pattern. Each operation searches the index and iterates all the matching rows.
///
/// Patterns are given as `OPERATOR:value`, where the operator is `PREFIX`, `SUFFIX`, `CONTAINS` or `MATCHES` (a
/// generic `LIKE` pattern, with `%` and `_` wildcards).
///
/// Run with `ant microbench -Dbenchmark.name=LikeTrieMemoryIndexBench`.
public class LikeTrieMemoryIndexBench
{
    static final AbstractBounds<PartitionPosition> ALL_DATA_RANGE = DataRange.allData(Murmur3Partitioner.instance).keyRange();

    static final String[] WORDS = {
        "alpha", "beta", "data", "home", "user", "logs", "cache", "node", "report", "test",
        "main", "java", "config", "index", "table", "shard", "backup", "archive", "image", "video",
        "docs", "src", "lib", "bin", "etc", "var", "opt", "share", "local", "build",
        "target", "classes", "meta", "info", "system", "public", "gamma", "delta", "tmp", "music",
        "café", "naïve", "東京", "日本"
    };

    static final String[] EXTENSIONS = { ".txt", ".log", ".json", ".db", ".bin", ".csv", "" };

    /// File-system-like paths built from a small vocabulary with a skewed distribution, so that terms share prefixes
    /// like a real directory tree. A few words are not ASCII.
    public static class PathTermsBench extends AbstractBench
    {
        @Param({ "SUFFIX:.json",                    // %.json
                 "CONTAINS:/data/",                 // %/data/%
                 "CONTAINS:café",                   // %café%, multi-byte literal
                 "MATCHES:%/data/%.log",
                 "PREFIX:alpha/beta/",              // alpha/beta/%
                 "MATCHES:alpha/%/report____.csv",  // literal prefix, single-character wildcards
                 "MATCHES:%/__/%",                  // two-character directory names (only multi-byte ones exist)
                 "MATCHES:%a%e%i%o%u%" })           // many wildcards
        String pattern;

        @Override
        String pattern()
        {
            return pattern;
        }

        @Override
        String term(Random rand)
        {
            StringBuilder sb = new StringBuilder();
            int depth = 1 + rand.nextInt(10);
            for (int i = 0; i < depth; ++i)
                sb.append(WORDS[skewed(rand, WORDS.length)]).append('/');
            sb.append(WORDS[skewed(rand, WORDS.length)])
              .append(rand.nextInt(10000))
              .append(EXTENSIONS[rand.nextInt(EXTENSIONS.length)]);
            return sb.toString();
        }
    }

    /// Strings of 64 random lowercase letters, as in [ReadTrieMemoryIndexBench]: the terms share little structure.
    public static class RandomTermsBench extends AbstractBench
    {
        @Param({ "SUFFIX:xy",          // %xy
                 "CONTAINS:abc",       // %abc%
                 "PREFIX:ab",          // ab%
                 "MATCHES:_a%",
                 "MATCHES:a%b%c",
                 "MATCHES:%a_b_c%" })
        String pattern;

        @Override
        String pattern()
        {
            return pattern;
        }

        @Override
        String term(Random rand)
        {
            char[] chars = new char[64];
            for (int i = 0; i < chars.length; ++i)
                chars[i] = (char) ('a' + rand.nextInt(26));
            return new String(chars);
        }
    }

    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(value = 1, jvmArgsAppend = { "-Xmx4G", "-Xms4G" })
    @Threads(1)
    @State(Scope.Benchmark)
    public static abstract class AbstractBench
    {
        private static final String KEYSPACE = "test_keyspace";
        private static final String TABLE = "test_table";
        private static final String PARTITION_KEY = "key";
        private static final String STRING_COLUMN = "string";
        private static final String STRING_INDEX = "string_index";

        @Param({ "100000", "1000000" })
        int numberOfTerms;

        TrieMemoryIndex index;
        Expression expression;

        abstract String pattern();

        abstract String term(Random rand);

        @Setup(Level.Trial)
        public void setup() throws IOException
        {
            DatabaseDescriptor.daemonInitialization();
            IndexContext context = stringIndexContext();
            index = new TrieMemoryIndex(context);

            Random rand = new Random(1);
            ByteBuffer[] terms = new ByteBuffer[numberOfTerms];
            for (int i = 0; i < numberOfTerms; i++)
            {
                terms[i] = UTF8Type.instance.decompose(term(rand));
                DecoratedKey key = Murmur3Partitioner.instance.decorateKey(UUIDType.instance.decompose(TimeUUID.Generator.nextTimeUUID().asUUID()));
                index.add(key, Clustering.EMPTY, terms[i], allocatedBytes -> {}, allocatedBytes -> {});
            }

            String[] parts = pattern().split(":", 2);
            Operator operator = Operator.valueOf("LIKE_" + parts[0]);
            ByteBuffer value = UTF8Type.instance.decompose(parts[1]);
            expression = new Expression(context).add(operator, value);
            if (expression.getOp() != Expression.Op.PATTERN)
                throw new IllegalStateException("Pattern " + pattern() + " is not served by pattern matching: " + expression);

            // Check the result against a brute-force evaluation of the pattern over all terms.
            TriePattern triePattern = LikePatterns.forPatternOperator(operator, value, null);
            int expected = 0;
            for (ByteBuffer term : terms)
                if (triePattern.matches(term))
                    ++expected;
            int actual = search();
            if (actual != expected)
                throw new AssertionError("Pattern " + pattern() + " matched " + actual + " rows, expected " + expected);
            System.out.format("Pattern %s matches %,d of %,d rows\n", pattern(), expected, numberOfTerms);
        }

        @Benchmark
        public int search() throws IOException
        {
            int count = 0;
            try (KeyRangeIterator iterator = index.search(expression, ALL_DATA_RANGE))
            {
                while (iterator.hasNext())
                {
                    iterator.next();
                    ++count;
                }
            }
            return count;
        }

        private static IndexContext stringIndexContext()
        {
            TableMetadata table = TableMetadata.builder(KEYSPACE, TABLE)
                                               .addPartitionKeyColumn(PARTITION_KEY, UUIDType.instance)
                                               .addRegularColumn(STRING_COLUMN, UTF8Type.instance)
                                               .partitioner(Murmur3Partitioner.instance)
                                               .caching(CachingParams.CACHE_NOTHING)
                                               .build();

            Map<String, String> options = new HashMap<>();
            options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getCanonicalName());
            options.put(NonTokenizingOptions.CASE_SENSITIVE, "true");
            options.put(IndexTarget.TARGET_OPTION_NAME, STRING_COLUMN);

            IndexMetadata metadata = IndexMetadata.fromSchemaMetadata(STRING_INDEX, IndexMetadata.Kind.CUSTOM, options);
            Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(table, metadata);
            return new IndexContext(table.keyspace,
                                    table.name,
                                    table.id,
                                    table.partitionKeyType,
                                    table.comparator,
                                    target.left,
                                    target.right,
                                    metadata,
                                    MockSchema.newCFS(table));
        }
    }

    /// Returns a value in `[0, n)`, with smaller values more likely.
    static int skewed(Random rand, int n)
    {
        double d = rand.nextDouble();
        return (int) (d * d * n);
    }
}
