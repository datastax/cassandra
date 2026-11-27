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

package org.apache.cassandra.test.microbench.index.sai.v1;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.cql3.LikePatterns;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.tries.TriePattern;
import org.apache.cassandra.index.sai.disk.v1.trie.PatternTermsDictionaryReader;
import org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryReader;
import org.apache.cassandra.index.sai.utils.PrefixRange;
import org.apache.cassandra.io.tries.IncrementalTrieWriter;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.PageAware;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
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

import static java.nio.charset.StandardCharsets.UTF_8;

/// Compares two ways of finding the terms of an on-disk SAI terms dictionary that match a `LIKE` pattern:
///   - `FilterRange`: scan the range of the terms that start with the literal prefix of the pattern, and keep the ones
///     that match it (the earlier implementation of the on-disk `LIKE` search did this with a Lucene automaton);
///   - `TriePattern`: walk the terms trie with the pattern, skipping the branches that cannot match
///     ([PatternTermsDictionaryReader]).
///
/// The terms trie is written to memory and read through an in-memory [Rebufferer], so this measures the CPU cost of
/// finding the terms and their postings offsets, without I/O. Reading and merging the postings, which is the same for
/// both, is not included.
///
/// Patterns are given as `OPERATOR:value`, where the operator is `PREFIX`, `SUFFIX`, `CONTAINS` or `MATCHES` (a
/// generic `LIKE` pattern, with `%` and `_` wildcards).
///
/// Run with `ant microbench -Dbenchmark.name=LikeTermsDictionaryBench`.
public class LikeTermsDictionaryBench
{
    static final ByteComparable.Version VERSION = ByteComparable.Version.OSS50;

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

    /// Strings of 64 random lowercase letters: the terms share little structure.
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
        @Param({ "100000", "1000000" })
        int numberOfTerms;

        @Param({ "FilterRange", "TriePattern" })
        String implementation;

        ByteBuffer trieData;
        long root;

        byte[] literalPrefix;
        TriePattern triePattern;

        abstract String pattern();

        abstract String term(Random rand);

        @Setup(Level.Trial)
        public void setup() throws IOException
        {
            // The trie writer needs the terms in order, without duplicates.
            TreeSet<byte[]> terms = new TreeSet<>(Arrays::compareUnsigned);
            Random rand = new Random(1);
            while (terms.size() < numberOfTerms)
                terms.add(term(rand).getBytes(UTF_8));

            PagedDataOutputBuffer out = new PagedDataOutputBuffer();
            IncrementalTrieWriter<Long> writer = IncrementalTrieWriter.open(TrieTermsDictionaryReader.trieSerializer, out, VERSION);
            long postingsOffset = 0;
            for (byte[] term : terms)
                writer.add(ByteComparable.preencoded(VERSION, term), postingsOffset += 1 + (term[0] & 0xF));
            root = writer.complete();
            trieData = out.asNewBuffer();

            String[] parts = pattern().split(":", 2);
            Operator operator = Operator.valueOf("LIKE_" + parts[0]);
            ByteBuffer value = ByteBuffer.wrap(parts[1].getBytes(UTF_8));
            triePattern = LikePatterns.forPatternOperator(operator, value, null);
            literalPrefix = triePattern.literalPrefix();

            long[] byRange = rangeSearch();
            long[] byPattern = patternSearch();
            if (!Arrays.equals(byRange, byPattern))
                throw new AssertionError("Pattern " + pattern() + " matched " + byPattern[0] + " terms with the trie pattern, but "
                                         + byRange[0] + " filtering the range");
            System.out.format("Pattern %s matches %,d of %,d terms (trie of %,d bytes)\n",
                              pattern(), byPattern[0], numberOfTerms, trieData.remaining());
        }

        @Benchmark
        public long select()
        {
            long[] result = implementation.equals("FilterRange") ? rangeSearch() : patternSearch();
            return result[1];
        }

        /// Returns the number of matching terms and the sum of their postings offsets.
        long[] rangeSearch()
        {
            ByteComparable lower = literalPrefix.length == 0 ? null : ByteComparable.preencoded(VERSION, literalPrefix);
            byte[] upperBytes = PrefixRange.upperBound(literalPrefix);
            ByteComparable upper = upperBytes == null ? null : ByteComparable.preencoded(VERSION, upperBytes);
            long count = 0;
            long sum = 0;
            try (TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(new BufferRebufferer(trieData), root, lower, upper, true, true, VERSION))
            {
                while (reader.hasNext())
                {
                    Pair<ByteComparable, Long> entry = reader.next();
                    if (triePattern.matches(ByteBuffer.wrap(ByteSourceInverse.readBytes(entry.left.asComparableBytes(VERSION)))))
                    {
                        ++count;
                        sum += entry.right;
                    }
                }
            }
            return new long[]{ count, sum };
        }

        /// Returns the number of matching terms and the sum of their postings offsets.
        long[] patternSearch()
        {
            long count = 0;
            long sum = 0;
            try (PatternTermsDictionaryReader reader = new PatternTermsDictionaryReader(new BufferRebufferer(trieData), root, triePattern, true, false, VERSION))
            {
                for (long offset = reader.nextAsLong(); offset != TrieTermsDictionaryReader.NOT_FOUND; offset = reader.nextAsLong())
                {
                    ++count;
                    sum += offset;
                }
            }
            return new long[]{ count, sum };
        }
    }

    /// Returns a value in `[0, n)`, with smaller values more likely.
    static int skewed(Random rand, int n)
    {
        double d = rand.nextDouble();
        return (int) (d * d * n);
    }

    /// In-memory output buffer with the paging parameters of a file, for the page-aware trie writer.
    static class PagedDataOutputBuffer extends DataOutputBuffer
    {
        @Override
        public int maxBytesInPage()
        {
            return PageAware.PAGE_SIZE;
        }

        @Override
        public void padToPageBoundary() throws IOException
        {
            PageAware.pad(this);
        }

        @Override
        public int bytesLeftInPage()
        {
            long position = position();
            return (int) (PageAware.pageLimit(position) - position);
        }

        @Override
        public long paddedPosition()
        {
            return PageAware.padded(position());
        }
    }

    /// Rebufferer over an in-memory buffer holding the whole file.
    static class BufferRebufferer implements Rebufferer, Rebufferer.BufferHolder
    {
        final ByteBuffer buffer;

        BufferRebufferer(ByteBuffer buffer)
        {
            this.buffer = buffer;
        }

        @Override
        public ChannelProxy channel()
        {
            return null;
        }

        @Override
        public ByteBuffer buffer()
        {
            return buffer;
        }

        @Override
        public long fileLength()
        {
            return buffer.remaining();
        }

        @Override
        public double getCrcCheckChance()
        {
            return 0;
        }

        @Override
        public long adjustPosition(long position)
        {
            return position;
        }

        @Override
        public long positionForSkip(long currentPosition, int bytesToSkip)
        {
            return currentPosition + bytesToSkip;
        }

        @Override
        public long remainingBytes(long position)
        {
            return Math.max(0, fileLength() - position);
        }

        @Override
        public BufferHolder rebuffer(long position)
        {
            return this;
        }

        @Override
        public long offset()
        {
            return 0;
        }

        @Override
        public void release()
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public void closeReader()
        {
        }
    }
}
