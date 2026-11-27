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

package org.apache.cassandra.test.microbench.tries;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
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

/// Measures [Trie#patternMatched], against a baseline (`FilterAll`) that walks the whole trie and glob-matches every
/// key. In the patterns, `*` matches any sequence of characters and `?` any single character.
///
/// The trie holds file-system-like paths (e.g. `alpha/data/node/report1234.csv`) built from a small vocabulary with a
/// skewed distribution, so that keys share prefixes like a real directory tree. Each operation lists all matches of
/// the pattern and returns their count.
///
/// Run with `ant microbench -Dbenchmark.name=TriePatternMatchBench`.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = { "-Xmx2G", "-Xms2G" })
@Threads(1)
@State(Scope.Benchmark)
public class TriePatternMatchBench
{
    static final ByteComparable.Version VERSION = ByteComparable.Version.OSS50;

    static final String[] WORDS = {
        "alpha", "beta", "data", "home", "user", "logs", "cache", "node", "report", "test",
        "main", "java", "config", "index", "table", "shard", "backup", "archive", "image", "video",
        "docs", "src", "lib", "bin", "etc", "var", "opt", "share", "local", "build",
        "target", "classes", "meta", "info", "system", "public", "gamma", "delta", "tmp", "music"
    };

    static final String[] EXTENSIONS = { ".txt", ".log", ".json", ".db", ".bin", ".csv", "" };

    @Param({ "alpha/beta/*",                                // prefix, branches outside it are skipped
             "*.json",                                      // suffix, whole trie walked with few active states
             "*/data/*.log",                                // stars around a literal segment
             "alpha/*/report????.csv",                      // prefix, star and single-character wildcards
             "*a*e*i*o*u*",                                 // many stars, many active states
             "*/????/*/????/*/????/*/????/*/????/*.txt",    // 40 positions, many active states
             "????/data/*",                                 // no star before the literals, skipped by byte
             "alpha/????/node/*",
             "????/????/????/data????.log",                 // no star at all
             "??????????????????????????????????????????????????????????????????????*",                // 71 positions
             "*/????/*/????/*/????/*/????/*/????/*/????/*/????/*/????/*/????/*/????/*.txt" })  // 75 positions
    String pattern;

    @Param({ "PatternMatched", "FilterAll" })
    String implementation;

    @Param({ "1000", "100000" })
    int count;

    @Param({ "FORWARD" })
    Direction direction;

    Trie<byte[]> trie;
    Trie<byte[]> matched;
    byte[] patternBytes;

    @Setup(Level.Trial)
    public void setup() throws TrieSpaceExhaustedException
    {
        InMemoryTrie<byte[]> trie = InMemoryTrie.shortLived(VERSION);
        Random rand = new Random(1);
        for (int i = 0; i < count; ++i)
        {
            byte[] key = randomPath(rand).getBytes(StandardCharsets.UTF_8);
            trie.putRecursive(ByteComparable.preencoded(VERSION, key), key, (existing, update) -> update);
        }
        this.trie = trie;

        patternBytes = pattern.getBytes(StandardCharsets.UTF_8);
        switch (implementation)
        {
            case "PatternMatched":
                matched = trie.patternMatched(pattern);
                break;
            case "FilterAll":
                matched = null;
                break;
            default:
                throw new IllegalArgumentException("Unknown implementation " + implementation);
        }

        int expected = filterAll();
        int actual = matches();
        if (actual != expected)
            throw new AssertionError("Pattern " + pattern + " matched " + actual + " keys, expected " + expected);
        System.out.format("Pattern %s matches %,d of %,d keys\n", pattern, expected, count);
    }

    @Benchmark
    public int matches()
    {
        if (matched == null)
            return filterAll();

        int matches = 0;
        for (byte[] ignored : matched.values(direction))
            ++matches;
        return matches;
    }

    int filterAll()
    {
        int matches = 0;
        for (byte[] key : trie.values(direction))
            if (globMatches(patternBytes, key))
                ++matches;
        return matches;
    }

    static String randomPath(Random rand)
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

    /// Returns a value in `[0, n)`, with smaller values more likely.
    static int skewed(Random rand, int n)
    {
        double d = rand.nextDouble();
        return (int) (d * d * n);
    }

    /// Glob match of UTF-8 bytes: '?' matches any one character, '*' any sequence of characters.
    static boolean globMatches(byte[] pattern, byte[] key)
    {
        int p = 0;
        int k = 0;
        int starP = -1;
        int starK = -1;
        while (k < key.length)
        {
            if (p < pattern.length && pattern[p] == '*')
            {
                starP = p++;
                starK = k;
            }
            else if (p < pattern.length && pattern[p] == '?')
            {
                ++p;
                k = nextCharacter(key, k);
            }
            else if (p < pattern.length && pattern[p] == key[k])
            {
                ++p;
                ++k;
            }
            else if (starP >= 0)
            {
                p = starP + 1;
                k = starK = nextCharacter(key, starK);
            }
            else
                return false;
        }
        while (p < pattern.length && pattern[p] == '*')
            ++p;
        return p == pattern.length;
    }

    /// The position after the UTF-8 character at the given position.
    static int nextCharacter(byte[] key, int k)
    {
        do
        {
            ++k;
        }
        while (k < key.length && (key[k] & 0xC0) == 0x80);
        return k;
    }
}
