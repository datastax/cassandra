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
package org.apache.cassandra.index.sai.disk.v1;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.util.stream.Collectors.toList;

public class InvertedIndexBuilder
{
    /**
     * Builds a list of {@link TermsEnum} instances with the given number of terms and postings with the intention of
     * building them in the same way that the TrieMemoryIndex would in preparation for writing to disk.
     */
    public static List<TermsEnum> buildStringTermsEnum(Version version, int terms, int postings, Supplier<String> termsGenerator, IntSupplier postingsGenerator)
    {
        final List<ByteBuffer> sortedTerms = Stream.generate(termsGenerator)
                                                   .distinct()
                                                   .limit(terms)
                                                   .sorted()
                                                   .map(UTF8Type.instance::decompose)
                                                   .collect(toList());

        final List<TermsEnum> termsEnum = new ArrayList<>();
        for (ByteBuffer term : sortedTerms)
        {
            final LongArrayList postingsList = new LongArrayList();

            IntStream.generate(postingsGenerator)
                     .distinct()
                     .limit(postings)
                     .sorted()
                     .forEach(postingsList::add);

            // This logic feels a bit fragile, but it mimics the way we call unescape in the TrieMemoryIndex
            // before writing to the on disk format.
            var encoded = version.onDiskFormat().encodeForTrie(term, UTF8Type.instance);
            termsEnum.add(new TermsEnum(term, encoded, postingsList));
        }
        return termsEnum;
    }

    /**
     * Convenience wrapper for a term's original bytes, its byte comparable encoding, and its associated postings list.
     */
    public static class TermsEnum
    {
        // Store the original term to ensure that searching by it is successful
        final ByteBuffer originalTermBytes;
        final ByteComparable byteComparableBytes;
        final LongArrayList postings;

        TermsEnum(ByteBuffer originalTermBytes, ByteComparable byteComparableBytes, LongArrayList postings)
        {
            this.originalTermBytes = originalTermBytes;
            this.byteComparableBytes = byteComparableBytes;
            this.postings = postings;
        }

        public Pair<ByteComparable, LongArrayList> toPair()
        {
            return Pair.create(byteComparableBytes, postings);
        }
    }
}
