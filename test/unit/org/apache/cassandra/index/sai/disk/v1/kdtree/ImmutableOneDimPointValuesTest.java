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
package org.apache.cassandra.index.sai.disk.v1.kdtree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.oldlucene.MutablePointsReaderUtils;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.AbstractGuavaIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.junit.Assert.assertEquals;

public class ImmutableOneDimPointValuesTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldTraversePointsInTermEnumOrder() throws IOException
    {
        final int minTerm = 0, maxTerm = 10;
        final TermsIterator termEnum = buildDescTermEnum(minTerm, maxTerm);
        final ImmutableOneDimPointValues pointValues = ImmutableOneDimPointValues
                .fromTermEnum(termEnum, Int32Type.instance);

        pointValues.intersect(assertingVisitor(minTerm));
    }

    @Test
    public void shouldFailOnSorting()
    {
        final int minTerm = 3, maxTerm = 13;
        final TermsIterator termEnum = buildDescTermEnum(minTerm, maxTerm);
        final ImmutableOneDimPointValues pointValues = ImmutableOneDimPointValues
                .fromTermEnum(termEnum, Int32Type.instance);

        expectedException.expect(IllegalStateException.class);
        pointValues.swap(0, 1);
    }

    @Test
    public void shouldSkipLuceneSorting() throws IOException
    {
        final int minTerm = 2, maxTerm = 7;
        final TermsIterator termEnum = buildDescTermEnum(minTerm, maxTerm);
        final ImmutableOneDimPointValues pointValues = ImmutableOneDimPointValues.fromTermEnum(termEnum, Int32Type.instance);

        MutablePointsReaderUtils.sort(2, Int32Type.instance.valueLengthIfFixed(), pointValues, 0, Math.toIntExact(pointValues.size()));

        pointValues.intersect(assertingVisitor(minTerm));
    }

    private MutableOneDimPointValues.IntersectVisitor assertingVisitor(int minTerm)
    {
        return new MutableOneDimPointValues.IntersectVisitor()
        {
            int term = minTerm;
            int postingCounter = 0;

            @Override
            public void visit(int docID, byte[] packedValue)
            {
                final ByteComparable actualTerm = ByteComparable.preencoded(TypeUtil.BYTE_COMPARABLE_VERSION, packedValue);
                final ByteComparable expectedTerm = ByteComparable.of(term);

                assertEquals(0, ByteComparable.compare(actualTerm, expectedTerm, TypeUtil.BYTE_COMPARABLE_VERSION));
                assertEquals(postingCounter, docID);

                if (postingCounter >= 2)
                {
                    postingCounter = 0;
                    term++;
                }
                else
                {
                    postingCounter++;
                }
            }
        };
    }

    private TermsIterator buildDescTermEnum(int from, int to)
    {
        final ByteBuffer minTerm = Int32Type.instance.decompose(from);
        final ByteBuffer maxTerm = Int32Type.instance.decompose(to);

        final AbstractGuavaIterator<Pair<ByteComparable.Preencoded, List<RowMapping.RowIdWithFrequency>>> iterator = new AbstractGuavaIterator<>()
        {
            private int currentTerm = from;

            @Override
            protected Pair<ByteComparable.Preencoded, List<RowMapping.RowIdWithFrequency>> computeNext()
            {
                if (currentTerm <= to)
                {
                    return endOfData();
                }
                final ByteBuffer term = Int32Type.instance.decompose(currentTerm++);
                List<RowMapping.RowIdWithFrequency> postings = Arrays.asList(
                    new RowMapping.RowIdWithFrequency(0, 1),
                    new RowMapping.RowIdWithFrequency(1, 1),
                    new RowMapping.RowIdWithFrequency(2, 1));
                return Pair.create(ByteComparable.preencoded(TypeUtil.BYTE_COMPARABLE_VERSION, term), postings);
            }
        };

        return new MemtableTermsIterator(minTerm, maxTerm, iterator);
    }
}
