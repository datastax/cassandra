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
package org.apache.cassandra.dht;

import org.junit.Test;

import org.quicktheories.core.Gen;

import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.longs;

public class AbstractBoundsQuickTest
{
    private static final long MAX_TOKEN = Murmur3Partitioner.MAXIMUM;

    @Test
    public void testIntersects()
    {
        qt().forAll(bounds(), bounds())
            .check((r1, r2) -> {
                boolean intersects = r1.intersects(r2);
                // Check commutativity
                assertThat(r2.intersects(r1)).isEqualTo(intersects);

                assertThat(intersects).isEqualTo(stupidIntersects(r1, r2));

                return true;
            });
    }

    boolean stupidIntersects(AbstractBounds<Token> l, AbstractBounds<Token> r)
    {
        if (isPoint(l))
            return l.left.isMinimum() || r.contains(l.left);
        if (isPoint(r))
            return r.left.isMinimum() || l.contains(r.left);

        // Range.intersects is already tested
        return toRange(l).intersects(r);
    }

    private static Range<Token> toRange(AbstractBounds<Token> bounds)
    {
        if (bounds instanceof Range)
            return (Range<Token>) bounds;

        Token l = bounds.left;
        if (bounds.inclusiveLeft())
            l = l.prevValidToken();
        Token r = bounds.right;
        if (!bounds.inclusiveRight())
            r = r.prevValidToken();

        return new Range<>(l, r);
    }

    private static boolean isPoint(AbstractBounds<Token> l)
    {
        return l.inclusiveLeft() && l.inclusiveRight() && l.left.equals(l.right);
    }

    private Gen<AbstractBounds<Token>> bounds()
    {
        return longs().between(0, MAX_TOKEN)
                      .zip(integers().between(0, 6), this::createBoundary) // 14% chance min
               .zip(longs().between(0, MAX_TOKEN)
                           .zip(integers().between(-1, 6), this::createBoundary), // 12.5% chance point, 12.5% chance min
                    this::createAbstractBounds);
    }

    private AbstractBounds.Boundary<Token> createBoundary(long pos, int minOrInclusive)
    {
        if (minOrInclusive < 0)
            return null;    // point bounds
        Token t;
        if (minOrInclusive == 0)
            t = Murmur3Partitioner.instance.getMinimumToken();
        else
            t = new Murmur3Partitioner.LongToken(pos);

        return new AbstractBounds.Boundary(t, minOrInclusive % 2 == 0);
    }

    private AbstractBounds<Token> createAbstractBounds(AbstractBounds.Boundary<Token> left, AbstractBounds.Boundary<Token> right)
    {
        if (right == null)
        {
            return AbstractBounds.bounds(left.boundary, true, left.boundary, true);
        }

        if (!left.inclusive && right.inclusive)
            return new Range<>(left.boundary, right.boundary); // ranges can be wraparound

        // For other cases, create a normal bounds
        int cmp = left.boundary.compareTo(right.boundary);
        if (cmp < 0)
            return AbstractBounds.bounds(left, right);
        else if (cmp > 0)
            return AbstractBounds.bounds(right, left);
        else
            return AbstractBounds.bounds(left.boundary, true, left.boundary, true);
    }
}
