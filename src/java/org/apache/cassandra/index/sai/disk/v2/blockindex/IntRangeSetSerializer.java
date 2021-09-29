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

package org.apache.cassandra.index.sai.disk.v2.blockindex;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class IntRangeSetSerializer
{
    public static RangeSet<Integer> deserialize(IndexInput input) throws IOException
    {
        final int size = input.readVInt();
        int lastValue = 0;
        RangeSet<Integer> rangeSet = TreeRangeSet.create();
        for (int x = 0; x < size; x++)
        {
            final int lowerDelta = input.readVInt();
            final int lower = lowerDelta + lastValue;
            lastValue = lower;

            final int upperDelta = input.readVInt();
            final int upper = upperDelta + lastValue;
            lastValue = upper;

            rangeSet.add(Range.closed(lower, upper));
        }
        return rangeSet;
    }

    public static void serialize(RangeSet<Integer> rangeSet, IndexOutput out) throws IOException
    {
        int lastValue = 0;
        Set<Range<Integer>> ranges = rangeSet.asRanges();
        out.writeVInt(ranges.size());
        for (Range<Integer> range : ranges)
        {
            final int lower = range.lowerEndpoint();
            out.writeVInt(lower - lastValue);
            lastValue = lower;

            final int upper = range.upperEndpoint();
            out.writeVInt(upper - lastValue);
            lastValue = upper;
        }
    }
}
