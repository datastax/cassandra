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
package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.util.Comparator;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Contains a segment id and a position for CommitLogSegment identification.
 * Used for both replay and general CommitLog file reading.
 */
public class CommitLogSegmentPosition implements Comparable<CommitLogSegmentPosition>
{
    public static final CommitLogSegmentPositionSerializer serializer = new CommitLogSegmentPositionSerializer();

    // NONE is used for SSTables that are streamed from other nodes and thus have no relationship
    // with our local commitlog. The values satisfy the criteria that
    //  - no real commitlog segment will have the given id
    //  - it will sort before any real CommitLogSegmentPosition, so it will be effectively ignored by getCommitLogSegmentPosition
    public static final CommitLogSegmentPosition NONE = new CommitLogSegmentPosition(-1, 0);

    public final long segmentId;
    public final int position;

    public static final Comparator<CommitLogSegmentPosition> comparator = new Comparator<CommitLogSegmentPosition>()
    {
        public int compare(CommitLogSegmentPosition o1, CommitLogSegmentPosition o2)
        {
            if (o1.segmentId != o2.segmentId)
            	return Long.compare(o1.segmentId,  o2.segmentId);

            return Integer.compare(o1.position, o2.position);
        }
    };

    public CommitLogSegmentPosition(long segmentId, int position)
    {
        this.segmentId = segmentId;
        assert position >= 0;
        this.position = position;
    }

    public int compareTo(CommitLogSegmentPosition other)
    {
        return comparator.compare(this, other);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CommitLogSegmentPosition that = (CommitLogSegmentPosition) o;

        if (position != that.position) return false;
        return segmentId == that.segmentId;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (segmentId ^ (segmentId >>> 32));
        result = 31 * result + position;
        return result;
    }

    @Override
    public String toString()
    {
        return "CommitLogSegmentPosition(" +
               "segmentId=" + segmentId +
               ", position=" + position +
               ')';
    }

    public CommitLogSegmentPosition clone()
    {
        return new CommitLogSegmentPosition(segmentId, position);
    }


    public static class CommitLogSegmentPositionSerializer implements ISerializer<CommitLogSegmentPosition>
    {
        public void serialize(CommitLogSegmentPosition clsp, DataOutputPlus out) throws IOException
        {
            out.writeLong(clsp.segmentId);
            out.writeInt(clsp.position);
        }

        public CommitLogSegmentPosition deserialize(DataInputPlus in) throws IOException
        {
            return new CommitLogSegmentPosition(in.readLong(), in.readInt());
        }

        public long serializedSize(CommitLogSegmentPosition clsp)
        {
            return TypeSizes.sizeof(clsp.segmentId) + TypeSizes.sizeof(clsp.position);
        }
    }
}
