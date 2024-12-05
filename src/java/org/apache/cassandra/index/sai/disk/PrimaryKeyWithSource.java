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

package org.apache.cassandra.index.sai.disk;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class PrimaryKeyWithSource implements PrimaryKey
{
    private final PrimaryKey primaryKey;
    private final SSTableId<?> source;
    private final long sourceRowId;
    private final long ssTableMinTimestamp;
    private final long ssTableMaxTimestamp;

    public PrimaryKeyWithSource(PrimaryKey primaryKey, Memtable memtable)
    {
        assert primaryKey != null : "Cannot construct a PrimaryKeyWithSource with a null primaryKey";
        this.primaryKey = primaryKey;
        // We don't need to store the source for memtables because their lookups are generally cheap.
        // We use the memtable to get the min timestamp for the relevant min timestamp.
        this.source = null;
        this.sourceRowId = -1; // memtables don't have row ids
        this.ssTableMinTimestamp = memtable.getMinTimestamp();
        this.ssTableMaxTimestamp = Long.MAX_VALUE; // memtables don't have max timestamps
    }

    public PrimaryKeyWithSource(PrimaryKey primaryKey, SSTableId<?> sstableId, long sstableRowId, long sstableMinTimestamp, long sstableMaxTimestamp)
    {
        assert primaryKey != null : "Cannot construct a PrimaryKeyWithSource with a null primaryKey";
        this.primaryKey = primaryKey;
        this.source = sstableId;
        this.sourceRowId = sstableRowId;
        this.ssTableMinTimestamp = sstableMinTimestamp;
        this.ssTableMaxTimestamp = sstableMaxTimestamp;
    }

    public long getSourceRowId()
    {
        assert sourceRowId >= 0 : "Source row id is not set";
        return sourceRowId;
    }

    public boolean matchesSource(SSTableId<?> sstableId)
    {
        return sstableId.equals(source);
    }

    public boolean isInTimestampWindow(long minTimestamp, long maxTimestamp)
    {
        return ssTableMinTimestamp <= maxTimestamp && ssTableMaxTimestamp >= minTimestamp;
    }

    @Override
    public Token token()
    {
        return primaryKey.token();
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return primaryKey.partitionKey();
    }

    @Override
    public Clustering clustering()
    {
        return primaryKey.clustering();
    }

    @Override
    public PrimaryKey loadDeferred()
    {
        return primaryKey.loadDeferred();
    }

    @Override
    public ByteSource asComparableBytes(ByteComparable.Version version)
    {
        return primaryKey.asComparableBytes(version);
    }

    @Override
    public ByteSource asComparableBytesMinPrefix(ByteComparable.Version version)
    {
        return primaryKey.asComparableBytesMinPrefix(version);
    }

    @Override
    public ByteSource asComparableBytesMaxPrefix(ByteComparable.Version version)
    {
        return primaryKey.asComparableBytesMaxPrefix(version);
    }

    @Override
    public int compareTo(PrimaryKey o)
    {
        if (source != null && o instanceof PrimaryKeyWithSource)
        {
            var other = (PrimaryKeyWithSource) o;
            if (source.equals(other.source))
                return Long.compare(sourceRowId, other.sourceRowId);
        }
        return primaryKey.compareTo(o);
    }

    @Override
    public boolean equals(Object o)
    {
        if (source != null && o instanceof PrimaryKeyWithSource)
        {
            var other = (PrimaryKeyWithSource) o;
            if (source.equals(other.source))
                return sourceRowId == other.sourceRowId;
        }
        return primaryKey.equals(o);
    }

    @Override
    public int hashCode()
    {
        return primaryKey.hashCode();
    }

    @Override
    public String toString()
    {
        return String.format("%s (source sstable: %s, %s)", primaryKey, source, sourceRowId);
    }
}
