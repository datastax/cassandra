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

package org.apache.cassandra.db.memtable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.OpOrder;

public abstract class AbstractMemtable implements Memtable
{
    protected final AtomicLong liveDataSize = new AtomicLong(0);
    protected final AtomicLong currentOperations = new AtomicLong(0);
    protected final ColumnsCollector columnsCollector;
    protected final StatsCollector statsCollector = new StatsCollector();
    private final long creationNano = System.nanoTime();
    // The smallest timestamp for all partitions stored in this memtable
    protected AtomicLong minTimestamp = new AtomicLong(Long.MAX_VALUE);
    // the write barrier for directing writes to this memtable or the next during a switch
    protected volatile OpOrder.Barrier writeBarrier;
    protected TableMetadataRef metadata;

    public AbstractMemtable(TableMetadataRef metadataRef)
    {
        this.metadata = metadataRef;
        this.columnsCollector = new ColumnsCollector(metadata.get().regularAndStaticColumns());
    }

    public void switchOut(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        assert this.writeBarrier == null;
        this.writeBarrier = writeBarrier;
        // This can prepare the memtable data for deletion; it will still be used while the flush is proceeding.
        // A setDiscarded call will follow.
    }

    // decide if this memtable should take the write, or if it should go to the next memtable
    public boolean accepts(OpOrder.Group opGroup, CommitLogPosition commitLogPosition)
    {
        // if the barrier hasn't been set yet, then this memtable is still taking ALL writes
        OpOrder.Barrier barrier = this.writeBarrier;
        if (barrier == null)
            return true;
        // if the barrier has been set, the write is for this memtable if it was issued before it
        return barrier.isAfter(opGroup);
    }

    public TableMetadata metadata()
    {
        return metadata.get();
    }

    public long getLiveDataSize()
    {
        return liveDataSize.get();
    }

    public long getOperations()
    {
        return currentOperations.get();
    }

    public long getMinTimestamp()
    {
        return minTimestamp.get();
    }

    protected void updateMin(AtomicLong minTracker, long newValue)
    {
        while (true)
        {
            long memtableMinTimestamp = minTracker.get();
            if (memtableMinTimestamp <= newValue)
                break;
            if (minTracker.compareAndSet(memtableMinTimestamp, newValue))
                break;
        }
    }

    RegularAndStaticColumns columns()
    {
        return columnsCollector.get();
    }

    EncodingStats encodingStats()
    {
        return statsCollector.get();
    }

    protected static class ColumnsCollector
    {
        private final HashMap<ColumnMetadata, AtomicBoolean> predefined = new HashMap<>();
        private final ConcurrentSkipListSet<ColumnMetadata> extra = new ConcurrentSkipListSet<>();

        ColumnsCollector(RegularAndStaticColumns columns)
        {
            for (ColumnMetadata def : columns.statics)
                predefined.put(def, new AtomicBoolean());
            for (ColumnMetadata def : columns.regulars)
                predefined.put(def, new AtomicBoolean());
        }

        public void update(RegularAndStaticColumns columns)
        {
            for (ColumnMetadata s : columns.statics)
                update(s);
            for (ColumnMetadata r : columns.regulars)
                update(r);
        }

        private void update(ColumnMetadata definition)
        {
            AtomicBoolean present = predefined.get(definition);
            if (present != null)
            {
                if (!present.get())
                    present.set(true);
            }
            else
            {
                extra.add(definition);
            }
        }

        public RegularAndStaticColumns get()
        {
            RegularAndStaticColumns.Builder builder = RegularAndStaticColumns.builder();
            for (Map.Entry<ColumnMetadata, AtomicBoolean> e : predefined.entrySet())
                if (e.getValue().get())
                    builder.add(e.getKey());
            return builder.addAll(extra).build();
        }
    }

    protected static class StatsCollector
    {
        private final AtomicReference<EncodingStats> stats = new AtomicReference<>(EncodingStats.NO_STATS);

        public void update(EncodingStats newStats)
        {
            while (true)
            {
                EncodingStats current = stats.get();
                EncodingStats updated = current.mergeWith(newStats);
                if (stats.compareAndSet(current, updated))
                    return;
            }
        }

        public EncodingStats get()
        {
            return stats.get();
        }
    }

    protected abstract class AbstractFlushCollection<P extends Partition> implements FlushCollection<P>
    {
        public long dataSize()
        {
            return getLiveDataSize();
        }

        public CommitLogPosition commitLogLowerBound()
        {
            return AbstractMemtable.this.getCommitLogLowerBound();
        }

        public CommitLogPosition commitLogUpperBound()
        {
            return AbstractMemtable.this.getCommitLogUpperBound();
        }

        public EncodingStats encodingStats()
        {
            return AbstractMemtable.this.encodingStats();
        }

        public RegularAndStaticColumns columns()
        {
            return AbstractMemtable.this.columns();
        }
    }
}
