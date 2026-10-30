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

package org.apache.cassandra.index.sai.disk.vector;

import java.util.Collection;

import org.apache.cassandra.db.compaction.AbstractTableOperation;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.TimeUUID;

/**
 * A {@link org.apache.cassandra.db.compaction.TableOperation} that makes an on-disk vector
 * graph merge visible to Cassandra's normal compaction observability surface.
 *
 * <p>The merge runs from an {@code SSTableFlushObserver.complete()} callback — <em>after</em>
 * the parent compaction's {@code CompactionIterator} has been drained — so the parent
 * compaction already reads 100% while the merge burns CPU/IO. Registering an instance of this
 * class with {@link org.apache.cassandra.db.compaction.ActiveOperations#onOperationStart} for
 * the duration of the merge makes it appear as its own row in {@code nodetool compactionstats}
 * and {@code system_views.sstable_tasks} while it runs, and counts toward
 * {@code CompactionMetrics.pendingTasks}.
 *
 * <p>Progress is reported cumulatively in {@link Unit#RANGES} (graph nodes/ordinals). A coarse
 * Phase-1 caller sets {@link #setCompleted(long)} at the phase boundaries it can observe; a
 * fine-grained caller forwards jvector's per-batch counters via {@link #report(long, long)}.
 * The value is read live each time {@link #getProgress()} is polled.
 */
public class VectorMergeOperation extends AbstractTableOperation
{
    private final TableMetadata metadata;
    private final TimeUUID operationId;
    private final Collection<SSTableReader> sstables;
    private volatile long total;
    private volatile long completed;
    // RANGES (ordinals) during the graph merge proper; flips to BYTES for the epilogue
    // (footer CRC re-read + canonical-codes tail) via reportBytes so the un-metered
    // "ratio 1.0, 0 units/s" window becomes visible byte progress instead.
    private volatile Unit unit = Unit.RANGES;

    /**
     * @param metadata    the table whose vector index is being merged
     * @param operationId a unique id for this operation (e.g. {@code UUIDGen.getTimeUUID()})
     * @param sstables    the source SSTables participating in the merge (may be empty)
     * @param total       the total work for this merge, in {@link Unit#RANGES} (surviving ordinals)
     */
    public VectorMergeOperation(TableMetadata metadata, TimeUUID operationId, Collection<SSTableReader> sstables, long total)
    {
        this.metadata = metadata;
        this.operationId = operationId;
        this.sstables = sstables;
        this.total = total;
    }

    @Override
    public Progress getProgress()
    {
        return new OperationProgress(metadata, OperationType.INDEX_BUILD, completed, total, unit, operationId, sstables, null);
    }

    @Override
    public boolean isGlobal()
    {
        return false;
    }

    /**
     * Cumulative, fine-grained progress sink (Phase 3): forwards a monotonic {@code completed}
     * (and possibly refined {@code total}) observed inside the merge.
     *
     * @param completed cumulative work done so far, in {@link Unit#RANGES}
     * @param total     the (possibly now-known) total work, in {@link Unit#RANGES}
     */
    public void report(long completed, long total)
    {
        this.completed = completed;
        // jvector reports a per-phase total that may be -1 until known; keep the prior (estimated)
        // total rather than letting the completion ratio go negative.
        if (total > 0)
            this.total = total;
    }

    /**
     * Coarse progress sink (Phase 1): sets cumulative {@code completed} at an observable phase
     * boundary of the merge.
     *
     * @param completed cumulative work done so far, in {@link Unit#RANGES}
     */
    public void setCompleted(long completed)
    {
        this.completed = completed;
    }

    /**
     * Epilogue progress sink: switches the operation to {@link Unit#BYTES} and reports cumulative
     * byte progress for the post-merge work (footer CRC re-read, canonical-codes tail). The
     * counter reset (ordinals → bytes) is deliberate: observers see the ratio restart and climb
     * through the epilogue instead of sitting at 1.0 with no unit flow.
     */
    public void reportBytes(long completed, long total)
    {
        this.unit = Unit.BYTES;
        this.completed = completed;
        if (total > 0)
            this.total = total;
    }
}
