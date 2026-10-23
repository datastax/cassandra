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

package org.apache.cassandra.db.compaction;

import javax.annotation.Nullable;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * A per-compaction-thread tap on the row-merge stream, letting an index
 * implementation observe, for every merged row, WHICH source version's cells won
 * reconciliation — provenance that is otherwise erased by the time the merged row
 * reaches the output sstable's flush observers.
 *
 * <p>Consumers (today: the SAI vector graph merge, see
 * {@code doc/vector_merge_ordinal_identity.md}) register a {@link Sink} on the
 * compaction thread before rows flow; {@link CompactionIterator}'s merge listener
 * calls {@link #emit} for every merged row while a sink is registered (a single
 * null check otherwise), and clears the slot when the iterator closes. The hook is
 * deliberately index-agnostic: it exposes only core types.
 *
 * <p>Ordering contract: for any given row, {@code emit} fires on the compaction
 * thread strictly before the output writer's flush observers see that row (the
 * listener sits at the merge, upstream of garbage-skipping and purging). Rows
 * dropped downstream are emitted here but never consumed — sinks must tolerate
 * that (e.g. by bounded, identity-validated buffering).
 */
public final class CompactionRowSourceTagging
{
    /** Receives one callback per merged row. Implementations must be cheap: this runs on the compaction hot path. */
    public interface Sink
    {
        /**
         * @param partitionKey the merged row's partition key
         * @param merged       the reconciled row (pre-purge)
         * @param versions     per-source row versions, aligned with the compaction's scanner list;
         *                     {@code null} entries for sources not containing the row
         * @param resolver     maps a version index (+ key) to the backing source sstable
         */
        void onMergedRow(DecoratedKey partitionKey, Row merged, Row[] versions, SourceResolver resolver);
    }

    /**
     * Resolves a merge version index to the source sstable that produced it. A single scanner may back
     * several non-overlapping sstables (leveled / unified range scanners), so the partition key is
     * required to pick the one whose bounds cover the row.
     */
    public interface SourceResolver
    {
        /** @return the backing sstable for {@code versionIdx} covering {@code key}, or null if unresolvable */
        @Nullable
        SSTableReader sourceOf(int versionIdx, DecoratedKey key);
    }

    private static final ThreadLocal<Sink> CURRENT = new ThreadLocal<>();

    private CompactionRowSourceTagging()
    {
    }

    /**
     * Register {@code sink} for the current thread's compaction. Idempotent by design: re-registering
     * the same instance (e.g. across output-writer switches) is a no-op; registering a different
     * instance replaces the previous one (a thread runs one compaction at a time).
     */
    public static void register(Sink sink)
    {
        CURRENT.set(sink);
    }

    /** The sink registered on this thread, or null. */
    @Nullable
    public static Sink current()
    {
        return CURRENT.get();
    }

    /** Clear the current thread's registration. Called by {@link CompactionIterator#close()}. */
    public static void clear()
    {
        CURRENT.remove();
    }

    /** Called by the compaction row-merge listener for every merged row. */
    static void emit(DecoratedKey partitionKey, Row merged, Row[] versions, SourceResolver resolver)
    {
        Sink sink = CURRENT.get();
        if (sink != null)
            sink.onMergedRow(partitionKey, merged, versions, resolver);
    }
}
