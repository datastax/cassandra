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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionRowSourceTagging;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * The SAI vector-merge {@link CompactionRowSourceTagging.Sink}: for every merged row, records
 * per registered vector column WHICH source sstable's cell won reconciliation, keyed by row
 * identity, in a small per-thread ring. The vector merge's segment builder consumes entries
 * synchronously on the same compaction thread when the flush observer hands it the row
 * (see {@code doc/vector_merge_ordinal_identity.md}).
 *
 * <p>Winner detection uses CELL REFERENCE IDENTITY: Cassandra's cell reconciliation returns one
 * of the input cell objects, so the winning version is the one whose cell for the column
 * {@code ==} the merged row's cell. Exact under timestamp ties and value tie-breaks by
 * construction, zero extra I/O.
 *
 * <p>The ring is bounded and identity-validated: rows dropped downstream (purge, garbage
 * skipping) leave entries that are simply overwritten by ring wrap; consumers match on
 * {@code (partition key, clustering, column)}, never on position. Capacity only needs to cover
 * the pipeline's small lookahead between the merge listener and the flush observer (one row on
 * the current pipeline); 64 leaves two orders of magnitude of slack.
 *
 * <p>Threading: {@link #onMergedRow} and {@link #consume} both run on the compaction thread —
 * the ring itself is thread-confined. Only column registration is cross-thread-safe.
 */
public final class VectorSourceTagRing implements CompactionRowSourceTagging.Sink
{
    private static final int CAPACITY = 64; // power of two

    /** One ring slot: a row's identity plus per-column winning source. */
    private static final class Entry
    {
        DecoratedKey key;
        Clustering<?> clustering;
        // Parallel arrays sized to the (tiny) registered-column count at write time.
        ColumnMetadata[] columns;
        SSTableId<?>[] winners;
    }

    private final Entry[] ring = new Entry[CAPACITY];
    private int head; // next slot to write

    /**
     * Columns to tag. Registered by each vector index's writer at construction; never removed
     * for the compaction's lifetime (the whole sink is dropped when the compaction iterator
     * closes). ConcurrentHashMap-backed set only for safe publication — mutation happens before
     * rows flow on the compaction thread.
     */
    private final Set<ColumnMetadata> columns = ConcurrentHashMap.newKeySet();

    public VectorSourceTagRing()
    {
        for (int i = 0; i < CAPACITY; i++)
            ring[i] = new Entry();
    }

    /**
     * Register interest in {@code column} on the CURRENT thread's sink, creating and
     * registering the sink if this thread has none. Returns the ring so the caller can
     * later {@link #consume}. Idempotent per (thread, column).
     */
    public static VectorSourceTagRing acquireForThread(ColumnMetadata column)
    {
        CompactionRowSourceTagging.Sink sink = CompactionRowSourceTagging.current();
        VectorSourceTagRing ours = sink instanceof VectorSourceTagRing ? (VectorSourceTagRing) sink : null;
        if (ours == null)
        {
            ours = new VectorSourceTagRing();
            CompactionRowSourceTagging.register(ours);
        }
        ours.columns.add(column);
        return ours;
    }

    @Override
    public void onMergedRow(DecoratedKey partitionKey, Row merged, Row[] versions,
                            CompactionRowSourceTagging.SourceResolver resolver)
    {
        // Cheap pre-check: only rows carrying at least one registered column are recorded.
        int hits = 0;
        for (ColumnMetadata column : columns)
        {
            if (merged.getCell(column) != null)
                hits++;
        }
        if (hits == 0)
            return;

        Entry e = ring[head];
        head = (head + 1) & (CAPACITY - 1);
        e.key = partitionKey;
        e.clustering = merged.clustering();
        if (e.columns == null || e.columns.length != hits)
        {
            e.columns = new ColumnMetadata[hits];
            e.winners = new SSTableId<?>[hits];
        }

        int slot = 0;
        for (ColumnMetadata column : columns)
        {
            Cell<?> mergedCell = merged.getCell(column);
            if (mergedCell == null)
                continue;
            SSTableId<?> winner = null;
            for (int i = 0; i < versions.length; i++)
            {
                Row v = versions[i];
                // Reference identity: reconciliation returns one of the input cells.
                if (v != null && v.getCell(column) == mergedCell)
                {
                    SSTableReader source = resolver.sourceOf(i, partitionKey);
                    if (source != null)
                        winner = source.descriptor.id;
                    org.slf4j.LoggerFactory.getLogger(VectorSourceTagRing.class).info(
                            "DEBUG onMergedRow: key={}, col={}, match at versionIdx={}, source={}, winner={}",
                            partitionKey, column.name, i, source != null ? source.descriptor.id : "null", winner);
                    break;
                }
            }
            if (winner == null)
            {
                org.slf4j.LoggerFactory.getLogger(VectorSourceTagRing.class).info(
                        "DEBUG onMergedRow: NO version matched mergedCell for key={}, col={}, versions.length={}",
                        partitionKey, column.name, versions.length);
            }
            e.columns[slot] = column;
            e.winners[slot] = winner;
            slot++;
        }
    }

    /**
     * Look up (and logically consume) the winning source sstable recorded for
     * {@code (partitionKey, clustering, column)}. Returns null when the row was never tagged
     * (no compaction listener pass, identity mismatch, or ring wrap) — the caller treats that
     * as an anomaly. Runs on the compaction thread.
     */
    @Nullable
    public SSTableId<?> consume(DecoratedKey partitionKey, Clustering<?> clustering, ColumnMetadata column)
    {
        // Scan newest-first: the entry for the row being flushed is almost always the one
        // written immediately before this call.
        for (int back = 1; back <= CAPACITY; back++)
        {
            Entry e = ring[(head - back) & (CAPACITY - 1)];
            if (e.key == null)
                return null; // reached never-written slots
            if (!e.key.equals(partitionKey) || !clusteringEquals(e.clustering, clustering))
                continue;
            for (int i = 0; i < e.columns.length; i++)
            {
                if (e.columns[i] == column || e.columns[i].equals(column))
                    return e.winners[i];
            }
            return null;
        }
        return null;
    }

    private static boolean clusteringEquals(Clustering<?> a, Clustering<?> b)
    {
        if (a == b)
            return true;
        if (a == null || b == null)
            return false;
        return a.equals(b);
    }
}
