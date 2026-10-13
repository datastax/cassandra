/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.MapEntry;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.util.AtomicFixedBitSet;
import io.github.jbellis.jvector.util.BitSet;
import io.github.jbellis.jvector.util.RamUsageEstimator;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings.CompactionVectorPostings;
import org.apache.cassandra.io.util.File;

/**
 * Tracks which vectors from the source graphs survive a compaction, and which rows of the compacted segment
 * refer to each of them.
 *
 * <p>As the compacted segment's rows are ingested via {@link #put}, each row is attributed to the source
 * graph node ({@link VectorHandle}) that supplies its vector. From this the class provides:
 * <ul>
 *     <li>the postings of each surviving node, i.e. the output rowids that use its vector ({@link #get});</li>
 *     <li>the set of surviving nodes of each source graph ({@link #liveNodes(int)}), which the graph
 *         compactor uses to decide what to carry over;</li>
 *     <li>the ordinal mappings from source nodes to nodes of the merged graph ({@link #ordinalMappers()}).</li>
 * </ul>
 * The postings and the surviving-node sets are always consistent with each other: a node is live if and
 * only if at least one output row refers to it.
 *
 * <p>{@link #put} may be called concurrently. The other accessors, except {@link #size()} and
 * {@link #isEmpty()}, should be used only after ingestion has finished.
 *
 * <p>The instance owns a temporary on-disk map; {@link #close()} releases it.
 */
public final class VectorMergePostings implements AutoCloseable
{
    private final ChronicleMap<VectorHandle, CompactionVectorPostings> map;
    private final File mapFile;
    private final int[] idUpperBounds;
    /** Per-source live sets. */
    private final AtomicFixedBitSet[] live;
    /** Number of distinct surviving source nodes, i.e. the number of bits set across {@link #live}. */
    private final AtomicInteger survivors = new AtomicInteger();
    /** Highest output rowid passed to {@link #put}, or -1 if none. */
    private final AtomicInteger maxRowId = new AtomicInteger(-1);

    /**
     * @param mapFile       temp file backing the map; deleted by {@link #close()}
     * @param idUpperBounds per-source graph id upper bound, indexed by source; fixes the number of sources
     * @param estimatedRows expected number of output rows, used to size the map
     */
    public VectorMergePostings(File mapFile, int[] idUpperBounds, long estimatedRows) throws IOException
    {
        this.mapFile = mapFile;
        this.idUpperBounds = idUpperBounds.clone();
        this.live = new AtomicFixedBitSet[idUpperBounds.length];
        for (int s = 0; s < idUpperBounds.length; s++)
            live[s] = new AtomicFixedBitSet(idUpperBounds[s]);

        int entries = (int) Math.clamp((long) (1.1 * estimatedRows), 1000L, (long) Integer.MAX_VALUE - 100_000);
        this.map = ChronicleMapBuilder.of(VectorHandle.class, CompactionVectorPostings.class)
                                      .averageKeySize(VectorHandle.BYTES)
                                      .keySizeMarshaller(SizeMarshaller.constant(VectorHandle.BYTES))
                                      .keyMarshaller(new VectorHandle.Marshaller())
                                      .averageValueSize((double) VectorPostings.emptyBytesUsed()
                                                        + RamUsageEstimator.NUM_BYTES_OBJECT_REF
                                                        + 2 * Integer.BYTES)
                                      .valueMarshaller(new VectorPostings.Marshaller())
                                      .entries(entries)
                                      .createPersistedTo(mapFile.toJavaIOFile());
    }

    /**
     * Records that output row {@code segmentRowId} takes its vector from {@code handle}'s source node, and
     * marks that node live. Safe to call concurrently.
     *
     * @throws IllegalArgumentException if the handle does not name a node of a source graph
     */
    public void put(VectorHandle handle, int segmentRowId)
    {
        if (handle.source() < 0 || handle.source() >= idUpperBounds.length)
            throw new IllegalArgumentException(String.format("%s names a source outside [0, %d)", handle, idUpperBounds.length));
        if (handle.nodeId() < 0 || handle.nodeId() >= idUpperBounds[handle.source()])
            throw new IllegalArgumentException(String.format("%s names a node outside [0, %d)", handle, idUpperBounds[handle.source()]));

        try (ExternalMapQueryContext<VectorHandle, CompactionVectorPostings, ?> ctx = map.queryContext(handle))
        {
            //noinspection LockAcquiredButNotSafelyReleased
            ctx.writeLock().lock();
            MapAbsentEntry<VectorHandle, CompactionVectorPostings> absent = ctx.absentEntry();
            if (absent != null)
            {
                CompactionVectorPostings postings = new CompactionVectorPostings(0, segmentRowId);
                absent.doInsert(ctx.wrapValueAsData(postings));
            }
            else
            {
                MapEntry<VectorHandle, CompactionVectorPostings> entry = ctx.entry();
                assert entry != null;
                CompactionVectorPostings postings = entry.value().get();
                postings.add(segmentRowId);
                Data<CompactionVectorPostings> updated = ctx.wrapValueAsData(postings);
                entry.doReplaceValue(updated);
            }
            // still under the entry's write lock, so the bit and the entry become visible together
            markLive(handle);
            maxRowId.accumulateAndGet(segmentRowId, Math::max);
        }
    }

    /**
     * Marks the node live and counts it if it is newly so.
     */
    private void markLive(VectorHandle handle)
    {
        AtomicFixedBitSet bits = live[handle.source()];
        if (!bits.getAndSet(handle.nodeId()))
            survivors.incrementAndGet();
    }

    /** The output rowids attributed to {@code handle}'s node, or null if no row was. */
    public CompactionVectorPostings get(VectorHandle handle)
    {
        return map.get(handle);
    }

    /** Number of distinct surviving source nodes. */
    public int size()
    {
        return survivors.get();
    }

    public boolean isEmpty()
    {
        return survivors.get() == 0;
    }

    /** The highest output rowid ingested, or -1 if none. */
    public int maxRowId()
    {
        return maxRowId.get();
    }

    /** The exclusive upper bound of node ids in source {@code source}'s graph. */
    public int idUpperBound(int source)
    {
        return idUpperBounds[source];
    }

    /**
     * The surviving nodes of source {@code source}, sized exactly to that source's id upper bound. This is the
     * live set itself, not a copy: call after ingest is drained, and do not mutate it.
     */
    public BitSet liveNodes(int source)
    {
        return live[source];
    }

    /**
     * The surviving nodes of every source, indexed by source. The sets are the live sets themselves, not
     * copies: call after ingest is drained, and do not mutate them.
     */
    public List<? extends BitSet> liveNodes()
    {
        return List.of(live);
    }

    /**
     * Builds the per-source {@link OrdinalMapper}s jvector's compactor needs, assigning the surviving nodes
     * output ordinals {@code [0, size())}. Pure CPU over the live sets, no vector I/O. Call after ingest is
     * drained.
     *
     * <p>Ordinals are assigned in ascending node-id order across all sources in parallel: for each node id,
     * every source is visited in turn and the next output ordinal goes to whichever source has that node
     * live. A node with local id N therefore gets a lower output ordinal than any node with local id
     * M &gt; N, whichever source either came from, which is the ordering a ONE_TO_ONE-compatible output needs.
     *
     * @return one mapper per source, indexed by source
     */
    public List<OrdinalMapper> ordinalMappers()
    {
        List<Map<Integer, Integer>> oldToNew = new ArrayList<>(live.length);
        int maxIdBound = 0;
        for (AtomicFixedBitSet bits : live)
        {
            oldToNew.add(new HashMap<>(Math.max(16, (int) (bits.cardinality() / 0.75f) + 1)));
            maxIdBound = Math.max(maxIdBound, bits.length());
        }

        int nextOrdinal = 0;
        for (int nodeId = 0; nodeId < maxIdBound; nodeId++)
        {
            for (int s = 0; s < live.length; s++)
            {
                if (nodeId < live[s].length() && live[s].get(nodeId))
                    oldToNew.get(s).put(nodeId, nextOrdinal++);
            }
        }

        List<OrdinalMapper> mappers = new ArrayList<>(live.length);
        for (Map<Integer, Integer> m : oldToNew)
            mappers.add(new OrdinalMapper.MapMapper(m));
        return mappers;
    }

    /** The number of source graphs, i.e. the valid range of {@code source} is {@code [0, sourceCount())}. */
    public int sourceCount()
    {
        return idUpperBounds.length;
    }

    @Override
    public void close() throws IOException
    {
        map.close();
        mapFile.delete();
    }
}
