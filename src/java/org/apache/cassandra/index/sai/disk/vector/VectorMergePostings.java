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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

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
import io.github.jbellis.jvector.util.DocIdSetIterator;
import io.github.jbellis.jvector.util.RamUsageEstimator;
import org.agrona.collections.IntArrayList;

import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter.Structure;
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
    /** Total number of {@link #put} calls, i.e. total rows attributed to any vector. */
    private final AtomicInteger totalRows = new AtomicInteger(0);
    /** True once any handle has received more than one row (i.e. the {@code absent==null} branch of {@link #put} fires). */
    private final AtomicBoolean hasMultiRowVectors = new AtomicBoolean(false);

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
                hasMultiRowVectors.set(true);
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
            totalRows.incrementAndGet();
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
     * output ordinals {@code [0, size())}. Call after ingest is drained.
     *
     * <p>Ordinals are assigned in ascending output-rowId order: for each rowId in {@code [0, maxRowId]},
     * the first (lowest) rowId belonging to a surviving node determines that node's ordinal. This ordering
     * guarantees that {@code min(rowIds(ordinal)) == ordinal} for every non-empty ordinal, which is the
     * invariant required by {@link Structure#ONE_TO_ONE} and {@link Structure#ONE_TO_MANY}.
     *
     * <p>One ChronicleMap lookup per surviving node populates two parallel {@code rowId}-indexed arrays
     * ({@code sourceByRowId}, {@code nodeIdByRowId}); those are then walked once in rowId order to assign
     * ordinals.
     *
     * @return one mapper per source, indexed by source
     */
    public List<OrdinalMapper> ordinalMappers()
    {
        int maxRow = maxRowId();

        // Step 1: build parallel rowId → (source, nodeId) arrays.
        int[] sourceByRowId = new int[maxRow + 1];
        int[] nodeIdByRowId = new int[maxRow + 1];
        java.util.Arrays.fill(sourceByRowId, -1);

        for (int s = 0; s < live.length; s++)
        {
            BitSet survivingS = live[s];
            for (int nodeId = survivingS.nextSetBit(0);
                 nodeId >= 0 && nodeId != DocIdSetIterator.NO_MORE_DOCS;
                 nodeId = survivingS.nextSetBit(nodeId + 1))
            {
                CompactionVectorPostings p = get(new VectorHandle(s, nodeId));
                if (p == null) continue;
                IntArrayList ids = p.getRowIds();
                for (int i = 0; i < ids.size(); i++)
                {
                    int rowId = ids.getInt(i);
                    if (rowId >= 0 && rowId <= maxRow)
                    {
                        sourceByRowId[rowId] = s;
                        nodeIdByRowId[rowId] = nodeId;
                    }
                }
            }
        }

        // Step 2: walk rowIds in order; on first encounter of each (source, nodeId) assign next ordinal.
        List<Map<Integer, Integer>> oldToNew = new ArrayList<>(live.length);
        for (int s = 0; s < live.length; s++)
            oldToNew.add(new HashMap<>(Math.max(16, (int) (live[s].cardinality() / 0.75f) + 1)));

        int nextOrdinal = 0;
        for (int rowId = 0; rowId <= maxRow; rowId++)
        {
            int s = sourceByRowId[rowId];
            if (s < 0) continue; // no vector for this rowId
            Map<Integer, Integer> map = oldToNew.get(s);
            int nodeId = nodeIdByRowId[rowId];
            if (!map.containsKey(nodeId))
                map.put(nodeId, nextOrdinal++);
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

    /**
     * Builds the function from merged ordinal to its rowId list, using the compactor's {@code effective} mappers.
     * Validates that {@code effective.size() == sourceCount()}, that every mapped ordinal falls in
     * {@code [0, size())}, and that no merged ordinal is claimed twice. Because the number of surviving nodes
     * equals {@code size()}, all ordinals are guaranteed to be filled once these checks pass.
     *
     * @param effective per-source ordinal mappers as returned by the compactor's {@code effectiveRemappers()}
     * @return a function from merged ordinal to rowId list; returns {@code null} for an empty (hole) ordinal
     * @throws IllegalStateException if validation fails
     */
    public IntFunction<IntArrayList> rowIdsOf(List<OrdinalMapper> effective)
    {
        if (effective.size() != sourceCount())
            throw new IllegalStateException("rowIdsOf: expected " + sourceCount() + " mappers but got " + effective.size());

        int totalOrdinals = size();
        long[] handles = new long[totalOrdinals];
        var claimed = new java.util.BitSet(totalOrdinals);
        int mapped = 0;
        for (int s = 0; s < effective.size(); s++)
        {
            OrdinalMapper mapper = effective.get(s);
            BitSet survivingS = live[s];
            for (int nodeId = survivingS.nextSetBit(0);
                 nodeId >= 0 && nodeId != DocIdSetIterator.NO_MORE_DOCS;
                 nodeId = survivingS.nextSetBit(nodeId + 1))
            {
                int merged = mapper.oldToNew(nodeId);
                if (merged < 0 || merged >= totalOrdinals)
                    throw new IllegalStateException(String.format(
                            "rowIdsOf: source %d node %d mapped to merged ordinal %d, outside [0, %d)",
                            s, nodeId, merged, totalOrdinals));
                if (claimed.get(merged))
                    throw new IllegalStateException(String.format(
                            "rowIdsOf: merged ordinal %d claimed twice (second claimant: source %d node %d)",
                            merged, s, nodeId));
                claimed.set(merged);
                handles[merged] = new VectorHandle(s, nodeId).pack();
                mapped++;
            }
        }
        if (mapped != totalOrdinals)
            throw new IllegalStateException("rowIdsOf: mapped " + mapped + " nodes but expected " + totalOrdinals);

        return g -> {
            var p = get(VectorHandle.unpack(handles[g]));
            return p == null ? null : p.getRowIds();
        };
    }

    /**
     * Determines the best {@link Structure} for writing the postings of this merge output.
     *
     * <p>Because {@link #ordinalMappers()} assigns ordinals in ascending output-rowId order, the invariant
     * {@code min(rowIds(ordinal)) == ordinal} holds by construction — provided the compactor preserved
     * the proposed ordinal assignment ({@code !reordered}) and every row in {@code [0, maxRowId]} has a
     * vector ({@code totalRows == maxRowId + 1}).  Structure is then determined by:
     *
     * <ol>
     *   <li>{@code reordered} → {@link Structure#ZERO_OR_ONE_TO_MANY}: compactor renumbered ordinals.</li>
     *   <li>{@code maxRowId + 1 > totalRows} → {@link Structure#ZERO_OR_ONE_TO_MANY}: some rows have no vector.</li>
     *   <li>{@code !hasMultiRowVectors} → {@link Structure#ONE_TO_ONE}: bijection between ordinals and rows.</li>
     *   <li>{@code !tooManyOrdinalMappingHoles} → {@link Structure#ONE_TO_MANY}: within the hole-density threshold.</li>
     *   <li>otherwise → {@link Structure#ZERO_OR_ONE_TO_MANY}.</li>
     * </ol>
     *
     * @param effective per-source ordinal mappers returned by the compactor; used only to check {@code reordered}
     * @param reordered {@code true} when the compactor changed the proposed ordinal assignment
     */
    public Structure detectStructure(List<OrdinalMapper> effective, boolean reordered)
    {
        if (reordered)
            return Structure.ZERO_OR_ONE_TO_MANY;

        int maxRow = maxRowId();
        if (maxRow + 1 > totalRows.get())
            return Structure.ZERO_OR_ONE_TO_MANY;

        if (!hasMultiRowVectors.get())
            return Structure.ONE_TO_ONE;

        if (!V5VectorPostingsWriter.tooManyOrdinalMappingHoles(size(), maxRow + 1))
            return Structure.ONE_TO_MANY;

        return Structure.ZERO_OR_ONE_TO_MANY;
    }

    @Override
    public void close() throws IOException
    {
        map.close();
        mapFile.delete();
    }
}
