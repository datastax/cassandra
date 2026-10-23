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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.locks.InterProcessLock;
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
import org.agrona.collections.IntArrayList;

import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter.Structure;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings.CompactionVectorPostings;
import org.apache.cassandra.io.util.FileUtils;

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
 *     <li>the ordinal mappings from source nodes to nodes of the merged graph ({@link #ordinalMapping()}).</li>
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
    /** Maps each surviving source vector (by handle) to the merged rowIds that use it (the postings per node). */
    private final ChronicleMap<VectorHandle, CompactionVectorPostings> mergedRowIdsBySourceVector;
    /** Retained solely to delete the temp file on {@link #close()}; ChronicleMap does not delete its own backing file. */
    private final org.apache.cassandra.io.util.File mergedRowIdsBySourceVectorFile;

    /** Maps each merged rowId to the packed source vector handle that supplies its vector (-1L if none). */
    private final MappedLongArray sourceVectorByMergedRowId;
    /** Retained solely to delete the temp file on {@link #close()}; {@link MappedLongArray} does not delete its own backing file. */
    private final org.apache.cassandra.io.util.File sourceVectorByMergedRowIdFile;

    /** Per-source exclusive upper bound on node ids; fixes source count and validates handles. */
    private final int[] idUpperBounds;
    /** Per-source live-node sets: bit {@code n} is set once node {@code n} has been assigned at least one output row. */
    private final AtomicFixedBitSet[] liveNodesBySource;

    /** Number of distinct surviving source nodes, i.e. the number of bits set across {@link #liveNodesBySource}. */
    private final AtomicInteger survivorsCount = new AtomicInteger();
    /** Highest output rowid passed to {@link #put}, or -1 if none. */
    private final AtomicInteger maxRowId = new AtomicInteger(-1);
    /** Total number of {@link #put} calls, i.e. total rows attributed to any vector. */
    private final AtomicInteger totalRows = new AtomicInteger(0);
    /** True once any source vector is shared by more than one merged row (i.e. the {@code absent==null} branch of {@link #put} fires). */
    private final AtomicBoolean hasDuplicateVectors = new AtomicBoolean(false);


    /**
     * @param idUpperBounds  per-source graph id upper bound, indexed by source; fixes the number of sources
     * @param estimatedRows  expected number of output rows, used to size the map
     */
    public VectorMergePostings(int[] idUpperBounds, long estimatedRows) throws IOException
    {
        this.mergedRowIdsBySourceVectorFile = FileUtils.createTempFile("merge_postings_chronicle_map", ".map");
        this.sourceVectorByMergedRowIdFile = FileUtils.createTempFile("merge_postings_source_vector_by_merged_row_id", ".map");
        this.idUpperBounds = idUpperBounds.clone();
        this.liveNodesBySource = IntStream.of(idUpperBounds)
                                          .mapToObj(AtomicFixedBitSet::new)
                                          .toArray(AtomicFixedBitSet[]::new);

        int entries = (int) Math.clamp((long) (1.1 * estimatedRows), 1000L, (long) Integer.MAX_VALUE - 100_000);
        this.sourceVectorByMergedRowId = new MappedLongArray(sourceVectorByMergedRowIdFile.toJavaIOFile(), entries);
        this.mergedRowIdsBySourceVector = ChronicleMapBuilder.of(VectorHandle.class, CompactionVectorPostings.class)
                                      .averageKeySize(VectorHandle.BYTES)
                                      .keySizeMarshaller(SizeMarshaller.constant(VectorHandle.BYTES))
                                      .keyMarshaller(new VectorHandle.Marshaller())
                                      .averageValueSize((double) VectorPostings.emptyBytesUsed()
                                                        + RamUsageEstimator.NUM_BYTES_OBJECT_REF
                                                        + 2 * Integer.BYTES)
                                      .valueMarshaller(new VectorPostings.Marshaller())
                                      .entries(entries)
                                      .createPersistedTo(mergedRowIdsBySourceVectorFile.toJavaIOFile());
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

        try (ExternalMapQueryContext<VectorHandle, CompactionVectorPostings, ?> ctx = mergedRowIdsBySourceVector.queryContext(handle))
        {
            InterProcessLock lock = ctx.writeLock();
            lock.lock();
            try
            {
                MapAbsentEntry<VectorHandle, CompactionVectorPostings> absent = ctx.absentEntry();
                if (absent != null)
                {
                    CompactionVectorPostings postings = new CompactionVectorPostings(0, segmentRowId);
                    absent.doInsert(ctx.wrapValueAsData(postings));
                }
                else
                {
                    hasDuplicateVectors.set(true);
                    MapEntry<VectorHandle, CompactionVectorPostings> entry = ctx.entry();
                    assert entry != null;
                    CompactionVectorPostings postings = entry.value().get();
                    postings.add(segmentRowId);
                    Data<CompactionVectorPostings> updated = ctx.wrapValueAsData(postings);
                    entry.doReplaceValue(updated);
                }
                // still under the entry's write lock, so the bit and the entry become visible together
                markLive(handle);
                sourceVectorByMergedRowId.put(segmentRowId, handle.pack());
                maxRowId.accumulateAndGet(segmentRowId, Math::max);
                totalRows.incrementAndGet();
            }
            finally
            {
                lock.unlock();
            }
        }
    }

    /**
     * Marks the node live and counts it if it is newly so.
     */
    private void markLive(VectorHandle handle)
    {
        AtomicFixedBitSet bits = liveNodesBySource[handle.source()];
        if (!bits.getAndSet(handle.nodeId()))
            survivorsCount.incrementAndGet();
    }

    /** The output rowids attributed to {@code handle}'s node, or null if no row was. */
    public CompactionVectorPostings get(VectorHandle handle)
    {
        return mergedRowIdsBySourceVector.get(handle);
    }

    /** Number of distinct surviving source nodes. */
    public int size()
    {
        return survivorsCount.get();
    }

    public boolean isEmpty()
    {
        return survivorsCount.get() == 0;
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
     * liveNodes set itself, not a copy: call after ingest is drained, and do not mutate it.
     */
    public BitSet liveNodes(int source)
    {
        return liveNodesBySource[source];
    }

    /**
     * The surviving nodes of every source, indexed by source. The sets are the liveNodes sets themselves, not
     * copies: call after ingest is drained, and do not mutate them.
     */
    public List<? extends BitSet> liveNodesBySource()
    {
        return List.of(liveNodesBySource);
    }

    /**
     * Builds the per-source {@link OrdinalMapper}s jvector's compactor needs, assigning the surviving nodes
     * output ordinals {@code [0, size())}. Call after ingest is drained.
     *
     * <p>Ordinals are assigned in ascending output-rowId order and currently no holes in numbering are produced.
     * Therefore, only ONE_TO_ONE and ZERO_OR_ONE_TO_MANY structures are supported.</p>
     *
     * <p>One ChronicleMap lookup per surviving node populates two parallel {@code rowId}-indexed arrays
     * ({@code sourceByRowId}, {@code nodeIdByRowId}); those are then walked once in rowId order to assign
     * ordinals.
     *
     * @return one mapper per source, indexed by source
     */
    public List<OrdinalMapper> ordinalMapping()
    {
        int maxRow = maxRowId();

        List<Map<Integer, Integer>> oldToNew = new ArrayList<>(liveNodesBySource.length);
        for (AtomicFixedBitSet liveNodes : liveNodesBySource)
            oldToNew.add(new HashMap<>(Math.max(16, (int) (liveNodes.cardinality() / 0.75f) + 1)));

        int nextOrdinal = 0;
        for (int rowId = 0; rowId <= maxRow; rowId++)
        {
            long packed = sourceVectorByMergedRowId.get(rowId);
            if (packed == -1L)
                continue; // no vector for this rowId
            VectorHandle handle = VectorHandle.unpack(packed);
            Map<Integer, Integer> map = oldToNew.get(handle.source());
            if (!map.containsKey(handle.nodeId()))
                map.put(handle.nodeId(), nextOrdinal++);
        }

        return oldToNew.stream()
                       .map(OrdinalMapper.MapMapper::new)
                       .collect(Collectors.toList());
    }

    /** The number of source graphs, i.e. the valid range of {@code source} is {@code [0, sourceCount())}. */
    public int sourceCount()
    {
        return idUpperBounds.length;
    }

    /** Returns the temporary backing files owned by this instance; exposed for testing only. */
    @VisibleForTesting
    java.io.File[] backingFiles()
    {
        return new java.io.File[]{ mergedRowIdsBySourceVectorFile.toJavaIOFile(), sourceVectorByMergedRowIdFile.toJavaIOFile() };
    }

    /**
     * Builds the function from merged ordinal to its rowId list, using the compactor's {@code effective} mappers.
     *
     * @param effective per-source ordinal mappers as returned by the compactor's {@code effectiveRemappers()}
     * @return a function from merged ordinal to rowId list; returns {@code null} for an empty (hole) ordinal
     * @throws IllegalStateException if mapper count does not match {@link #sourceCount()}
     */
    public IntFunction<IntArrayList> rowIdsByMergedOrdinal(List<OrdinalMapper> effective)
    {
        if (effective.size() != sourceCount())
            throw new IllegalStateException("rowIdsByMergedOrdinal: expected " + sourceCount() + " mappers but got " + effective.size());

        return rowId -> {
            for (int s = 0; s < effective.size(); s++)
            {
                int nodeId = effective.get(s).newToOld(rowId);
                if (nodeId != OrdinalMapper.OMITTED)
                {
                    CompactionVectorPostings p = get(new VectorHandle(s, nodeId));
                    return p == null ? null : p.getRowIds();
                }
            }
            return null;
        };
    }

    /**
     * Determines the {@link Structure} for writing the postings of this merge output by inspecting
     * the effective ordinal mapping directly.
     *
     * <p>Returns {@link Structure#ONE_TO_ONE} only when every condition holds:
     * <ul>
     *   <li>No vector has more than one row (no duplicate-vector rows).</li>
     *   <li>There are no gaps: every rowId in {@code [0, maxRowId]} has a vector ({@code maxRowId + 1 == totalRows}).</li>
     *   <li>For every {@code rowId}, the effective mapper maps the owning node to ordinal {@code rowId}
     *       (i.e., {@code rowId == vectorOrdinal} for all rows).</li>
     * </ul>
     * Otherwise returns {@link Structure#ZERO_OR_ONE_TO_MANY}.
     *
     * @param effective per-source ordinal mappers as returned by the compactor's {@code effectiveRemappers()}
     */
    public Structure discoverStructure(List<OrdinalMapper> effective)
    {
        // Quick exits: duplicates or gaps mean ONE_TO_ONE is impossible regardless of the mapping.
        if (hasDuplicateVectors.get())
            return Structure.ZERO_OR_ONE_TO_MANY;

        int maxRow = maxRowId();
        if (maxRow + 1 != totalRows.get())
            return Structure.ZERO_OR_ONE_TO_MANY;

        // Verify that the effective mapping sends each row's vector to the ordinal equal to that rowId.
        // sourceVectorByMergedRowId.get(rowId) == -1 means no vector for that rowId (gap already ruled out above, but
        // the array may be longer than maxRow + 1, so we only iterate up to maxRow inclusive).
        for (int rowId = 0; rowId <= maxRow; rowId++)
        {
            long packed = sourceVectorByMergedRowId.get(rowId);
            if (packed == -1L)
                return Structure.ZERO_OR_ONE_TO_MANY;
            VectorHandle h = VectorHandle.unpack(packed);
            int ordinal = effective.get(h.source()).oldToNew(h.nodeId());
            if (ordinal != rowId)
                return Structure.ZERO_OR_ONE_TO_MANY;
        }
        return Structure.ONE_TO_ONE;
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(sourceVectorByMergedRowId, mergedRowIdsBySourceVector);
        sourceVectorByMergedRowIdFile.delete();
        mergedRowIdsBySourceVectorFile.delete();
    }
}
