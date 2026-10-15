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

package org.apache.cassandra.index.sai.disk.v5;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.util.FixedBitSet;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import net.openhft.chronicle.map.ChronicleMap;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter.Structure;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * RemappedPostings describes how original vector ordinals were renumbered ({@link #ordinalMapper}) and
 * holds a map of row id to original vector ordinal for rows that are NOT the first row associated with
 * their vector.
 * <p>
 * Example, using digits as ordianls and letters as row ids.  Postings map contains
 * 0 -> B, C
 * 1 -> A
 * 2 -> D
 * <p>
 * The ordinalMapper would map 0 <-> B, 1 <-> A, 2 <-> D and the extraPostings would be {C -> 0}
 */
public class RemappedPostings
{
    private static final Logger logger = LoggerFactory.getLogger(RemappedPostings.class);

    /** relationship of vector ordinals to row ids */
    public final Structure structure;
    /** the largest vector ordinal in the postings (inclusive) */
    public final int maxNewOrdinal;
    /** the largest rowId in the postings (inclusive) */
    public final int maxRowId;
    /** map from rowId to [original] vector ordinal; package-private for the deprecated V5VectorPostingsWriter encoders */
    @Nullable
    final Int2IntHashMap extraPostings;
    /** public api */
    public final OrdinalMapper ordinalMapper;

    /** visible for V2VectorPostingsWriter.remapPostings, everyone else should use factory methods */
    public RemappedPostings(Structure structure, int maxNewOrdinal, int maxRowId, Int2IntHashMap extraPostings, OrdinalMapper ordinalMapper)
    {
        this.structure = structure;
        this.maxNewOrdinal = maxNewOrdinal;
        this.maxRowId = maxRowId;
        this.extraPostings = extraPostings;
        this.ordinalMapper = ordinalMapper;
    }

    /**
     * Returns a writer for these postings, already configured with this object's {@code maxNewOrdinal},
     * {@code structure} and {@code maxRowId}.
     * <p>
     * The rowIds of a new ordinal come from the postings of the vector it maps back to; an omitted
     * ordinal is a hole. For {@link Structure#ONE_TO_ONE} the ordinal is its own rowId, and neither
     * {@code vectorValues} nor {@code postingsMap} is touched.
     * <p>
     * Callers should chain {@link V5VectorPostingsWriter#skipRowIdSorting()} when they know every
     * vector's rowId list is already in ascending order (e.g. compaction, where rows are added serially
     * in ascending rowId order). For the memtable flush path the lists are in insertion order and must
     * be sorted by the writer.
     */
    public V5VectorPostingsWriter writer(RandomAccessVectorValues vectorValues,
                                         Map<? extends VectorFloat<?>, ? extends VectorPostings<?>> postingsMap)
    {
        return V5VectorPostingsWriter.create(maxNewOrdinal, rowIdsOf(vectorValues, postingsMap))
                                     .structure(structure)
                                     .maxRowId(maxRowId);
    }

    private IntFunction<IntArrayList> rowIdsOf(RandomAccessVectorValues vectorValues,
                                               Map<? extends VectorFloat<?>, ? extends VectorPostings<?>> postingsMap)
    {
        if (structure == Structure.ONE_TO_ONE)
        {
            return ordinal -> {
                IntArrayList ids = new IntArrayList(1, IntArrayList.DEFAULT_NULL_VALUE);
                ids.addInt(ordinal);
                return ids;
            };
        }

        return newOrdinal -> {
            int oldOrdinal = ordinalMapper.newToOld(newOrdinal);
            if (oldOrdinal == OrdinalMapper.OMITTED)
                return new IntArrayList(); // a hole
            return postingsMap.get(vectorValues.getVector(oldOrdinal)).getRowIds();
        };
    }

    /**
     * This method describes the mapping done during construction of the graph so that we can easily create
     * an appropriate V5VectorPostingsWriter.  No ordinal remapping is performed because (V5) compaction writes
     * vectors to disk as they are added to the graph, so there is no opportunity to reorder the way there is
     * in a Memtable index.
     */
    public static RemappedPostings describeForCompaction(Structure structure, int graphSize, int maxRowId, int maxOrdinal, ChronicleMap<VectorFloat<?>, VectorPostings.CompactionVectorPostings> postingsMap)
    {
        assert !postingsMap.isEmpty(); // flush+compact should skip writing an index component in this case

        if (structure == Structure.ONE_TO_ONE)
        {
            return new RemappedPostings(Structure.ONE_TO_ONE,
                                        graphSize - 1,
                                        graphSize - 1,
                                        null,
                                        new OrdinalMapper.IdentityMapper(graphSize - 1));
        }

        if (structure == Structure.ONE_TO_MANY)
        {
            // compute extraOrdinals from the postingsMap
            Int2IntHashMap extraOrdinals = new Int2IntHashMap(Integer.MIN_VALUE);
            postingsMap.forEachEntry(entry -> {
                VectorPostings.CompactionVectorPostings.Marshaller.recordExtraOrdinals(entry, extraOrdinals);
            });

            Set<Integer> skippedOrdinals = extraOrdinals.keySet();
            return new RemappedPostings(Structure.ONE_TO_MANY,
                                        maxOrdinal,
                                        maxRowId,
                                        extraOrdinals,
                                        new OmissionAwareIdentityMapper(maxOrdinal, skippedOrdinals::contains));
        }

        assert structure == Structure.ZERO_OR_ONE_TO_MANY : structure;
        return createGenericIdentityMapping(postingsMap, maxRowId, maxOrdinal);
    }

    public static <T> RemappedPostings remapForMemtable(Map<VectorFloat<?>, ? extends VectorPostings<T>> postingsMap, Version version)
    {
        assert V5OnDiskFormat.writeV5VectorPostings(version);

        BiMap<Integer, Integer> ordinalMap = HashBiMap.create();
        Int2IntHashMap extraPostings = new Int2IntHashMap(Integer.MIN_VALUE);
        int minRow = Integer.MAX_VALUE;
        int maxRow = Integer.MIN_VALUE;
        int maxNewOrdinal = Integer.MIN_VALUE;
        int maxOldOrdinal = Integer.MIN_VALUE;
        int totalRowsAssigned = 0;

        // build the ordinalMap and extraPostings
        for (VectorPostings<T> vectorPostings : postingsMap.values())
        {
            assert !vectorPostings.isEmpty(); // deleted vectors should be cleaned out before remapping
            int[] a = vectorPostings.getRowIds().toIntArray();
            Arrays.sort(a);
            int rowId = a[0];
            int oldOrdinal = vectorPostings.getOrdinal();
            maxOldOrdinal = max(maxOldOrdinal, oldOrdinal);
            minRow = min(minRow, rowId);
            maxRow = max(maxRow, a[a.length - 1]);
            assert !ordinalMap.containsKey(oldOrdinal); // vector <-> ordinal should be unique
            ordinalMap.put(oldOrdinal, rowId);
            maxNewOrdinal = max(maxNewOrdinal, rowId);
            totalRowsAssigned += a.length; // all row ids should also be unique, but we can't easily check that
            if (a.length > 1)
            {
                for (int i = 1; i < a.length; i++)
                    extraPostings.put(a[i], oldOrdinal);
            }
        }
        assert totalRowsAssigned == 0 || totalRowsAssigned <= maxRow + 1: "rowids are not unique -- " + totalRowsAssigned + " >= " + maxRow;

        // derive the correct structure
        Structure structure;
        if (totalRowsAssigned > 0 && (minRow != 0 || totalRowsAssigned < maxRow + 1))
        {
            logger.debug("Not all rows are assigned vectors, cannot remap one-to-many");
            structure = Structure.ZERO_OR_ONE_TO_MANY;
        }
        else
        {
            structure = extraPostings.isEmpty()
                      ? Structure.ONE_TO_ONE
                      : Structure.ONE_TO_MANY;
            // override one-to-many to generic if there are too many holes
            if (structure == Structure.ONE_TO_MANY && V5VectorPostingsWriter.tooManyOrdinalMappingHoles(postingsMap.size(), maxRow))
                structure = Structure.ZERO_OR_ONE_TO_MANY;
            logger.debug("Remapped postings include {} unique vectors and {} 'extra' rows sharing them. Structure is {}",
                         ordinalMap.size(), extraPostings.size(), structure);
        }

        // create the mapping
        if (structure == Structure.ZERO_OR_ONE_TO_MANY)
            return createGenericRenumberedMapping(ordinalMap.keySet(), maxOldOrdinal, maxRow);
        // For ONE_TO_MANY, maxVectorOrdinal must equal maxRowId (all rows including extras fit within
        // [0, maxRow]), so use maxRow as both maxNewOrdinal and maxRowId.
        OrdinalMapper ordinalMapper = new BiMapMapper(maxRow, ordinalMap);
        return new RemappedPostings(structure, maxNewOrdinal, maxRow, extraPostings, ordinalMapper);
    }

    /**
     * return an exhaustive zero-to-many mapping with the live ordinals renumbered sequentially
     */
    private static RemappedPostings createGenericRenumberedMapping(Set<Integer> liveOrdinals, int maxOldOrdinal, int maxRow)
    {
        Int2IntHashMap oldToNew = new Int2IntHashMap(maxOldOrdinal, 0.65f, Integer.MIN_VALUE);
        int nextOrdinal = 0;
        for (int i = 0; i <= maxOldOrdinal; i++) {
            if (liveOrdinals.contains(i))
                oldToNew.put(i, nextOrdinal++);
        }
        return new RemappedPostings(Structure.ZERO_OR_ONE_TO_MANY,
                                    nextOrdinal - 1,
                                    maxRow,
                                    null,
                                    new OrdinalMapper.MapMapper(oldToNew));
    }

    /**
     * return an exhaustive zero-to-many mapping with no renumbering
     */
    public static <T> RemappedPostings createGenericIdentityMapping(Map<VectorFloat<?>, ? extends VectorPostings<T>> postingsMap)
    {
        int maxOldOrdinal = postingsMap.values().stream().mapToInt(VectorPostings::getOrdinal).max().orElseThrow();
        int maxRow = postingsMap.values().stream().flatMap(p -> p.getRowIds().stream()).mapToInt(i -> i).max().orElseThrow();
        FixedBitSet presentOrdinals = new FixedBitSet(maxOldOrdinal + 1);
        for (Map.Entry<VectorFloat<?>, ? extends VectorPostings<T>> entry : postingsMap.entrySet())
            presentOrdinals.set(entry.getValue().getOrdinal());
        return new RemappedPostings(Structure.ZERO_OR_ONE_TO_MANY,
                                    maxOldOrdinal,
                                    maxRow,
                                    null,
                                    new OmissionAwareIdentityMapper(maxOldOrdinal, i -> !presentOrdinals.get(i)));
    }

    /**
     * return an exhaustive zero-to-many mapping with no renumbering
     */
    public static RemappedPostings createGenericIdentityMapping(ChronicleMap<VectorFloat<?>, VectorPostings.CompactionVectorPostings> postingsMap, int maxRowId, int maxOldOrdinal)
    {
        FixedBitSet presentOrdinals = new FixedBitSet(maxOldOrdinal + 1);

        // Iterate the whole map using the low level API that avoids deserialization penalties.
        postingsMap.forEachEntry(entry -> {
            presentOrdinals.set(VectorPostings.CompactionVectorPostings.Marshaller.extractOrdinal(entry));
        });

        return new RemappedPostings(Structure.ZERO_OR_ONE_TO_MANY,
                                    maxOldOrdinal,
                                    maxRowId,
                                    null,
                                    new OmissionAwareIdentityMapper(maxOldOrdinal, i -> !presentOrdinals.get(i)));
    }

    public static class BiMapMapper implements OrdinalMapper
    {
        private final int maxOrdinal;
        private final BiMap<Integer, Integer> ordinalMap;

        public BiMapMapper(int maxNewOrdinal, BiMap<Integer, Integer> ordinalMap)
        {
            this.maxOrdinal = maxNewOrdinal;
            this.ordinalMap = ordinalMap;
        }

        @Override
        public int maxOrdinal()
        {
            return maxOrdinal;
        }

        @Override
        public int oldToNew(int i)
        {
            return ordinalMap.get(i);
        }

        @Override
        public int newToOld(int i)
        {
            return ordinalMap.inverse().getOrDefault(i, OMITTED);
        }
    }

    private static class OmissionAwareIdentityMapper implements OrdinalMapper
    {
        private final int maxVectorOrdinal;
        private final IntPredicate toSkip;

        public OmissionAwareIdentityMapper(int maxVectorOrdinal, IntPredicate toSkip)
        {
            this.maxVectorOrdinal = maxVectorOrdinal;
            this.toSkip = toSkip;
        }

        @Override
        public int maxOrdinal()
        {
            return maxVectorOrdinal;
        }

        @Override
        public int oldToNew(int i)
        {
            return i;
        }

        @Override
        public int newToOld(int i)
        {
            return toSkip.test(i) ? OrdinalMapper.OMITTED : i;
        }
    }
}
