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

package org.apache.cassandra.index.sai.disk.v5;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.IntUnaryOperator;
import java.util.stream.StreamSupport;

import org.junit.Before;
import org.junit.Test;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.SparseBits;
import io.github.jbellis.jvector.vector.ArrayVectorFloat;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.cql.VectorTester;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter.Structure;
import org.apache.cassandra.index.sai.disk.vector.ConcurrentVectorValues;
import org.apache.cassandra.index.sai.disk.vector.RamAwareVectorValues;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class V5OnDiskOrdinalsMapTest extends VectorTester
{
    @Before
    public void setup() throws Throwable
    {
        super.setup();
        // this can be removed once LATEST is >= DC
        SAIUtil.setLatestVersion(Version.DC);
    }

    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private static final int DIMENSION = 2; // not relevant to postings mapping, nothing gained by using something larger

    @Test
    public void testRandomPostings() throws Exception
    {
        for (int i = 0; i < 10; i++) {
            for (Structure structure: Structure.values()) {
                testRandomPostingsOnce(structure);
            }
        }
    }

    @Test
    public void testOneToOne() throws Exception
    {
        var vv = generateVectors(4);
        Map<VectorFloat<?>, VectorPostings<String>> postingsMap = emptyPostingsMap();
        // 0 -> C
        // 1 -> A
        // 2 -> B
        postingsMap.put(vv.getVector(0), createPostings(0, "C"));
        postingsMap.put(vv.getVector(1), createPostings(1, "A"));
        postingsMap.put(vv.getVector(2), createPostings(2, "B"));
        computeRowIds(postingsMap);
        validate(Structure.ONE_TO_ONE, postingsMap, vv);
    }

    @Test
    public void testOneToMany() throws Exception
    {
        var vv = generateVectors(4);
        Map<VectorFloat<?>, VectorPostings<String>> postingsMap = emptyPostingsMap();
        // 0 -> B
        // 1 -> C, A
        // 2 -> D
        postingsMap.put(vv.getVector(0), createPostings(0, "B"));
        postingsMap.put(vv.getVector(1), createPostings(1, "C", "A"));
        postingsMap.put(vv.getVector(2), createPostings(2, "D"));
        computeRowIds(postingsMap);
        validate(Structure.ONE_TO_MANY, postingsMap, vv);
    }

    @Test
    public void testOneToManyLarger() throws Exception
    {
        var vv = generateVectors(4);
        Map<VectorFloat<?>, VectorPostings<String>> postingsMap = emptyPostingsMap();
        // 0 -> B
        // 1 -> C, A, E
        // 2 -> D, F
        // 3 -> G
        postingsMap.put(vv.getVector(0), createPostings(0, "B"));
        postingsMap.put(vv.getVector(1), createPostings(1, "C", "A", "E"));
        postingsMap.put(vv.getVector(2), createPostings(2, "D", "F"));
        postingsMap.put(vv.getVector(3), createPostings(3, "G"));
        computeRowIds(postingsMap);
        validate(Structure.ONE_TO_MANY, postingsMap, vv);
    }

    @Test
    public void testGeneric() throws Exception
    {
        var vv = generateVectors(4);
        Map<VectorFloat<?>, VectorPostings<String>> postingsMap = emptyPostingsMap();
        // 0 -> B
        // 1 -> C, A
        // 2 -> E    (no D, this makes it not 1:many)
        postingsMap.put(vv.getVector(0), createPostings(0, "B"));
        postingsMap.put(vv.getVector(1), createPostings(1, "C", "A"));
        postingsMap.put(vv.getVector(2), createPostings(2, "E"));
        computeRowIds(postingsMap);
        validate(Structure.ZERO_OR_ONE_TO_MANY, postingsMap, vv);
    }

    @Test
    public void testEmpty() throws Exception
    {
        var vv = generateVectors(0);
        Map<VectorFloat<?>, VectorPostings<String>> postingsMap = emptyPostingsMap();
        validate(Structure.ONE_TO_ONE, postingsMap, vv);
    }

    private static void computeRowIds(Map<VectorFloat<?>, VectorPostings<String>> postingsMap)
    {
        for (var p: postingsMap.entrySet())
        {
            p.getValue().computeRowIds(s -> {
                assert s.length() == 1;
                return s.charAt(0) - 'A';
            });
        }
    }

    private VectorPostings<String> createPostings(int ordinal, String... postings)
    {
        var vp = new VectorPostings<>(List.of(postings));
        vp.setOrdinal(ordinal);
        return vp;
    }

    private void testRandomPostingsOnce(Structure structure) throws Exception
    {
        var vectorValues = generateVectors(100 + randomInt(1000));
        var postingsMap = generatePostingsMap(vectorValues, structure);
        // call computeRowIds because that's how VectorPostings is designed.
        // since we're natively using ints as our rowids (not PrimaryKey objects) this can be the identity
        for (var p: postingsMap.entrySet())
            p.getValue().computeRowIds(x -> x);

        validate(structure, postingsMap, vectorValues);
    }

    private static int randomInt(int maxValueInclusive)
    {
        return getRandom().nextIntBetween(0, maxValueInclusive);
    }

    private <T> void validate(Structure structure, Map<VectorFloat<?>, VectorPostings<T>> postingsMap, RamAwareVectorValues vectorValues) throws IOException
    {
        // build the remapping and write the postings
        var remapped = V5VectorPostingsWriter.remapPostings(postingsMap);
        assert remapped.structure == structure : remapped.structure + " != " + structure;
        File tempFile = createTempFile("testfile");
        PostingsMetadata postingsMd = writePostings(tempFile, vectorValues, postingsMap, remapped);

        // test V5OnDiskOrdinalsMap
        try (FileHandle.Builder builder = new FileHandle.Builder(new ChannelProxy(tempFile)).compressed(false);
             FileHandle fileHandle = builder.complete())
        {
            var odom = new V5OnDiskOrdinalsMap(fileHandle, postingsMd.postingsOffset, postingsMd.postingsLength);
            assertEquals(structure, odom.structure);

            // check row -> ordinal and ordinal -> rows
            try (var rowIdsView = odom.getRowIdsView();
                 var ordinalsView = odom.getOrdinalsView())
            {
                for (var vp : postingsMap.values())
                {
                    var rowIds = vp.getRowIds().toIntArray();
                    Arrays.sort(rowIds);

                    var newOrdinal = remapped.ordinalMapper.oldToNew(vp.getOrdinal());
                    var it = rowIdsView.getSegmentRowIdsMatching(newOrdinal);
                    int[] a = StreamSupport.intStream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false).toArray();
                    Arrays.sort(a);

                    assertArrayEquals(rowIds, a);

                    for (int i = 0; i < rowIds.length; i++)
                    {
                        int ordinal = ordinalsView.getOrdinalForRowId(rowIds[i]);
                        assertEquals(newOrdinal, ordinal);
                    }
                }
            }

            // test buildOrdinalBits
            try (var ordinalsView = odom.getOrdinalsView())
            {
                for (int i = 0; i < 10; i++)
                {
                    // neither min nor max rowId has to actually exist in the postings
                    int minRowId = randomInt(vectorValues.size());
                    int maxRowId = minRowId + randomInt(vectorValues.size());
                    var bits = ordinalsView.buildOrdinalBits(minRowId, maxRowId, SparseBits::new);

                    var expectedBits = new SparseBits();
                    for (var vp : postingsMap.values())
                    {
                        for (int rowId : vp.getRowIds())
                        {
                            if (rowId >= minRowId && rowId <= maxRowId)
                                expectedBits.set(remapped.ordinalMapper.oldToNew(vp.getOrdinal()));
                        }
                    }

                    assertEqualsBits(expectedBits, bits, remapped.maxRowId);
                }
            }

            odom.close();
        }
    }

    private void assertEqualsBits(Bits b1, Bits b2, int maxBit)
    {
        for (int i = 0; i <= maxBit; i++)
            assertEquals(b1.get(i), b2.get(i));
    }

    private static <T> PostingsMetadata writePostings(File tempFile,
                                                  RandomAccessVectorValues vectorValues,
                                                  Map<VectorFloat<?>, VectorPostings<T>> postingsMap, V5VectorPostingsWriter.RemappedPostings remapped) throws IOException
    {
        SequentialWriter writer = new SequentialWriter(tempFile,
                                                       SequentialWriterOption.newBuilder().finishOnClose(true).build());

        long postingsOffset = writer.position();
        long postingsPosition = new V5VectorPostingsWriter<T>(remapped)
                                    .writePostings(writer, vectorValues, postingsMap);
        long postingsLength = postingsPosition - postingsOffset;

        writer.close();
        return new PostingsMetadata(postingsOffset, postingsLength);
    }

    private static class PostingsMetadata
    {
        public final long postingsOffset;
        public final long postingsLength;

        public PostingsMetadata(long postingsOffset, long postingsLength)
        {
            this.postingsOffset = postingsOffset;
            this.postingsLength = postingsLength;
        }
    }

    private static Map<VectorFloat<?>, VectorPostings<Integer>> generatePostingsMap(RandomAccessVectorValues vectorValues, Structure structure)
    {
        Map<VectorFloat<?>, VectorPostings<Integer>> postingsMap = emptyPostingsMap();

        // generate a list of rowIds that we'll initially assign 1:1 to ordinals,
        // leaving holes in the rowid sequence if we want ZERO_OR_ONE_TO_MANY
        var rowIds = new IntArrayList(vectorValues.size(), IntArrayList.DEFAULT_NULL_VALUE);
        IntUnaryOperator populator = structure == Structure.ZERO_OR_ONE_TO_MANY
                                   ? i -> 2 * i
                                   : i -> i;
        for (int i = 0; i < vectorValues.size(); i++)
            rowIds.add(populator.applyAsInt(i));

        // assign each ordinal a random rowid, without replacement
        for (int ordinal = 0; ordinal < vectorValues.size(); ordinal++)
        {
            var vector = vectorValues.getVector(ordinal);
            var rowIdIdx = randomIndex(rowIds);
            var vp = new VectorPostings<>(rowIds.getInt(rowIdIdx));
            rowIds.remove(rowIdIdx);
            vp.setOrdinal(ordinal);
            postingsMap.put(vector, vp);
        }

        if (structure == Structure.ONE_TO_ONE)
            return postingsMap;

        // make some of them 1:many
        int extraRows = 1 + randomInt(vectorValues.size());
        for (int i = 0; i < extraRows; i++)
        {
            var vector = randomVector(vectorValues);
            var vp = postingsMap.get(vector);
            vp.add(populator.applyAsInt(i + vectorValues.size()));
        }

        return postingsMap;
    }

    private static <T> ConcurrentSkipListMap<VectorFloat<?>, VectorPostings<T>> emptyPostingsMap()
    {
        return new ConcurrentSkipListMap<>((a, b) -> {
            return Arrays.compare(((ArrayVectorFloat) a).get(), ((ArrayVectorFloat) b).get());
        });
    }

    private static VectorFloat<?> randomVector(RandomAccessVectorValues ravv)
    {
        return ravv.getVector(randomInt(ravv.size() - 1));
    }

    private static int randomIndex(List<?> L)
    {
        return randomInt(L.size() - 1); // randomInt(max) is inclusive
    }

    /**
     * Generate `count` non-random vectors
     */
    private ConcurrentVectorValues generateVectors(int count)
    {
        var vectorValues = new ConcurrentVectorValues(DIMENSION);
        for (int i = 0; i < count; i++)
        {
            float[] rawVector = new float[DIMENSION];
            Arrays.fill(rawVector, (float) i);
            vectorValues.add(i, vts.createFloatVector(rawVector));
        }
        return vectorValues;
    }

    /**
     * Create a temporary file with the given prefix.
     */
    private static File createTempFile(String prefix)
    {
        File file = FileUtils.createTempFile(prefix, "tmp");
        file.deleteOnExit();
        return file;
    }

}
