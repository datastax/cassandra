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

package org.apache.cassandra.index.sai.disk.v2.blockindex;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeMultimap;
import com.google.common.collect.TreeRangeSet;
import org.apache.commons.lang.SerializationUtils;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.cursors.IntLongCursor;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictCompress;
import org.agrona.collections.LongArrayList;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.BlockPackedWriter;
import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.index.sai.disk.v1.LeafOrderMap;
import org.apache.cassandra.index.sai.disk.v2.V2PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v2.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v2.postings.PostingsWriter;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.utils.SharedIndexInput;
import org.apache.cassandra.io.tries.IncrementalDeepTrieWriterPageAware;
import org.apache.cassandra.io.tries.IncrementalTrieWriter;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.GrowableByteArrayDataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.packed.DirectWriter;

import static org.apache.cassandra.index.sai.disk.format.IndexComponent.KD_TREE_POSTING_LISTS;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.POSTING_LISTS;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.TERMS_DATA;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.TERMS_INDEX;
import static org.apache.cassandra.index.sai.disk.v1.NumericValuesWriter.BLOCK_SIZE;
import static org.apache.cassandra.index.sai.disk.v1.TrieTermsDictionaryReader.trieSerializer;
import static org.apache.cassandra.index.sai.disk.v2.blockindex.BytesUtil.fixedLength;

/**
 * Buffer 2 blocks at a time a determine if the block values are all the same
 * to continue writing a larger posting list, or the usual 1k block size.
 *
 * The trie stores the min block values and the payload is the min and max leaf ids of the term.
 * When a term has a large posting list the min and max leaf id's are different.
 */
public class BlockIndexWriter
{
    public static final int LEAF_SIZE = 3;
    // TODO: when the previous leaf min value is the same,
    //       write the leaf file pointer to the first occurence of the min value
    public static final Collection<IndexComponent> components = Lists.newArrayList(TERMS_DATA, TERMS_INDEX, POSTING_LISTS, KD_TREE_POSTING_LISTS);
    private final LongArrayList leafBytesFPs = new LongArrayList();
    private final LongArrayList realLeafBytesFPs = new LongArrayList();
    private final LongArrayList realLeafCompressedBytesFPs = new LongArrayList();
    private final IntArrayList realLeafCompressedLengths = new IntArrayList();

    private final IntArrayList realLeafBytesLengths = new IntArrayList();

    final List<BytesRef> blockMinValues = new ArrayList();
    final IndexOutput valuesOut, compressedValuesOut;
    final IndexOutputWriter indexOut;
    final IndexOutput leafPostingsOut, orderMapOut;

    final BitSet leafValuesSame = new BitSet(); // marked when all values in a leaf are the same

    final BytesRefBuilder termBuilder = new BytesRefBuilder();
    final BytesRefBuilder lastTermBuilder = new BytesRefBuilder();
    final boolean temporary;
    private final PostingsWriter postingsWriter;
    final IndexDescriptor indexDescriptor;
    final IndexContext indexContext;

    // leaf id to postings file pointer
    private final TreeMap<Integer,Long> leafToPostingsFP = new TreeMap();
    // leaf id to block order map file pointer
    private final TreeMap<Integer,Long> leafToOrderMapFP = new TreeMap();
    final RangeSet<Integer> multiBlockLeafRanges = TreeRangeSet.create();
    final BytesRefBuilder lastAddedTerm = new BytesRefBuilder();

    private BlockBuffer currentBuffer = new BlockBuffer(), previousBuffer = new BlockBuffer();
    private int termOrdinal = 0; // number of unique terms
    private int leaf;

    public static class BlockBuffer
    {
        final int[] lengths = new int[LEAF_SIZE];
        final int[] prefixes = new int[LEAF_SIZE];
        final long[] postings = new long[LEAF_SIZE];
        boolean allLeafValuesSame = true;
        int leaf = -1;
        int leafOrdinal = 0;

        BytesRef minValue;

        private final GrowableByteArrayDataOutput scratchOut = new GrowableByteArrayDataOutput(8 * 1024);
        private final GrowableByteArrayDataOutput prefixScratchOut = new GrowableByteArrayDataOutput(8 * 1024);
        private final GrowableByteArrayDataOutput lengthsScratchOut = new GrowableByteArrayDataOutput(8 * 1024);

        final RowIDLeafOrdinal[] rowIDLeafOrdinals = new RowIDLeafOrdinal[LEAF_SIZE];
        {
            for (int x = 0; x < rowIDLeafOrdinals.length; x++)
            {
                rowIDLeafOrdinals[x] = new RowIDLeafOrdinal();
            }
        }

        public boolean isEmpty()
        {
            return leafOrdinal == 0;
        }

        public void reset()
        {
            scratchOut.reset();
            prefixScratchOut.reset();
            lengthsScratchOut.reset();
            leafOrdinal = 0;
            leaf = -1;
            allLeafValuesSame = true;
            minValue = null;
            for (int x = 0; x < rowIDLeafOrdinals.length; x++)
            {
                rowIDLeafOrdinals[x].leafOrdinal = -1;
                rowIDLeafOrdinals[x].rowID = -1;
            }
        }
    }

    public BlockIndexWriter(IndexDescriptor indexDescriptor,
                            IndexContext indexContext,
                            boolean temporary) throws IOException
    {
        this.indexContext = indexContext;
        this.indexDescriptor = indexDescriptor;
        this.temporary = temporary;
        this.valuesOut = indexDescriptor.openPerIndexOutput(TERMS_DATA, indexContext, true, temporary);
        this.indexOut = indexDescriptor.openPerIndexOutput(TERMS_INDEX, indexContext, true, temporary);
        this.leafPostingsOut = indexDescriptor.openPerIndexOutput(POSTING_LISTS, indexContext, true, temporary);
        this.orderMapOut = indexDescriptor.openPerIndexOutput(IndexComponent.ORDER_MAP, indexContext, true, temporary);
        this.compressedValuesOut = indexDescriptor.openPerIndexOutput(IndexComponent.COMPRESSED_TERMS_DATA, indexContext, true, temporary);
        this.postingsWriter = new PostingsWriter(leafPostingsOut);
    }

    public long addAll(TermsIterator termsIterator) throws IOException
    {
        long numRows = 0;
        while (termsIterator.hasNext())
        {
            ByteComparable term = termsIterator.next();
            try (PostingList postings = termsIterator.postings())
            {
                while (true)
                {
                    final long rowID = postings.nextPosting();
                    if (rowID == PostingList.END_OF_STREAM)
                    {
                        break;
                    }
                    add(term, rowID);
                    numRows++;
                }
            }
        }
        return numRows;
    }

    public static class RowPoint
    {
        public long rowID;
        public long pointID;

        @Override
        public String toString()
        {
            return "RowPoint{" +
                   "rowID=" + rowID +
                   ", pointID=" + pointID +
                   '}';
        }
    }

    public interface RowPointIterator extends Closeable
    {
        public RowPoint next() throws IOException;

        public RowPoint current();
    }

    public static IndexContext createIndexContext(String columnName, String indexName, AbstractType<?> validator)
    {
        return new IndexContext("test_ks",
                                "test_cf",
                                UTF8Type.instance,
                                new ClusteringComparator(),
                                ColumnMetadata.regularColumn("sai", "internal", columnName, validator),
                                IndexMetadata.fromSchemaMetadata(indexName, IndexMetadata.Kind.CUSTOM, null),
                                IndexWriterConfig.emptyConfig(),
                                null);
    }

    // iterator row id order
    public RowPointIterator rowPointIterator() throws IOException
    {
        final V2PerIndexFiles perIndexFiles = new V2PerIndexFiles(indexDescriptor, indexContext, temporary);

        final DirectReaders.Reader orderMapReader = DirectReaders.getReaderForBitsPerValue((byte) DirectWriter.unsignedBitsRequired(LEAF_SIZE - 1));

        final IndexInput orderMapInput = perIndexFiles.openInput(IndexComponent.ORDER_MAP);

        final SeekingRandomAccessInput orderMapRandoInput = new SeekingRandomAccessInput(orderMapInput);

        final BlockIndexReader.BlockIndexReaderContext context = new BlockIndexReader.BlockIndexReaderContext();
        context.leafLevelPostingsInput = new SharedIndexInput(perIndexFiles.openInput(IndexComponent.POSTING_LISTS));

        final List<RowPointIterator> iterators = new ArrayList<>();

        // create ordered readers
        for (Map.Entry<Integer,Long> entry : this.leafToPostingsFP.entrySet())
        {
            final long postingsFP = entry.getValue();
            final PostingsReader postingsReader = new PostingsReader(context.leafLevelPostingsInput, postingsFP, QueryEventListener.PostingListEventListener.NO_OP);

            final RowPoint rowPoint = new RowPoint();

            // leaf with ordered map
            if (this.leafToOrderMapFP.containsKey(entry.getKey()))
            {
                final long orderMapFP = this.leafToOrderMapFP.get(entry.getKey());

                final long start = entry.getKey() * LEAF_SIZE;

                final RowPointIterator iterator = new RowPointIterator()
                {
                    int i = 0;

                    @Override
                    public RowPoint current()
                    {
                        return rowPoint;
                    }

                    @Override
                    public RowPoint next() throws IOException
                    {
                        final long rowid = postingsReader.nextPosting();
                        if (rowid == PostingList.END_OF_STREAM)
                        {
                            // TODO: current needs to return null
                            return null;
                        }
                        final int ordinal = (int)orderMapReader.get(orderMapRandoInput, orderMapFP, i);
                        rowPoint.pointID = start + ordinal;
                        rowPoint.rowID = rowid;
                        i++;
                        return rowPoint;
                    }

                    @Override
                    public void close() throws IOException
                    {
                        FileUtils.closeQuietly(postingsReader);
                    }
                };
                iterators.add(iterator);
            }
            else
            {
                // leaf with no ordered map so the postings row id order is the order
                final long start = entry.getKey() * LEAF_SIZE;

                final RowPointIterator iterator = new RowPointIterator()
                {
                    int i = 0;

                    @Override
                    public RowPoint current()
                    {
                        return rowPoint;
                    }

                    @Override
                    public RowPoint next() throws IOException
                    {
                        final long rowid = postingsReader.nextPosting();
                        if (rowid == PostingList.END_OF_STREAM)
                        {
                            // TODO: current needs to return null
                            return null;
                        }
                        rowPoint.pointID = start + i;
                        rowPoint.rowID = rowid;
                        i++;
                        return rowPoint;
                    }

                    @Override
                    public void close() throws IOException
                    {
                        FileUtils.closeQuietly(postingsReader);
                    }
                };
                iterators.add(iterator);
            }
        }
        return new MergeRowPoints(iterators, context, perIndexFiles);
    }

    public BlockIndexMeta finish() throws IOException
    {
        flushLastBuffers();

        final V2PerIndexFiles perIndexFiles = new V2PerIndexFiles(indexDescriptor, indexContext, temporary);

        final long valuesOutLastBytesFP = valuesOut.getFilePointer();

        // write the block min values index
        IncrementalTrieWriter termsIndexWriter = new IncrementalDeepTrieWriterPageAware<>(trieSerializer, indexOut.asSequentialWriter());
        int distinctCount = 0;
        int start = 0;
        int leafIdx = 0;

        // write distinct min block terms and the min and max leaf id's encoded as a long
        final BytesRefBuilder lastTerm = new BytesRefBuilder();
        for (leafIdx = 0; leafIdx < blockMinValues.size(); leafIdx++)
        {
            final BytesRef minValue = blockMinValues.get(leafIdx);
            if (leafIdx > 0)
            {
                final BytesRef prevMinValue = blockMinValues.get(leafIdx - 1);
                if (!minValue.equals(prevMinValue))
                {
                    final int startLeaf = start;
                    final int endLeaf = leafIdx - 1;

                    if (leafValuesSame.get(endLeaf))
                    {
                        if (startLeaf < endLeaf)
                        {
                            multiBlockLeafRanges.add(Range.closed(startLeaf, endLeaf));
                        }
                    }
                    else
                    {
                        if (startLeaf < endLeaf - 1)
                        {
                            multiBlockLeafRanges.add(Range.closed(startLeaf, endLeaf - 1));
                        }
                    }

                    long encodedLong = (((long)startLeaf) << 32) | (endLeaf & 0xffffffffL);
                    // TODO: when the start and end leaf's are the same encode a single int
                    termsIndexWriter.add(fixedLength(prevMinValue), new Long(encodedLong));
                    lastTerm.clear();
                    lastTerm.append(prevMinValue);
                    distinctCount = 0;
                    start = leafIdx;
                }
            }
            // TODO: assert that these results match the multi-block rangeset
            if (leafIdx == blockMinValues.size() - 1 && leafIdx > 0)
            {
                final int endLeaf = leafIdx;
                BytesRef prevMinValue = blockMinValues.get(leafIdx - 1);
                long encodedLong = (((long)start) << 32) | (endLeaf & 0xffffffffL);
                if (minValue.equals(prevMinValue))
                {
                    if (leafValuesSame.get(endLeaf))
                    {
                        if (start < endLeaf)
                        {
                            multiBlockLeafRanges.add(Range.closed(start, endLeaf));
                        }
                    }
                    else
                    {
                        if (start < endLeaf - 1)
                        {
                            multiBlockLeafRanges.add(Range.closed(start, endLeaf - 1));
                        }
                    }
                }

                if (!minValue.equals(lastTerm.get()))
                {
                    assert minValue.compareTo(lastTerm.get()) > 0;

                    //System.out.println("termsIndexWriter last add minValue=" + NumericUtils.sortableBytesToInt(minValue.bytes, 0));
                    termsIndexWriter.add(fixedLength(minValue), new Long(encodedLong));
                    lastTerm.clear();
                    lastTerm.append(minValue);
                }
            }
            distinctCount++;
        }

        assert leafBytesFPs.size() == blockMinValues.size()
        : "leafFilePointers.size=" + leafBytesFPs.size() + " blockMinValues.size=" + blockMinValues.size();

        final int numLeaves = leafBytesFPs.size();

        final long leafFilePointersFP = valuesOut.getFilePointer();

        for (int x = 0; x < realLeafBytesFPs.size(); x++)
        {
            valuesOut.writeVLong(realLeafBytesFPs.get(x));
        }

        final TreeMap<Integer,Integer> nodeIDToLeafOrdinal = new TreeMap();

        rotateToTree(1, 0, leafBytesFPs.size() - 1, nodeIDToLeafOrdinal);

        final TreeMap<Integer, Long> nodeIDToLeafPointer = new TreeMap<>();

        long[] leafBlockFPs = leafBytesFPs.toLongArray();

        assert numLeaves == leafBlockFPs.length;

        // this is wacky lucene code that rearranges the leaf file pointers
        if (numLeaves > 1)
        {
            int levelCount = 2;
            while (true)
            {
                if (numLeaves >= levelCount && numLeaves <= 2 * levelCount)
                {
                    int lastLevel = 2 * (numLeaves - levelCount);
                    assert lastLevel >= 0;
                    if (lastLevel != 0)
                    {
                        // Last level is partially filled, so we must rotate the leaf FPs to match.  We do this here, after loading
                        // at read-time, so that we can still delta code them on disk at write:
                        long[] newLeafBlockFPs = new long[numLeaves];
                        System.arraycopy(leafBlockFPs, lastLevel, newLeafBlockFPs, 0, leafBlockFPs.length - lastLevel);
                        System.arraycopy(leafBlockFPs, 0, newLeafBlockFPs, leafBlockFPs.length - lastLevel, lastLevel);
                        leafBlockFPs = newLeafBlockFPs;
                    }
                    break;
                }

                levelCount *= 2;
            }
        }

        // traverse to gather into nodeIDToLeafPointer
        recursePackIndex(leafBlockFPs,
                         0l,
                         1,
                         true,
                         nodeIDToLeafPointer);

        // TODO: the "leafPointer" is actually the leaf id because
        //       the binary tree code requires unique values
        //       due to the same block min values having the same file pointer
        //       the actual leaf file pointer can't be used here
        final TreeMap<Long, Integer> leafPointerToNodeID = new TreeMap<>();
        for (Map.Entry<Integer,Long> entry : nodeIDToLeafPointer.entrySet())
        {
            leafPointerToNodeID.put(entry.getValue(), entry.getKey());
        }

        final TreeMap<Integer,Integer> leafToNodeID = new TreeMap<>();

        int ordinal = 0;
        for (Map.Entry<Long, Integer> entry : leafPointerToNodeID.entrySet())
        {
            nodeIDToLeafOrdinal.put(entry.getValue(), ordinal);
            leafToNodeID.put(ordinal, entry.getValue());
            ordinal++;
        }

        final IntLongHashMap nodeIDPostingsFP = new IntLongHashMap();

        final long nodeIDToLeafOrdinalFP = valuesOut.getFilePointer();
        valuesOut.writeVInt(nodeIDToLeafOrdinal.size());

        for (Map.Entry<Integer, Integer> entry : nodeIDToLeafOrdinal.entrySet())
        {
            valuesOut.writeVInt(entry.getKey());
            valuesOut.writeVInt(entry.getValue());

            int nodeID = entry.getKey();
            int leafOrdinal = entry.getValue();

            // nodeID >= numLeaves is a leaf node
            if (nodeID >= numLeaves)
            {
                final Long postingsFP = leafToPostingsFP.get(leafOrdinal);

                // postingsFP may be null with same value multi-block postings
                if (postingsFP != null)
                {
                    nodeIDPostingsFP.put(nodeID, postingsFP);
                }
            }
        }

        final long leafIDPostingsFP_FP = leafPostingsOut.getFilePointer();
        this.leafPostingsOut.writeVInt(leafToPostingsFP.size());
        for (Map.Entry<Integer,Long> entry : leafToPostingsFP.entrySet())
        {
            this.leafPostingsOut.writeVInt(entry.getKey());
            this.leafPostingsOut.writeVLong(entry.getValue());
        }

        final long orderMapFP = this.orderMapOut.getFilePointer();
        orderMapOut.writeVInt(this.leafToOrderMapFP.size());
        for (Map.Entry<Integer, Long> entry : this.leafToOrderMapFP.entrySet())
        {
            orderMapOut.writeVInt(entry.getKey());
            orderMapOut.writeVLong(entry.getValue());
        }

        final long indexFP = termsIndexWriter.complete();

        // write multiBlockLeafRanges
        final long multiBlockLeafRangesFP = this.leafPostingsOut.getFilePointer();
        IntRangeSetSerializer.serialize(multiBlockLeafRanges, this.leafPostingsOut);

        // write leafValuesSame
        final long leafValuesSameFP;
        final long leafValuesSamePostingsFP;
        if (this.leafValuesSame.cardinality() > 0)
        {
            leafValuesSameFP = leafPostingsOut.getFilePointer();
            leafValuesSamePostingsFP = BitSetSerializer.serialize(this.leafValuesSame, this.leafPostingsOut);
        }
        else
        {
            leafValuesSameFP = -1;
            leafValuesSamePostingsFP = -1;
        }
        final long nodeIDPostingsFP_FP = leafPostingsOut.getFilePointer();

        this.leafPostingsOut.writeVInt(nodeIDPostingsFP.size());
        for (IntLongCursor cursor : nodeIDPostingsFP)
        {
            this.leafPostingsOut.writeVInt(cursor.key);
            this.leafPostingsOut.writeVLong(cursor.value);
        }

        // close leaf postings now because MultiLevelPostingsWriter reads leaf postings
        this.leafPostingsOut.close();

        final SharedIndexInput leafPostingsInput = new SharedIndexInput(this.indexDescriptor.openPerIndexInput(POSTING_LISTS, indexContext, temporary));

        final MultiLevelPostingsWriter multiLevelPostingsWriter = new MultiLevelPostingsWriter(leafPostingsInput,
                                                                                               IndexWriterConfig.defaultConfig("indexName"),
                                                                                               nodeIDPostingsFP,
                                                                                               leafBytesFPs.size(),
                                                                                               nodeIDToLeafOrdinal,
                                                                                               this.multiBlockLeafRanges,
                                                                                               leafToNodeID);

        final IndexOutput bigPostingsOut = this.indexDescriptor.openPerIndexOutput(IndexComponent.KD_TREE_POSTING_LISTS, indexContext, true, temporary);

        final TreeMultimap<Integer, Long> nodeIDToMultilevelPostingsFP = multiLevelPostingsWriter.finish(bigPostingsOut);

        final long nodeIDToMultilevelPostingsFP_FP = bigPostingsOut.getFilePointer();
        bigPostingsOut.writeVInt(nodeIDToMultilevelPostingsFP.size());
        for (Map.Entry<Integer,Long> entry : nodeIDToMultilevelPostingsFP.entries())
        {
            bigPostingsOut.writeVInt(entry.getKey());
            bigPostingsOut.writeZLong(entry.getValue());
        }

        leafPostingsInput.close();
        bigPostingsOut.close();
        termsIndexWriter.close();
        indexOut.close();
        orderMapOut.close();
        valuesOut.close();

        // get samples from bytes for the zstd dictionary
        long totalDict = 110 * 1024;

        byte[] zstdDictionary = new byte[(int)totalDict];

        long totalSamples = totalDict * 100;
        long numSampleBlocks = totalSamples / 1024;
        long sampleChunkSize = valuesOutLastBytesFP / numSampleBlocks;

        List<byte[]> bytesList = new ArrayList<>();

        // gather 1kb chunks as samples
        try (IndexInput bytesInput = indexDescriptor.openPerIndexInput(TERMS_DATA, indexContext, temporary))
        {
            for (int x = 0; x < numSampleBlocks; x++)
            {
                bytesInput.seek(x * sampleChunkSize);
                // if the file is less then the sample size of 1024, use the file size
                byte[] samplesBytes = new byte[Math.min(1024, (int)bytesInput.length())];
                bytesInput.readBytes(samplesBytes, 0, samplesBytes.length);
                bytesList.add(samplesBytes);
            }
        }

        final int zstdDictionaryLen = (int) Zstd.trainFromBuffer(bytesList.toArray(new byte[0][]), zstdDictionary);
        final long zstdDictionaryFP;

        System.out.println("ZSTD dictionary zstdDictionaryLen=" + zstdDictionaryLen + " zstdSamplesArray.len=" + bytesList.size());

        if (zstdDictionaryLen == -1)
        {
            zstdDictionaryFP = -1;
        }
        else
        {
            zstdDictionaryFP = this.compressedValuesOut.getFilePointer();
            this.compressedValuesOut.writeVInt(zstdDictionaryLen);
            this.compressedValuesOut.copyBytes(new ByteArrayDataInput(zstdDictionary, 0, zstdDictionaryLen), zstdDictionaryLen);
        }

        assert realLeafBytesFPs.size() == realLeafBytesLengths.size();

        final long compressedLeafFPs_FP;

        if (zstdDictionaryFP != -1)
        {
            try (ZstdDictCompress dictCompress = new ZstdDictCompress(zstdDictionary, 0, zstdDictionaryLen, 1);
                 IndexInput bytesInput = perIndexFiles.openInput(TERMS_DATA))//openPerIndexInput(TERMS_DATA, indexName))
            {
                byte[] rawBytes = new byte[10];
                byte[] compressedBytes = new byte[10];

                for (int x = 0; x < realLeafBytesFPs.size(); x++)
                {
                    long bytesFP = realLeafBytesFPs.get(x);

                    // don't compress > 1
                    if (x > 0 && realLeafBytesFPs.get(x - 1) == bytesFP)
                    {
                        realLeafCompressedBytesFPs.add(realLeafCompressedBytesFPs.get(x - 1));
                        realLeafCompressedLengths.add(realLeafCompressedLengths.get(x - 1));
                    }
                    else
                    {
                        int bytesLen = realLeafBytesLengths.get(x);

                        rawBytes = ArrayUtil.grow(rawBytes, bytesLen);
                        bytesInput.seek(bytesFP);
                        bytesInput.readBytes(rawBytes, 0, bytesLen);

                        long maxCompressedSize = Zstd.compressBound(bytesLen);
                        compressedBytes = ArrayUtil.grow(compressedBytes, (int) maxCompressedSize);

                        Zstd.compressFastDict(compressedBytes, 0, rawBytes, 0, bytesLen, dictCompress);
                        long compressedFP = this.compressedValuesOut.getFilePointer();
                        this.compressedValuesOut.writeBytes(compressedBytes, 0, compressedBytes.length);
                        realLeafCompressedBytesFPs.add(compressedFP);
                        realLeafCompressedLengths.add(compressedBytes.length);
                    }
                }
            }

            compressedLeafFPs_FP = compressedValuesOut.getFilePointer();
            compressedValuesOut.writeVInt(realLeafCompressedBytesFPs.size());
            for (int x = 0; x < realLeafCompressedBytesFPs.size(); x++)
            {
                compressedValuesOut.writeVLong(realLeafCompressedBytesFPs.get(x));
                compressedValuesOut.writeVInt(realLeafCompressedLengths.get(x));
                compressedValuesOut.writeVInt(realLeafBytesLengths.get(x));
            }

            assert realLeafBytesFPs.size() == realLeafCompressedBytesFPs.size();

            this.compressedValuesOut.close();
        }
        else
        {
            compressedLeafFPs_FP = -1;
        }

        // TODO: when there are duplicate row ids it means
        //       this isn't a single value per row index and so cannot have a row id -> point id map
        final IndexOutput rowPointOut = indexDescriptor.openPerIndexOutput(IndexComponent.ROW_ID_POINT_ID_MAP, indexContext);
        final BlockPackedWriter rowPointWriter = new BlockPackedWriter(rowPointOut, BLOCK_SIZE);
        // write the row id -> point id map
        final RowPointIterator rowPointIterator = this.rowPointIterator();
        long i = 0;
        long lastRowID = -1;
        while (true)
        {
            final RowPoint rowPoint = rowPointIterator.next();
            if (rowPoint == null)
            {
                break;
            }
            // fill in the gaps of the row ids
            if (rowPoint.rowID - lastRowID > 1)
            {
                for (int x = 0; x < rowPoint.rowID - lastRowID - 1; x++)
                {
                    rowPointWriter.add(-1);
                    i++;
                }
            }

            // assert there are no gaps

            // TODO: fix
            // assert rowPoint.rowID == i : "rowPoint.rowID="+rowPoint.rowID+" i="+i+" lastRowID="+lastRowID;

            rowPointWriter.add(rowPoint.pointID);
            lastRowID = rowPoint.rowID;
            i++;
        }
        final long rowPointMap_FP = rowPointWriter.finish();
        rowPointIterator.close();
        rowPointOut.close();

        // all files are written, create the CRC check
        final HashMap<IndexComponent, FileValidator.FileInfo> fileInfoMap = FileValidator.generate(Lists.newArrayList(components),
                                                                                                   perIndexFiles);

        final byte[] fileInfoMapBytes = SerializationUtils.serialize(fileInfoMap);
        perIndexFiles.close();

        System.out.println("minTerm="+Arrays.toString(minTerm.bytes));
        System.out.println("maxTerm="+Arrays.toString(realLastTerm.toBytesRef().bytes));

        return new BlockIndexMeta(orderMapFP,
                                  indexFP,
                                  leafFilePointersFP,
                                  leafBytesFPs.size(),
                                  nodeIDToLeafOrdinalFP,
                                  multiBlockLeafRangesFP,
                                  nodeIDToMultilevelPostingsFP_FP,
                                  zstdDictionaryFP,
                                  leafValuesSameFP,
                                  leafValuesSamePostingsFP,
                                  nodeIDPostingsFP_FP,
                                  compressedLeafFPs_FP,
                                  numRows,
                                  minRowID,
                                  maxRowID,
                                  minTerm,
                                  BytesRef.deepCopyOf(realLastTerm.toBytesRef()), // last term
                                  leafIDPostingsFP_FP,
                                  new BytesRef(fileInfoMapBytes),
                                  rowPointMap_FP);
    }

    private long minRowID = Long.MAX_VALUE;
    private long maxRowID = -1;
    private long numRows = 0;
    private BytesRef minTerm = null;

    final BytesRefBuilder realLastTerm = new BytesRefBuilder();

    public void add(ByteComparable term, long rowID) throws IOException
    {
        minRowID = Math.min(minRowID, rowID);
        maxRowID = Math.max(maxRowID, rowID);

        termBuilder.clear();
        realLastTerm.clear();

        int length = BytesUtil.gatherBytes(term, termBuilder);

        realLastTerm.append(termBuilder.get());

        if (minTerm == null)
        {
            minTerm = BytesRef.deepCopyOf(termBuilder.toBytesRef());
        }

        if (currentBuffer.leafOrdinal > 0 && !termBuilder.get().equals(lastAddedTerm.get()))
        {
            currentBuffer.allLeafValuesSame = false;
        }

        boolean newTerm = false;

        if (lastAddedTerm.length() > 0 && !termBuilder.get().equals(lastAddedTerm.get()))
        {
            newTerm = true;
            termOrdinal++;
        }

        lastAddedTerm.clear();
        lastAddedTerm.append(termBuilder.get());

        if (lastTermBuilder.length() == 0) // new block
        {
            assert currentBuffer.leafOrdinal == 0;

            assert currentBuffer.isEmpty();

            lastTermBuilder.append(termBuilder);
            currentBuffer.prefixes[currentBuffer.leafOrdinal] = 0;
            currentBuffer.lengths[currentBuffer.leafOrdinal] = termBuilder.get().length;
            final BytesRef minValue = BytesRef.deepCopyOf(termBuilder.get());
            blockMinValues.add(minValue);

            currentBuffer.leaf = leaf;
            currentBuffer.minValue = minValue;
        }
        else
        {
            //System.out.println("prefix=" + lastTermBuilder.get().utf8ToString() + " term=" + termBuilder.get().utf8ToString());
            int prefix = BytesUtil.bytesDifference(lastTermBuilder.get(), termBuilder.get());
            if (prefix == -1) prefix = length;
            currentBuffer.prefixes[currentBuffer.leafOrdinal] = prefix;
            currentBuffer.lengths[currentBuffer.leafOrdinal] = termBuilder.get().length;
        }
        // System.out.println("term=" + termBuilder.get().utf8ToString() + " prefix=" + currentBuffer.prefixes[currentBuffer.leafOrdinal] + " length=" + currentBuffer.lengths[currentBuffer.leafOrdinal]);

        int prefix = currentBuffer.prefixes[currentBuffer.leafOrdinal];
        int len = termBuilder.get().length - currentBuffer.prefixes[currentBuffer.leafOrdinal];

        if (currentBuffer.leafOrdinal == 0)
        {
            currentBuffer.prefixes[currentBuffer.leafOrdinal] = termBuilder.get().length;
        }

        //System.out.println("write leafIndex=" + leafOrdinal + " prefix=" + prefix + " len=" + len);
        currentBuffer.scratchOut.writeBytes(termBuilder.get().bytes, prefix, len);
        currentBuffer.postings[currentBuffer.leafOrdinal] = rowID;
        currentBuffer.leafOrdinal++;

        if (currentBuffer.leafOrdinal == LEAF_SIZE)
        {
            termBuilder.clear();
            lastTermBuilder.clear();
            flushPreviousBufferAndSwap();
        }
        numRows++;
    }

    private void flushLastBuffers() throws IOException
    {
        if (!previousBuffer.isEmpty())
        {
            writeLeaf(previousBuffer);
            writePostingsAndOrderMap(previousBuffer);

            // if the previous buffer has the all the same value as the current buffer
            // then the postings writer is kept open
            if (!currentBuffer.isEmpty()
                && currentBuffer.allLeafValuesSame
                && previousBuffer.allLeafValuesSame
                && previousBuffer.minValue.equals(currentBuffer.minValue))
            {
            }
            else
            {
                final long postingsFP = postingsWriter.completePostings();
                this.leafToPostingsFP.put(previousBuffer.leaf, postingsFP);
            }
        }

        if (!currentBuffer.isEmpty())
        {
            writeLeaf(currentBuffer);
            writePostingsAndOrderMap(currentBuffer);

            final long postingsFP = postingsWriter.completePostings();
            this.leafToPostingsFP.put(currentBuffer.leaf, postingsFP);
        }
    }

    private void flushPreviousBufferAndSwap() throws IOException
    {
        if (!previousBuffer.isEmpty())
        {
            writeLeaf(previousBuffer);
            writePostingsAndOrderMap(previousBuffer);

            // if the previous buffer has the all the same value as the current buffer
            // then the postings writer is kept open
            if (!currentBuffer.isEmpty()
                && currentBuffer.allLeafValuesSame
                && previousBuffer.allLeafValuesSame
                && previousBuffer.minValue.equals(currentBuffer.minValue))
            {
            }
            else
            {
                final long postingsFP = postingsWriter.completePostings();
                this.leafToPostingsFP.put(previousBuffer.leaf, postingsFP);
            }
            previousBuffer.reset();
        }

        // swap buffer pointers
        BlockBuffer next = previousBuffer;
        previousBuffer = currentBuffer;
        currentBuffer = next;
        leaf++;
    }

    protected void writeLeaf(BlockBuffer buffer) throws IOException
    {
        final BytesRef minValue = blockMinValues.get(buffer.leaf);

        assert minValue.equals(buffer.minValue);

        // System.out.println("   writeLeaf leaf=" + buffer.leaf + " minValue=" + NumericUtils.sortableBytesToInt(buffer.minValue.bytes, 0));

        this.leafBytesFPs.add((long) buffer.leaf);

        if (buffer.allLeafValuesSame)
        {
            leafValuesSame.set(buffer.leaf);
        }

        if (buffer.leaf > 0)
        {
            // previous min block value is the same so point to that one and don't write anything
            final BytesRef prevMinValue = blockMinValues.get(buffer.leaf - 1);
            if (minValue.equals(prevMinValue) && buffer.allLeafValuesSame)
            {
                long previousRealFP = this.realLeafBytesFPs.get(this.realLeafBytesFPs.size() - 1);
                this.realLeafBytesFPs.add(previousRealFP);

                int previousRealLen = this.realLeafBytesLengths.get(this.realLeafBytesLengths.size() - 1);
                this.realLeafBytesLengths.add(previousRealLen);
                return;
            }
        }

        long filePointer = valuesOut.getFilePointer();
        final int maxLength = Arrays.stream(buffer.lengths).max().getAsInt();
        LeafOrderMap.write(buffer.lengths, buffer.leafOrdinal, maxLength, buffer.lengthsScratchOut);
        final int maxPrefix = Arrays.stream(buffer.prefixes).max().getAsInt();
        LeafOrderMap.write(buffer.prefixes, buffer.leafOrdinal, maxPrefix, buffer.prefixScratchOut);

        valuesOut.writeInt(buffer.leafOrdinal); // value count
        valuesOut.writeInt(buffer.lengthsScratchOut.getPosition());
        valuesOut.writeInt(buffer.prefixScratchOut.getPosition());
        valuesOut.writeByte((byte) DirectWriter.unsignedBitsRequired(maxLength));
        valuesOut.writeByte((byte) DirectWriter.unsignedBitsRequired(maxPrefix));
        valuesOut.writeBytes(buffer.lengthsScratchOut.getBytes(), 0, buffer.lengthsScratchOut.getPosition());
        valuesOut.writeBytes(buffer.prefixScratchOut.getBytes(), 0, buffer.prefixScratchOut.getPosition());
        valuesOut.writeBytes(buffer.scratchOut.getBytes(), 0, buffer.scratchOut.getPosition());

        long bytesLength = valuesOut.getFilePointer() - filePointer;
        this.realLeafBytesLengths.add((int)bytesLength);
        this.realLeafBytesFPs.add(filePointer);
    }

    // writes postings and the order map only if the row ids are not in ascending order
    protected void writePostingsAndOrderMap(BlockBuffer buffer) throws IOException
    {
        assert buffer.leafOrdinal > 0;

        for (int x = 0; x < buffer.leafOrdinal; x++)
        {
            buffer.rowIDLeafOrdinals[x].rowID = buffer.postings[x];
            buffer.rowIDLeafOrdinals[x].leafOrdinal = x;
        }

        // sort by row id
        Arrays.sort(buffer.rowIDLeafOrdinals, 0, buffer.leafOrdinal, (obj1, obj2) -> Long.compare(obj1.rowID, obj2.rowID));

        // write sorted by row id postings to the postings writer
        boolean inRowIDOrder = true;
        for (int x = 0; x < buffer.leafOrdinal; x++)
        {
            long rowID = buffer.rowIDLeafOrdinals[x].rowID;
            if (buffer.rowIDLeafOrdinals[x].leafOrdinal != x)
            {
                inRowIDOrder = false;
            }
            postingsWriter.add(rowID);
        }

        // write an order map if the row ids are not in order
        if (!inRowIDOrder)
        {
            final long orderMapFP = orderMapOut.getFilePointer();
            final int bits = DirectWriter.unsignedBitsRequired(LEAF_SIZE - 1);
            final DirectWriter orderMapWriter = DirectWriter.getInstance(orderMapOut, buffer.leafOrdinal, bits);
            for (int i = 0; i < buffer.leafOrdinal; i++)
            {
                orderMapWriter.add(buffer.rowIDLeafOrdinals[i].leafOrdinal);
            }
            orderMapWriter.finish();
            leafToOrderMapFP.put(buffer.leaf, orderMapFP);
        }
    }

    private long getLeftMostLeafBlockFP(long[] leafBlockFPs, int nodeID)
    {
        // lucene comment...
        //
        // TODO: can we do this cheaper, e.g. a closed form solution instead of while loop?  Or
        // change the recursion while packing the index to return this left-most leaf block FP
        // from each recursion instead?
        //
        // Still, the overall cost here is minor: this method's cost is O(log(N)), and while writing
        // we call it O(N) times (N = number of leaf blocks)
        while (nodeID < leafBlockFPs.length)
        {
            nodeID *= 2;
        }
        int leafID = nodeID - leafBlockFPs.length;
        long result = leafBlockFPs[leafID];
        if (result < 0)
        {
            throw new AssertionError(result + " for leaf " + leafID);
        }
        return result;
    }

    private void rotateToTree(int nodeID, int offset, int count, Map<Integer,Integer> nodeIDToLeafOrdinal)
    {
        if (count == 1)
        {
            nodeIDToLeafOrdinal.put(nodeID, offset + 1);
        }
        else if (count > 1)
        {
            // Internal index node: binary partition of count
            int countAtLevel = 1;
            int totalCount = 0;
            while (true)
            {
                int countLeft = count - totalCount;
                if (countLeft <= countAtLevel)
                {
                    // This is the last level, possibly partially filled:
                    int lastLeftCount = Math.min(countAtLevel / 2, countLeft);
                    assert lastLeftCount >= 0;
                    int leftHalf = (totalCount - 1) / 2 + lastLeftCount;

                    int rootOffset = offset + leftHalf;

                    nodeIDToLeafOrdinal.put(nodeID, rootOffset + 1);

                    // Recurse left
                    rotateToTree(2 * nodeID, offset, leftHalf, nodeIDToLeafOrdinal);

                    // Recurse right
                    rotateToTree(2 * nodeID + 1, rootOffset + 1, count - leftHalf - 1, nodeIDToLeafOrdinal);
                    return;
                }
                totalCount += countAtLevel;
                countAtLevel *= 2;
            }
        }
        else
        {
            assert count == 0;
        }
    }

    private void recursePackIndex(long[] leafBlockFPs,
                                  long minBlockFP,
                                  int nodeID,
                                  boolean isLeft,
                                  TreeMap<Integer,Long> nodeIDToLeafPointer) throws IOException
    {
        if (nodeID >= leafBlockFPs.length)
        {
            int leafID = nodeID - leafBlockFPs.length;
            long fakeFP = leafBlockFPs[leafID];

            // In the unbalanced case it's possible the left most node only has one child:
            if (leafID < leafBlockFPs.length)
            {
                nodeIDToLeafPointer.put(nodeID, fakeFP);
                return;
            }
            return;
        }
        else
        {
            long leftBlockFP;
            if (isLeft == false)
            {
                leftBlockFP = getLeftMostLeafBlockFP(leafBlockFPs, nodeID);
            }
            else
            {
                // The left tree's left most leaf block FP is always the minimal FP:
                leftBlockFP = minBlockFP;
            }

            recursePackIndex(leafBlockFPs, leftBlockFP, 2 * nodeID,true, nodeIDToLeafPointer);
            recursePackIndex(leafBlockFPs, leftBlockFP, 2 * nodeID + 1, false, nodeIDToLeafPointer);

            return;
        }
    }

    public static class RowIDLeafOrdinal
    {
        public int leafOrdinal;
        public long rowID;

        @Override
        public String toString()
        {
            return "{" +
                   "leafOrdinal=" + leafOrdinal +
                   ", rowID=" + rowID +
                   '}';
        }
    }
}
