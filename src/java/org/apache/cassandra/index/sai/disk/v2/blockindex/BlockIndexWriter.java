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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeMultimap;
import com.google.common.collect.TreeRangeSet;

import org.apache.commons.lang3.SerializationUtils;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.cursors.IntLongCursor;
import org.agrona.collections.LongArrayList;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.BlockPackedWriter;
import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.index.sai.disk.v1.LeafOrderMap;
import org.apache.cassandra.index.sai.disk.v2.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v2.postings.PostingsWriter;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.utils.SharedIndexInput;
import org.apache.cassandra.io.tries.IncrementalDeepTrieWriterPageAware;
import org.apache.cassandra.io.tries.IncrementalTrieWriter;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.store.GrowableByteArrayDataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.packed.DirectWriter;

import static org.apache.cassandra.index.sai.disk.v1.TrieTermsDictionaryReader.trieSerializer;
import static org.apache.cassandra.index.sai.disk.v2.blockindex.BytesUtil.fixedLength;

/**
 * Buffer 2 blocks at a time a determine if the block values are all the same
 * to continue writing a larger posting list, or the usual 1k block size.
 *
 * The trie stores the min block values and the payload is the min and max leaf ids of the term.
 * When a term has a large posting list the min and max leaf id's are different.
 */
public class BlockIndexWriter implements Closeable
{
    public static final int LEAF_SIZE = Integer.getInteger("cassandra.sai.block.index.leaf.size", 1024);
    public static final int BLOCK_SIZE = Integer.getInteger("cassandra.sai.block.index.block.size", 128);

    // TODO: when the previous leaf min value is the same,
    //       write the leaf file pointer to the first occurence of the min value
    private final LongArrayList leafBytesFPs = new LongArrayList();
    private final LongArrayList realLeafBytesFPs = new LongArrayList();
    private final IntArrayList realLeafBytesLengths = new IntArrayList();
    private final BlockIndexFileProvider fileProvider;
    private final boolean temporary;

    final List<BytesRef> blockMinValues = new ArrayList();
    final IndexOutput valuesOut;
    final IndexOutputWriter indexOut;
    final IndexOutput leafPostingsOut, orderMapOut;

    final BitSet leafValuesSame = new BitSet(); // marked when all values in a leaf are the same

    final BytesRefBuilder termBuilder = new BytesRefBuilder();
    final BytesRefBuilder lastTermBuilder = new BytesRefBuilder();
    private final PostingsWriter postingsWriter;

    // leaf id to postings file pointer
    private final TreeMap<Integer,Long> leafToPostingsFP = new TreeMap();
    // leaf id to block order map file pointer
    private final TreeMap<Integer,Long> leafToOrderMapFP = new TreeMap();
    final RangeSet<Integer> multiBlockLeafRanges = TreeRangeSet.create();
    final BytesRefBuilder lastAddedTerm = new BytesRefBuilder();

    private long minRowID = Long.MAX_VALUE;
    private long maxRowID = -1;
    private long numRows = 0;
    private BytesRef minTerm = null;

    final BytesRefBuilder realLastTerm = new BytesRefBuilder();

    private BlockBuffer currentBuffer = new BlockBuffer(), previousBuffer = new BlockBuffer();
    private int leaf;

    public BlockIndexWriter(BlockIndexFileProvider fileProvider, boolean temporary) throws IOException
    {
        this.fileProvider = fileProvider;
        this.temporary = temporary;
        this.valuesOut = fileProvider.openValuesOutput(temporary);
        this.indexOut = fileProvider.openIndexOutput(temporary);
        this.leafPostingsOut = fileProvider.openLeafPostingsOutput(temporary);
        this.orderMapOut = fileProvider.openOrderMapOutput(temporary);
        this.postingsWriter = new PostingsWriter(leafPostingsOut);
    }

    public long addAll(TermsIterator termsIterator, long segmentRowIdOffset) throws IOException
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
                    add(term, rowID + segmentRowIdOffset);
                    numRows++;
                }
            }
        }
        return numRows;
    }

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
            int prefix = BytesUtil.bytesDifference(lastTermBuilder.get(), termBuilder.get());
            if (prefix == -1) prefix = length;
            currentBuffer.prefixes[currentBuffer.leafOrdinal] = prefix;
            currentBuffer.lengths[currentBuffer.leafOrdinal] = termBuilder.get().length;
        }

        int prefix = currentBuffer.prefixes[currentBuffer.leafOrdinal];
        int len = termBuilder.get().length - currentBuffer.prefixes[currentBuffer.leafOrdinal];

        if (currentBuffer.leafOrdinal == 0)
        {
            currentBuffer.prefixes[currentBuffer.leafOrdinal] = termBuilder.get().length;
        }

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

    public BlockIndexMeta finish() throws IOException
    {
        flushLastBuffers();

        // If nothing has been written then return early with an empty BlockIndexMeta
        if (numRows == 0)
            return new BlockIndexMeta();

        BlockIndexWriterContext context = new BlockIndexWriterContext();

        writeBlockMinValuesIndex(context);

        // write multiBlockLeafRanges
        context.multiBlockLeafRangesFP = this.leafPostingsOut.getFilePointer();
        IntRangeSetSerializer.serialize(multiBlockLeafRanges, this.leafPostingsOut);

        // write leafValuesSame
        if (this.leafValuesSame.cardinality() > 0)
        {
            context.leafValuesSameFP = leafPostingsOut.getFilePointer();
            context.leafValuesSamePostingsFP = BitSetSerializer.serialize(this.leafValuesSame, this.leafPostingsOut);
        }
        else
        {
            context.leafValuesSameFP = -1;
            context.leafValuesSamePostingsFP = -1;
        }
        final long nodeIDPostingsFP_FP = leafPostingsOut.getFilePointer();

        this.leafPostingsOut.writeVInt(context.nodeIDPostingsFP.size());
        for (IntLongCursor cursor : context.nodeIDPostingsFP)
        {
            this.leafPostingsOut.writeVInt(cursor.key);
            this.leafPostingsOut.writeVLong(cursor.value);
        }

        // close leaf postings now because MultiLevelPostingsWriter reads leaf postings
        leafPostingsOut.close();
        orderMapOut.close();
        valuesOut.close();
        indexOut.close();

        writeBigPostings(context);

        assert realLeafBytesFPs.size() == realLeafBytesLengths.size();

        writePointIdMap(context);

        // all files are written, create the CRC check
        byte[] fileInfoMapBytes;
        if (temporary)
            fileInfoMapBytes = new byte[]{};
        else
            fileInfoMapBytes = SerializationUtils.serialize(this.fileProvider.fileInfoMap());

        return new BlockIndexMeta(context.orderMapFP,
                                  context.indexFP,
                                  context.leafFilePointersFP,
                                  leafBytesFPs.size(),
                                  context.nodeIDToLeafOrdinalFP,
                                  context.multiBlockLeafRangesFP,
                                  context.nodeIDToMultilevelPostingsFP_FP,
                                  context.leafValuesSameFP,
                                  context.leafValuesSamePostingsFP,
                                  nodeIDPostingsFP_FP,
                                  numRows,
                                  minRowID,
                                  maxRowID,
                                  minTerm,
                                  realLastTerm.toBytesRef(), // last term
                                  context.leafIDPostingsFP_FP,
                                  new BytesRef(fileInfoMapBytes),
                                  context.rowPointMap_FP);
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(valuesOut, indexOut, leafPostingsOut, orderMapOut);
    }

    private void writePointIdMap(BlockIndexWriterContext context) throws IOException
    {
        // TODO: when there are duplicate row ids it means
        //       this isn't a single value per row index and so cannot have a row id -> point id map
        try (final IndexOutput rowPointOut = this.fileProvider.openPointIdMapOutput(false);
             SharedIndexInput leafPostingsInput = this.fileProvider.openLeafPostingsInput(temporary))
        {
            final BlockPackedWriter rowPointWriter = new BlockPackedWriter(rowPointOut, BLOCK_SIZE);
            // write the row id -> point id map
            final RowPointIterator rowPointIterator = this.rowPointIterator(leafPostingsInput);
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
                    }
                }

                // assert there are no gaps

                // TODO: fix
                // assert rowPoint.rowID == i : "rowPoint.rowID="+rowPoint.rowID+" i="+i+" lastRowID="+lastRowID;

                rowPointWriter.add(rowPoint.pointID);
                lastRowID = rowPoint.rowID;
            }
            context.rowPointMap_FP = rowPointWriter.finish();
            rowPointIterator.close();
        }
    }

    private void writeBigPostings(BlockIndexWriterContext context) throws IOException
    {
        try (final SharedIndexInput leafPostingsInput = this.fileProvider.openLeafPostingsInput(temporary);
             final IndexOutput bigPostingsOut = this.fileProvider.openMultiPostingsOutput(temporary))
        {

            final MultiLevelPostingsWriter multiLevelPostingsWriter = new MultiLevelPostingsWriter(leafPostingsInput,
                                                                                                   IndexWriterConfig.defaultConfig("indexName"),
                                                                                                   context.nodeIDPostingsFP,
                                                                                                   leafBytesFPs.size(),
                                                                                                   context.nodeIDToLeafOrdinal,
                                                                                                   this.multiBlockLeafRanges,
                                                                                                   context.leafToNodeID);

            final TreeMultimap<Integer, Long> nodeIDToMultilevelPostingsFP = multiLevelPostingsWriter.finish(bigPostingsOut);

            context.nodeIDToMultilevelPostingsFP_FP = bigPostingsOut.getFilePointer();
            bigPostingsOut.writeVInt(nodeIDToMultilevelPostingsFP.size());
            for (Map.Entry<Integer, Long> entry : nodeIDToMultilevelPostingsFP.entries())
            {
                bigPostingsOut.writeVInt(entry.getKey());
                bigPostingsOut.writeZLong(entry.getValue());
            }
        }
    }

    private void writeBlockMinValuesIndex(BlockIndexWriterContext context) throws IOException
    {
        // write the block min values index
        try (IncrementalTrieWriter termsIndexWriter = new IncrementalDeepTrieWriterPageAware<>(trieSerializer, indexOut.asSequentialWriter()))
        {
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

                        long encodedLong = (((long) startLeaf) << 32) | (endLeaf & 0xffffffffL);
                        // TODO: when the start and end leaf's are the same encode a single int
                        termsIndexWriter.add(fixedLength(prevMinValue), new Long(encodedLong));
                        lastTerm.clear();
                        lastTerm.append(prevMinValue);
                        start = leafIdx;
                    }
                }
                // TODO: assert that these results match the multi-block rangeset
                if (leafIdx == blockMinValues.size() - 1 && leafIdx > 0)
                {
                    final int endLeaf = leafIdx;
                    BytesRef prevMinValue = blockMinValues.get(leafIdx - 1);
                    long encodedLong = (((long) start) << 32) | (endLeaf & 0xffffffffL);
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
            }

            assert leafBytesFPs.size() == blockMinValues.size()
            : "leafFilePointers.size=" + leafBytesFPs.size() + " blockMinValues.size=" + blockMinValues.size();

            final int numLeaves = leafBytesFPs.size();

            context.leafFilePointersFP = valuesOut.getFilePointer();

            for (int x = 0; x < realLeafBytesFPs.size(); x++)
            {
                valuesOut.writeVLong(realLeafBytesFPs.get(x));
            }

            rotateToTree(1, 0, leafBytesFPs.size() - 1, context.nodeIDToLeafOrdinal);

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
            recursePackIndex(leafBlockFPs, 0l, 1, true, nodeIDToLeafPointer);

            // TODO: the "leafPointer" is actually the leaf id because
            //       the binary tree code requires unique values
            //       due to the same block min values having the same file pointer
            //       the actual leaf file pointer can't be used here
            final TreeMap<Long, Integer> leafPointerToNodeID = new TreeMap<>();
            for (Map.Entry<Integer, Long> entry : nodeIDToLeafPointer.entrySet())
            {
                leafPointerToNodeID.put(entry.getValue(), entry.getKey());
            }

            int ordinal = 0;
            for (Map.Entry<Long, Integer> entry : leafPointerToNodeID.entrySet())
            {
                context.nodeIDToLeafOrdinal.put(entry.getValue(), ordinal);
                context.leafToNodeID.put(ordinal, entry.getValue());
                ordinal++;
            }

            context.nodeIDToLeafOrdinalFP = valuesOut.getFilePointer();
            valuesOut.writeVInt(context.nodeIDToLeafOrdinal.size());

            for (Map.Entry<Integer, Integer> entry : context.nodeIDToLeafOrdinal.entrySet())
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
                        context.nodeIDPostingsFP.put(nodeID, postingsFP);
                    }
                }
            }

            context.leafIDPostingsFP_FP = leafPostingsOut.getFilePointer();
            this.leafPostingsOut.writeVInt(leafToPostingsFP.size());
            for (Map.Entry<Integer, Long> entry : leafToPostingsFP.entrySet())
            {
                this.leafPostingsOut.writeVInt(entry.getKey());
                this.leafPostingsOut.writeVLong(entry.getValue());
            }

            context.orderMapFP = this.orderMapOut.getFilePointer();
            orderMapOut.writeVInt(this.leafToOrderMapFP.size());
            for (Map.Entry<Integer, Long> entry : this.leafToOrderMapFP.entrySet())
            {
                orderMapOut.writeVInt(entry.getKey());
                orderMapOut.writeVLong(entry.getValue());
            }

            context.indexFP = termsIndexWriter.complete();
        }
    }
    // iterator row id order
    private RowPointIterator rowPointIterator(SharedIndexInput leafPostingsInput) throws IOException
    {
        final DirectReaders.Reader orderMapReader = DirectReaders.getReaderForBitsPerValue((byte) DirectWriter.unsignedBitsRequired(LEAF_SIZE - 1));

        final IndexInput orderMapInput = this.fileProvider.openOrderMapInput(temporary);

        final SeekingRandomAccessInput orderMapRandoInput = new SeekingRandomAccessInput(orderMapInput);

        final List<RowPointIterator> iterators = new ArrayList<>();

        // create ordered readers
        for (Map.Entry<Integer,Long> entry : this.leafToPostingsFP.entrySet())
        {
            final long postingsFP = entry.getValue();
            final PostingsReader postingsReader = new PostingsReader(leafPostingsInput.sharedCopy(), postingsFP, QueryEventListener.PostingListEventListener.NO_OP);

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
        return new MergeRowPoints(iterators, orderMapRandoInput);
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

    private static class BlockBuffer
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

    private static class BlockIndexWriterContext
    {
        long leafFilePointersFP = -1;
        long nodeIDToLeafOrdinalFP = -1;
        long leafIDPostingsFP_FP = -1;
        long orderMapFP = -1;
        long indexFP = -1;
        long rowPointMap_FP = -1;
        long nodeIDToMultilevelPostingsFP_FP = -1;
        long multiBlockLeafRangesFP = -1;
        long leafValuesSameFP = -1;
        long leafValuesSamePostingsFP = -1;
        IntLongHashMap nodeIDPostingsFP = new IntLongHashMap();
        TreeMap<Integer, Integer> nodeIDToLeafOrdinal = new TreeMap();
        TreeMap<Integer, Integer> leafToNodeID = new TreeMap<>();
    }

    private static class RowPoint
    {
        public long rowID;
        public long pointID;

        @Override
        public String toString()
        {
            return "RowPoint{rowID=" + rowID + ", pointID=" + pointID + '}';
        }
    }

    private interface RowPointIterator extends Closeable
    {
        public RowPoint next() throws IOException;

        public RowPoint current();
    }

    private static class MergeRowPoints implements RowPointIterator
    {
        final MergeQueue queue;
        final List<Closeable> toCloseList;
        final RowPoint state = new RowPoint();

        public MergeRowPoints(List<RowPointIterator> iterators, Closeable... toClose) throws IOException
        {
            toCloseList = new ArrayList<>(iterators);
            Arrays.stream(toClose).forEach(c -> toCloseList.add(c));
            queue = new MergeQueue(iterators.size());
            for (RowPointIterator it : iterators)
            {
                it.next(); // init iterator
                queue.add(it);
            }
        }

        @Override
        public RowPoint current()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public RowPoint next() throws IOException
        {
            if (queue.size() != 0)
            {
                final RowPointIterator iterator = queue.top();

                final RowPoint topState = iterator.current();

                state.rowID = topState.rowID;
                state.pointID = topState.pointID;

                final RowPoint nextState = iterator.next();

                if (nextState != null)
                {
                    queue.updateTop();
                }
                else
                {
                    queue.pop();
                }
                return state;
            }
            else
            {
                // queue is exhausted
                return null;
            }
        }

        @Override
        public void close() throws IOException
        {
            FileUtils.close(toCloseList);
        }

        static final Comparator<RowPoint> comparator = (a, b) -> {
            int cmp1 = Long.compare(a.rowID, b.rowID);
            if (cmp1 == 0)
            {
                return Long.compare(a.pointID, b.pointID);
            }
            else
            {
                return cmp1;
            }
        };

        private static class MergeQueue extends PriorityQueue<RowPointIterator>
        {
            public MergeQueue(int maxSize)
            {
                super(maxSize);
            }

            @Override
            public boolean lessThan(RowPointIterator a, RowPointIterator b)
            {
                assert a != b;

                int cmp = comparator.compare(a.current(), b.current());

                if (cmp < 0)
                {
                    return true;
                }
                else return false;
            }
        }
    }

    private static class RowIDLeafOrdinal
    {
        public int leafOrdinal;
        public long rowID;

        @Override
        public String toString()
        {
            return "{leafOrdinal=" + leafOrdinal + ", rowID=" + rowID + '}';
        }
    }
}
