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

package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeMultimap;
import com.google.common.collect.TreeRangeSet;

import com.carrotsearch.hppc.IntLongHashMap;
import org.agrona.collections.LongArrayList;
import org.apache.cassandra.index.sai.disk.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.io.tries.IncrementalDeepTrieWriterPageAware;
import org.apache.cassandra.io.tries.IncrementalTrieWriter;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.GrowableByteArrayDataOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.packed.DirectWriter;

import static org.apache.cassandra.index.sai.disk.v1.BlockIndexReader.fixedLength;
import static org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat.BLOCK_SIZE;

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
    private final LongArrayList leafBytesFPs = new LongArrayList();
    private final LongArrayList realLeafBytesFPs = new LongArrayList();

    final List<BytesRef> blockMinValues = new ArrayList();
    final IndexOutput out;
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

    private BlockBuffer currentBuffer = new BlockBuffer(), previousBuffer = new BlockBuffer();
    private int termOrdinal = 0; // number of unique terms
    private int leaf;

    final Directory directory;

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

    public BlockIndexWriter(IndexOutput out,
                            IndexOutputWriter indexOut,
                            IndexOutput orderMapOut,
                            Directory directory) throws IOException
    {
        this.out = out;
        this.indexOut = indexOut;
        this.leafPostingsOut = directory.createOutput("leafpostings", IOContext.DEFAULT);
        this.orderMapOut = orderMapOut;
        this.directory = directory;

        postingsWriter = new PostingsWriter(leafPostingsOut, BLOCK_SIZE, false);
    }

    public static class BlockIndexMeta
    {
        public final long orderMapFP;
        public final long indexFP;
        public final long leafFilePointersFP;
        public final int numLeaves;
        public final long nodeIDToLeafOrdinalFP;
        public final IntLongHashMap nodeIDPostingsFP;
        public final RangeSet<Integer> multiBlockLeafOrdinalRanges;
        public final BitSet leafValuesSame;

        // there can be multiple postings for a node id if the
        // edges are same value multi block postings
        public final TreeMultimap<Integer, Long> multiNodeIDToPostingsFP;

        public BlockIndexMeta(long orderMapFP,
                              long indexFP,
                              long leafFilePointersFP,
                              int numLeaves,
                              long nodeIDToLeafOrdinalFP,
                              IntLongHashMap nodeIDPostingsFP,
                              RangeSet<Integer> multiBlockLeafOrdinalRanges,
                              BitSet leafValuesSame,
                              TreeMultimap<Integer, Long> multiNodeIDToPostingsFP)
        {
            this.orderMapFP = orderMapFP;
            this.indexFP = indexFP;
            this.leafFilePointersFP = leafFilePointersFP;
            this.numLeaves = numLeaves;
            this.nodeIDToLeafOrdinalFP = nodeIDToLeafOrdinalFP;
            this.nodeIDPostingsFP = nodeIDPostingsFP;
            this.multiBlockLeafOrdinalRanges = multiBlockLeafOrdinalRanges;
            this.leafValuesSame = leafValuesSame;
            this.multiNodeIDToPostingsFP = multiNodeIDToPostingsFP;
        }
    }

    public BlockIndexMeta finish() throws IOException
    {
        flushLastBuffers();

        System.out.println("unique terms="+(this.termOrdinal + 1)); // 2, 1 for starting at 0

        // write the block min values index
        IncrementalTrieWriter termsIndexWriter = new IncrementalDeepTrieWriterPageAware<>(TrieTermsDictionaryReader.trieSerializer, indexOut.asSequentialWriter());
        int distinctCount = 0;
        int start = 0;
        int leafIdx = 0;

        // write distinct min block terms and the min and max leaf id's encoded as a long
        BytesRefBuilder lastTerm = new BytesRefBuilder();
        for (leafIdx = 0; leafIdx < blockMinValues.size(); leafIdx++)
        {
            BytesRef minValue = blockMinValues.get(leafIdx);
            if (leafIdx > 0)
            {
                BytesRef prevMinValue = blockMinValues.get(leafIdx - 1);
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

                    //System.out.println("termsIndexWriter write term="+prevMinValue.utf8ToString()+" startLeaf="+startLeaf+" endLeaf="+endLeaf);
                    long encodedLong = (((long)startLeaf) << 32) | (endLeaf & 0xffffffffL);
                    // TODO: when the start and end leaf's are the same encode a single int
                    //System.out.println("termsIndexWriter add minValue="+NumericUtils.sortableBytesToInt(prevMinValue.bytes, 0));
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
                //System.out.println("termsIndexWriter write2 prevMinValue="+prevMinValue.utf8ToString()+" start="+start+" endLeaf="+endLeaf);
                long encodedLong = (((long)start) << 32) | (endLeaf & 0xffffffffL);
                if (minValue.equals(prevMinValue))
                {
                    System.out.println("start="+start+" endLeaf="+endLeaf);
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

        System.out.println("numLeaves="+numLeaves);

        final long leafFilePointersFP = out.getFilePointer();

        for (int x = 0; x < realLeafBytesFPs.size(); x++)
        {
            out.writeVLong(realLeafBytesFPs.get(x));
        }

        final TreeMap<Integer,Integer> nodeIDToLeafOrdinal = new TreeMap();

        rotateToTree(1, 0, leafBytesFPs.size() - 1, nodeIDToLeafOrdinal);

        System.out.println("leafFilePointers.size=" + leafBytesFPs.size() + " nodeIDToLeafOrdinal=" + nodeIDToLeafOrdinal);

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

        System.out.println("nodeIDToLeafPointer="+nodeIDToLeafPointer);
        System.out.println("realLeafFilePointers=" + realLeafBytesFPs);


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

        System.out.println("nodeIDToLeafOrdinal2="+nodeIDToLeafOrdinal);

        final IntLongHashMap nodeIDPostingsFP = new IntLongHashMap();

        final long nodeIDToLeafOrdinalFP = out.getFilePointer();
        out.writeVInt(nodeIDToLeafOrdinal.size());

        for (Map.Entry<Integer, Integer> entry : nodeIDToLeafOrdinal.entrySet())
        {
            out.writeVInt(entry.getKey());
            out.writeVInt(entry.getValue());

            int nodeID = entry.getKey();
            int leafOrdinal = entry.getValue();

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

        final long orderMapFP = this.orderMapOut.getFilePointer();
        orderMapOut.writeVInt(this.leafToOrderMapFP.size());
        for (Map.Entry<Integer, Long> entry : this.leafToOrderMapFP.entrySet())
        {
            orderMapOut.writeVInt(entry.getKey());
            orderMapOut.writeVLong(entry.getValue());
        }

        final long indexFP = termsIndexWriter.complete();

        System.out.println("leafToPostingsFP=" + leafToPostingsFP);

        // close leaf postings because MultiLevelPostingsWriter read leaf postings
        this.leafPostingsOut.close();

        final IndexInput leafPostingsInput = directory.openInput("leafpostings", IOContext.DEFAULT);

        MultiLevelPostingsWriter multiLevelPostingsWriter
        = new MultiLevelPostingsWriter(leafPostingsInput,
                                       IndexWriterConfig.defaultConfig("indexName"),
                                       nodeIDPostingsFP,
                                       leafBytesFPs.size(),
                                       nodeIDToLeafOrdinal,
                                       this.multiBlockLeafRanges,
                                       leafToNodeID);

        final IndexOutput bigPostingsOut = directory.createOutput("bigpostings", IOContext.DEFAULT);
        final TreeMultimap<Integer, Long> nodeIDToMultilevelPostingsFP = multiLevelPostingsWriter.finish(bigPostingsOut);


        leafPostingsInput.close();
        bigPostingsOut.close();
        termsIndexWriter.close();
        indexOut.close();
        orderMapOut.close();
        out.close();

        return new BlockIndexMeta(orderMapFP,
                                  indexFP,
                                  leafFilePointersFP,
                                  leafBytesFPs.size(),
                                  nodeIDToLeafOrdinalFP,
                                  nodeIDPostingsFP,
                                  multiBlockLeafRanges,
                                  leafValuesSame,
                                  nodeIDToMultilevelPostingsFP);
    }

    public void add(ByteComparable term, long rowID) throws IOException
    {

        termBuilder.clear();
        int length = gatherBytes(term, termBuilder);

        if (currentBuffer.leafOrdinal > 0 && !termBuilder.get().equals(lastAddedTerm.get()))
        {
            currentBuffer.allLeafValuesSame = false;
        }

        if (lastAddedTerm.length() > 0 && !termBuilder.get().equals(lastAddedTerm.get()))
        {
//            System.out.println("term=" + termBuilder.get().utf8ToString()
//                               + " termOrdinal=" + termOrdinal
//                               + " lastAddedTerm=" + lastAddedTerm.get().utf8ToString());
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
            BytesRef minValue = BytesRef.deepCopyOf(termBuilder.get());
            blockMinValues.add(minValue);
            currentBuffer.leaf = leaf;
            currentBuffer.minValue = minValue;
        }
        else
        {
            //System.out.println("prefix=" + lastTermBuilder.get().utf8ToString() + " term=" + termBuilder.get().utf8ToString());
            int prefix = bytesDifference(lastTermBuilder.get(), termBuilder.get());
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
                System.out.println("   flushPreviousBufferAndSwap completePostings previousBuffer.leaf="+previousBuffer.leaf);
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

        //System.out.println("  writeLeaf buffer.leaf=" + buffer.leaf + " minValue=" + minValue.utf8ToString() + " allLeafValuesSame=" + buffer.allLeafValuesSame);

        if (buffer.leaf > 0)
        {
            // previous min block value is the same so point to that one and don't write anything
            final BytesRef prevMinValue = blockMinValues.get(buffer.leaf - 1);
            if (minValue.equals(prevMinValue) && buffer.allLeafValuesSame)
            {
                long previousRealFP = this.realLeafBytesFPs.get(this.realLeafBytesFPs.size() - 1);
                this.realLeafBytesFPs.add(previousRealFP);
                return;
            }
        }

        long filePointer = out.getFilePointer();
        final int maxLength = Arrays.stream(buffer.lengths).max().getAsInt();
        LeafOrderMap.write(buffer.lengths, buffer.leafOrdinal, maxLength, buffer.lengthsScratchOut);
        final int maxPrefix = Arrays.stream(buffer.prefixes).max().getAsInt();
        LeafOrderMap.write(buffer.prefixes, buffer.leafOrdinal, maxPrefix, buffer.prefixScratchOut);

        out.writeInt(buffer.leafOrdinal); // value count
        out.writeInt(buffer.lengthsScratchOut.getPosition());
        out.writeInt(buffer.prefixScratchOut.getPosition());
        out.writeByte((byte) DirectWriter.unsignedBitsRequired(maxLength));
        out.writeByte((byte) DirectWriter.unsignedBitsRequired(maxPrefix));
        out.writeBytes(buffer.lengthsScratchOut.getBytes(), 0, buffer.lengthsScratchOut.getPosition());
        out.writeBytes(buffer.prefixScratchOut.getBytes(), 0, buffer.prefixScratchOut.getPosition());
        out.writeBytes(buffer.scratchOut.getBytes(), 0, buffer.scratchOut.getPosition());

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

        System.out.println("inRowIDOrder="+inRowIDOrder+" rowIDLeafOrdinals="+Arrays.toString(buffer.rowIDLeafOrdinals));

        // write an order map if the row ids are not in order
        if (!inRowIDOrder)
        {
            final long orderMapFP = orderMapOut.getFilePointer();
            final int bits = DirectWriter.unsignedBitsRequired(LEAF_SIZE - 1);
            final DirectWriter writer = DirectWriter.getInstance(orderMapOut, buffer.leafOrdinal, bits);
            for (int i = 0; i < buffer.leafOrdinal; i++)
            {
                writer.add(buffer.rowIDLeafOrdinals[i].leafOrdinal);
            }
            writer.finish();
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
        //System.out.println("ROTATE: nodeID=" + nodeID + " offset=" + offset + " count=" + count + " bpd=" + bytesPerDim + " index.length=" + index.length);
        if (count == 1)
        {
            // Leaf index node
            //System.out.println("  leaf index node");
            //System.out.println("  index[" + nodeID + "] = blockStartValues[" + offset + "]");

            nodeIDToLeafOrdinal.put(nodeID, offset + 1);

            //System.arraycopy(leafBlockStartValues.get(offset), 0, index, nodeID * (1 + bytesPerDim) + 1, bytesPerDim);
        }
        else if (count > 1)
        {
            // Internal index node: binary partition of count
            int countAtLevel = 1;
            int totalCount = 0;
            while (true)
            {
                int countLeft = count - totalCount;
                //System.out.println("    cycle countLeft=" + countLeft + " coutAtLevel=" + countAtLevel);
                if (countLeft <= countAtLevel)
                {
                    // This is the last level, possibly partially filled:
                    int lastLeftCount = Math.min(countAtLevel / 2, countLeft);
                    assert lastLeftCount >= 0;
                    int leftHalf = (totalCount - 1) / 2 + lastLeftCount;

                    int rootOffset = offset + leftHalf;
          /*
          System.out.println("  last left count " + lastLeftCount);
          System.out.println("  leftHalf " + leftHalf + " rightHalf=" + (count-leftHalf-1));
          System.out.println("  rootOffset=" + rootOffset);
          */

                    nodeIDToLeafOrdinal.put(nodeID, rootOffset + 1);

                    //System.arraycopy(leafBlockStartValues.get(rootOffset), 0, index, nodeID * (1 + bytesPerDim) + 1, bytesPerDim);
                    //System.out.println("  index[" + nodeID + "] = blockStartValues[" + rootOffset + "]");

                    // TODO: we could optimize/specialize, when we know it's simply fully balanced binary tree
                    // under here, to save this while loop on each recursion

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

    public static int bytesDifference(BytesRef priorTerm, BytesRef currentTerm)
    {
        int mismatch = FutureArrays.mismatch(priorTerm.bytes, priorTerm.offset, priorTerm.offset + priorTerm.length, currentTerm.bytes, currentTerm.offset, currentTerm.offset + currentTerm.length);
        return mismatch;
    }

    public static int gatherBytes(ByteComparable term, BytesRefBuilder builder)
    {
        final ByteSource byteSource = term.asComparableBytes(ByteComparable.Version.OSS41);
        int length = 0;
        // gather the term bytes from the byteSource
        while (true)
        {
            final int val = byteSource.next();
            if (val != ByteSource.END_OF_STREAM)
            {
                ++length;
                builder.append((byte) val);
            }
            else
            {
                break;
            }
        }
        return length;
    }
}
