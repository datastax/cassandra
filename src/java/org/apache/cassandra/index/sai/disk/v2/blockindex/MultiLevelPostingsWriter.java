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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntLongHashMap;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.MergePostingList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.v2.postings.PForDeltaPostingsWriter;
import org.apache.cassandra.index.sai.disk.v2.postings.PostingsReader;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.SharedIndexInput;
import org.apache.lucene.store.IndexOutput;

import static com.google.common.base.Preconditions.checkArgument;

public class MultiLevelPostingsWriter
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Multimap<Integer, Integer> nodeToChildLeaves = HashMultimap.create();

    final TreeSet<Integer> leafNodeIDs = new TreeSet<>();
    private final IndexWriterConfig config;
    final TreeMap<Integer,Integer> nodeIDToLeafOrdinal;
    final IntLongHashMap nodeIDPostingsFP;
    final SharedIndexInput leafPostingsInput;
    final int numLeaves;
    final RangeSet<Integer> multiBlockLeafRanges;
    final TreeMap<Integer,Integer> leafToNodeID;

    public MultiLevelPostingsWriter(SharedIndexInput leafPostingsInput,
                                    IndexWriterConfig config,
                                    IntLongHashMap nodeIDPostingsFP,
                                    int numLeaves,
                                    TreeMap<Integer,Integer> nodeIDToLeafOrdinal,
                                    final RangeSet<Integer> multiBlockLeafRanges,
                                    TreeMap<Integer,Integer> leafToNodeID)
    {
        this.leafPostingsInput = leafPostingsInput;
        this.config = config;
        this.nodeIDPostingsFP = nodeIDPostingsFP;
        this.numLeaves = numLeaves;
        this.nodeIDToLeafOrdinal = nodeIDToLeafOrdinal;
        this.multiBlockLeafRanges = multiBlockLeafRanges;
        this.leafToNodeID = leafToNodeID;
    }

    @SuppressWarnings("resource")
    // TODO: possibly writing lower level leaf nodes twice?
    public final TreeMultimap<Integer, Long> finish(IndexOutput out) throws IOException
    {
        traverse(new BinaryTreeIndex(numLeaves),
                 new IntArrayList());

//        checkState(postings.size() == leafFPToNodeID.size(),
//                   "Expected equal number of postings lists (%s) and leaf offsets (%s).",
//                   postings.size(), leafFPToNodeID.size());

        final PForDeltaPostingsWriter postingsWriter = new PForDeltaPostingsWriter(out);

        final List<Integer> internalNodeIDs =
        nodeToChildLeaves.keySet()
                         .stream()
                         .filter(i -> nodeToChildLeaves.get(i).size() >= config.getBkdPostingsMinLeaves())
                         .collect(Collectors.toList());

        //final long startFP = out.getFilePointer();
        final Stopwatch flushTime = Stopwatch.createStarted();
        final TreeMultimap<Integer, Long> nodeIDToPostingsFP = TreeMultimap.create();
        for (final int nodeID : internalNodeIDs)
        {
            Collection<Integer> leafNodeIDs = nodeToChildLeaves.get(nodeID);

            assert leafNodeIDs.size() > 0;

            for (int n : leafNodeIDs)
            {
                assert n >= numLeaves;
            }

            final List<Integer> leafNodeIDsCopy = new ArrayList<>(leafNodeIDs);

            assert nodeIDToLeafOrdinal.containsKey(nodeID);

            int maxLeaf = -1;
            int minLeaf = Integer.MAX_VALUE;

            // skip leaf nodes they're not overlapping with multi-block postings
            // if there are overlapping same value multi-block postings, add the multi-block postings
            // file pointers, and remove the multi-block node ids from leafNodeIDs
            // so the same value multi-block postings aren't added to the aggregated node posting list
            for (final int leafNodeID : leafNodeIDs)
            {
                final int leaf = nodeIDToLeafOrdinal.get(leafNodeID);
                maxLeaf = Math.max(leaf, maxLeaf);
                minLeaf = Math.min(leaf, minLeaf);
            }

            // if there are multi-block ranges then remove their node ids from the ultimate posting list
            assert minLeaf != Integer.MAX_VALUE;
            assert maxLeaf != -1;

            final Range<Integer> multiBlockMin = multiBlockLeafRanges.rangeContaining(minLeaf);
            final Range<Integer> multiBlockMax = multiBlockLeafRanges.rangeContaining(maxLeaf);

//            System.out.println("minLeaf=" + minLeaf + " multiBlockMin=" + multiBlockMin);
//            System.out.println("maxLeaf=" + maxLeaf + " multiBlockMax=" + multiBlockMax);

            if (multiBlockMin != null)
            {
                final int startLeaf = multiBlockMin.lowerEndpoint();
                final int endLeaf = multiBlockMin.upperEndpoint();

                Integer leafNodeID = this.leafToNodeID.get(endLeaf);

                assert leafNodeID != null;

                assert nodeIDPostingsFP.containsKey(leafNodeID);

                long multiBlockPostingsFP = nodeIDPostingsFP.get(leafNodeID);

                nodeIDToPostingsFP.put(leafNodeID, multiBlockPostingsFP);

                // remove leaf node's that are in the multi-block posting list
                for (int leaf = startLeaf; leaf <= endLeaf; leaf++)
                {
                    assert this.multiBlockLeafRanges.contains(leaf);

                    Integer toRemoveNodeID = this.leafToNodeID.get(leaf);
                    assert toRemoveNodeID != null;
                    boolean removed = leafNodeIDsCopy.remove(toRemoveNodeID);
                }
            }

            if (multiBlockMax != null)
            {
                final int startLeaf = multiBlockMax.lowerEndpoint();
                final int endLeaf = multiBlockMax.upperEndpoint();

                Integer leafNodeID = this.leafToNodeID.get(endLeaf);

                assert nodeIDPostingsFP.containsKey(leafNodeID);

                long multiBlockPostingsFP = nodeIDPostingsFP.get(leafNodeID);

                nodeIDToPostingsFP.put(leafNodeID, multiBlockPostingsFP);

                for (int leaf = startLeaf; leaf <= endLeaf; leaf++)
                {
                    assert this.multiBlockLeafRanges.contains(leaf);

                    Integer toRemoveNodeID = this.leafToNodeID.get(leaf);
                    assert toRemoveNodeID != null;
                    boolean removed = leafNodeIDsCopy.remove(toRemoveNodeID);
                }
            }

            final PriorityQueue<PostingList.PeekablePostingList> postingLists = new PriorityQueue<>(100, Comparator.comparingLong(PostingList.PeekablePostingList::peek));

            for (final Integer leafNodeID : leafNodeIDsCopy)
            {
                assert leafNodeID.intValue() >= numLeaves; // is leaf node

                if (nodeIDPostingsFP.containsKey(leafNodeID))
                {
                    final long postingsFP = nodeIDPostingsFP.get(leafNodeID);
                    final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(leafPostingsInput, postingsFP);
                    final PostingsReader reader = new PostingsReader(leafPostingsInput, postingsFP, QueryEventListener.PostingListEventListener.NO_OP);
                    postingLists.add(reader.peekable());
                }
            }

            // TODO: get the start and end for the multi block postings

            if (postingLists.size() > 0)
            {
                final PostingList mergedPostingList = MergePostingList.merge(postingLists);
                long regularPostingsFP = postingsWriter.write(mergedPostingList);
                // During compaction we could end up with an empty postings due to deletions.
                // The writer will return a fp of -1 if no postings were written.
                if (regularPostingsFP >= 0)
                {
                    nodeIDToPostingsFP.put(nodeID, regularPostingsFP * -1);
                }
            }
        }
        flushTime.stop();

        final long indexFilePointer = out.getFilePointer();
        postingsWriter.complete();
        return nodeIDToPostingsFP;
    }

    private void onLeaf(int leafNodeID, IntArrayList pathToRoot)
    {
        checkArgument(!pathToRoot.containsInt(leafNodeID));
        checkArgument(pathToRoot.isEmpty() || leafNodeID > pathToRoot.get(pathToRoot.size() - 1));

        leafNodeIDs.add(leafNodeID);

        for (int i = 0; i < pathToRoot.size(); i++)
        {
            final int level = i + 1;
            if (isLevelEligibleForPostingList(level))
            {
                final int nodeID = pathToRoot.get(i);
                nodeToChildLeaves.put(nodeID, leafNodeID);
            }
        }
    }

    private void traverse(BinaryTreeIndex tree,
                          IntArrayList pathToRoot)
    {
        if (tree.isLeafNode())
        {
            // In the unbalanced case it's possible the left most node only has one child:
            if (tree.nodeExists())
            {
                onLeaf(tree.getNodeID(), pathToRoot);
            }
        }
        else
        {
            final int nodeID = tree.getNodeID();
            final IntArrayList currentPath = new IntArrayList();
            currentPath.addAll(pathToRoot);
            currentPath.add(nodeID);

            tree.pushLeft();
            traverse(tree, currentPath);
            tree.pop();

            tree.pushRight();
            traverse(tree, currentPath);
            tree.pop();
        }
    }

    private boolean isLevelEligibleForPostingList(int level)
    {
        return level > 1 && level % config.getBkdPostingsSkip() == 0;
    }
}
