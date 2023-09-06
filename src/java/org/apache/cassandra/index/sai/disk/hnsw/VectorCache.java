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

package org.apache.cassandra.index.sai.disk.hnsw;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.util.hnsw.HnswGraph;
import org.jctools.maps.NonBlockingHashMapLong;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Caches vectors intelligently, preferring vectors that occur in higher levels of the graph
 * and vectors that are closer (in the edge-wise, not similarity, sense) to the level's entry points.
 */
public abstract class VectorCache
{
    public abstract float[] get(int ordinal);

    public static VectorCache load(HnswGraph hnsw, OnDiskVectors vectors, int capacity) throws IOException
    {
        if (capacity <= 0)
            return new EmptyVectorCache();
        return new NBHMVectorCache(hnsw, vectors, capacity);
    }

    public abstract long ramBytesUsed();

    private static final class EmptyVectorCache extends VectorCache
    {
        @Override
        public float[] get(int ordinal)
        {
            return null;
        }

        @Override
        public long ramBytesUsed()
        {
            return 0;
        }
    }

    private static final class NBHMVectorCache extends VectorCache
    {
        private final NonBlockingHashMapLong<float[]> cache = new NonBlockingHashMapLong<>();
        private final int dimension;

        public NBHMVectorCache(HnswGraph hnsw, OnDiskVectors vectors, int capacityRemaining) throws IOException
        {
            dimension = vectors.dimension();
            var topLevel = hnsw.numLevels() - 1;
            var visitedNodes = new HashSet<>(List.of(hnsw.entryNode())); // resets between levels

            // we deliberately don't cache level 0 since that's going to have the worst efficiency
            for (int level = topLevel; level > 0 && capacityRemaining > 0; level--)
            {
                // start with the visited set from the previous level
                var nodeQueue = new LinkedList<>(visitedNodes);
                visitedNodes.clear();

                // for each node in the queue, add its neighbors to the queue, and cache it if not yet cached
                while (!nodeQueue.isEmpty() && capacityRemaining > 0)
                {
                    var node = nodeQueue.poll();
                    if (!visitedNodes.add(node))
                        continue;

                    if (!cache.containsKey((long) node))
                    {
                        try
                        {
                            var vector = new float[dimension];
                            vectors.readVector(node, vector);
                            cache.put(node, vector);
                            capacityRemaining -= dimension * Float.BYTES;
                        }
                        catch (IOException e)
                        {
                            throw new RuntimeException(e);
                        }
                    }

                    // add neighbors of current node to queue
                    hnsw.seek(level, node);
                    while (true)
                    {
                        var neighbor = hnsw.nextNeighbor();
                        if (neighbor == NO_MORE_DOCS)
                            break;
                        if (!visitedNodes.contains(neighbor))
                            nodeQueue.add(neighbor);
                    }
                }
            }
        }

        @Override
        public float[] get(int ordinal)
        {
            return cache.get(ordinal);
        }

        @Override
        public long ramBytesUsed()
        {
            return RamEstimation.concurrentHashMapRamUsed(cache.size())
                   + (long) cache.size() * Float.BYTES * dimension;
        }
    }
}