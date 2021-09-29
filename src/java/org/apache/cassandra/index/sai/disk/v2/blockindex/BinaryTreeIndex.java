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


import org.apache.lucene.index.PointValues;

public class BinaryTreeIndex
{
    final int leafNodeOffset;
    protected int nodeID;
    // level is 1-based so that we can do level-1 w/o checking each time:
    protected int level;

    public BinaryTreeIndex(int leafNodeOffset)
    {
        this.leafNodeOffset = leafNodeOffset;
        nodeID = 1;
        level = 1;
    }

    public int getNodeID()
    {
        return nodeID;
    }

    public int getLevel()
    {
        return level;
    }


    public void pushLeft()
    {
        nodeID *= 2;
        level++;
    }

    public void pushRight()
    {
        nodeID = nodeID * 2 + 1;
        level++;
    }

    public void pop()
    {
        nodeID /= 2;
        level--;
    }

    public boolean isLeafNode()
    {
        return nodeID >= leafNodeOffset;
    }

    public boolean nodeExists()
    {
        return nodeID - leafNodeOffset < leafNodeOffset;
    }

    static interface BinaryTreeVisitor
    {
        PointValues.Relation compare(int minOrdinal, int maxOrdinal);
    }

    static class BinaryTreeRangeVisitor implements BinaryTreeVisitor
    {
        public final Bound lower, upper;

        public BinaryTreeRangeVisitor(Bound lower, Bound upper)
        {
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public PointValues.Relation compare(int minValue, int maxValue)
        {
            boolean crosses = false;

            if (lower != null)
            {
                int maxCmp = Integer.compare(maxValue, lower.bound);
                if (lower.greaterThan(maxCmp))
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;

                int minCmp = Integer.compare(minValue, lower.bound);
                crosses |= lower.greaterThan(minCmp);
            }

            if (upper != null)
            {
                int minCmp = Integer.compare(minValue, upper.bound);
                if (upper.smallerThan(minCmp))
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;

                int maxCmp = Integer.compare(maxValue, upper.bound);
                crosses |= upper.smallerThan(maxCmp);
            }

            if (crosses)
            {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
            else
            {
                return PointValues.Relation.CELL_INSIDE_QUERY;
            }
        }

        public static class Bound
        {
            public final int bound;
            public final boolean exclusive;

            public Bound(int bound, boolean exclusive)
            {
                this.bound = bound;
                this.exclusive = exclusive;
            }

            public boolean smallerThan(int cmp)
            {
                return cmp > 0 || (cmp == 0 && exclusive);
            }

            public boolean greaterThan(int cmp)
            {
                return cmp < 0 || (cmp == 0 && exclusive);
            }
        }
    }
}
