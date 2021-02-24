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

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class PartitionKeysMeta
{
    public final int maxPointsInLeafNode;
    public final long pointCount;
    public final int bytesPerDim;
    public final int numDims;
    public final int numLeafs;
    public final long bkdPosition;
    public final long bkdOffset;
    public final long bkdLength;

    public PartitionKeysMeta(IndexInput input) throws IOException
    {
        this.maxPointsInLeafNode = input.readInt();
        this.pointCount = input.readLong();
        this.bytesPerDim = input.readInt();
        this.numDims = input.readInt();
        this.numLeafs = input.readInt();
        this.bkdPosition = input.readLong();
        this.bkdOffset = input.readLong();
        this.bkdLength = input.readLong();
    }

    public PartitionKeysMeta(int maxPointsInLeafNode, long pointCount, int bytesPerDim, int numDims, int numLeafs, long bkdPosition, long bkdOffset, long bkdLength)
    {
        this.maxPointsInLeafNode = maxPointsInLeafNode;
        this.pointCount = pointCount;
        this.bytesPerDim = bytesPerDim;
        this.numDims = numDims;
        this.numLeafs = numLeafs;
        this.bkdPosition = bkdPosition;
        this.bkdOffset = bkdOffset;
        this.bkdLength = bkdLength;
    }

    public void write(IndexOutput out) throws IOException
    {
        out.writeInt(maxPointsInLeafNode);
        out.writeLong(pointCount);
        out.writeInt(bytesPerDim);
        out.writeInt(numDims);
        out.writeInt(numLeafs);
        out.writeLong(bkdPosition);
        out.writeLong(bkdOffset);
        out.writeLong(bkdLength);
    }
}
