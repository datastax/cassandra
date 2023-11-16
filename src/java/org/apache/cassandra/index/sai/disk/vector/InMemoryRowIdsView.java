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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;
import java.util.HashMap;

import org.agrona.collections.IntArrayList;

public class InMemoryRowIdsView implements RowIdsView
{
    private final HashMap<Integer, int[]> rowIdMapping;

    public InMemoryRowIdsView()
    {
        rowIdMapping = new HashMap<>();
    }

    public void put(int vectorOrdinal, int rowId)
    {
        rowIdMapping.compute(vectorOrdinal, (ignored, rowIds) -> {
            if (rowIds == null)
            {
                return new int[] { rowId };
            }
            else
            {
                // Most vector use cases have a single rowId per vectorOrdinal, so this optimizes for that.
                // If that doesn't hold true, we can optimize use a data structure optimized for growing efficiently.
                assert rowIds[rowIds.length - 1] < rowId : "RowIds must be added in ascending order";
                int[] newRowIds = new int[rowIds.length + 1];
                System.arraycopy(rowIds, 0, newRowIds, 0, rowIds.length);
                newRowIds[rowIds.length] = rowId;
                return newRowIds;
            }
        });
    }

    @Override
    public int[] getSegmentRowIdsMatching(int vectorOrdinal) throws IOException
    {
        return rowIdMapping.get(vectorOrdinal);
    }

    @Override
    public void close()
    {

    }
}
