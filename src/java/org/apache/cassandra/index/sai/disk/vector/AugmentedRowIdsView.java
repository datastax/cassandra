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

import com.carrotsearch.hppc.IntObjectHashMap;

/**
 * An augmented view of RowIdsView that takes an in memory mapping of vectorOrdinal -> rowIds and uses that
 * as the primary source of rowIds. If a vectorOrdinal is not found in the mapping, it falls back to the
 * delegate RowIdsView.
 */
public class AugmentedRowIdsView implements RowIdsView
{
    private final IntObjectHashMap<int[]> augmentedMapping;
    private final RowIdsView delegate;

    public AugmentedRowIdsView(IntObjectHashMap<int[]> augmentedMapping, RowIdsView delegate)
    {
        this.augmentedMapping = augmentedMapping;
        this.delegate = delegate;
    }

    @Override
    public int[] getSegmentRowIdsMatching(int vectorOrdinal) throws IOException
    {
        int[] rowIds = augmentedMapping.get(vectorOrdinal);
        if (rowIds != null)
            return rowIds;
        return delegate.getSegmentRowIdsMatching(vectorOrdinal);
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
