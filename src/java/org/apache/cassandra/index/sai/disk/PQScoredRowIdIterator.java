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

package org.apache.cassandra.index.sai.disk;

import java.util.PriorityQueue;

import org.apache.cassandra.index.sai.disk.vector.ScoredRowId;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * An iterator over {@link ScoredRowId} sorted by score descending backed by a {@link PriorityQueue}.
 */
public class PQScoredRowIdIterator implements CloseableIterator<ScoredRowId>
{
    private final PriorityQueue<ScoredRowId> scoredRowIdIterator;
    public PQScoredRowIdIterator(PriorityQueue<ScoredRowId> scoredRowIds)
    {
        scoredRowIdIterator = scoredRowIds;
    }

    @Override
    public void close() {}

    @Override
    public boolean hasNext()
    {
        return scoredRowIdIterator.peek() != null;
    }

    @Override
    public ScoredRowId next()
    {
        return scoredRowIdIterator.poll();
    }
}
