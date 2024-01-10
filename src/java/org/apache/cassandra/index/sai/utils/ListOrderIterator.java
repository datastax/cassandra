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

package org.apache.cassandra.index.sai.utils;

import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * A {@link ListOrderIterator} that iterates over a collection of {@link ScoredPrimaryKey}s without modifying the underlying list.
 */
public class ListOrderIterator extends OrderIterator
{
    private final PeekingIterator<ScoredPrimaryKey> keyQueue;

    /**
     * Create a new {@link ListOrderIterator} that iterates over the provided list of keys.
     * @param keys the list of keys to iterate over
     */
    public ListOrderIterator(List<ScoredPrimaryKey> keys)
    {
        this.keyQueue = Iterators.peekingIterator(keys.iterator());
    }

    @Override
    protected ScoredPrimaryKey computeNext()
    {
        return keyQueue.hasNext() ? keyQueue.next() : endOfData();
    }
}
