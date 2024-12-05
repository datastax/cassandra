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

package org.apache.cassandra.index.sai.iterators;

import java.io.IOException;

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;

/**
 * A {@link KeyRangeIterator} that performs a non-reducing left join between two iterators so that all of the
 * primary keys from the left iterator are returned, and only the primary keys from the right iterator that match
 * the primary keys from the left iterator are returned.
 */
public class KeyRangeNonReducingLeftJoinIterator extends KeyRangeIterator
{
    private final KeyRangeIterator left;
    private final KeyRangeIterator right;

    public KeyRangeNonReducingLeftJoinIterator(KeyRangeIterator left, KeyRangeIterator right)
    {
        super(left);
        this.left = left;
        this.right = right;
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        left.skipTo(nextKey);
    }

    @Override
    protected PrimaryKey computeNext()
    {
        if (!left.hasNext())
            return endOfData();

        // TODO is this skipTo performant enough? What about on large sstables with an empty column?
        right.skipTo(left.peek());

        return (right.hasNext() && right.peek().equals(left.peek()))
               ? right.next()
               : left.next();
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(left);
        FileUtils.closeQuietly(right);
    }
}
