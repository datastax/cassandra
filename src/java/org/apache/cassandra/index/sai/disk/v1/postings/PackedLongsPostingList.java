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
package org.apache.cassandra.index.sai.disk.v1.postings;

import java.io.IOException;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * Adapter class for {@link PackedLongValues} to expose it as {@link PostingList}.
 */
public class PackedLongsPostingList implements PostingList
{
    private final PackedLongValues.Iterator iterator;
    private final PackedLongValues values;

    public PackedLongsPostingList(PackedLongValues values)
    {
        this.values = values;
        iterator = values.iterator();
    }

    @Override
    public int nextPosting()
    {
        if (iterator.hasNext())
        {
            // This is assumed to be safe because we only insert segment row ids, which are always integers,
            // into the packed longs object
            return Math.toIntExact(iterator.next());
        }
        else
        {
            return PostingList.END_OF_STREAM;
        }
    }

    @Override
    public int size()
    {
        // We know that the size of the packed longs object is less than or equal to Integer.MAX_VALUE
        return Math.toIntExact(values.size());
    }

    @Override
    public int advance(int targetRowID) throws IOException
    {
        throw new UnsupportedOperationException();
    }
}

