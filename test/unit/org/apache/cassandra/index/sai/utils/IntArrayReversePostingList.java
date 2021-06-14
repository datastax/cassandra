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

import java.io.IOException;

import org.apache.cassandra.index.sai.disk.ReversePostingList;

public class IntArrayReversePostingList implements ReversePostingList
{
    final long[] array;
    private int pos;
    
    public IntArrayReversePostingList(long[] array)
    {
        this.array = array;
        pos = array.length;
    }

    @Override
    public long getOrdinal()
    {
        return pos;
    }

    @Override
    public long nextPosting() throws IOException
    {
        pos--;
        if (pos < 0) return REVERSE_END_OF_STREAM;
        return array[pos];
    }

    @Override
    public long advance(long target) throws IOException
    {
        return slowAdvance(target);
    }

    @Override
    public long size()
    {
        return array.length;
    }

    protected final long slowAdvance(long target) throws IOException
    {
        long doc;
        do
        {
            doc = nextPosting();
        } while (doc > target);

        assert doc <= target : "docid=" + doc + " target=" + target;
        return doc;
    }
}
