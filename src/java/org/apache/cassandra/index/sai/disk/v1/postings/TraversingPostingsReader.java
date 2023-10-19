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
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.store.IndexInput;

public class TraversingPostingsReader implements PostingList
{
    private final Expression exp;
    private final IndexInput input;
    private final PeekingIterator<Pair<ByteSource,Long>> iterable;
    private final QueryEventListener.PostingListEventListener listener;

    private PostingsReader currentReader;

    public TraversingPostingsReader(Expression exp, IndexInput input, PeekingIterator<Pair<ByteSource,Long>> iterable, QueryEventListener.PostingListEventListener listener) throws IOException
    {
        this.exp = exp;
        this.input = input;
        this.iterable = iterable;
        this.listener = listener;
    }

    public long nextPosting() throws IOException
    {
        if (currentReader != null)
        {
            var nextPosting = currentReader.nextPosting();
            if (nextPosting != PostingList.END_OF_STREAM)
                return nextPosting;
        }
        while (iterable.hasNext())
        {
            var next = iterable.next();
            byte[] nextBytes = ByteSourceInverse.readBytes(next.left);
            if (exp.isSatisfiedBy(ByteBuffer.wrap(nextBytes)))
            {
                // TODO need to read all of the values
                // TODO how expensive am I? Is there a better way to do this?
                currentReader = new PostingsReader(input, next.right, listener);
                var nextPosting = currentReader.nextPosting();
                if (nextPosting != PostingList.END_OF_STREAM)
                    return nextPosting;
            }
        }
        return PostingList.END_OF_STREAM;
    }

    @Override
    public long size()
    {
        // TODO lazily realize this?? This short circuit if this returns 0.
        // What does vector do, does it run the comparison at creation or defer it?
        return 1;
    }

    @Override
    public long advance(long targetRowID) throws IOException
    {
        throw new UnsupportedOperationException("Cannot advance a scanning postings reader");
    }

    @Override
    public void close() throws IOException
    {
        input.close();
    }
}
