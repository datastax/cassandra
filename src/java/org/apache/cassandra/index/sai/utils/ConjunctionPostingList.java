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
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.lucene.util.CollectionUtil;

public class ConjunctionPostingList implements PostingList
{
    final PostingList lead1, lead2;
    final PostingList[] others;

    private int hits = 0;
    private int misses = 0;

    private final List<PostingList> toClose;

    public ConjunctionPostingList(List<PostingList> lists)
    {
        assert lists.size() >= 2;

        toClose = new ArrayList<>(lists);

        CollectionUtil.timSort(lists, (o1, o2) -> Long.compare(o1.size(), o2.size()));
        lead1 = lists.get(0);
        lead2 = lists.get(1);
        others = lists.subList(2, lists.size()).toArray(new PostingList[0]);
    }

    @Override
    public void close() throws IOException
    {
        lead1.close();
        lead2.close();
        for (PostingList list : others)
        {
            list.close();
        }
    }

    @Override
    public long advance(PrimaryKey nextPrimaryKey) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrimaryKey mapRowId(long rowId)
    {
        throw new UnsupportedOperationException();
    }

    private long doNext(long targetRowID) throws IOException
    {
        advanceHead:
        for (;;)
        {
            assert targetRowID == lead1.currentPosting() : "targetRowID="+targetRowID+" lead1.currentPosting="+lead1.currentPosting();

            final long next2 = lead2.advance(targetRowID);

            System.out.println("lead2.advance(targetRowID)="+targetRowID+" next2="+next2);

            if (next2 != targetRowID)
            {
                targetRowID = lead1.advance(next2);
                if (next2 != targetRowID)
                {
                    misses++;
                    continue;
                }
            }

            // then find agreement with other iterators
            for (PostingList other : others)
            {
                // other.targetRowID may already be equal to targetRowID if we "continued advanceHead"
                // on the previous iteration and the advance on the lead scorer exactly matched.
                if (other.currentPosting() < targetRowID)
                {
                    final long next = other.advance(targetRowID);
                    if (next > targetRowID)
                    {
                        misses++;
                        // iterator beyond the current targetRowID - advance lead and continue to the new highest targetRowID.
                        targetRowID = lead1.advance(next);
                        continue advanceHead;
                    }
                }
            }

            // success - all iterators are on the same targetRowID
            System.out.println("success - all iterators are on the same targetRowID="+targetRowID);
            hits++;
            return targetRowID;
        }
    }

    @Override
    public long currentPosting() {
        return lead1.currentPosting();
    }

    @Override
    public long size() {
        return lead1.size(); // overestimate
    }

    @Override
    public long advance(long target) throws IOException {
        return doNext(lead1.advance(target));
    }

    public long docID() {
        return lead1.currentPosting();
    }

    @Override
    public long nextPosting() throws IOException {
        return doNext(lead1.nextPosting());
    }
}
