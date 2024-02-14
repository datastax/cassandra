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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;

/**
 * A range iterator based on {@link PostingList}.
 *
 * <ol>
 *   <li> fetch next unique segment row id from posting list or skip to specific segment row id if {@link #skipTo(PrimaryKey)} is called </li>
 *   <li> add segmentRowIdOffset to obtain the sstable row id </li>
 *   <li> produce a {@link PrimaryKey} from {@link PrimaryKeyMap#primaryKeyFromRowId(long)} which is used
 *       to avoid fetching duplicated keys due to partition-level indexing on wide partition schema.
 *       <br/>
 *       Note: in order to reduce disk access in multi-index query, partition keys will only be fetched for intersected tokens
 *       in {@link org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher}.
 *  </li>
 * </ol>
 *
 */

@NotThreadSafe
public class PostingListRangeIterator extends RangeIterator
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Stopwatch timeToExhaust = Stopwatch.createStarted();
    private final QueryContext queryContext;

    private final PostingList postingList;
    private final IndexContext indexContext;
    private final PrimaryKeyMap primaryKeyMap;
    private final IndexSearcherContext searcherContext;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private boolean needsSkipping = false;
    private Token skipToToken = null;
    private long lastSegmentRowId = -1;

    /**
     * Create a direct PostingListRangeIterator where the underlying PostingList is materialised
     * immediately so the posting list size can be used.
     */
    public PostingListRangeIterator(IndexContext indexContext,
                                    PrimaryKeyMap primaryKeyMap,
                                    IndexSearcherContext searcherContext)
    {
        super(searcherContext.minimumKey, searcherContext.maximumKey, searcherContext.count());

        this.indexContext = indexContext;
        this.primaryKeyMap = primaryKeyMap;
        this.postingList = searcherContext.postingList;
        this.searcherContext = searcherContext;
        this.queryContext = this.searcherContext.context;
    }

    @Override
    protected void performSkipTo(Token nextToken)
    {
        if (skipToToken != null && skipToToken.compareTo(nextToken) >= 0)
            return;

        skipToToken = nextToken;
        needsSkipping = true;
    }

    private PrimaryKeyWithSource createPrimaryKeyWithSource(long rowId)
    {
        var primaryKey = primaryKeyMap.primaryKeyFromRowId(rowId);
        return new PrimaryKeyWithSource(primaryKey, primaryKeyMap.getSSTableId(), rowId);
    }

    @Override
    protected PrimaryKey computeNext()
    {
        try
        {
            queryContext.checkpoint();

            // just end the iterator if we don't have a postingList or current segment is skipped
            if (exhausted())
                return endOfData();

            long rowId = getNextRowId();
            if (rowId == PostingList.END_OF_STREAM)
                return endOfData();

            return createPrimaryKeyWithSource(rowId);
        }
        catch (Throwable t)
        {
            if (!(t instanceof AbortedOperationException))
                logger.error(indexContext.logMessage("Unable to provide next token!"), t);

            closeOnException(t);
            throw Throwables.cleaned(t);
        }
    }

    private void closeOnException(Throwable t)
    {
        FileUtils.closeQuietly(this);
    }

    @Override
    public void close() throws IOException
    {
        if (isClosed.compareAndSet(false, true))
        {
            if (logger.isTraceEnabled())
            {
                // timeToExhaust.stop() throws on already stopped stopwatch
                final long closedInMills = timeToExhaust.stop().elapsed(TimeUnit.MILLISECONDS);
                logger.trace(indexContext.logMessage("PostinListRangeIterator exhausted after {} ms"), closedInMills);
            }

            FileUtils.closeQuietly(postingList, primaryKeyMap);
        }
        else {
            logger.warn("PostingListRangeIterator is already closed",
                        new IllegalStateException("PostingListRangeIterator is already closed"));
        }

    }

    private boolean exhausted()
    {
        return needsSkipping && skipToToken.compareTo(getMaximum().token()) > 0;
    }

    @Override
    protected IntersectionResult performIntersect(PrimaryKey otherKey)
    {
        // TODO is this guard valuable or too expensive? It seems like preventing unnecessary calls
        // to advance is worth it.
        if (getMaximum().compareTo(otherKey) < 0)
            return IntersectionResult.EXHAUSTED;
        if (otherKey.compareTo(getMinimum()) < 0)
            return IntersectionResult.MISS;
        try
        {
            long targetRowID;
            if (otherKey instanceof PrimaryKeyWithSource
                && ((PrimaryKeyWithSource) otherKey).getSourceSstableId().equals(primaryKeyMap.getSSTableId()))
            {
                // We know the row is in the sstable, but not whether it's in the postinglist.
                targetRowID = ((PrimaryKeyWithSource) otherKey).getSourceRowId();
            }
            else
            {
                targetRowID = primaryKeyMap.exactRowIdOrInvertedCeiling(otherKey);
                // nextKey is larger than max token in token file
                if (targetRowID == Long.MIN_VALUE)
                    return IntersectionResult.EXHAUSTED;
                // nextKey is not in this sstable, so it cannot be in the posting list
                else if (targetRowID < 0)
                    return IntersectionResult.MISS;
            }

            long targetSegmentRowID = targetRowID - searcherContext.segmentRowIdOffset;
            if (lastSegmentRowId > targetSegmentRowID)
                return IntersectionResult.MISS;
            else if (targetSegmentRowID == lastSegmentRowId)
                return IntersectionResult.MATCH;

            // It is cheaper to get nextPosting, and since nextPosting() will return either targetSegmentRowID or
            // something greater, just call that.
            if (lastSegmentRowId + 1 == targetSegmentRowID)
                lastSegmentRowId = postingList.nextPosting();
            else
                lastSegmentRowId = postingList.advance(targetSegmentRowID);

            if (lastSegmentRowId == PostingList.END_OF_STREAM)
                return IntersectionResult.EXHAUSTED;
            else if (lastSegmentRowId == targetSegmentRowID)
                return IntersectionResult.MATCH;

            // Create a deferred PrimaryKey.
            next = createPrimaryKeyWithSource(lastSegmentRowId + searcherContext.getSegmentRowIdOffset());
            state = State.READY;
            return IntersectionResult.MISS;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * reads the next sstable row ID from the underlying posting list, potentially skipping to get there.
     */
    private long getNextRowId() throws IOException
    {
        long segmentRowId;
        if (needsSkipping)
        {
            long targetRowID = primaryKeyMap.exactRowIdOrInvertedCeiling(skipToToken);
            // skipToToken is larger than max token in token file
            if (targetRowID == Long.MIN_VALUE)
                return PostingList.END_OF_STREAM;
            if (targetRowID < 0)
                targetRowID = -targetRowID - 1;
            segmentRowId = postingList.advance(targetRowID - searcherContext.getSegmentRowIdOffset());
            needsSkipping = false;
        }
        else
        {
            do
            {
                segmentRowId = postingList.nextPosting();
                // Do not produce a duplicate segment row id.
            } while (segmentRowId == lastSegmentRowId && segmentRowId != PostingList.END_OF_STREAM);
        }
        lastSegmentRowId = segmentRowId;
        return segmentRowId != PostingList.END_OF_STREAM
               ? segmentRowId + searcherContext.getSegmentRowIdOffset()
               : PostingList.END_OF_STREAM;
    }
}
