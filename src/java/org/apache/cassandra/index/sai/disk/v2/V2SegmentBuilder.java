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
package org.apache.cassandra.index.sai.disk.v2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.RAMStringIndexer;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.BytesRefUtil;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexFileProvider;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexMeta;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexWriter;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRefBuilder;

import static org.apache.cassandra.index.sai.disk.v2.blockindex.BytesUtil.gatherBytes;

/**
 * Creates an on-heap index data structure to be flushed to an SSTable index.
 */
@NotThreadSafe
public class V2SegmentBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(V2SegmentBuilder.class);

    // Served as safe net in case memory limit is not triggered or when merger merges small segments..
    public static final long LAST_VALID_SEGMENT_ROW_ID = ((long)Integer.MAX_VALUE / 2) - 1L;
    private static long testLastValidSegmentRowId = -1;

    /** The number of column indexes being built globally. (Starts at one to avoid divide by zero.) */
    public static final AtomicLong ACTIVE_BUILDER_COUNT = new AtomicLong(1);

    /** Minimum flush size, dynamically updated as segment builds are started and completed/aborted. */
    private static volatile long minimumFlushBytes;

    private final IndexDescriptor indexDescriptor;
    private final IndexContext indexContext;
    private final AbstractType<?> termComparator;

    private final NamedMemoryLimiter limiter;
    long totalBytesAllocated;

    private boolean flushed = false;
    private boolean active = true;

    // segment metadata
    long segmentRowIdOffset = 0;
    int rowCount = 0;

    final RAMStringIndexer ramIndexer;
    final BytesRefBuilder stringBuffer = new BytesRefBuilder();

    public V2SegmentBuilder(IndexDescriptor indexDescriptor, IndexContext indexContext, NamedMemoryLimiter limiter)
    {
        this.indexDescriptor = indexDescriptor;
        this.indexContext = indexContext;
        this.termComparator = indexContext.getValidator();
        this.limiter = limiter;

        minimumFlushBytes = limiter.limitBytes() / ACTIVE_BUILDER_COUNT.getAndIncrement();

        logger.debug("Creating segment builder with minimum flush bytes = {}. Active builder count = {}", minimumFlushBytes, ACTIVE_BUILDER_COUNT.get() - 1);

        this.ramIndexer = new RAMStringIndexer(termComparator);
    }

    public long add(ByteBuffer term, long sstableRowId)
    {

        final ByteSource byteSource = TypeUtil.instance.asComparableBytes(term.duplicate(), termComparator, ByteComparable.Version.OSS41, false);
        stringBuffer.clear();
        gatherBytes(byteSource, stringBuffer);
        if (rowCount == 0)
        {
            // use first global rowId in the segment as segment rowId offset
            segmentRowIdOffset = sstableRowId;
        }
        int segmentRowId = RowMapping.castToSegmentRowId(sstableRowId, segmentRowIdOffset);

        rowCount++;

        long bytesAllocated = ramIndexer.add(stringBuffer.get(), segmentRowId);
        totalBytesAllocated += bytesAllocated;
        return bytesAllocated;
    }

    public boolean isEmpty()
    {
        return ramIndexer.rowCount == 0;
    }

    public BlockIndexMeta flush(IndexDescriptor indexDescriptor, IndexContext columnContext, boolean temporary) throws IOException
    {
        assert !flushed;
        flushed = true;

        if (rowCount == 0)
        {
            logger.warn(columnContext.logMessage("No rows to index during flush of SSTable {}."), indexDescriptor.descriptor);
            return null;
        }

        try (BlockIndexFileProvider fileProvider = new PerIndexFileProvider(indexDescriptor, columnContext))
        {
            BlockIndexWriter writer = new BlockIndexWriter(fileProvider, temporary);
            writer.addAll(ramIndexer.getTermsWithPostings(), temporary ? 0 : segmentRowIdOffset);
            BlockIndexMeta metadata =  writer.finish();
            if (!temporary)
            {
                try (final IndexOutput out = fileProvider.openMetadataOutput())
                {
                    metadata.write(out);
                }
            }
            return metadata;
        }
    }

    public boolean shouldFlush(long sstableRowId)
    {
        // If we've hit the minimum flush size and we've breached the global limit, flush a new segment:
        boolean reachMemoryLimit = limiter.usageExceedsLimit() && hasReachedMinimumFlushSize();

        if (reachMemoryLimit)
        {
            logger.debug(indexContext.logMessage("Global limit of {} and minimum flush size of {} exceeded. " +
                                                 "Current builder usage is {} for {} cells. Global Usage is {}. Flushing..."),
                         FBUtilities.prettyPrintMemory(limiter.limitBytes()),
                         FBUtilities.prettyPrintMemory(minimumFlushBytes),
                         FBUtilities.prettyPrintMemory(totalBytesAllocated),
                         rowCount,
                         FBUtilities.prettyPrintMemory(limiter.currentBytesUsed()));
        }

        return reachMemoryLimit || exceedsSegmentLimit(sstableRowId);
    }

    /**
     * This method does three things:
     *
     * 1.) It decrements active builder count and updates the global minimum flush size to reflect that.
     * 2.) It releases the builder's memory against its limiter.
     * 3.) It defensively marks the builder inactive to make sure nothing bad happens if we try to close it twice.
     *
     * @return the number of bytes currently used by the memory limiter
     */
    public void release(String messageHeader)
    {
        if (active)
        {
            minimumFlushBytes = limiter.limitBytes() / ACTIVE_BUILDER_COUNT.decrementAndGet();
            long usageAfterRelease = limiter.decrement(totalBytesAllocated);
            active = false;
            logger.debug(indexContext.logMessage("{} for SSTable {} released {}. Global segment memory usage now at {}."),
                         messageHeader,
                         indexDescriptor.descriptor,
                         FBUtilities.prettyPrintMemory(totalBytesAllocated),
                         FBUtilities.prettyPrintMemory(usageAfterRelease));
        }

        logger.warn(indexContext.logMessage("Attempted to release storage attached index segment builder memory after builder marked inactive."));
    }

    private boolean hasReachedMinimumFlushSize()
    {
        return totalBytesAllocated >= minimumFlushBytes;
    }

    /**
     * @return true if next SSTable row ID exceeds max segment row ID
     */
    private boolean exceedsSegmentLimit(long ssTableRowId)
    {
        if (rowCount == 0)
            return false;

        // To handle the case where there are many non-indexable rows. eg. rowId-1 and rowId-3B are indexable,
        // the rest are non-indexable. We should flush them as 2 separate segments, because rowId-3B is going
        // to cause error in on-disk index structure with 2B limitation.
        return ssTableRowId - segmentRowIdOffset > LAST_VALID_SEGMENT_ROW_ID;
    }
}
