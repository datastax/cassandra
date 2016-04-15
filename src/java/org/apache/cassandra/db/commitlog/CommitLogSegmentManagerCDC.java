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

package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.DirectorySizeCalculator;

public class CommitLogSegmentManagerCDC extends AbstractCommitLogSegmentManager
{
    static final Logger logger = LoggerFactory.getLogger(CommitLogSegmentManagerCDC.class);
    private final CDCSizeCalculator cdcSizeCalculator;

    public CommitLogSegmentManagerCDC(final CommitLog commitLog, String storageDirectory)
    {
        super(commitLog, storageDirectory);
        cdcSizeCalculator = new CDCSizeCalculator(new File(DatabaseDescriptor.getCDCOverflowLocation()));
        cdcSizeCalculator.start();
    }

    @Override
    void start()
    {
        super.start();
        cdcSizeCalculator.start();
    }

    /**
     * We don't care about the "delete" parameter on CDC as we always keep the segments around until consumers
     * delete them.
     */
    @Override
    public void discard(CommitLogSegment segment, boolean delete)
    {
        segment.close();
        FileUtils.renameWithConfirm(segment.logFile.getAbsolutePath(), DatabaseDescriptor.getCDCOverflowLocation() + File.separator + segment.logFile.getName());
        addSize(-segment.onDiskSize());

        // This can race with an atCapacity check and thus over-increment our view of our size on disk. The actual size
        // on disk should be updated async on the next failed allocate() call and rectified, so while we can lose a sub-ms
        // window of mutations during that recalc due to a race w/overprovisioning of size, it's preferable compared to
        // a synchronous check of this on every allocation.
        cdcSizeCalculator.addAndGet(segment.onDiskSize());

        // Submit a recalculation of the actual on-disk size on the executor to get a more accurate picture and take
        // into account any changes prompted by user consumption.
        cdcSizeCalculator.submitOverflowSizeRecalculation();
    }

    /**
     * Initiates the shutdown process for the management thread. Also stops the cdc on-disk size calculator executor.
     */
    @Override
    public void shutdown()
    {
        run = false;
        cdcSizeCalculator.shutdown();
        wakeManager();
    }

    /**
     * Reserve space in the current segment for the provided mutation or, if there isn't space available,
     * create a new segment. For CDC segments, allocation can, and will, fail if we're at CDC capacity.
     *
     * @return the provided Allocation object, or null if we are at allowable allocated CDC capacity on disk
     */
    @Override
    public CommitLogSegment.Allocation allocate(Mutation mutation, int size)
    {
        /*
         * We can race and have multiple writers in allocate, check atCapacity, and show that they can successfully
         * continue at this point, leading to allocation of an extra segment(s) in advanceAllocatingFrom, thus violating
         * our bounds on allowable CDC space taken up on disk. This shouldn't lead to too much excess due to limitations
         * on max mutation size + thread writer pool amount, but we may want to revisit if this becomes an issue in the
         * future.
         *
         * The reason we allow this is because advanceAllocatingFrom and the management thread have an invariant that
         * all attempts at allocation must succeed.
        */
        if (atCapacity())
        {
            cdcSizeCalculator.submitOverflowSizeRecalculation();
            throw new WriteTimeoutException(WriteType.CDC, ConsistencyLevel.LOCAL_ONE, 0, 1);
        }

        CommitLogSegment segment = allocatingFrom();

        CommitLogSegment.Allocation alloc;
        while ( null == (alloc = segment.allocate(mutation, size)) )
        {
            // Failed to allocate, so move to a new segment with enough room if possible.
            advanceAllocatingFrom(segment);
            segment = allocatingFrom;
        }

        return alloc;
    }

    /**
     * Returns total size allowed by yaml for this segment type less the commitlog size of this manager
     */
    long unusedCapacity()
    {
        long total = DatabaseDescriptor.getCommitLogSpaceInMBCDC() * 1024 * 1024;
        long currentSize = size.get();
        long overflowSize = cdcSizeCalculator.getAllocatedSize();
        logger.trace("Total cdc commitlog segment space used is {} out of {}. Active segments: {} Overflow: {}", currentSize + overflowSize, total, currentSize, overflowSize);
        return total - (currentSize + overflowSize);
    }

    /**
     * Move files to cdc_overflow after replay, since recovery will flush to SSTable and these mutations won't be available
     * in the CL subsystem otherwise.
     */
    void handleReplayedSegment(final File file)
    {
        // (don't decrease managed size, since this was never a "live" segment)
        logger.trace("Moving (Unopened) segment {} to cdc overflow after replay", file);
        FileUtils.renameWithConfirm(file.getAbsolutePath(), DatabaseDescriptor.getCDCOverflowLocation() + File.separator + file.getName());
    }

    /*
     * We operate with a somewhat "lazy" tracked view of how much space is in the cdc_overflow, as synchronous size
     * tracking on each allocation call to try and catch when consumers delete files is cpu-bound and linear and could
     * lead to very long delays in new segment allocation, thus long delays in thread signaling to wake waiting
     * allocation / writer threads.
     *
     * While we can race and have our discard logic incorrectly add a discarded segment's size to this value, it will
     * be corrected on the next pass at allocation when a new re-calculation task is submitted. Essentially, if you're
     * flirting with the edge of your possible allocation space for CDC, it's possible that some mutations will be
     * rejected if we happen to race right at this boundary until next recalc succeeds. With the default 4G CDC space
     * and 32MB segment, that should be a sub-half-ms re-calc process on a respectable modern CPU. "Should".
     *
     * Reference DirectorySizerBench for more information about performance of this recalc.
     */
    @VisibleForTesting
    public boolean atCapacity()
    {
        return (cdcSizeCalculator.getAllocatedSize() + size.get()) >= (long)DatabaseDescriptor.getCommitLogSpaceInMBCDC() * 1024 * 1024;
    }

    private class CDCSizeCalculator extends DirectorySizeCalculator
    {
        private final RateLimiter rateLimiter = RateLimiter.create(4);
        private ExecutorService cdcSizeCalculationExecutor;

        CDCSizeCalculator(File path)
        {
            super(path);
        }

        /**
         * Needed for stop/restart during unit tests
         */
        public void start()
        {
            cdcSizeCalculationExecutor = Executors.newSingleThreadExecutor();
        }

        public void submitOverflowSizeRecalculation()
        {
            try
            {
                cdcSizeCalculationExecutor.submit(() -> {
                        rateLimiter.acquire();
                        calculateSize();
                });
            }
            catch (RejectedExecutionException e)
            {
                // Do nothing. Means we have one in flight so this req. should be satisfied when it completes.
            }
        }

        private void calculateSize()
        {
            try
            {
                rebuildFileList();
                Files.walkFileTree(path.toPath(), this);
            }
            catch (IOException ie)
            {
                CommitLog.instance.handleCommitError("Failed CDC Size Calculation", ie);
            }
        }

        public long addAndGet(long toAdd)
        {
            return size.addAndGet(toAdd);
        }

        public void shutdown()
        {
            cdcSizeCalculationExecutor.shutdown();
        }
    }

    /**
     * Only use for testing / validation that size summer is working. Not for production use.
     */
    @VisibleForTesting
    public long updateCDCOverflowSize()
    {
        cdcSizeCalculator.submitOverflowSizeRecalculation();

        // Give the update time to run
        try
        {
            Thread.sleep(250);
        }
        catch (InterruptedException e) {}

        return cdcSizeCalculator.getAllocatedSize();
    }

    public SegmentManagerType getSegmentManagerType()
    {
        return SegmentManagerType.CDC;
    }
}
