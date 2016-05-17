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
import java.util.concurrent.atomic.AtomicLong;

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
        cdcSizeCalculator = new CDCSizeCalculator(new File(DatabaseDescriptor.getCDCLogLocation()));
    }

    @Override
    void start()
    {
        super.start();
        cdcSizeCalculator.start();
    }

    @Override
    public void discard(CommitLogSegment segment, boolean delete)
    {
        segment.close();
        addSize(-segment.onDiskSize());
        if (segment.containsCDCMutations.get())
        {
            // Remove the worst-case full segment size we track in allocate()
            cdcSizeCalculator.removeUnflushedSize(DatabaseDescriptor.getCommitLogSegmentSize());
            FileUtils.renameWithConfirm(segment.logFile.getAbsolutePath(), DatabaseDescriptor.getCDCLogLocation() + File.separator + segment.logFile.getName());

            cdcSizeCalculator.addFlushedSize(segment.onDiskSize());
            cdcSizeCalculator.submitOverflowSizeRecalculation();
        }
        else
        {
            if (delete)
                FileUtils.deleteWithConfirm(segment.logFile);
        }
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
     * create a new segment. For CDC mutations, allocation is expected to throw WTE if we're at CDC capacity.
     *
     * We allow advancement of non-CDC enabled segments as all CDC mutations should be rejected until space is cleared
     * in cdc_raw. Subsequent non-CDC mutations won't flag cdc on the CommitLogSegment and thus the segment will be deleted
     * on discard() rather than further pushing the total CDC on disk footprint.
     *
     * @return the provided Allocation object
     * @throws WriteTimeoutException If CDC directory is at configured limit, we throw an exception rather than allocate.
     */
    @Override
    public CommitLogSegment.Allocation allocate(Mutation mutation, int size) throws WriteTimeoutException
    {
        CommitLogSegment segment = allocatingFrom();
        CommitLogSegment.Allocation alloc;

        // If we're at our capacity of unflushed + flushed cdc data but the current segment a) contains cdc data and b)
        // still has room left to allow this allocation, we allow it.
        if (mutation.trackedByCDC() && atCapacity())
        {
            if (!segment.containsCDCMutations.get() || (null == (alloc = segment.allocate(mutation, size))))
            {
                cdcSizeCalculator.submitOverflowSizeRecalculation();
                throw new WriteTimeoutException(WriteType.CDC, ConsistencyLevel.LOCAL_ONE, 0, 1);
            }
            return alloc;
        }
        else
        {
            while ( null == (alloc = segment.allocate(mutation, size)) )
            {
                if (mutation.trackedByCDC() && atCapacity())
                {
                    cdcSizeCalculator.submitOverflowSizeRecalculation();
                    throw new WriteTimeoutException(WriteType.CDC, ConsistencyLevel.LOCAL_ONE, 0, 1);
                }

                // Failed to allocate, so move to a new segment with enough room if possible.
                advanceAllocatingFrom(segment);
                segment = allocatingFrom;
            }
        }

        // Operate with a worst-case estimate of full DBD.getCommitLogSegmentSize as being our final size of CDC CLS
        if (mutation.trackedByCDC() && segment.containsCDCMutations.compareAndSet(false, true))
            cdcSizeCalculator.addUnflushedSize(DatabaseDescriptor.getCommitLogSegmentSize());

        return alloc;
    }

    /**
     * Move files to cdc_raw after replay, since recovery will flush to SSTable and these mutations won't be available
     * in the CL subsystem otherwise.
     */
    void handleReplayedSegment(final File file)
    {
        logger.trace("Moving (Unopened) segment {} to cdc_raw directory after replay", file);
        FileUtils.renameWithConfirm(file.getAbsolutePath(), DatabaseDescriptor.getCDCLogLocation() + File.separator + file.getName());
        cdcSizeCalculator.addFlushedSize(file.length());
    }

    /*
     * We operate with a somewhat "lazy" tracked view of how much space is in the cdc_raw. While we deterministically
     * increment the size of the cdc_raw directory on each segment discard, we cannot afford to synchronously process the full
     * directory size to decrement based on file consumption. Similarly, adding piping for a consumer to notify the C*
     * process is a large complexity burden and would introduce a longer delay between deletion and C*'s view of the dir
     * size rather than just recalculating it ourselves.
     *
     * Synchronous size recalculation on each allocation call could lead to very long delays in new segment allocation,
     * thus long delays in thread signaling to wake waiting allocation / writer threads. Similarly, kicking off an async
     * recalc of the directory on each allocation would lead to unnecessary garbage generation in the form of canceled futures.
     *
     * While we can race and have our discard logic incorrectly add a discarded segment's size to this value, it will
     * be corrected on the next pass at allocation when a new re-calculation task is submitted. Essentially, if you're
     * flirting with the edge of your possible allocation space for CDC, it's likely that some mutations will be
     * rejected if we happen to race right at this boundary until next recalc succeeds. With the default 4G CDC space
     * and 32MB segment, most trivial local benchmarks place that at < 500 usec.
     *
     * Reference DirectorySizerBench for more information about performance of the directory size recalc.
     */
    @VisibleForTesting
    public boolean atCapacity()
    {
        return cdcSizeCalculator.getTotalCDCSizeOnDisk() >= (long)DatabaseDescriptor.getCommitLogSpaceInMBCDC() * 1024 * 1024;
    }

    /**
     * Tracks total disk usage of CDC subsystem, defined by the summation of all unflushed CommitLogSegments with CDC
     * data in them and all segments archived into cdc_raw.
     *
     * Allows atomic increment/decrement of unflushed size, however only allows increment on flushed and requires a full
     * directory walk to determine actual on-disk size of cdc_raw as we have an external consumer deleting those files.
     *
     * TODO: linux performs approximately 25% better with the following one-liner instead of this walker:
     *      Arrays.stream(path.listFiles()).mapToLong(File::length).sum();
     * However this solution is 375% slower on Windows. Revisit this and split logic to per-OS
     */
    private class CDCSizeCalculator extends DirectorySizeCalculator
    {
        private AtomicLong unflushedCDCSizeOnDisk = new AtomicLong(0);
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
            unflushedCDCSizeOnDisk.set(0);
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

        public long addFlushedSize(long toAdd)
        {
            return size.addAndGet(toAdd);
        }

        public void addUnflushedSize(long value)
        {
            unflushedCDCSizeOnDisk.addAndGet(value);
        }

        public void removeUnflushedSize(long value)
        {
            unflushedCDCSizeOnDisk.addAndGet(-value);
        }

        public long getTotalCDCSizeOnDisk()
        {
            return size.get() + unflushedCDCSizeOnDisk.get();
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
            Thread.sleep(DatabaseDescriptor.getCDCDiskCheckInterval() + 10);
        }
        catch (InterruptedException e) {}

        return cdcSizeCalculator.getAllocatedSize();
    }
}
