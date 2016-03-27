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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.DirectorySizeCalculator;
import org.apache.cassandra.utils.JVMStabilityInspector;

public class CommitLogSegmentManagerCDC extends AbstractCommitLogSegmentManager
{
    static final Logger logger = LoggerFactory.getLogger(CommitLogSegmentManagerCDC.class);

    private AtomicLong cdcOverflowSize = new AtomicLong(0);
    private final CDCSizeCalculator cdcSizeCalculator;
    private ExecutorService cdcSizeCalculationExecutor;

    // We don't want every failed allocation to create a runnable for the queue, so we so we guard w/an atomic bool
    private AtomicBoolean reCalculating = new AtomicBoolean(false);

    public CommitLogSegmentManagerCDC(final CommitLog commitLog, String storageDirectory)
    {
        super(commitLog, storageDirectory);
        cdcSizeCalculator = new CDCSizeCalculator(new File(DatabaseDescriptor.getCDCOverflowLocation()));
    }

    @Override
    void start()
    {
        cdcSizeCalculationExecutor = Executors.newSingleThreadExecutor();
        super.start();
    }

    private void maybeUpdateCDCSizeCounterAsync()
    {
        if (reCalculating.compareAndSet(false, true))
        {
            try
            {
                cdcSizeCalculationExecutor.submit(() -> updateCDCDirectorySize());
            }
            catch (Exception e)
            {
                reCalculating.compareAndSet(true, false);
                if (!(e instanceof RejectedExecutionException))
                    JVMStabilityInspector.inspectCommitLogThrowable(e);
            }
        }
    }

    private void updateCDCDirectorySize()
    {
        try
        {
            // Rather than burning CPU by allowing spinning on this recalc if we have long-running or intermittent CDC
            // consumption, we sleep the size calculation thread based on user configuration.
            Thread.sleep(DatabaseDescriptor.getCDCDiskCheckInterval());
        }
        catch (InterruptedException ie)
        {
            // Do nothing if we're interrupted. It's a shutdown.
            return;
        }

        try
        {
            cdcSizeCalculator.calculateSize();
            cdcOverflowSize.set(cdcSizeCalculator.getAllocatedSize());
        }
        catch (IOException ie)
        {
            CommitLog.instance.handleCommitError("IEXception trying to calculate size for CDC folder.", ie);
        }

        wakeManager();
        if (!reCalculating.compareAndSet(true, false))
            throw new AssertionError();
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
        cdcOverflowSize.addAndGet(segment.onDiskSize());
    }

    /**
     * Initiates the shutdown process for the management thread. Also stops the cdc on-disk size calculator executor.
     */
    @Override
    public void shutdown()
    {
        run = false;
        cdcSizeCalculationExecutor.shutdown();
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
            maybeUpdateCDCSizeCounterAsync();
            return null;
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
        long overflowSize = cdcOverflowSize.get();
        logger.trace("Total cdc commitlog segment space used is {} out of {}. Active segments: {} Overflow: {}", currentSize + overflowSize, total, currentSize, overflowSize);
        return total - (currentSize + overflowSize);
    }

    /*
     * We operate with a somewhat "lazy" tracked view of how much space is in the cdc_overflow, as synchronous size
     * tracking on each allocation call is cpu-bound without an upper limit and could lead to very long delays in
     * new segment allocation, thus long delays in thread signaling to wake waiting allocation / writer threads.
     *
     * While we can race and have our discard logic incorrectly add a discarded segment's size to this value, it will
     * be corrected on the next pass at allocation when a new re-calculation task is submit. Essentially, if you're
     * flirting with the edge of your possible allocation space for CDC, it's possible that some mutations will be
     * rejected if we happen to race right at this boundary until next recalc succeeds. With the default 8G CDC space
     * and 32MB segment, that should be a sub-ms re-calc process on a respectable modern CPU. "Should".
     *
     * Reference DirectorySizerBench for more information about performance of this recalc.
     */
    @VisibleForTesting
    public boolean atCapacity()
    {
        return (cdcOverflowSize.get() + size.get()) >= (long)DatabaseDescriptor.getCommitLogSpaceInMBCDC() * 1024 * 1024;
    }

    private class CDCSizeCalculator extends DirectorySizeCalculator
    {
        CDCSizeCalculator(File path)
        {
            super(path);
        }

        public void calculateSize() throws IOException
        {
            rebuildFileList();
            Files.walkFileTree(path.toPath(), this);
        }
    }

    /**
     * Only use for testing / validation that size summer is working. Not for production use.
     */
    @VisibleForTesting
    public long updateCDCOverflowSize()
    {
        boolean set = false;
        while (!set) set = reCalculating.compareAndSet(false, true);
        updateCDCDirectorySize();
        return cdcOverflowSize.get();
    }

    public SegmentManagerType getSegmentManagerType()
    {
        return SegmentManagerType.CDC;
    }
}
