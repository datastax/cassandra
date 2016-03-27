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
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.AbstractDirectorySizer;

public class CommitLogSegmentManagerCDC extends AbstractCommitLogSegmentManager
{
    static final Logger logger = LoggerFactory.getLogger(CommitLogSegmentManagerCDC.class);

    private AtomicLong cdc_overflow_size = new AtomicLong(0);
    private final CDCSizeSummer cdcSizeSummer;
    private final ExecutorService cdcSizeCalculationExecutor;

    // We don't want every failed allocation thread to trigger the management thread to repeatedly slam the executor
    // with dummy runnables that will just be discarded, so we guard w/an atomic bool that we flip if we're in the process
    // of recalculating and flip off when we're done. We need the CAS / ownership aspect to avoid checking a volatile and
    // possibly racing
    private AtomicBoolean reCalculating = new AtomicBoolean(false);

    public CommitLogSegmentManagerCDC(final CommitLog commitLog, String storageDirectory)
    {
        super(commitLog, storageDirectory);
        cdcSizeSummer = new CDCSizeSummer(new File(DatabaseDescriptor.getCDCOverflowLocation()));
        cdcSizeCalculationExecutor = new ThreadPoolExecutor(1, 1, 1000, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new ThreadPoolExecutor.DiscardPolicy());
    }

    /**
     * Update on-disk size used by cdc_overflow, and check if our total active + overflow is >= limit. If so, return false.
     *
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
    @Override
    public boolean canAllocateNewSegments()
    {
        // In order to prevent repeatedly hammering the executor with Runnables just to be dropped if we're at heavy
        // allocation rates and the management thread is repeatedly being awakened by failed mutation allocations, we
        // guard it with an atomic boolean.

        // That being said, if we're on a new segment allocation and not currently in progress on recalculating this,
        // we want to go ahead and kick off another. It's a pretty time-consuming process depending on # of files, env,
        // and CPU speed so it's quite possible for us to get behind here even during the run of a single recalc.
        if (reCalculating.compareAndSet(false, true))
        {
            asyncUpdateCDCOnDiskSize();
        }
        return !atCapacity();
    }

    private void asyncUpdateCDCOnDiskSize()
    {
        cdcSizeCalculationExecutor.submit(() -> updateCDCDirectorySize());
    }

    private void updateCDCDirectorySize()
    {
        try
        {
            Thread.sleep(DatabaseDescriptor.getCDCDiskCheckInterval());
        }
        catch (InterruptedException ie)
        {
            // Do nothing if we're interrupted. It's a shutdown.
            return;
        }

        try
        {
            cdcSizeSummer.calculateSize();
            cdc_overflow_size.set(cdcSizeSummer.getAllocatedSize());
        }
        catch (IOException ie)
        {
            CommitLog.instance.handleCommitError("IEXception trying to calculate size for CDC folder.", ie);
        }

        if (!reCalculating.compareAndSet(true, false))
            throw new AssertionError();
        wakeManager();
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

        // This will race with a canAllocateNewSegments call and thus over-increment our view of our size
        // on disk. The actual size on disk should be updated on the next manager wake call and rectified.
        cdc_overflow_size.addAndGet(segment.onDiskSize());
    }

    /**
     * Initiates the shutdown process for the management thread. Also stops the cdc on-disk size calculator executor.
     */
    @Override
    public void shutdown()
    {
        run = false;
        cdcSizeCalculationExecutor.shutdownNow();
        wakeManager();
    }

    private class CDCSizeSummer extends AbstractDirectorySizer
    {
        CDCSizeSummer(File path)
        {
            super(path);
        }

        public void calculateSize() throws IOException
        {
            rebuildFileList();
            Files.walkFileTree(path.toPath(), this);
        }

        public boolean isAcceptable(Path file)
        {
            // Add up everything in there. Don't care what it is - if it's in this folder, we consider it fair game.
            return true;
        }
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
        CommitLogSegment segment = allocatingFrom();

        CommitLogSegment.Allocation alloc;
        while ( null == (alloc = segment.allocate(mutation, size)) )
        {
            if (atCapacity())
            {
                // Try and get a recount if anything's been deleted and we haven't caught it
                asyncUpdateCDCOnDiskSize();
                return null;
            }

            // Failed to allocate, so move to a new segment with enough room if possible.
            advanceAllocatingFrom(segment);
            segment = allocatingFrom;
        }

        return alloc;
    }

    private boolean atCapacity()
    {
        return (cdc_overflow_size.get() + size.get()) >= (long)DatabaseDescriptor.getTotalCDCSpaceInMB() * 1024 * 1024;
    }

    /**
     * Only use for testing / validation that size summer is working. Not for production use.
     */
    @VisibleForTesting
    public long checkCDCOverflowSizeOnDisk()
    {
        updateCDCDirectorySize();
        return cdc_overflow_size.get();
    }
}

