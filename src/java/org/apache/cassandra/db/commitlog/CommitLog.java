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

import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.CRC32;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.AbstractCommitLogSegmentManager.SegmentManagerType;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CommitLogMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.db.commitlog.CommitLogSegment.Allocation;
import static org.apache.cassandra.db.commitlog.CommitLogSegment.CommitLogSegmentFileComparator;
import static org.apache.cassandra.db.commitlog.CommitLogSegment.ENTRY_OVERHEAD_SIZE;
import static org.apache.cassandra.utils.FBUtilities.updateChecksum;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/*
 * Commit Log tracks every write operation into the system. The aim of the commit log is to be able to
 * successfully recover data that was not stored to disk via the Memtable.
 */
public class CommitLog implements CommitLogMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLog.class);

    public static final CommitLog instance = CommitLog.construct();

    // we only permit records HALF the size of a commit log, to ensure we don't spin allocating many mostly
    // empty segments when writing large records
    final long MAX_MUTATION_SIZE = DatabaseDescriptor.getMaxMutationSize();

    private final AbstractCommitLogSegmentManager segmentManagerStandard;
    private final AbstractCommitLogSegmentManager segmentManagerCDC;
    private final List<AbstractCommitLogSegmentManager> segmentManagers = new ArrayList<>();

    public final CommitLogArchiver archiver;
    final CommitLogMetrics metrics;
    final AbstractCommitLogService executor;

    final ICompressor compressor;
    public ParameterizedClass compressorClass;
    public EncryptionContext encryptionContext;

    private static CommitLog construct()
    {
        CommitLog log = new CommitLog(CommitLogArchiver.construct());

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(log, new ObjectName("org.apache.cassandra.db:type=Commitlog"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return log.start();
    }

    @VisibleForTesting
    CommitLog(CommitLogArchiver archiver)
    {
        compressorClass = DatabaseDescriptor.getCommitLogCompression();
        ICompressor compressor = compressorClass != null ? CompressionParams.createCompressor(compressorClass) : null;
        DatabaseDescriptor.createAllDirectories();
        encryptionContext = DatabaseDescriptor.getEncryptionContext();

        this.compressor = compressor;
        this.archiver = archiver;
        metrics = new CommitLogMetrics();

        executor = DatabaseDescriptor.getCommitLogSync() == Config.CommitLogSync.batch
                ? new BatchCommitLogService(this)
                : new PeriodicCommitLogService(this);

        segmentManagerStandard = new CommitLogSegmentManagerStandard(this, DatabaseDescriptor.getCommitLogLocation());
        segmentManagerCDC = new CommitLogSegmentManagerCDC(this, DatabaseDescriptor.getCDCLogLocation());

        // Addition order matching SegmentManagerType enum for later ordinal() access
        segmentManagers.add(segmentManagerStandard);
        segmentManagers.add(segmentManagerCDC);

        // register metrics
        metrics.attach(executor, segmentManagerStandard, segmentManagerCDC);
    }

    CommitLog start()
    {
        executor.start();
        segmentManagers.forEach(AbstractCommitLogSegmentManager::start);
        return this;
    }

    /**
     * Perform recovery on commit log segments located on-disk
     *
     * @return the total number of mutatiosn replayed
     */
    public int recoverSegmentsOnDisk() throws IOException
    {
        int result = 0;
        for (AbstractCommitLogSegmentManager clsm : segmentManagers)
            result += recoverSegmentManager(clsm);
        return result;
    }

    /**
     * Perform recovery on commit logs located in the directory specified by the config file.
     *
     * @return the number of mutations replayed
     */
    private int recoverSegmentManager(AbstractCommitLogSegmentManager manager) throws IOException
    {
        // If createReserveSegments is already flipped, the CLSM is running and recovery has already taken place.
        if (manager.createReserveSegments)
            return 0;

        FilenameFilter unmanagedFilesFilter = new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                // we used to try to avoid instantiating commitlog (thus creating an empty segment ready for writes)
                // until after recover was finished.  this turns out to be fragile; it is less error-prone to go
                // ahead and allow writes before recover, and just skip active segments when we do.
                return CommitLogDescriptor.isValid(name) && CommitLogSegment.shouldReplay(name);
            }
        };

        // submit all files for this segment manager for archiving prior to recovery - CASSANDRA-6904
        for (File file : new File(manager.storageDirectory).listFiles(unmanagedFilesFilter))
        {
            archiver.maybeArchive(file.getPath(), file.getName());
            archiver.maybeWaitForArchiving(file.getName());
        }

        assert archiver.archivePending.isEmpty() : "Not all commit log archive tasks were completed before restore";
        archiver.maybeRestoreArchive();

        File[] files = new File(manager.storageDirectory).listFiles(unmanagedFilesFilter);
        int replayed = 0;
        if (files.length == 0)
        {
            logger.info("No commitlog files found; skipping replay");
        }
        else
        {
            Arrays.sort(files, new CommitLogSegmentFileComparator());
            logger.info("Replaying {}", StringUtils.join(files, ", "));
            replayed = recoverFiles(files);
            logger.info("Log replay complete, {} replayed mutations", replayed);

            for (File f : files)
                manager.deleteUntrackedCommitLogSegment(f);
        }

        manager.enableReserveSegmentCreation();
        return replayed;
    }

    /**
     * Perform recovery on a list of commit log files.
     *
     * @param clogs   the list of commit log files to replay
     * @return the number of mutations replayed
     */
    public int recoverFiles(File... clogs) throws IOException
    {
        CommitLogReplayer replayer = CommitLogReplayer.construct(this);
        replayer.replayFiles(clogs);
        return replayer.blockForWrites();
    }

    public void recoverPath(String path) throws IOException
    {
        CommitLogReplayer replayer = CommitLogReplayer.construct(this);
        replayer.replayPath(new File(path), false);
        replayer.blockForWrites();
    }

    /**
     * Perform recovery on a single commit log. Kept w/sub-optimal name due to coupling w/MBean / JMX
     */
    public void recover(String path) throws IOException
    {
        recoverPath(path);
    }

    /**
     * @return a CommitLogSegmentPosition which, if {@code >= one} returned from add(), implies add() was started
     * (but not necessarily finished) prior to this call
     */
    public CommitLogSegmentPosition getCurrentSegmentPosition(SegmentManagerType type)
    {
        return segmentManagers.get(type.ordinal()).getCurrentSegmentPosition();
    }

    /**
     * Return CommitLogSegmentPosition for Keyspace
     */
    public CommitLogSegmentPosition getCurrentSegmentPosition(Keyspace ks)
    {
        return getCurrentSegmentPosition(AbstractCommitLogSegmentManager.getSegmentManagerType(ks));
    }

    public CommitLogSegmentPosition getCurrentSegmentPosition(String ks)
    {
        return getCurrentSegmentPosition(AbstractCommitLogSegmentManager.getSegmentManagerType(ks));
    }

    /**
     * Flushes all dirty CFs, waiting for them to free and recycle any segments they were retaining
     */
    public void forceRecycleAllSegments(Iterable<UUID> droppedCfs)
    {
        segmentManagers.forEach(x -> x.forceRecycleAll(droppedCfs));
    }

    /**
     * Flushes all dirty CFs, waiting for them to free and recycle any segments they were retaining
     */
    public void forceRecycleAllSegments()
    {
        segmentManagers.forEach(x -> x.forceRecycleAll(Collections.<UUID>emptyList()));
    }

    /**
     * Forces a disk flush on the commit log files that need it.  Blocking.
     */
    public void sync(boolean syncAllSegments) throws IOException
    {
        for (AbstractCommitLogSegmentManager mgr : segmentManagers)
            mgr.sync(syncAllSegments);
    }

    /**
     * Preempts the CLExecutor, telling to to sync immediately
     */
    public void requestExtraSync()
    {
        executor.requestExtraSync();
    }

    /**
     * Add a Mutation to the commit log. Depending on which type of CommitLogSegmentManager is servicing this request,
     * it's possible for this to fail via a thrown WriteTimeoutException.
     *
     * @param mutation the Mutation to add to the log
     */
    public CommitLogSegmentPosition add(Keyspace keyspace, Mutation mutation) throws WriteTimeoutException
    {
        assert mutation != null;

        int size = (int) Mutation.serializer.serializedSize(mutation, MessagingService.current_version);

        int totalSize = size + ENTRY_OVERHEAD_SIZE;
        if (totalSize > MAX_MUTATION_SIZE)
        {
            throw new IllegalArgumentException(String.format("Mutation of %s is too large for the maximum size of %s",
                                                             FBUtilities.prettyPrintMemory(totalSize),
                                                             FBUtilities.prettyPrintMemory(MAX_MUTATION_SIZE)));
        }

        AbstractCommitLogSegmentManager segmentManager = getSegmentManager(AbstractCommitLogSegmentManager.getSegmentManagerType(keyspace));
        Allocation alloc = segmentManager.allocate(mutation, totalSize);

        // CDC allocations can fail if we're at our allowable on-disk threshold
        if (alloc == null)
        {
            assert AbstractCommitLogSegmentManager.getSegmentManagerType(keyspace) == SegmentManagerType.CDC;
            throw new WriteTimeoutException(WriteType.CDC, ConsistencyLevel.LOCAL_ONE, 0, 1);
        }

        CRC32 checksum = new CRC32();
        final ByteBuffer buffer = alloc.getBuffer();
        try (BufferedDataOutputStreamPlus dos = new DataOutputBufferFixed(buffer))
        {
            // checksummed length
            dos.writeInt(size);
            updateChecksumInt(checksum, size);
            buffer.putInt((int) checksum.getValue());

            // checksummed mutation
            Mutation.serializer.serialize(mutation, dos, MessagingService.current_version);
            updateChecksum(checksum, buffer, buffer.position() - size, size);
            buffer.putInt((int) checksum.getValue());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, alloc.getSegment().getPath());
        }
        finally
        {
            alloc.markWritten();
        }

        executor.finishWriteFor(alloc);
        return alloc.getCommitLogSegmentPosition();
    }

    /**
     * Modifies the per-CF dirty cursors of any commit log segments for the column family according to the position
     * given. Discards any commit log segments that are no longer used.
     *
     * @param cfId    the column family ID that was flushed
     * @param context the commit log segment position of the flush
     */
    public void discardCompletedSegments(final UUID cfId, final CommitLogSegmentPosition context, SegmentManagerType type)
    {
        logger.trace("discard completed log segments for {}, table {}", context, cfId);
        AbstractCommitLogSegmentManager targetSegmentManager = getSegmentManager(type);

        // Go thru the active segment files, which are ordered oldest to newest, marking the
        // flushed CF as clean, until we reach the segment file containing the CommitLogSegmentPosition passed
        // in the arguments. Any segments that become unused after they are marked clean will be
        // recycled or discarded.
        for (Iterator<CommitLogSegment> iter = targetSegmentManager.getActiveSegments().iterator(); iter.hasNext();)
        {
            CommitLogSegment segment = iter.next();
            segment.markClean(cfId, context);

            if (segment.isUnused())
            {
                logger.trace("Commit log segment {} is unused", segment);
                targetSegmentManager.recycleSegment(segment);
            }
            else
            {
                logger.trace("Not safe to delete{} commit log segment {}; dirty is {}",
                        (iter.hasNext() ? "" : " active"), segment, segment.dirtyString());
            }

            // Don't mark or try to delete any newer segments once we've reached the one containing the
            // position of the flush.
            if (segment.contains(context))
                break;
        }
    }

    @Override
    public String getArchiveCommand()
    {
        return archiver.archiveCommand;
    }

    @Override
    public String getRestoreCommand()
    {
        return archiver.restoreCommand;
    }

    @Override
    public String getRestoreDirectories()
    {
        return archiver.restoreDirectories;
    }

    @Override
    public long getRestorePointInTime()
    {
        return archiver.restorePointInTime;
    }

    @Override
    public String getRestorePrecision()
    {
        return archiver.precision.toString();
    }

    public List<String> getActiveSegmentNames()
    {
        List<String> segmentNames = new ArrayList<>();
        segmentManagers.forEach(mgr -> mgr.getActiveSegments().forEach(seg -> segmentNames.add(seg.getName())));
        return segmentNames;
    }

    public List<String> getArchivingSegmentNames()
    {
        return new ArrayList<>(archiver.archivePending.keySet());
    }

    @Override
    public long getActiveContentSize()
    {
        int size = 0;
        for (AbstractCommitLogSegmentManager clsm : segmentManagers)
            for (CommitLogSegment seg : clsm.getActiveSegments())
                size += seg.contentSize();
        return size;
    }

    @Override
    public long getActiveOnDiskSize()
    {
        int size = 0;
        for (AbstractCommitLogSegmentManager clsm : segmentManagers)
            size += clsm.onDiskSize();
        return size;
    }

    @Override
    public Map<String, Double> getActiveSegmentCompressionRatios()
    {
        Map<String, Double> segmentRatios = new TreeMap<>();
        segmentManagers.forEach(clsm -> clsm.getActiveSegments().forEach(
            seg -> segmentRatios.put(seg.getName(), 1.0 * seg.onDiskSize() / seg.contentSize())));
        return segmentRatios;
    }

    /**
     * Shuts down the threads used by the commit log, blocking until completion.
     */
    public void shutdownBlocking() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination();

        for (AbstractCommitLogSegmentManager clsm : segmentManagers)
        {
            clsm.shutdown();
            clsm.awaitTermination();
        }
    }

    /**
     * FOR TESTING PURPOSES
     * @return the number of files recovered
     */
    public int resetUnsafe(boolean deleteSegments) throws IOException
    {
        stopUnsafe(deleteSegments);
        return restartUnsafe();
    }

    /**
     * FOR TESTING PURPOSES
     */
    public void stopUnsafe(boolean deleteSegments)
    {
        executor.shutdown();
        try
        {
            executor.awaitTermination();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        segmentManagers.forEach(x -> x.stopUnsafe(deleteSegments));
        CommitLogSegment.resetReplayLimit();
        if (deleteSegments)
            for (File f : new File(DatabaseDescriptor.getCDCOverflowLocation()).listFiles())
                FileUtils.delete(f);

    }

    /**
     * FOR TESTING PURPOSES
     */
    public int restartUnsafe() throws IOException
    {
        segmentManagers.forEach(AbstractCommitLogSegmentManager::start);
        executor.restartUnsafe();
        try
        {
            return recoverSegmentsOnDisk();
        }
        catch (FSWriteError e)
        {
            // Workaround for a class of races that keeps showing up on Windows tests.
            // stop/start/reset path on Windows with segment deletion is very touchy/brittle
            // and the timing keeps getting screwed up. Rather than chasing our tail further
            // or rewriting the CLSM, just report that we didn't recover anything back up
            // the chain. This will silence most intermittent test failures on Windows
            // and appropriately fail tests that expected segments to be recovered that
            // were not.
            return 0;
        }
    }

    @VisibleForTesting
    public static boolean handleCommitError(String message, Throwable t)
    {
        JVMStabilityInspector.inspectCommitLogThrowable(t);
        switch (DatabaseDescriptor.getCommitFailurePolicy())
        {
            // Needed here for unit tests to not fail on default assertion
            case die:
            case stop:
                StorageService.instance.stopTransports();
                //$FALL-THROUGH$
            case stop_commit:
                logger.error(String.format("%s. Commit disk failure policy is %s; terminating thread", message, DatabaseDescriptor.getCommitFailurePolicy()), t);
                return false;
            case ignore:
                logger.error(message, t);
                return true;
            default:
                throw new AssertionError(DatabaseDescriptor.getCommitFailurePolicy());
        }
    }

    @VisibleForTesting
    public AbstractCommitLogSegmentManager getSegmentManager(SegmentManagerType type)
    {
        return segmentManagers.get(type.ordinal());
    }
}
