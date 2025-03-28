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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.CDCWriteException;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.metrics.CommitLogMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.STARTUP;
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
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 10, TimeUnit.SECONDS);

    public static final CommitLog instance = CommitLog.construct();

    private volatile AbstractCommitLogSegmentManager segmentManager;

    private final BiPredicate<File, String> unmanagedFilesFilter = (dir, name) -> CommitLogDescriptor.isValid(name) && segmentManager.shouldReplay(name);

    public final CommitLogArchiver archiver;
    public final CommitLogMetrics metrics;
    final AbstractCommitLogService executor;
    private Set<String> segmentsWithInvalidOrFailedMutations;

    volatile Configuration configuration;
    private boolean started = false;

    @VisibleForTesting
    final MonotonicClock clock;

    private static CommitLog construct()
    {
        CommitLog log = new CommitLog(CommitLogArchiver.construct(), DatabaseDescriptor.getCommitLogSegmentMgrProvider());

        MBeanWrapper.instance.registerMBean(log, "org.apache.cassandra.db:type=Commitlog");
        return log;
    }

    @VisibleForTesting
    CommitLog(CommitLogArchiver archiver)
    {
        this(archiver, DatabaseDescriptor.getCommitLogSegmentMgrProvider());
    }

    @VisibleForTesting
    CommitLog(CommitLogArchiver archiver, Function<CommitLog, AbstractCommitLogSegmentManager> segmentManagerProvider)
    {
        this.configuration = new Configuration(DatabaseDescriptor.getCommitLogCompression(),
                                               DatabaseDescriptor.getEncryptionContext());
        DatabaseDescriptor.createAllDirectories();

        this.archiver = archiver;
        metrics = new CommitLogMetrics();

        this.clock = MonotonicClock.preciseTime;

        switch (DatabaseDescriptor.getCommitLogSync())
        {
            case periodic:
                executor = new PeriodicCommitLogService(this, clock);
                break;
            case batch:
                executor = new BatchCommitLogService(this, clock);
                break;
            case group:
                executor = new GroupCommitLogService(this, clock);
                break;
            default:
                throw new IllegalArgumentException("Unknown commitlog service type: " + DatabaseDescriptor.getCommitLogSync());
        }

        segmentManager = segmentManagerProvider.apply(this);

        // register metrics
        metrics.attach(executor, segmentManager);
    }

    /**
     * Tries to start the CommitLog if not already started.
     */
    synchronized public CommitLog start()
    {
        if (started)
            return this;

        try
        {
            segmentManager.start();
            executor.start();
            started = true;
        } catch (Throwable t)
        {
            started = false;
            throw t;
        }
        return this;
    }

    public boolean isStarted()
    {
        return started;
    }

    private File[] getUnmanagedFiles()
    {
        File[] files = segmentManager.storageDirectory.tryList(unmanagedFilesFilter);
        if (files == null)
            return new File[0];
        return files;
    }

    /**
     * Updates the commit log storage directory and re-initializes the segment manager accordingly.
     * <p/>
     * Used by CNDB.
     *
     * @param commitLogLocation storage directory to update to
     * @return this commit log with updated storage directory
     */
    public CommitLog forPath(File commitLogLocation)
    {
        segmentManager = new CommitLogSegmentManagerStandard(this, commitLogLocation);
        return this;
    }

    /**
     * Perform recovery on commit logs located in the directory specified by the config file.
     * The recovery is executed as a commit log read followed by a flush.
     *
     * @param flushReason the reason for flushing that fallows commit log reading, use
     *                    {@link org.apache.cassandra.db.ColumnFamilyStore.FlushReason#STARTUP} when recovering on a
     *                    node start. Use {@link org.apache.cassandra.db.ColumnFamilyStore.FlushReason#REMOTE_REPLAY}
     *                    when replying commit logs to a remote storage.
     * @return keyspaces and the corresponding number of partition updates
     * @throws IOException
     */
    public Map<Keyspace, Integer> recoverSegmentsOnDisk(ColumnFamilyStore.FlushReason flushReason) throws IOException
    {
        // submit all files for this segment manager for archiving prior to recovery - CASSANDRA-6904
        // The files may have already been archived by normal CommitLog operation. This may cause errors in this
        // archiving pass, which we should not treat as serious.
        for (File file : getUnmanagedFiles())
        {
            archiver.maybeArchive(file.path(), file.name());
            archiver.maybeWaitForArchiving(file.name());
        }

        assert archiver.archivePending.isEmpty() : "Not all commit log archive tasks were completed before restore";
        archiver.maybeRestoreArchive();

        // List the files again as archiver may have added segments.
        File[] files = getUnmanagedFiles();
        Map<Keyspace, Integer> replayedKeyspaces = Collections.emptyMap();
        if (files.length == 0)
        {
            logger.info("No commitlog files found; skipping replay");
        }
        else
        {
            Arrays.sort(files, new CommitLogSegmentFileComparator());
            logger.info("Replaying {}", StringUtils.join(files, ", "));
            replayedKeyspaces = recoverFiles(flushReason, files);
            logger.info("Log replay complete, {} replayed mutations", replayedKeyspaces.values().stream().reduce(Integer::sum).orElse(0));

            for (File f : files)
            {
                boolean hasInvalidOrFailedMutations = segmentsWithInvalidOrFailedMutations.contains(f.name());
                segmentManager.handleReplayedSegment(f, hasInvalidOrFailedMutations);
            }
        }

        return replayedKeyspaces;
    }

    /**
     * Perform recovery on a list of commit log files. The recovery is executed as a commit log read followed by a
     * flush.
     *
     * @param flushReason the reason for flushing that follows commit log reading
     * @param clogs   the list of commit log files to replay
     * @return keyspaces and the corresponding number of partition updates
     */
    @VisibleForTesting
    public Map<Keyspace, Integer> recoverFiles(ColumnFamilyStore.FlushReason flushReason, File... clogs) throws IOException
    {
        CommitLogReplayer replayer = CommitLogReplayer.construct(this, getLocalHostId());
        replayer.replayFiles(clogs);

        Map<Keyspace, Integer> res = replayer.blockForWrites(flushReason);
        segmentsWithInvalidOrFailedMutations = replayer.getSegmentWithInvalidOrFailedMutations();
        return res;
    }

    public void recoverPath(String path, boolean tolerateTruncation) throws IOException
    {
        CommitLogReplayer replayer = CommitLogReplayer.construct(this, getLocalHostId());
        replayer.replayPath(new File(PathUtils.getPath(path)), tolerateTruncation);
        replayer.blockForWrites(STARTUP);
    }

    private static UUID getLocalHostId()
    {
        return StorageService.instance.getLocalHostUUID();
    }

    /**
     * Perform recovery on a single commit log. Kept w/sub-optimal name due to coupling w/MBean / JMX
     */
    public void recover(String path) throws IOException
    {
        recoverPath(path, false);
    }

    /**
     * @return a CommitLogPosition which, if {@code >= one} returned from add(), implies add() was started
     * (but not necessarily finished) prior to this call
     */
    public CommitLogPosition getCurrentPosition()
    {
        return segmentManager.getCurrentPosition();
    }

    /**
     * Flushes all dirty CFs, waiting for them to free and recycle any segments they were retaining
     */
    public void forceRecycleAllSegments(Collection<TableId> droppedTables)
    {
        segmentManager.forceRecycleAll(droppedTables);
    }

    /**
     * Flushes all dirty CFs, waiting for them to free and recycle any segments they were retaining
     */
    public void forceRecycleAllSegments()
    {
        segmentManager.forceRecycleAll(Collections.emptyList());
    }

    /**
     * Forces a disk flush on the commit log files that need it.  Blocking.
     */
    public void sync(boolean flush) throws IOException
    {
        segmentManager.sync(flush);
    }

    /**
     * Preempts the CLExecutor, telling to to sync immediately
     */
    public void requestExtraSync()
    {
        executor.requestExtraSync();
    }

    /**
     * If there was an exception when sync-ing, and if the commit log failure policy is
     * {@link Config.CommitFailurePolicy#fail_writes} then mutations will be rejected until
     * the sync error is cleared, which happens after a successful sync.
     * @return
     */
    @VisibleForTesting
    public boolean shouldRejectMutations()
    {
        return executor.getSyncError() &&
                DatabaseDescriptor.getCommitFailurePolicy() == Config.CommitFailurePolicy.fail_writes;
    }

    /**
     * Add a Mutation to the commit log. If CDC is enabled, this can fail.
     *
     * @param mutation the Mutation to add to the log
     */
    public CommitLogPosition add(Mutation mutation) throws CDCWriteException
    {
        assert mutation != null;

        mutation.validateSize(MessagingService.current_version, ENTRY_OVERHEAD_SIZE);

        if (shouldRejectMutations())
        {
            String errorMsg = "Rejecting mutation due to a failure sync-ing commit log segments";
            noSpamLogger.error(errorMsg);
            throw new FSWriteError(new IllegalStateException(errorMsg), segmentManager.allocatingFrom().getPath());
        }

        try (DataOutputBuffer dob = DataOutputBuffer.scratchBuffer.get())
        {
            Mutation.serializer.serialize(mutation, dob, MessagingService.current_version);
            int size = dob.getLength();
            int totalSize = size + ENTRY_OVERHEAD_SIZE;
            Allocation alloc = segmentManager.allocate(mutation, totalSize);

            CRC32 checksum = new CRC32();
            final ByteBuffer buffer = alloc.getBuffer();
            try (BufferedDataOutputStreamPlus dos = new DataOutputBufferFixed(buffer))
            {
                // checksummed length
                dos.writeInt(size);
                updateChecksumInt(checksum, size);
                buffer.putInt((int) checksum.getValue());

                // checksummed mutation
                dos.write(dob.getData(), 0, size);
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
            return alloc.getCommitLogPosition();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, segmentManager.allocatingFrom().getPath());
        }
    }

    /**
     * Modifies the per-CF dirty cursors of any commit log segments for the column family according to the position
     * given. Discards any commit log segments that are no longer used.
     *
     * @param id         the table that was flushed
     * @param lowerBound the lowest covered replay position of the flush
     * @param lowerBound the highest covered replay position of the flush
     */
    public void discardCompletedSegments(final TableId id, final CommitLogPosition lowerBound, final CommitLogPosition upperBound)
    {
        logger.trace("discard completed log segments for {}-{}, table {}", lowerBound, upperBound, id);

        // Go thru the active segment files, which are ordered oldest to newest, marking the
        // flushed CF as clean, until we reach the segment file containing the CommitLogPosition passed
        // in the arguments. Any segments that become unused after they are marked clean will be
        // recycled or discarded.
        for (Iterator<CommitLogSegment> iter = segmentManager.getActiveSegments().iterator(); iter.hasNext();)
        {
            CommitLogSegment segment = iter.next();
            segment.markClean(id, lowerBound, upperBound);

            if (segment.isUnused())
            {
                logger.debug("Commit log segment {} is unused", segment);
                segmentManager.archiveAndDiscard(segment);
            }
            else
            {
                if (logger.isTraceEnabled())
                    logger.trace("Not safe to delete{} commit log segment {}; dirty is {}",
                            (iter.hasNext() ? "" : " active"), segment, segment.dirtyString());
            }

            // Don't mark or try to delete any newer segments once we've reached the one containing the
            // position of the flush.
            if (segment.contains(upperBound))
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
        Collection<CommitLogSegment> segments = segmentManager.getActiveSegments();
        List<String> segmentNames = new ArrayList<>(segments.size());
        for (CommitLogSegment seg : segments)
            segmentNames.add(seg.getName());
        return segmentNames;
    }

    public List<String> getArchivingSegmentNames()
    {
        return new ArrayList<>(archiver.archivePending.keySet());
    }

    @Override
    public long getActiveContentSize()
    {
        long size = 0;
        for (CommitLogSegment seg : segmentManager.getActiveSegments())
            size += seg.contentSize();
        return size;
    }

    @Override
    public long getActiveOnDiskSize()
    {
        return segmentManager.onDiskSize();
    }

    @Override
    public Map<String, Double> getActiveSegmentCompressionRatios()
    {
        Map<String, Double> segmentRatios = new TreeMap<>();
        for (CommitLogSegment seg : segmentManager.getActiveSegments())
            segmentRatios.put(seg.getName(), 1.0 * seg.onDiskSize() / seg.contentSize());
        return segmentRatios;
    }

    /**
     * Shuts down the threads used by the commit log, blocking until completion.
     * TODO this should accept a timeout, and throw TimeoutException
     */
    synchronized public void shutdownBlocking() throws InterruptedException
    {
        if (!started)
            return;

        started = false;
        executor.shutdown();
        executor.awaitTermination();
        segmentManager.shutdown();
        segmentManager.awaitTermination();
    }

    /**
     * FOR TESTING PURPOSES
     * @return keyspaces and the corresponding number of partition updates
     */
    @VisibleForTesting
    synchronized public Map<Keyspace, Integer> resetUnsafe(boolean deleteSegments) throws IOException
    {
        stopUnsafe(deleteSegments);
        resetConfiguration();
        return restartUnsafe();
    }

    /**
     * FOR TESTING PURPOSES.
     */
    @VisibleForTesting
    synchronized public void resetConfiguration()
    {
        configuration = new Configuration(DatabaseDescriptor.getCommitLogCompression(),
                                          DatabaseDescriptor.getEncryptionContext());
    }

    /**
     * FOR TESTING PURPOSES
     */
    @VisibleForTesting
    synchronized public void stopUnsafe(boolean deleteSegments)
    {
        if (!started)
            return;

        started = false;
        executor.shutdown();
        try
        {
            executor.awaitTermination();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        segmentManager.stopUnsafe(deleteSegments);
        segmentManager.resetReplayLimit();
        if (DatabaseDescriptor.isCDCEnabled() && deleteSegments)
            for (File f : DatabaseDescriptor.getCDCLogLocation().tryList())
                FileUtils.deleteWithConfirm(f);
    }

    /**
     * FOR TESTING PURPOSES
     */
    @VisibleForTesting
    synchronized public Map<Keyspace, Integer> restartUnsafe() throws IOException
    {
        started = false;
        return start().recoverSegmentsOnDisk(ColumnFamilyStore.FlushReason.STARTUP);
    }

    public static long freeDiskSpace()
    {
        return PathUtils.tryGetSpace(DatabaseDescriptor.getCommitLogLocation().toPath(), FileStore::getUsableSpace) - DatabaseDescriptor.getMinFreeSpacePerDriveInBytes();
    }

    @VisibleForTesting
    public static boolean handleCommitError(String message, Throwable t)
    {
        JVMStabilityInspector.inspectCommitLogThrowable(message, t);
        switch (DatabaseDescriptor.getCommitFailurePolicy())
        {
            // Needed here for unit tests to not fail on default assertion
            case die:
            case stop:
                StorageService.instance.stopTransports();
                //$FALL-THROUGH$
            case stop_commit:
                String errorMsg = String.format("%s. Commit disk failure policy is %s; terminating thread.", message, DatabaseDescriptor.getCommitFailurePolicy());
                logger.error(addAdditionalInformationIfPossible(errorMsg), t);
                return false;
            case fail_writes:
            case ignore:
                logger.error(addAdditionalInformationIfPossible(message), t);
                return true;
            default:
                throw new AssertionError(DatabaseDescriptor.getCommitFailurePolicy());
        }
    }

    /**
     * Add additional information to the error message if the commit directory does not have enough free space.
     *
     * @param msg the original error message
     * @return the message with additional information if possible
     */
    private static String addAdditionalInformationIfPossible(String msg)
    {
        long unallocatedSpace = freeDiskSpace();
        int segmentSize = DatabaseDescriptor.getCommitLogSegmentSize();

        if (unallocatedSpace < segmentSize)
        {
            return String.format("%s. %d bytes required for next commitlog segment but only %d bytes available. Check %s to see if not enough free space is the reason for this error.",
                                 msg, segmentSize, unallocatedSpace, DatabaseDescriptor.getCommitLogLocation());
        }
        return msg;
    }

    public AbstractCommitLogSegmentManager getSegmentManager()
    {
        return segmentManager;
    }

    public static final class Configuration
    {
        /**
         * The compressor class.
         */
        private final ParameterizedClass compressorClass;

        /**
         * The compressor used to compress the segments.
         */
        private final ICompressor compressor;

        /**
         * The encryption context used to encrypt the segments.
         */
        private EncryptionContext encryptionContext;

        public Configuration(ParameterizedClass compressorClass, EncryptionContext encryptionContext)
        {
            this.compressorClass = compressorClass;
            this.compressor = compressorClass != null ? CompressionParams.createCompressor(compressorClass) : null;
            this.encryptionContext = encryptionContext;
        }

        /**
         * Checks if the segments must be compressed.
         * @return <code>true</code> if the segments must be compressed, <code>false</code> otherwise.
         */
        public boolean useCompression()
        {
            return compressor != null;
        }

        /**
         * Checks if the segments must be encrypted.
         * @return <code>true</code> if the segments must be encrypted, <code>false</code> otherwise.
         */
        public boolean useEncryption()
        {
            return encryptionContext.isEnabled();
        }

        /**
         * Returns the compressor used to compress the segments.
         * @return the compressor used to compress the segments
         */
        public ICompressor getCompressor()
        {
            return compressor;
        }

        /**
         * Returns the compressor class.
         * @return the compressor class
         */
        public ParameterizedClass getCompressorClass()
        {
            return compressorClass;
        }

        /**
         * Returns the compressor name.
         * @return the compressor name.
         */
        public String getCompressorName()
        {
            return useCompression() ? compressor.getClass().getSimpleName() : "none";
        }

        /**
         * Returns the encryption context used to encrypt the segments.
         * @return the encryption context used to encrypt the segments
         */
        public EncryptionContext getEncryptionContext()
        {
            return encryptionContext;
        }
    }
}
