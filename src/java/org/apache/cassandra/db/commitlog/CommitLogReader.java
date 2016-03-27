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

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.UnknownColumnFamilyException;
import org.apache.cassandra.db.commitlog.ICommitLogReadHandler.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.utils.JVMStabilityInspector;


import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

public class CommitLogReader
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogReader.class);

    private static final int LEGACY_END_OF_SEGMENT_MARKER = 0;
    private static final int ALL_MUTATIONS = -1;
    private final CRC32 checksum;
    private final Map<UUID, AtomicInteger> invalidMutations;

    private byte[] buffer;

    public CommitLogReader()
    {
        checksum = new CRC32();
        invalidMutations = new HashMap<>();
        buffer = new byte[4096];
    }

    public Set<Map.Entry<UUID, AtomicInteger>> getInvalidMutations()
    {
        return invalidMutations.entrySet();
    }

    public void readAllFiles(ICommitLogReadHandler handler, File[] files) throws IOException
    {
        for (int i = 0; i < files.length; i++)
            readCommitLogSegment(handler, files[i], i + 1 == files.length);
    }

    public void readCommitLogSegment(ICommitLogReadHandler handler, File file, int mutationLimit, boolean tolerateTruncation) throws IOException
    {
        readCommitLogSegment(handler, file, CommitLogSegmentPosition.NONE, mutationLimit, tolerateTruncation);
    }

    public void readCommitLogSegment(ICommitLogReadHandler handler, File file, boolean tolerateTruncation) throws IOException
    {
        readCommitLogSegment(handler, file, CommitLogSegmentPosition.NONE, ALL_MUTATIONS, tolerateTruncation);
    }

    /**
     * Reads mutations from file, handing them off to handler
     * @param handler -
     * @param file -
     * @param startPosition Optional CommitLogSegmentPosition to serve as starting point for mutation replay, CommitLogSegmentPosition.NONE if all
     * @param mutationLimit Optional limit on # of mutations to replay. Local ALL_MUTATIONS serves as marker to play all.
     * @param tolerateTruncation Whether or not we should allow truncation of this file or throw if EOF found
     * @throws IOException
     */
    public void readCommitLogSegment(ICommitLogReadHandler handler,
                                     File file,
                                     CommitLogSegmentPosition startPosition,
                                     int mutationLimit,
                                     boolean tolerateTruncation) throws IOException
    {
        // just transform from the file name (no reading of headers) to determine version
        CommitLogDescriptor desc = CommitLogDescriptor.fromFileName(file.getName());

        try(ChannelProxy channel = new ChannelProxy(file);
            RandomAccessReader reader = RandomAccessReader.open(channel))
        {
            if (desc.version < CommitLogDescriptor.VERSION_21)
            {
                if (!handler.logAndCheckIfShouldSkip(file, desc))
                {
                    handler.prepReader(desc, reader);
                    ReadStatusTracker statusTracker = new ReadStatusTracker(ALL_MUTATIONS, tolerateTruncation);
                    statusTracker.errorContext = desc.fileName();
                    readSection(handler, reader, 0, (int) reader.length(), statusTracker, desc);
                }
                return;
            }

            final long segmentId = desc.id;
            try
            {
                desc = CommitLogDescriptor.readHeader(reader, DatabaseDescriptor.getEncryptionContext());
            }
            catch (Exception e)
            {
                // don't care about whether or not the handler thinks we can continue. We can't w/out descriptor.
                handler.shouldStopOnError(new CommitLogReadException(
                    String.format("Could not read commit log descriptor in file %s", file),
                    CommitLogReadErrorReason.UNRECOVERABLE_DESCRIPTOR_ERROR,
                    tolerateTruncation));
                return;
            }

            if (segmentId != desc.id)
            {
                if (handler.shouldStopOnError(new CommitLogReadException(String.format(
                    "Segment id mismatch (filename %d, descriptor %d) in file %s", segmentId, desc.id, file),
                    CommitLogReadErrorReason.RECOVERABLE_DESCRIPTOR_ERROR,
                    tolerateTruncation)))
                {
                    return;
                }
            }

            if (startPosition != CommitLogSegmentPosition.NONE && desc.id != startPosition.segmentId)
            {
                if (handler.shouldStopOnError(new CommitLogReadException(
                    String.format("Descriptor parsed segmentId (%d) mismatch with CommitLogSegmentPosition id (%d) on file %s",
                                  desc.id,
                                  startPosition.segmentId,
                                  file),
                    CommitLogReadErrorReason.RECOVERABLE_DESCRIPTOR_ERROR,
                    tolerateTruncation)))
                {
                    return;
                }
                // continue processing if no exception, but set offset to zero so we don't skip anything
                startPosition = new CommitLogSegmentPosition(-1, 0);
            }

            if (handler.logAndCheckIfShouldSkip(file, desc))
                return;

            CommitLogSegmentReader segmentReader;
            try
            {
                segmentReader = new CommitLogSegmentReader(handler, desc, reader, tolerateTruncation);
            }
            catch(Exception e)
            {
                handler.shouldStopOnError(new CommitLogReadException(
                    String.format("unable to create segment reader for commit log file: %s", e),
                    CommitLogReadErrorReason.UNKNOWN_ERROR,
                    tolerateTruncation));
                return;
            }

            try
            {
                ReadStatusTracker statusTracker = new ReadStatusTracker(mutationLimit, tolerateTruncation);
                for (CommitLogSegmentReader.SyncSegment syncSegment : segmentReader)
                {
                    statusTracker.tolerateErrorsInSection &= syncSegment.toleratesErrorsInSection;

                    if (handler.shouldSkipSegment(desc.id, syncSegment.endPosition))
                        continue;

                    statusTracker.errorContext = String.format("next section at %d in %s", syncSegment.fileStartPosition, desc.fileName());
                    readSection(handler, syncSegment.input, startPosition.position, syncSegment.endPosition, statusTracker, desc);
                    if (!statusTracker.shouldContinue())
                        break;
                }
            }
            // unfortunately, AbstractIterator cannot throw a checked exception,
            // so check to see if a RuntimeException is wrapping an IOException
            catch (RuntimeException re)
            {
                if (re.getCause() instanceof IOException)
                    throw (IOException) re.getCause();
                throw re;
            }
            logger.debug("Finished reading {}", file);
        }
    }

    /**
     * Reads a section of a file containing mutations
     *
     * @param handler       -
     * @param reader        FileDataInput / logical buffer containing commitlog mutations
     * @param startOffset   Offset within the file to start handing mutations off to handler
     * @param end           logical numeric end of the segment being read
     * @param statusTracker ReadStatusTracker with current state of mutation count, error state, etc
     * @param desc          Descriptor for CommitLog serialization
     */
    private void readSection(ICommitLogReadHandler handler,
                             FileDataInput reader,
                             int startOffset,
                             int end,
                             ReadStatusTracker statusTracker,
                             CommitLogDescriptor desc) throws IOException
    {
        while (statusTracker.shouldContinue() && reader.getFilePointer() < end && !reader.isEOF())
        {
            long mutationStart = reader.getFilePointer();
            if (logger.isTraceEnabled())
                logger.trace("Reading mutation at {}", mutationStart);

            long claimedCRC32 = 0;
            int serializedSize = 0;
            try
            {
                // any of the reads may hit EOF
                serializedSize = reader.readInt();
                if (serializedSize == LEGACY_END_OF_SEGMENT_MARKER)
                {
                    logger.trace("Encountered end of segment marker at {}", reader.getFilePointer());
                    statusTracker.flagError();
                    continue;
                }

                // Skip mutations that are before the requested offset
                if (mutationStart < startOffset)
                {
                    reader.seek(reader.getFilePointer() +
                                serializedSize +
                                CommitLogFormat.claimedChecksumSerializedSize(desc.version) +
                                CommitLogFormat.claimedCRC32SerializedSize(desc.version));
                    logger.debug("Skipping mutation before offset. Mutation start: {} Skipped to: {} startOffset: {}",
                                 mutationStart,
                                 reader.getFilePointer(),
                                 startOffset);
                    continue;
                }

                // Mutation must be at LEAST 10 bytes:
                // 3 each for a non-empty Keyspace and Key (including the
                // 2-byte length from writeUTF/writeWithShortLength) and 4 bytes for column count.
                // This prevents CRC by being fooled by special-case garbage in the file; see CASSANDRA-2128
                if (serializedSize < 10)
                {
                    if (handler.shouldStopOnError(new CommitLogReadException(
                                                    String.format("Invalid mutation size %d at %d in %s", serializedSize, mutationStart, statusTracker.errorContext),
                                                    CommitLogReadErrorReason.MUTATION_ERROR,
                                                    statusTracker.tolerateErrorsInSection)))
                    {
                        statusTracker.flagError();
                        continue;
                    }
                }

                long claimedSizeChecksum = CommitLogFormat.calculateClaimedChecksum(reader, desc.version);
                checksum.reset();
                CommitLogFormat.updateChecksum(checksum, serializedSize, desc.version);

                if (checksum.getValue() != claimedSizeChecksum)
                {
                    if (handler.shouldStopOnError(new CommitLogReadException(
                                                    String.format("Mutation size checksum failure at %d in %s", mutationStart, statusTracker.errorContext),
                                                    CommitLogReadErrorReason.MUTATION_ERROR,
                                                    statusTracker.tolerateErrorsInSection)))
                    {
                        statusTracker.flagError();
                        continue;
                    }
                }

                if (serializedSize > buffer.length)
                    buffer = new byte[(int) (1.2 * serializedSize)];
                reader.readFully(buffer, 0, serializedSize);

                claimedCRC32 = CommitLogFormat.calculateClaimedCRC32(reader, desc.version);
            }
            catch (EOFException eof)
            {
                if (handler.shouldStopOnError(new CommitLogReadException(
                                                String.format("Unexpected end of segment", mutationStart, statusTracker.errorContext),
                                                CommitLogReadErrorReason.EOF,
                                                statusTracker.tolerateErrorsInSection)))
                {
                    statusTracker.flagError();
                    continue;
                }
            }

            checksum.update(buffer, 0, serializedSize);
            if (claimedCRC32 != checksum.getValue())
            {
                if (handler.shouldStopOnError(new CommitLogReadException(
                                                String.format("Mutation checksum failure at %d in %s", mutationStart, statusTracker.errorContext),
                                                CommitLogReadErrorReason.MUTATION_ERROR,
                                                statusTracker.tolerateErrorsInSection)))
                {
                    statusTracker.flagError();
                    continue;
                }
            }
            readMutation(handler, buffer, serializedSize, reader.getFilePointer(), desc);
            statusTracker.addProcessedMutation();
        }
    }

    /**
     * Deserializes and passes a Mutation to the ICommitLogReadHandler requested
     *
     * @param handler
     * @param inputBuffer   raw byte array w/Mutation data
     * @param size          deserialized size of mutation
     * @param entryLocation filePointer offset of mutation within CommitLogSegment
     * @param desc          CommitLogDescriptor being worked on
     */
    @VisibleForTesting
    protected void readMutation(ICommitLogReadHandler handler,
                              byte[] inputBuffer,
                              int size,
                              final long entryLocation,
                              final CommitLogDescriptor desc) throws IOException
    {
        final Mutation mutation;
        try (RebufferingInputStream bufIn = new DataInputBuffer(inputBuffer, 0, size))
        {
            mutation = Mutation.serializer.deserialize(bufIn,
                                                       desc.getMessagingVersion(),
                                                       SerializationHelper.Flag.LOCAL);
            // doublecheck that what we read is [still] valid for the current schema
            for (PartitionUpdate upd : mutation.getPartitionUpdates())
                upd.validate();
        }
        catch (UnknownColumnFamilyException ex)
        {
            if (ex.cfId == null)
                return;
            AtomicInteger i = invalidMutations.get(ex.cfId);
            if (i == null)
            {
                i = new AtomicInteger(1);
                invalidMutations.put(ex.cfId, i);
            }
            else
                i.incrementAndGet();
            return;
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            File f = File.createTempFile("mutation", "dat");

            try (DataOutputStream out = new DataOutputStream(new FileOutputStream(f)))
            {
                out.write(inputBuffer, 0, size);
            }

            // Checksum passed so this error can't be permissible.
            handler.shouldStopOnError(new CommitLogReadException(String.format(
                    "Unexpected error deserializing mutation; saved to %s.  " +
                    "This may be caused by replaying a mutation against a table with the same name but incompatible schema.  " +
                    "Exception follows: %s", f.getAbsolutePath(), t),
                                                                 CommitLogReadErrorReason.MUTATION_ERROR,
                                                                 false));
            return;
        }

        if (logger.isTraceEnabled())
            logger.trace("replaying mutation for {}.{}: {}", mutation.getKeyspaceName(), mutation.key(),
                         "{" + StringUtils.join(mutation.getPartitionUpdates().iterator(), ", ") + "}");

        handler.handleMutation(mutation, size, entryLocation, desc);
    }

    /**
     * Helper methods to deal with changing formats of internals of the CommitLog without polluting deserialization code.
     */
    private static class CommitLogFormat
    {
        public static long calculateClaimedChecksum(FileDataInput input, int commitLogVersion) throws IOException
        {
            switch (commitLogVersion)
            {
                case CommitLogDescriptor.VERSION_12:
                case CommitLogDescriptor.VERSION_20:
                    return input.readLong();
                // Changed format in 2.1
                default:
                    return input.readInt() & 0xffffffffL;
            }
        }

        public static int claimedChecksumSerializedSize(int commitLogVersion) throws IOException
        {
            switch (commitLogVersion)
            {
                case CommitLogDescriptor.VERSION_12:
                case CommitLogDescriptor.VERSION_20:
                    return TypeSizes.LONG_SIZE;
                // Changed format in 2.1
                default:
                    return TypeSizes.INT_SIZE;
            }
        }

        public static void updateChecksum(CRC32 checksum, int serializedSize, int commitLogVersion)
        {
            switch (commitLogVersion)
            {
                case CommitLogDescriptor.VERSION_12:
                    checksum.update(serializedSize);
                    break;
                // Changed format in 2.0
                default:
                    updateChecksumInt(checksum, serializedSize);
                    break;
            }
        }

        public static long calculateClaimedCRC32(FileDataInput input, int commitLogVersion) throws IOException
        {
            switch (commitLogVersion)
            {
                case CommitLogDescriptor.VERSION_12:
                case CommitLogDescriptor.VERSION_20:
                    return input.readLong();
                // Changed format in 2.1
                default:
                    return input.readInt() & 0xffffffffL;
            }
        }

        public static int claimedCRC32SerializedSize(int commitLogVersion) throws IOException
        {
            switch (commitLogVersion)
            {
                case CommitLogDescriptor.VERSION_12:
                case CommitLogDescriptor.VERSION_20:
                    return TypeSizes.LONG_SIZE;
                // Changed format in 2.1
                default:
                    return TypeSizes.INT_SIZE;
            }
        }
    }

    private class ReadStatusTracker
    {
        private int mutationsLeft;
        public String errorContext = "";
        public boolean tolerateErrorsInSection;
        private boolean error;

        public ReadStatusTracker(int mutationLimit, boolean tolerateErrorsInSection)
        {
            this.mutationsLeft = mutationLimit;
            this.tolerateErrorsInSection = tolerateErrorsInSection;
        }

        public void addProcessedMutation()
        {
            if (mutationsLeft == ALL_MUTATIONS)
                return;
            --mutationsLeft;
        }

        public boolean shouldContinue()
        {
            return !error && (mutationsLeft != 0 || mutationsLeft == ALL_MUTATIONS);
        }

        public void flagError()
        {
            error = true;
        }
    }
}
