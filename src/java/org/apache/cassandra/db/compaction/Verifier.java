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
package org.apache.cassandra.db.compaction;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.DataIntegrityMetadata.FileDigestValidator;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.ReadPattern;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.UUIDGen;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class Verifier implements Closeable
{
    private final @Nullable CompactionRealm realm;
    private final SSTableReader sstable;

    private final ReadWriteLock fileAccessLock;
    private final RandomAccessReader dataFile;
    private final VerifyInfo verifyInfo;
    private final Options options;
    private final boolean isOffline;
    /**
     * Given a keyspace, return the set of local and pending token ranges.  By default {@link StorageService#getLocalAndPendingRanges(String)}
     * is expected, but for the standalone verifier case we can't use that, so this is here to allow the CLI to provide
     * the token ranges.
     */
    private final Function<String, ? extends Collection<Range<Token>>> tokenLookup;

    private int goodRows;

    private final OutputHandler outputHandler;
    private FileDigestValidator validator;

    /**
     * Creates an instance of Verifier without providing a CompactionRealm. This is only
     * allowed if the provided options have {@link Options#mutateRepairStatus} set to false.
     * @param sstable The SSTable reader used to verify the SSTable files
     * @param isOffline if set to true reading the SSTable data file is not rate limited
     * @param options the verification options
     */
    public Verifier(SSTableReader sstable, boolean isOffline, Options options)
    {
        this(sstable, new OutputHandler.LogOutput(), isOffline, options);
    }

    /**
     * Creates an instance of Verifier without providing a CompactionRealm. This is only
     * allowed if the provided options have {@link Options#mutateRepairStatus} set to false.
     * @param sstable The SSTable reader used to verify the SSTable files
     * @param outputHandler The output handler used for logging
     * @param isOffline if set to true reading the SSTable data file is not rate limited
     * @param options the verification options
     */
    public Verifier(SSTableReader sstable, OutputHandler outputHandler, boolean isOffline, Options options)
    {
        this(null, sstable, outputHandler, isOffline, options);
    }

    public Verifier(@Nullable CompactionRealm realm, SSTableReader sstable, boolean isOffline, Options options)
    {
        this(realm, sstable, new OutputHandler.LogOutput(), isOffline, options);
    }

    public Verifier(@Nullable CompactionRealm realm, SSTableReader sstable, OutputHandler outputHandler, boolean isOffline, Options options)
    {
        checkArgument(!options.mutateRepairStatus || realm != null,
                      "Compaction realm must be provided with option mutateRepairStatus=true");

        this.realm = realm;
        this.sstable = sstable;
        this.outputHandler = outputHandler;

        this.fileAccessLock = new ReentrantReadWriteLock();
        this.dataFile = isOffline
                        ? sstable.openDataReader(ReadPattern.SEQUENTIAL)
                        : sstable.openDataReader(CompactionManager.instance.getRateLimiter(), ReadPattern.SEQUENTIAL);
        this.verifyInfo = new VerifyInfo(dataFile, sstable, fileAccessLock.readLock());
        this.options = options;
        this.isOffline = isOffline;
        this.tokenLookup = options.tokenLookup;
    }

    public void verify()
    {
        boolean extended = options.extendedVerification;
        long rowStart = 0;

        outputHandler.output(String.format("Verifying %s (%s) with options %s", sstable, FBUtilities.prettyPrintMemory(dataFile.length()), options));
        if (options.checkVersion && !sstable.descriptor.version.isLatestVersion())
        {
            String msg = String.format("%s is not the latest version, run upgradesstables", sstable);
            outputHandler.output(msg);
            // don't use markAndThrow here because we don't want a CorruptSSTableException for this.
            throw new RuntimeException(msg);
        }

        outputHandler.output(String.format("Deserializing sstable metadata for %s ", sstable));
        try
        {
            EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);
            Map<MetadataType, MetadataComponent> sstableMetadata = sstable.descriptor.getMetadataSerializer().deserialize(sstable.descriptor, types);
            if (sstableMetadata.containsKey(MetadataType.VALIDATION) &&
                !((ValidationMetadata)sstableMetadata.get(MetadataType.VALIDATION)).partitioner.equals(sstable.getPartitioner().getClass().getCanonicalName()))
                throw new IOException("Partitioner does not match validation metadata");
        }
        catch (Throwable t)
        {
            outputHandler.warn(t);
            markAndThrow(t, false);
        }

        try
        {
            outputHandler.debug("Deserializing index for " + sstable);
            deserializeIndex(sstable);
        }
        catch (Throwable t)
        {
            outputHandler.warn(t);
            markAndThrow(t);
        }

        if (sstable.descriptor.getFormat().supportedComponents().contains(Component.SUMMARY))
        {
            try
            {
                outputHandler.debug("Deserializing index summary for " + sstable);
                deserializeIndexSummary(sstable);
            }
            catch (Throwable t)
            {
                outputHandler.output("Index summary is corrupt - if it is removed it will get rebuilt on startup " + sstable.descriptor.fileFor(Component.SUMMARY));
                outputHandler.warn(t);
            markAndThrow(t, false);
            }
        }

        try
        {
            outputHandler.debug("Deserializing bloom filter for "+sstable);
            deserializeBloomFilter(sstable);

        }
        catch (Throwable t)
        {
            outputHandler.warn(t);
            markAndThrow(t);
        }

        if (options.checkOwnsTokens && !isOffline && !(realm.getPartitioner() instanceof LocalPartitioner))
        {
            outputHandler.debug("Checking that all tokens are owned by the current node");
            try (KeyIterator iter = KeyIterator.forSSTable(sstable))
            {
                List<Range<Token>> ownedRanges = Range.normalize(tokenLookup.apply(realm.metadataRef().keyspace));
                if (ownedRanges.isEmpty())
                    return;
                RangeOwnHelper rangeOwnHelper = new RangeOwnHelper(ownedRanges);
                while (iter.hasNext())
                {
                    DecoratedKey key = iter.next();
                    rangeOwnHelper.validate(key);
                }
            }
            catch (Throwable t)
            {
                outputHandler.warn(t);
                markAndThrow(t);
            }
        }

        // Validate the secondary indexes. Only validate checksums if quick is disabled.
        validateSecondaryIndexes(!options.quick);

        if (options.quick)
            return;

        // Verify will use the Digest files, which works for both compressed and uncompressed sstables
        outputHandler.output(String.format("Checking computed hash of %s ", sstable));
        try
        {
            validator = null;

            if (sstable.descriptor.fileFor(Component.DIGEST).exists())
            {
                validator = DataIntegrityMetadata.fileDigestValidator(sstable.descriptor);
                validator.validate();
            }
            else
            {
                outputHandler.output("Data digest missing, assuming extended verification of disk values");
                extended = true;
            }
        }
        catch (IOException e)
        {
            outputHandler.warn(e);
            markAndThrow(e);
        }
        finally
        {
            FileUtils.closeQuietly(validator);
        }

        if (!extended)
            return;

        outputHandler.output("Extended Verify requested, proceeding to inspect values");

        try(PartitionIndexIterator indexIterator = sstable.allKeysIterator())
        {
            if (indexIterator.dataPosition() != sstable.getDataFileSliceDescriptor().dataStart)
                markAndThrow(new RuntimeException("First row position from index != 0: " + indexIterator.dataPosition()));

            List<Range<Token>> ownedRanges = isOffline ? Collections.emptyList() : Range.normalize(tokenLookup.apply(
            realm.metadata().keyspace));
            RangeOwnHelper rangeOwnHelper = new RangeOwnHelper(ownedRanges);
            DecoratedKey prevKey = null;

            while (!dataFile.isEOF())
            {
                verifyInfo.throwIfStopRequested();

                rowStart = dataFile.getFilePointer();
                outputHandler.debug("Reading row at " + rowStart);

                DecoratedKey key = null;
                try
                {
                    key = sstable.decorateKey(ByteBufferUtil.readWithShortLength(dataFile));
                }
                catch (Throwable th)
                {
                    throwIfFatal(th);
                    // check for null key below
                }

                if (options.checkOwnsTokens && !ownedRanges.isEmpty() && !(realm.getPartitioner() instanceof LocalPartitioner))
                {
                    try
                    {
                        rangeOwnHelper.validate(key);
                    }
                    catch (Throwable t)
                    {
                        outputHandler.warn(String.format("Key %s in sstable %s not owned by local ranges %s", key, sstable, ownedRanges), t);
                        markAndThrow(t);
                    }
                }

                ByteBuffer currentIndexKey = indexIterator.key();
                long nextRowPositionFromIndex = 0;
                try
                {
                    if (indexIterator.advance())
                    {
                        nextRowPositionFromIndex = indexIterator.dataPosition();
                    }
                    else
                    {
                        nextRowPositionFromIndex = dataFile.length();
                    }
                }
                catch (Throwable th)
                {
                    markAndThrow(th);
                }

                long dataStart = dataFile.getFilePointer();
                long dataStartFromIndex = currentIndexKey == null
                                        ? -1
                                        : rowStart + 2 + currentIndexKey.remaining();

                long dataSize = nextRowPositionFromIndex - dataStartFromIndex;
                // avoid an NPE if key is null
                String keyName = key == null ? "(unreadable key)" : ByteBufferUtil.bytesToHex(key.getKey());
                outputHandler.debug(String.format("row %s is %s", keyName, FBUtilities.prettyPrintMemory(dataSize)));

                try
                {
                    if (key == null || dataSize > dataFile.length())
                        markAndThrow(new RuntimeException(String.format("key = %s, dataSize=%d, dataFile.length() = %d", key, dataSize, dataFile.length())));

                    //mimic the scrub read path
                    try (UnfilteredRowIterator identity = SSTableIdentityIterator.create(sstable, dataFile, key);
                         UnfilteredRowIterator iterator = UnfilteredRowIterators.withValidation(identity, dataFile.getFile()))
                    {
                        if (options.validateAllRows)
                        {
                            // validate all rows and cells
                            while (iterator.hasNext())
                                iterator.next();
                        }
                    }

                    if ( (prevKey != null && prevKey.compareTo(key) > 0) || !key.getKey().equals(currentIndexKey) || dataStart != dataStartFromIndex )
                        markAndThrow(new RuntimeException("Key out of order: previous = "+prevKey + " : current = " + key));
                    
                    goodRows++;
                    prevKey = key;


                    outputHandler.debug(String.format("Row %s at %s valid, moving to next row at %s ", goodRows, rowStart, nextRowPositionFromIndex));
                    dataFile.seek(nextRowPositionFromIndex);
                }
                catch (Throwable th)
                {
                    markAndThrow(th);
                }
            }
        }
        catch (Throwable t)
        {
            throw Throwables.propagate(t);
        }

        outputHandler.output("Verify of " + sstable + " succeeded. All " + goodRows + " rows read successfully");
    }

    private void validateSecondaryIndexes(boolean validateChecksum)
    {
        if (realm == null)
        {
            outputHandler.output("Skipping secondary index component validation for " + sstable +
                                 " because the compaction realm is not available");
            return;
        }

        outputHandler.output(String.format("Checking secondary index components for %s, validateChecksum=%s", sstable, validateChecksum));
        try
        {
            var indexManager = realm.getIndexManager();
            for (IndexMetadata indexMetadata : sstable.metadata().indexes)
            {
                var index = indexManager.getIndexGroup(indexMetadata);
                if (index == null)
                    throw new IllegalStateException("Cannot verify index components for " + sstable + " because the index " + indexMetadata.name + " is not registered");
                index.validateComponents(sstable, validateChecksum);
            }
        }
        catch (Throwable t)
        {
            outputHandler.warn(t);
            markAndThrow(t);
        }
    }

    /**
     * Use the fact that check(..) is called with sorted tokens - we keep a pointer in to the normalized ranges
     * and only bump the pointer if the key given is out of range. This is done to avoid calling .contains(..) many
     * times for each key (with vnodes for example)
     */
    @VisibleForTesting
    public static class RangeOwnHelper
    {
        private final List<Range<Token>> normalizedRanges;
        private int rangeIndex = 0;
        private DecoratedKey lastKey;

        public RangeOwnHelper(List<Range<Token>> normalizedRanges)
        {
            this.normalizedRanges = normalizedRanges;
            Range.assertNormalized(normalizedRanges);
        }

        /**
         * check if the given key is contained in any of the given ranges
         *
         * Must be called in sorted order - key should be increasing
         *
         * @param key the key
         * @throws RuntimeException if the key is not contained
         */
        public void validate(DecoratedKey key)
        {
            if (!check(key))
                throw new RuntimeException("Key " + key + " is not contained in the given ranges");
        }

        /**
         * check if the given key is contained in any of the given ranges
         *
         * Must be called in sorted order - key should be increasing
         *
         * @param key the key
         * @return boolean
         */
        public boolean check(DecoratedKey key)
        {
            assert lastKey == null || key.compareTo(lastKey) > 0;
            lastKey = key;

            if (normalizedRanges.isEmpty()) // handle tests etc where we don't have any ranges
                return true;

            if (rangeIndex > normalizedRanges.size() - 1)
                throw new IllegalStateException("RangeOwnHelper can only be used to find the first out-of-range-token");

            while (!normalizedRanges.get(rangeIndex).contains(key.getToken()))
            {
                rangeIndex++;
                if (rangeIndex > normalizedRanges.size() - 1)
                    return false;
            }

            return true;
        }
    }

    private void deserializeIndex(SSTableReader sstable) throws IOException
    {
        try (PartitionIndexIterator it = sstable.allKeysIterator()) {
            //noinspection StatementWithEmptyBody
            ByteBuffer last = it.key();
            while (it.advance()) last = it.key(); // no-op, just check if index is readable
            if (!Objects.equals(last, sstable.last.getKey()))
                throw new CorruptSSTableException(new IOException("Failed to read partition index"), it.toString());
        }
    }

    private void deserializeIndexSummary(SSTableReader sstable) throws IOException
    {
        File file = sstable.descriptor.fileFor(Component.SUMMARY);
        TableMetadata metadata = sstable.metadata();
        try (DataInputStream iStream = new DataInputStream(Files.newInputStream(file.toPath())))
        {
            try (IndexSummary indexSummary = IndexSummary.serializer.deserialize(iStream,
                                                                                 sstable.getPartitioner(),
                                                                                 metadata.params.minIndexInterval,
                                                                                 metadata.params.maxIndexInterval))
            {
                ByteBufferUtil.readWithLength(iStream);
                ByteBufferUtil.readWithLength(iStream);
            }
        }
    }

    private void deserializeBloomFilter(SSTableReader sstable) throws IOException
    {
        File bfPath = sstable.descriptor.fileFor(Component.FILTER);
        if (bfPath.exists())
        {
            try (FileInputStreamPlus stream = bfPath.newInputStream();
                 IFilter bf = BloomFilter.getDeserializer().deserialize(stream, sstable.descriptor.version.hasOldBfFormat()))
            {
            }
        }
    }

    public void close()
    {
        fileAccessLock.writeLock().lock();
        try
        {
            FileUtils.closeQuietly(dataFile);
        }
        finally
        {
            fileAccessLock.writeLock().unlock();
        }
    }

    private void throwIfFatal(Throwable th)
    {
        if (th instanceof Error && !(th instanceof AssertionError || th instanceof IOError))
            throw (Error) th;
    }

    private void markAndThrow(Throwable cause)
    {
        markAndThrow(cause, true);
    }

    private void markAndThrow(Throwable cause, boolean mutateRepaired)
    {
        if (mutateRepaired && options.mutateRepairStatus) // if we are able to mutate repaired flag, an incremental repair should be enough
        {
            checkState(realm != null, "Cannot mutate repair status as compaction realm is null");
            try
            {
                realm.mutateRepairedWithLock(ImmutableList.of(sstable), ActiveRepairService.UNREPAIRED_SSTABLE, sstable.getPendingRepair(), sstable.isTransient());
            }
            catch(IOException ioe)
            {
                outputHandler.output("Error mutating repairedAt for SSTable " +  sstable.getFilename() + ", as part of markAndThrow");
            }
        }
        Exception e = new Exception(String.format("Invalid SSTable %s, please force %srepair", sstable.getFilename(), (mutateRepaired && options.mutateRepairStatus) ? "" : "a full "), cause);
        if (options.invokeDiskFailurePolicy)
            throw new CorruptSSTableException(e, sstable.getFilename());
        else
            throw new RuntimeException(e);
    }

    public AbstractTableOperation getVerifyInfo()
    {
        return verifyInfo;
    }

    private static class VerifyInfo extends AbstractTableOperation
    {
        private final RandomAccessReader dataFile;
        private final SSTableReader sstable;
        private final UUID verificationCompactionId;
        private final Lock fileReadLock;

        public VerifyInfo(RandomAccessReader dataFile, SSTableReader sstable, Lock fileReadLock)
        {
            this.dataFile = dataFile;
            this.sstable = sstable;
            this.fileReadLock = fileReadLock;
            verificationCompactionId = UUIDGen.getTimeUUID();
        }

        public OperationProgress getProgress()
        {
            fileReadLock.lock();
            try
            {
                return new OperationProgress(sstable.metadata(),
                                             OperationType.VERIFY,
                                             dataFile.getFilePointer(),
                                             dataFile.length(),
                                             verificationCompactionId,
                                             ImmutableSet.of(sstable));
            }
            catch (Exception e)
            {
                throw new RuntimeException();
            }
            finally
            {
                fileReadLock.unlock();
            }
        }

        public boolean isGlobal()
        {
            return false;
        }
    }

    public static Options.Builder options()
    {
        return new Options.Builder();
    }

    public static class Options
    {
        public final boolean invokeDiskFailurePolicy;
        public final boolean extendedVerification;
        public final boolean validateAllRows;
        public final boolean checkVersion;
        public final boolean mutateRepairStatus;
        public final boolean checkOwnsTokens;
        public final boolean quick;
        public final Function<String, ? extends Collection<Range<Token>>> tokenLookup;

        private Options(boolean invokeDiskFailurePolicy, boolean extendedVerification, boolean validateAllRows, boolean checkVersion, boolean mutateRepairStatus, boolean checkOwnsTokens, boolean quick, Function<String, ? extends Collection<Range<Token>>> tokenLookup)
        {
            this.invokeDiskFailurePolicy = invokeDiskFailurePolicy;
            this.extendedVerification = extendedVerification;
            this.validateAllRows = validateAllRows;
            this.checkVersion = checkVersion;
            this.mutateRepairStatus = mutateRepairStatus;
            this.checkOwnsTokens = checkOwnsTokens;
            this.quick = quick;
            this.tokenLookup = tokenLookup;

            if (validateAllRows && !extendedVerification)
                throw new IllegalArgumentException("validateAllRows must be enabled with extended verification");
        }

        @Override
        public String toString()
        {
            return "Options{" +
                   "invokeDiskFailurePolicy=" + invokeDiskFailurePolicy +
                   ", extendedVerification=" + extendedVerification +
                   ", validateAllRows=" + validateAllRows +
                   ", checkVersion=" + checkVersion +
                   ", mutateRepairStatus=" + mutateRepairStatus +
                   ", checkOwnsTokens=" + checkOwnsTokens +
                   ", quick=" + quick +
                   '}';
        }

        public static class Builder
        {
            private boolean invokeDiskFailurePolicy = false; // invoking disk failure policy can stop the node if we find a corrupt stable
            private boolean extendedVerification = false;
            private boolean validateAllRows = false; // whether to validate all rows in each partition in extended verification mode
            private boolean checkVersion = false;
            private boolean mutateRepairStatus = false; // mutating repair status can be dangerous
            private boolean checkOwnsTokens = false;
            private boolean quick = false;
            private Function<String, ? extends Collection<Range<Token>>> tokenLookup = StorageService.instance::getLocalAndPendingRanges;

            public Builder invokeDiskFailurePolicy(boolean param)
            {
                this.invokeDiskFailurePolicy = param;
                return this;
            }

            public Builder extendedVerification(boolean param)
            {
                this.extendedVerification = param;
                return this;
            }

            public Builder validateAllRows(boolean param)
            {
                this.validateAllRows = param;
                return this;
            }

            public Builder checkVersion(boolean param)
            {
                this.checkVersion = param;
                return this;
            }

            public Builder mutateRepairStatus(boolean param)
            {
                this.mutateRepairStatus = param;
                return this;
            }

            public Builder checkOwnsTokens(boolean param)
            {
                this.checkOwnsTokens = param;
                return this;
            }

            public Builder quick(boolean param)
            {
                this.quick = param;
                return this;
            }

            public Builder tokenLookup(Function<String, ? extends Collection<Range<Token>>> tokenLookup)
            {
                this.tokenLookup = tokenLookup;
                return this;
            }

            public Options build()
            {
                return new Options(invokeDiskFailurePolicy, extendedVerification, validateAllRows, checkVersion, mutateRepairStatus, checkOwnsTokens, quick, tokenLookup);
            }

        }
    }
}
