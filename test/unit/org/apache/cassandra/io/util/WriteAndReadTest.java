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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.AbstractLogTransaction;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IKeyFetcher;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiTableReader;
import org.apache.cassandra.io.sstable.format.bti.PartitionIndex;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_IO_TMPDIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class WriteAndReadTest
{
    private static final Logger logger = LoggerFactory.getLogger(WriteAndReadTest.class);

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testPartitionIndexFailure() throws IOException, InterruptedException
    {
        for (int i = 4001; i < 4200; ++i)
            testPartitionIndexFailure(i);
    }

    static class MockSSTableReader extends SSTableReader
    {

        protected MockSSTableReader(Builder<?, ?> builder, Owner owner)
        {
            super(builder, owner);
        }

        @Override
        public SSTableReader cloneWithRestoredStart(DecoratedKey restoredStart)
        {
            return null;
        }

        @Override
        public SSTableReader cloneWithNewStart(DecoratedKey newStart)
        {
            return null;
        }

        @Override
        public void releaseInMemoryComponents()
        {

        }

        @Override
        public long estimatedKeys()
        {
            return 0;
        }

        @Override
        public boolean mayContainAssumingKeyIsInRange(DecoratedKey key)
        {
            return false;
        }

        @Override
        public long estimatedKeysForRanges(Collection<Range<Token>> ranges)
        {
            return 0;
        }

        @Override
        public boolean isEstimationInformative()
        {
            return false;
        }

        @Override
        public Iterable<DecoratedKey> getKeySamples(Range<Token> range)
        {
            return null;
        }

        @Override
        protected AbstractRowIndexEntry getRowIndexEntry(PartitionPosition key, Operator op, boolean updateStats, SSTableReadsListener listener)
        {
            return null;
        }

        @Override
        public KeyReader keyReader() throws IOException
        {
            return null;
        }

        @Override
        public DecoratedKey firstKeyBeyond(PartitionPosition token)
        {
            return null;
        }

        @Override
        public IKeyFetcher openKeyFetcher(boolean isForSASI)
        {
            return null;
        }

        @Override
        public IVerifier getVerifier(ColumnFamilyStore cfs, OutputHandler outputHandler, boolean isOffline, IVerifier.Options options)
        {
            return null;
        }

        @Override
        public UnfilteredRowIterator rowIterator(DecoratedKey key, Slices slices, ColumnFilter columnFilter, boolean reversed, SSTableReadsListener listener)
        {
            return null;
        }

        @Override
        public UnfilteredPartitionIterator partitionIterator(ColumnFilter columnFilter, DataRange dataRange, SSTableReadsListener listener)
        {
            return null;
        }
    }

    // This tests failure on restore (DB-2489/DSP-17193) caused by chunk cache retaining
    // data from a previous version of a file with the same name.
    public void testPartitionIndexFailure(int length) throws IOException, InterruptedException
    {
        System.out.println("Prefix " + length);

        File parentDir = new File(JAVA_IO_TMPDIR.getString());
        Descriptor descriptor = new Descriptor(parentDir, "ks", "cf" + length, new SequenceBasedSSTableId(1));
        FileHandle.Builder indexFhBuilder = new FileHandle.Builder(descriptor.fileFor(BtiFormat.Components.PARTITION_INDEX))
                                            .withChunkCache(ChunkCache.instance)
                                            .bufferSize(PageAware.PAGE_SIZE);
        FileHandle.Builder dataFhBuilder = new FileHandle.Builder(descriptor.fileFor(SSTableFormat.Components.DATA))
                                           .withChunkCache(ChunkCache.instance)
                                           .bufferSize(PageAware.PAGE_SIZE);
        long root = length;
        long keyCount = root * length;
        long firstPos = keyCount * length;


        try (SequentialWriter writer = new SequentialWriter(descriptor.fileFor(BtiFormat.Components.PARTITION_INDEX),
                SequentialWriterOption.newBuilder()
                        .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                        .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKiB() * 1024)
                        .bufferType(BufferType.OFF_HEAP)
                        .finishOnClose(true)
                        .build()))

        {
            int i;
            for (i = 0; i + 8 <= length; i += 8)
                writer.writeLong(i);
            for (; i < length; i++)
                writer.write(i);

            // Do the final writes just like PartitionIndexWriter.complete
            writer.writeLong(firstPos);
            writer.writeLong(keyCount);
            writer.writeLong(root);
        }

        try (SequentialWriter writer = new SequentialWriter(descriptor.fileFor(SSTableFormat.Components.DATA),
                SequentialWriterOption.newBuilder()
                        .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                      .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKiB() * 1024)
                                      .bufferType(BufferType.OFF_HEAP)
                                      .finishOnClose(true)
                                      .build()))

        {
            writer.writeLong(length);
        }

        SSTableReader reader = null;
        // Now read it like PartitionIndex.load
        try (FileHandle ifh = indexFhBuilder.complete();
             FileHandle dfh = dataFhBuilder.complete())
        {
            BtiTableReader.Builder builder = new BtiTableReader.Builder(descriptor)
                                             .setComponents(Collections.singleton(BtiFormat.Components.PARTITION_INDEX))
                                             .setTableMetadataRef(TableMetadataRef.forOfflineTools(TableMetadata.minimal(descriptor.ksname, descriptor.cfname)))
                                             .setDataFile(dfh)
                                             .setPartitionIndex(new PartitionIndex(ifh, 0, 0, MockSchema.readerBounds(0), MockSchema.readerBounds(0), ByteComparable.Version.OSS50));
            reader = new MockSSTableReader(builder, null);
            reader.setup(false);

            try (FileDataInput index = ifh.createReader(ifh.dataLength() - 3 * 8))
            {
                long firstPosR = index.readLong();
                long keyCountR = index.readLong();
                long rootR = index.readLong();

                assertEquals(firstPos, firstPosR);
                assertEquals(keyCount, keyCountR);
                assertEquals(rootR, root);
            }
            try (FileDataInput data = dfh.createReader())
            {
                long lengthR = data.readLong();
                assertEquals(length, lengthR);
            }
        }
        finally
        {
            if (reader != null)
            {
                releaseReader(reader);
                CountDownLatch latch = new CountDownLatch(1);
                ScheduledExecutors.nonPeriodicTasks.execute(latch::countDown);
                latch.await();  // wait for the release to complete
                assertFalse(reader.getDataFile().exists());
            }
        }
    }

    private static void releaseReader(SSTableReader reader)
    {
        reader.markObsolete(new AbstractLogTransaction.ReaderTidier() {
            public void commit()
            {
                for (Component component : reader.descriptor.discoverComponents())
                    FileUtils.deleteWithConfirm(reader.descriptor.fileFor(component));
            }

            public Throwable abort(Throwable accumulate)
            {
                return null;
            }
        });
        reader.selfRef().release();
    }
}
