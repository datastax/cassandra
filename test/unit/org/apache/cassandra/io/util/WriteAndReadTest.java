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
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.AbstractLogTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.ScrubPartitionIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.PageAware;

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

        protected MockSSTableReader(Descriptor desc, Set<Component> components, TableMetadataRef metadata, FileHandle dfile, FileHandle ifile)
        {
            super(desc, components, metadata, 1000, null, OpenReason.NORMAL, null, null, dfile, ifile, null);
        }

        public void setup(boolean trackHotness)
        {
            tidy.setup(this, trackHotness);
            tidy.addCloseable(ifile);
            tidy.addCloseable(dfile);
            super.setup(trackHotness);
        }

        public boolean hasIndex()
        {
            return false;
        }

        public PartitionIndexIterator allKeysIterator() throws IOException
        {
            return null;
        }

        public ScrubPartitionIterator scrubPartitionsIterator() throws IOException
        {
            return null;
        }

        protected RowIndexEntry getPosition(PartitionPosition key, Operator op, boolean updateCacheAndStats, boolean permitMatchPastLast, SSTableReadsListener listener)
        {
            return null;
        }

        public UnfilteredRowIterator iterator(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener)
        {
            return null;
        }

        public UnfilteredRowIterator simpleIterator(FileDataInput dfile, DecoratedKey key, boolean tombstoneOnly)
        {
            return null;
        }

        public ISSTableScanner getScanner(ColumnFilter columns, DataRange dataRange, SSTableReadsListener listener)
        {
            return null;
        }

        public DecoratedKey keyAt(FileDataInput reader) throws IOException
        {
            return null;
        }

        public RandomAccessReader openKeyComponentReader()
        {
            return null;
        }
    }

    // This tests failure on restore (DB-2489/DSP-17193) caused by chunk cache retaining
    // data from a previous version of a file with the same name.
    public void testPartitionIndexFailure(int length) throws IOException, InterruptedException
    {
        System.out.println("Prefix " + length);

        File parentDir = new File(System.getProperty("java.io.tmpdir"));
        Descriptor descriptor = new Descriptor(parentDir, "ks", "cf" + length, new SequenceBasedSSTableId(1));
        try (FileHandle.Builder indexFhBuilder = new FileHandle.Builder(descriptor.fileFor(Component.PARTITION_INDEX))
                                                 .withChunkCache(ChunkCache.instance)
                                                 .bufferSize(PageAware.PAGE_SIZE);
             FileHandle.Builder dataFhBuilder = new FileHandle.Builder(descriptor.fileFor(Component.DATA))
                                                 .withChunkCache(ChunkCache.instance)
                                                 .bufferSize(PageAware.PAGE_SIZE))
        {
            long root = length;
            long keyCount = root * length;
            long firstPos = keyCount * length;


            try (SequentialWriter writer = new SequentialWriter(descriptor.fileFor(Component.PARTITION_INDEX),
                    SequentialWriterOption.newBuilder()
                            .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                            .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024)
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

            try (SequentialWriter writer = new SequentialWriter(descriptor.fileFor(Component.DATA),
                    SequentialWriterOption.newBuilder()
                            .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                          .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024)
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
                reader = new MockSSTableReader(descriptor, Collections.singleton(Component.PARTITION_INDEX), TableMetadataRef.forOfflineTools(TableMetadata.minimal(descriptor.ksname, descriptor.cfname)), dfh, ifh);
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
                    assertFalse(reader.getIndexFile().channel.getFile().exists());
                }
            }
        }
    }

    private static void releaseReader(SSTableReader reader)
    {
        reader.markObsolete(new AbstractLogTransaction.ReaderTidier() {
            public void commit()
            {
                for (Component component : MockSSTableReader.discoverComponentsFor(reader.descriptor))
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
