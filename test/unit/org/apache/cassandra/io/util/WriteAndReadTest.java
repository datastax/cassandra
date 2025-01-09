/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.io.util;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.utils.PageAware;

import static org.junit.Assert.assertEquals;

public class WriteAndReadTest
{
    private static final Logger logger = LoggerFactory.getLogger(WriteAndReadTest.class);

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testPartitionIndexFailure() throws IOException
    {
        for (int i = 4001; i < 4200; ++i)
            testPartitionIndexFailure(i);
    }

    // This tests failure on restore (DB-2489/DSP-17193) caused by chunk cache retaining
    // data from a previous version of a file with the same name.
    public void testPartitionIndexFailure(int length) throws IOException
    {
        System.out.println("Prefix " + length);

        File parentDir = new File(System.getProperty("java.io.tmpdir"));
        Descriptor descriptor = new Descriptor(parentDir, "ks", "cf" + length, new SequenceBasedSSTableId(1));
        try (FileHandle.Builder fhBuilder = new FileHandle.Builder(descriptor.fileFor(Component.PARTITION_INDEX))
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
                writer.sync();
            }

            File filePath;
            // Now read it like PartitionIndex.load
            try (FileHandle fh = fhBuilder.complete();
                 FileDataInput rdr = fh.createReader(fh.dataLength() - 3 * 8))
            {
                long firstPosR = rdr.readLong();
                long keyCountR = rdr.readLong();
                long rootR = rdr.readLong();

                assertEquals(firstPos, firstPosR);
                assertEquals(keyCount, keyCountR);
                assertEquals(rootR, root);

                filePath = rdr.getFile();
            }

            FileUtils.deleteWithConfirm(filePath);
        }
    }
}
