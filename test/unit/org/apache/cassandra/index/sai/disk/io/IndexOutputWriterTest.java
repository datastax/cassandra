package org.apache.cassandra.index.sai.disk.io;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;


/**
 * Tests for {@link IndexOutputWriter} created by {@link IndexFileUtils#openOutput}. Specifically focused on
 * checksumming.
 */
@RunWith(Parameterized.class)
public class IndexOutputWriterTest
{
    private static final Random RAND = new Random(42);

    static final int BUFFER_SIZE = 128;
    SequentialWriterOption writerOption = SequentialWriterOption.newBuilder()
                                          .bufferSize(BUFFER_SIZE)
                                          .bufferType(BufferType.OFF_HEAP)
                                          .finishOnClose(true)
                                          .build();

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Parameterized.Parameter
    public org.apache.cassandra.index.sai.disk.format.Version version;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Version.ALL.stream().map(v -> new Object[]{v}).collect(Collectors.toList());
    }

    // Open a checksummed writer.
    private IndexOutputWriter open(Path file, ByteOrder order, boolean append) throws IOException {
        return new IndexFileUtils(writerOption)
               .openOutput(new org.apache.cassandra.io.util.File(file.toFile()), order, append, version);
    }


    @Test
    public void checksumWriteMostSignificantBytesLittleEndian() throws Exception {
        checksumWriteMostSignificantBytes(ByteOrder.LITTLE_ENDIAN);
    }

    @Test
    public void checksumWriteMostSignificantBytesBigEndian() throws Exception {
        checksumWriteMostSignificantBytes(ByteOrder.BIG_ENDIAN);
    }

    public void checksumWriteMostSignificantBytes(ByteOrder order) throws Exception {
        Path path = Files.createTempFile("checksum", "test");
        try (IndexOutputWriter w = open(path, order, /*append*/ false)) {
            SequentialWriter writer = w.asSequentialWriter();
            // Write enough bytes to fill the buffer, then one more
            int bytesWritten = 0;
            while (bytesWritten < BUFFER_SIZE) {
                int bytes = RAND.nextInt(8) + 1;
                writer.writeMostSignificantBytes(RAND.nextLong(), bytes);
                bytesWritten += bytes;
            }

            // Close the writer to flush to disk.
            w.close();
            // Validate the checksum matches the on‑disk bytes
            assertEquals(crc32(Files.readAllBytes(path)), w.getChecksum());
        }
    }

    /** Compute CRC‑32 of one or more byte‑arrays. */
    private static long crc32(byte[]... chunks) {
        Checksum crc = new CRC32();
        for (byte[] c : chunks) crc.update(c, 0, c.length);
        return crc.getValue();
    }
}