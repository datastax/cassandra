/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;

/**
 * Verify the index/page-aware infrastructure also works with compression. This is not used anywhere
 * (superseded by EncryptedSequentialWriter/ChunkReader).
 */
@RunWith(Parameterized.class)
public class RowIndexCompressedTest extends RowIndexTest
{
    File offsetsFile;

    public RowIndexCompressedTest() throws IOException
    {
        this(FileUtils.createTempFile("ColumnTrieReaderTest", ""),
             FileUtils.createTempFile("ColumnTrieReaderTest", ".offsets"));
    }

    private RowIndexCompressedTest(File file, File offsetsFile) throws IOException
    {
        super(file,
              new CompressedSequentialWriter(file,
                      offsetsFile,
                      null,
                      SequentialWriterOption.newBuilder().finishOnClose(true).build(),
                      CompressionParams.lz4(4096, 4096), new MetadataCollector(
                              TableMetadata.builder("k", "t")
                                           .addPartitionKeyColumn("key", BytesType.instance)
                                           .addClusteringColumn("clustering", comparator.subtype(0))
                                           .build().comparator)
        ));

        this.offsetsFile = offsetsFile;
    }

    @Override
    public RowIndexReader completeAndRead() throws IOException
    {
        complete();

        try (FileHandle.Builder builder = new FileHandle.Builder(file)
                                                        .withCompressionMetadata(new CompressionMetadata(offsetsFile, file.length(), true))
                                                        .mmapped(accessMode == Config.DiskAccessMode.mmap))
        {
            fh = builder.complete();
            try (RandomAccessReader rdr = fh.createReader())
            {
                assertEquals("JUNK", rdr.readUTF());
                assertEquals("JUNK", rdr.readUTF());
            }
            return new RowIndexReader(fh, root, VERSION);
        }
    }
}
