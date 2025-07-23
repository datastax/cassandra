/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.sstable.format.bti;

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

        FileHandle.Builder builder = new FileHandle.Builder(file)
                                                        .withCompressionMetadata(CompressionMetadata.open(offsetsFile, file.length(), true))
                                                        .mmapped(accessMode == Config.DiskAccessMode.mmap);
        
        fh = builder.complete();
        try (RandomAccessReader rdr = fh.createReader())
        {
            assertEquals("JUNK", rdr.readUTF());
            assertEquals("JUNK", rdr.readUTF());
        }
        return new RowIndexReader(fh, root, VERSION);
    }
}
