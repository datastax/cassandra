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
import java.util.HashMap;
import java.util.Map;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.EncryptedSequentialWriter;
import org.apache.cassandra.io.compress.EncryptionConfig;
import org.apache.cassandra.io.compress.Encryptor;
import org.apache.cassandra.io.compress.EncryptorTest;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class RowIndexEncryptedTest extends RowIndexTest
{
    static final CompressionParams compressionParams;

    static
    {
        Map<String, String> opts = new HashMap<>();

        opts.put(CompressionParams.CLASS, Encryptor.class.getName());

        opts.put(EncryptionConfig.CIPHER_ALGORITHM, "AES/CBC/PKCS5Padding");
        opts.put(EncryptionConfig.SECRET_KEY_STRENGTH, Integer.toString(128));

        opts.put(EncryptionConfig.KEY_PROVIDER, EncryptorTest.KeyProviderFactoryStub.class.getName());

        compressionParams = CompressionParams.fromMap(opts);
    }

    public RowIndexEncryptedTest() throws IOException
    {
        this(FileUtils.createTempFile("ColumnTrieReaderTest", ""));
    }


    RowIndexEncryptedTest(File file) throws IOException
    {
        super(file,
              new EncryptedSequentialWriter(file,
                                            SequentialWriterOption.newBuilder().finishOnClose(true).build(),
                                            compressionParams.getSstableCompressor().encryptionOnly()));
    }

    @Override
    public RowIndexReader completeAndRead() throws IOException
    {
        complete();

        FileHandle.Builder builder = new FileHandle.Builder(file)
                .withCompressionMetadata(CompressionMetadata.encryptedOnly(compressionParams))
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
