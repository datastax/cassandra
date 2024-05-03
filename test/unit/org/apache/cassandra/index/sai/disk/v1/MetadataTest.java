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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexOutput;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;

public class MetadataTest extends SaiRandomizedTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private IndexDescriptor indexDescriptor;
    private String index;
    private IndexContext indexContext;

    @Before
    public void setup() throws Throwable
    {
        indexDescriptor = newIndexDescriptor();
        index = newIndex();
        indexContext = SAITester.createIndexContext(index, UTF8Type.instance);
    }

    @Test
    public void shouldReadWrittenMetadata() throws Exception
    {
        final Map<String, byte[]> data = new HashMap<>();
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (MetadataWriter writer = new MetadataWriter(components))
        {
            int num = nextInt(1, 50);
            for (int x = 0; x < num; x++)
            {
                byte[] bytes = nextBytes(0, 1024);

                String name = UUID.randomUUID().toString();

                data.put(name, bytes);
                try (IndexOutput builder = writer.builder(name))
                {
                    builder.writeBytes(bytes, 0, bytes.length);
                }
            }
        }
        components.markComplete();

        MetadataSource reader = MetadataSource.loadMetadata(indexDescriptor.perIndexComponents(indexContext));

        for (Map.Entry<String, byte[]> entry : data.entrySet())
        {
            final IndexInput input = reader.get(entry.getKey());
            assertNotNull(input);
            final byte[] expectedBytes = entry.getValue();
            assertEquals(expectedBytes.length, input.length());
            final byte[] actualBytes = new byte[expectedBytes.length];
            input.readBytes(actualBytes, 0, expectedBytes.length);
            assertArrayEquals(expectedBytes, actualBytes);
        }
    }

    @Test
    public void shouldFailWhenFileHasNoHeader() throws IOException
    {
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (IndexOutputWriter out = components.addOrGet(IndexComponentType.META).openOutput())
        {
            final byte[] bytes = nextBytes(13, 29);
            out.writeBytes(bytes, bytes.length);
        }

        expectedException.expect(CorruptIndexException.class);
        expectedException.expectMessage("codec header mismatch");
        MetadataSource.loadMetadata(components);
    }

    @Test
    public void shouldFailCrcCheckWhenFileIsTruncated() throws IOException
    {
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        final IndexOutputWriter output = writeRandomBytes(components);

        final File indexFile = output.getFile();
        final long length = indexFile.length();
        assertTrue(length > 0);
        final File renamed = new File(temporaryFolder.newFile());
        indexFile.move(renamed);
        assertFalse(output.getFile().exists());

        try (FileOutputStream outputStream = new FileOutputStream(output.getFile().toJavaIOFile());
             RandomAccessFile input = new RandomAccessFile(renamed.toJavaIOFile(), "r"))
        {
            // skip last byte when copying
            FileUtils.copyTo(input, outputStream, Math.toIntExact(length - 1));
        }

        expectedException.expect(CorruptIndexException.class);
        expectedException.expectMessage("misplaced codec footer (file truncated?)");
        MetadataSource.loadMetadata(components);
    }

    @Test
    public void shouldFailCrcCheckWhenFileIsCorrupted() throws IOException
    {
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        final IndexOutputWriter output = writeRandomBytes(components);

        final File indexFile = output.getFile();
        final long length = indexFile.length();
        assertTrue(length > 0);
        final File renamed = new File(temporaryFolder.newFile());
        indexFile.move(renamed);
        assertFalse(output.getFile().exists());

        try (FileOutputStream outputStream = new FileOutputStream(output.getFile().toJavaIOFile());
             RandomAccessFile file = new RandomAccessFile(renamed.toJavaIOFile(), "r"))
        {
            // copy most of the file untouched
            final byte[] buffer = new byte[Math.toIntExact(length - 1 - CodecUtil.footerLength())];
            file.read(buffer);
            outputStream.write(buffer);

            // corrupt a single byte at the end
            final byte last = (byte) file.read();
            outputStream.write(~last);

            // copy footer
            final byte[] footer = new byte[CodecUtil.footerLength()];
            file.read(footer);
            outputStream.write(footer);
        }

        expectedException.expect(CorruptIndexException.class);
        expectedException.expectMessage("checksum failed");
        MetadataSource.loadMetadata(components);
    }

    private IndexOutputWriter writeRandomBytes(IndexComponents.ForWrite components) throws IOException
    {
        try (MetadataWriter writer = new MetadataWriter(components))
        {
            byte[] bytes = nextBytes(11, 1024);

            try (IndexOutput builder = writer.builder("name"))
            {
                builder.writeBytes(bytes, 0, bytes.length);
            }
        }
        return components.addOrGet(components.metadataComponent()).openOutput();
    }
}
