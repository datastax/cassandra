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

package org.apache.cassandra.index.sai.disk.v2.sortedbytes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

import static org.apache.cassandra.index.sai.disk.v2.sortedbytes.SortedBytesWriter.DIRECT_MONOTONIC_BLOCK_SHIFT;

public class SortedBytesTest extends SaiRandomizedTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testLexicographicException() throws Exception
    {
        IndexDescriptor descriptor = newIndexDescriptor();

        try (IndexOutputWriter trieWriter = descriptor.openPerSSTableOutput(IndexComponent.KD_TREE);
             IndexOutputWriter bytesWriter = descriptor.openPerSSTableOutput(IndexComponent.TERMS_DATA);
             IndexOutputWriter blockFPWriter = descriptor.openPerSSTableOutput(IndexComponent.KD_TREE_POSTING_LISTS))
        {
            SortedBytesWriter writer = new SortedBytesWriter(trieWriter,
                                                             bytesWriter,
                                                             blockFPWriter);

            ByteBuffer buffer = Int32Type.instance.decompose(99999);
            ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS41);
            byte[] bytes = ByteSourceInverse.readBytes(byteSource);

            writer.add(ByteComparable.fixedLength(bytes));

            expectedException.expect(IllegalArgumentException.class);

            buffer = Int32Type.instance.decompose(444);
            byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS41);
            bytes = ByteSourceInverse.readBytes(byteSource);

            writer.add(ByteComparable.fixedLength(bytes));
            writer.finish();
        }
    }

    // verify DirectMonotonicReader works
    @Test
    public void testDirectMonotonicReader() throws Exception
    {
        int count = 1000;

        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        try (IndexOutput offsetsOut = dir.createOutput("offsets", IOContext.DEFAULT);
             IndexOutput metaOut = dir.createOutput("meta", IOContext.DEFAULT))
        {
            DirectMonotonicWriter offsetsWriter = DirectMonotonicWriter.getInstance(metaOut, offsetsOut, count, DIRECT_MONOTONIC_BLOCK_SHIFT);
            for (int x = 0; x < count; x++)
            {
                offsetsWriter.add(x);
            }
            offsetsWriter.finish();
        }

        IndexInput metaInput = dir.openInput("meta", IOContext.DEFAULT);
        DirectMonotonicReader.Meta meta = DirectMonotonicReader.loadMeta(metaInput, count, DIRECT_MONOTONIC_BLOCK_SHIFT);
        IndexInput offsetsInput = dir.openInput("offsets", IOContext.DEFAULT);
        RandomAccessInput offsetSlice = offsetsInput.randomAccessSlice(0, offsetsInput.length());

        LongValues offsetsReader = DirectMonotonicReader.getInstance(meta, offsetSlice);

        for (int x = 0; x < count; x++)
        {
            assertEquals(x, offsetsReader.get(x));
        }
    }

    @Test
    public void testSeekToTerm() throws Exception
    {
        IndexDescriptor descriptor = newIndexDescriptor();

        SortedBytesWriter.Meta meta;

        List<byte[]> terms = new ArrayList();

        try (IndexOutputWriter trieWriter = descriptor.openPerSSTableOutput(IndexComponent.KD_TREE);
             IndexOutputWriter bytesWriter = descriptor.openPerSSTableOutput(IndexComponent.TERMS_DATA);
             IndexOutputWriter blockFPWriter = descriptor.openPerSSTableOutput(IndexComponent.KD_TREE_POSTING_LISTS))
        {
            SortedBytesWriter writer = new SortedBytesWriter(trieWriter,
                                                             bytesWriter,
                                                             blockFPWriter);

            for (int x = 0; x < 1000 * 4; x++)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(x);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS41);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);
                terms.add(bytes);

                writer.add(ByteComparable.fixedLength(bytes));
            }

            meta = writer.finish();
        }

        // iterate on terms ascending
        try (FileHandle trieHandle = descriptor.createPerSSTableFileHandle(IndexComponent.KD_TREE);
             IndexInput blockFPInput = descriptor.openPerSSTableInput(IndexComponent.KD_TREE_POSTING_LISTS))
        {
            SortedBytesReader reader = new SortedBytesReader(meta,
                                                             trieHandle,
                                                             blockFPInput);
            for (int x = 0; x < terms.size(); x++)
            {
                long pointId = reader.seekToBytes(ByteComparable.fixedLength(terms.get(x)));
                assertEquals(x, pointId);
            }
        }

        // iterate on terms descending
        try (FileHandle trieHandle = descriptor.createPerSSTableFileHandle(IndexComponent.KD_TREE);
             IndexInput blockFPInput = descriptor.openPerSSTableInput(IndexComponent.KD_TREE_POSTING_LISTS))
        {
            SortedBytesReader reader = new SortedBytesReader(meta,
                                                             trieHandle,
                                                             blockFPInput);
            for (int x = terms.size() - 1; x >= 0; x--)
            {
                long pointId = reader.seekToBytes(ByteComparable.fixedLength(terms.get(x)));
                assertEquals(x, pointId);
            }
        }

        // iterate randomly
        try (FileHandle trieHandle = descriptor.createPerSSTableFileHandle(IndexComponent.KD_TREE);
             IndexInput blockFPInput = descriptor.openPerSSTableInput(IndexComponent.KD_TREE_POSTING_LISTS))
        {
            SortedBytesReader reader = new SortedBytesReader(meta,
                                                             trieHandle,
                                                             blockFPInput);
            for (int x = 0; x < terms.size(); x++)
            {
                int target = nextInt(0, terms.size());

                long pointId = reader.seekToBytes(ByteComparable.fixedLength(terms.get(target)));
                assertEquals(target, pointId);
            }
        }
    }

    @Test
    public void testSeekToPointId() throws Exception
    {
        IndexDescriptor descriptor = newIndexDescriptor();

        SortedBytesWriter.Meta meta = null;

        List<byte[]> terms = new ArrayList();

        try (IndexOutputWriter trieWriter = descriptor.openPerSSTableOutput(IndexComponent.KD_TREE);
             IndexOutputWriter bytesWriter = descriptor.openPerSSTableOutput(IndexComponent.TERMS_DATA);
             IndexOutputWriter blockFPWriter = descriptor.openPerSSTableOutput(IndexComponent.KD_TREE_POSTING_LISTS))
        {
            SortedBytesWriter writer = new SortedBytesWriter(trieWriter,
                                                             bytesWriter,
                                                             blockFPWriter);

            for (int x = 0; x < 1000 * 4; x++)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(x);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS41);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);
                terms.add(bytes);

                writer.add(ByteComparable.fixedLength(bytes));
            }

            meta = writer.finish();
        }

        // iterate ascending
        try (FileHandle trieHandle = descriptor.createPerSSTableFileHandle(IndexComponent.KD_TREE);
             IndexInput bytesInput = descriptor.openPerSSTableInput(IndexComponent.TERMS_DATA);
             IndexInput blockFPInput = descriptor.openPerSSTableInput(IndexComponent.KD_TREE_POSTING_LISTS))
        {
            SortedBytesReader reader = new SortedBytesReader(meta,
                                                             trieHandle,
                                                             blockFPInput);
            SortedBytesReader.Context context = reader.createContext();
            for (int x = 0; x < terms.size(); x++)
            {
                ByteComparable term = reader.seekToPointId(x, bytesInput, context);

                byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(ByteComparable.Version.OSS41));
                assertArrayEquals(terms.get(x), bytes);
            }
        }

        // iterate descending
        try (FileHandle trieHandle = descriptor.createPerSSTableFileHandle(IndexComponent.KD_TREE);
             IndexInput bytesInput = descriptor.openPerSSTableInput(IndexComponent.TERMS_DATA);
             IndexInput blockFPInput = descriptor.openPerSSTableInput(IndexComponent.KD_TREE_POSTING_LISTS))
        {
            SortedBytesReader reader = new SortedBytesReader(meta,
                                                             trieHandle,
                                                             blockFPInput);
            SortedBytesReader.Context context = reader.createContext();
            for (int x = terms.size() - 1; x >= 0; x--)
            {
                ByteComparable term = reader.seekToPointId(x, bytesInput, context);

                byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(ByteComparable.Version.OSS41));
                assertArrayEquals(terms.get(x), bytes);
            }
        }

        // iterate randomly
        try (FileHandle trieHandle = descriptor.createPerSSTableFileHandle(IndexComponent.KD_TREE);
             IndexInput bytesInput = descriptor.openPerSSTableInput(IndexComponent.TERMS_DATA);
             IndexInput blockFPInput = descriptor.openPerSSTableInput(IndexComponent.KD_TREE_POSTING_LISTS))
        {
            SortedBytesReader reader = new SortedBytesReader(meta,
                                                             trieHandle,
                                                             blockFPInput);
            SortedBytesReader.Context context = reader.createContext();
            for (int x = 0; x < terms.size(); x++)
            {
                int target = nextInt(0, terms.size());
                ByteComparable term = reader.seekToPointId(target, bytesInput, context);

                byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(ByteComparable.Version.OSS41));
                assertArrayEquals(terms.get(target), bytes);
            }
        }
    }
}
