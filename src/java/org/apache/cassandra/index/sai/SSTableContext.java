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
package org.apache.cassandra.index.sai;

import com.google.common.base.Objects;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.KeyFetcher;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.block.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.block.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.block.NumericValuesMeta;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

/**
 * SSTableContext is created for individual sstable shared across indexes to track per-sstable index files.
 *
 * SSTableContext itself will be released when receiving sstable removed notification, but its shared copies in individual
 * SSTableIndex will be released when in-flight read requests complete.
 */
public class SSTableContext extends SharedCloseableImpl
{
    public final SSTableReader sstable;
    public final IndexDescriptor indexDescriptor;
    public final LongArray.Factory tokenReaderFactory;
    public final LongArray.Factory offsetReaderFactory;
    public final KeyFetcher keyFetcher;

    private SSTableContext(SSTableReader sstable,
                           LongArray.Factory tokenReaderFactory,
                           LongArray.Factory offsetReaderFactory,
                           KeyFetcher keyFetcher,
                           IndexDescriptor indexDescriptor,
                           Cleanup cleanup)
    {
        super(cleanup);
        this.sstable = sstable;
        this.tokenReaderFactory = tokenReaderFactory;
        this.offsetReaderFactory = offsetReaderFactory;
        this.keyFetcher = keyFetcher;
        this.indexDescriptor = indexDescriptor;
    }

    private SSTableContext(SSTableContext copy)
    {
        super(copy);
        this.sstable = copy.sstable;
        this.tokenReaderFactory = copy.tokenReaderFactory;
        this.offsetReaderFactory = copy.offsetReaderFactory;
        this.indexDescriptor = copy.indexDescriptor;
        this.keyFetcher = copy.keyFetcher;
    }

    @SuppressWarnings("resource")
    public static SSTableContext create(SSTableReader sstable)
    {
        Ref<? extends SSTableReader> sstableRef = null;
        FileHandle token = null;
        FileHandle offset = null;
        LongArray.Factory tokenReaderFactory;
        LongArray.Factory offsetReaderFactory;
        KeyFetcher keyFetcher;
        IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable.descriptor);
        String offsetsComponentName = indexDescriptor.version.fileNameFormatter().format(IndexComponent.OFFSETS_VALUES, null);
        String tokensComponentName = indexDescriptor.version.fileNameFormatter().format(IndexComponent.TOKEN_VALUES, null);

        try
        {
            sstableRef = sstable.tryRef();

            if (sstableRef == null)
            {
                throw new IllegalStateException("Couldn't acquire reference to the sstable: " + sstable);
            }

            MetadataSource source = MetadataSource.loadGroupMetadata(indexDescriptor);
            NumericValuesMeta offsetsMeta = new NumericValuesMeta(source.get(offsetsComponentName));
            NumericValuesMeta tokensMeta = new NumericValuesMeta(source.get(tokensComponentName));

            token = indexDescriptor.createPerSSTableFileHandle(IndexComponent.TOKEN_VALUES);
            offset  = indexDescriptor.createPerSSTableFileHandle(IndexComponent.OFFSETS_VALUES);

            tokenReaderFactory = new BlockPackedReader(token, tokensMeta);
            offsetReaderFactory = new MonotonicBlockPackedReader(offset, offsetsMeta);
            keyFetcher = new KeyFetcher(sstable);

            Cleanup cleanup = new Cleanup(token, offset, sstableRef);

            return new SSTableContext(sstable, tokenReaderFactory, offsetReaderFactory, keyFetcher, indexDescriptor, cleanup);
        }
        catch (Throwable t)
        {
            if (sstableRef != null)
            {
                sstableRef.release();
            }

            throw Throwables.unchecked(Throwables.close(t, token, offset));
        }
    }

    @Override
    public SSTableContext sharedCopy()
    {
        return new SSTableContext(this);
    }

    /**
     * @return descriptor of attached sstable
     */
    public Descriptor descriptor()
    {
        return sstable.descriptor;
    }

    public SSTableReader sstable()
    {
        return sstable;
    }

    /**
     * @return disk usage of per-sstable index files
     */
    public long diskUsage()
    {
        return indexDescriptor.version.onDiskFormat()
                                      .perSSTableComponents()
                                      .stream()
                                      .map(indexDescriptor::fileFor)
                                      .filter(File::exists)
                                      .mapToLong(File::length)
                                      .sum();
    }

    /**
     * @return number of open files per {@link SSTableContext} instance
     */
    public int openFilesPerSSTable()
    {
        return indexDescriptor.version.onDiskFormat().openFilesPerSSTable();
    }

    @Override
    public String toString()
    {
        return "SSTableContext{" +
               "sstable=" + sstable.descriptor +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SSTableContext that = (SSTableContext) o;
        return Objects.equal(sstable.descriptor, that.sstable.descriptor);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(sstable.descriptor.hashCode());
    }

    private static class Cleanup implements RefCounted.Tidy
    {
        private final FileHandle token, offset;
        private final Ref<? extends SSTableReader> sstableRef;

        private Cleanup(FileHandle token, FileHandle offset, Ref<? extends SSTableReader> sstableRef)
        {
            this.token = token;
            this.offset = offset;
            this.sstableRef = sstableRef;
        }

        @Override
        public void tidy()
        {
            Throwable t = sstableRef.ensureReleased(null);
            t = Throwables.close(t, token, offset);

            Throwables.maybeFail(t);
        }

        @Override
        public String name()
        {
            return null;
        }
    }
}
