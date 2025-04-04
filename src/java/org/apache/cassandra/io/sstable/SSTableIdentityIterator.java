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
package org.apache.cassandra.io.sstable;

import java.io.IOError;
import java.io.IOException;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.UnfilteredValidation;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SSTableIdentityIterator implements Comparable<SSTableIdentityIterator>, UnfilteredRowIterator
{
    private final SSTableReader sstable;
    private final DecoratedKey key;
    private final DeletionTime partitionLevelDeletion;
    private final File file;

    protected final SSTableSimpleIterator iterator;
    private final Row staticRow;
    boolean isClosed = false;

    public SSTableIdentityIterator(SSTableReader sstable, DecoratedKey key, DeletionTime partitionLevelDeletion,
                                   File file, SSTableSimpleIterator iterator) throws IOException
    {
        super();
        this.sstable = sstable;
        this.key = key;
        this.partitionLevelDeletion = partitionLevelDeletion;
        this.file = file;
        this.iterator = iterator;
        this.staticRow = iterator.readStaticRow();
    }

    @SuppressWarnings("resource")
    public static SSTableIdentityIterator create(SSTableReader sstable, RandomAccessReader file, DecoratedKey key)
    {
        try
        {
            DeletionTime partitionLevelDeletion = DeletionTime.serializer.deserialize(file);
            if (!partitionLevelDeletion.validate())
                UnfilteredValidation.handleInvalid(sstable.metadata(), key, sstable, "partitionLevelDeletion="+partitionLevelDeletion.toString());
            DeserializationHelper helper = new DeserializationHelper(sstable.metadata(), sstable.descriptor.version.correspondingMessagingVersion(), DeserializationHelper.Flag.LOCAL);
            SSTableSimpleIterator iterator = SSTableSimpleIterator.create(sstable.metadata(), file, sstable.header, helper, partitionLevelDeletion);
            return new SSTableIdentityIterator(sstable, key, partitionLevelDeletion, file.getFile(), iterator);
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, file.getFile());
        }
    }

    @SuppressWarnings("resource")
    public static SSTableIdentityIterator create(SSTableReader sstable, FileDataInput dfile, RowIndexEntry indexEntry, DecoratedKey key, boolean tombstoneOnly)
    {
        try
        {
            dfile.seek(indexEntry.position);
            ByteBufferUtil.skipShortLength(dfile); // Skip partition key
            DeletionTime partitionLevelDeletion = DeletionTime.serializer.deserialize(dfile);
            if (!partitionLevelDeletion.validate())
                UnfilteredValidation.handleInvalid(sstable.metadata(), key, sstable, "partitionLevelDeletion="+partitionLevelDeletion.toString());

            DeserializationHelper helper = new DeserializationHelper(sstable.metadata(), sstable.descriptor.version.correspondingMessagingVersion(), DeserializationHelper.Flag.LOCAL);
            SSTableSimpleIterator iterator = tombstoneOnly
                    ? SSTableSimpleIterator.createTombstoneOnly(sstable.metadata(), dfile, sstable.header, helper, partitionLevelDeletion)
                    : SSTableSimpleIterator.create(sstable.metadata(), dfile, sstable.header, helper, partitionLevelDeletion);
            return new SSTableIdentityIterator(sstable, key, partitionLevelDeletion, dfile.getFile(), iterator);
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, dfile.getFile());
        }
    }

    public static SSTableIdentityIterator create(SSTableReader sstable, FileDataInput dfile, boolean tombstoneOnly)
    {
        try
        {
            DecoratedKey key = sstable.decorateKey(ByteBufferUtil.readWithShortLength(dfile));
            DeletionTime partitionLevelDeletion = DeletionTime.serializer.deserialize(dfile);
            if (!partitionLevelDeletion.validate())
                UnfilteredValidation.handleInvalid(sstable.metadata(), key, sstable, "partitionLevelDeletion="+partitionLevelDeletion.toString());

            DeserializationHelper helper = new DeserializationHelper(sstable.metadata(), sstable.descriptor.version.correspondingMessagingVersion(), DeserializationHelper.Flag.LOCAL);
            SSTableSimpleIterator iterator = tombstoneOnly
                                             ? SSTableSimpleIterator.createTombstoneOnly(sstable.metadata(), dfile, sstable.header, helper, partitionLevelDeletion)
                                             : SSTableSimpleIterator.create(sstable.metadata(), dfile, sstable.header, helper, partitionLevelDeletion);
            return new SSTableIdentityIterator(sstable, key, partitionLevelDeletion, dfile.getFile(), iterator);
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, dfile.getFile());
        }
    }

    public TableMetadata metadata()
    {
        return iterator.metadata;
    }

    public RegularAndStaticColumns columns()
    {
        return metadata().regularAndStaticColumns();
    }

    public boolean isReverseOrder()
    {
        return false;
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public boolean hasNext()
    {
        try
        {
            return iterator.hasNext();
        }
        catch (IndexOutOfBoundsException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, file);
        }
        catch (IOError e)
        {
            if (e.getCause() instanceof IOException)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e.getCause(), file);
            }
            else
            {
                throw e;
            }
        }
    }

    public Unfiltered next()
    {
        try
        {
            if (isClosed)
                throw new IllegalStateException("Iterator used after closing.");

            return doCompute();
        }
        catch (IndexOutOfBoundsException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, file);
        }
        catch (IOError e)
        {
            if (e.getCause() instanceof IOException)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException((Exception)e.getCause(), file);
            }
            else
            {
                throw e;
            }
        }
    }

    protected Unfiltered doCompute()
    {
        Unfiltered unfiltered = iterator.next();
        UnfilteredValidation.maybeValidateUnfiltered(unfiltered, metadata(), key, sstable);
        return unfiltered;
    }

    public void close()
    {
        // creator is responsible for closing file when finished
        isClosed = true;
    }

    public boolean isClosed()
    {
        return isClosed;
    }

    /**
     * Called to advance to the next partition and make sure that we process all outstanding rows if user did not
     * do so. Unlike next() and hasNext(), this can and will be called after the iterator is closed.
     */
    public void exhaust()
    {
        try
        {
            while (iterator.hasNext())
                iterator.next();
        }
        catch (IndexOutOfBoundsException | AssertionError e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, file);
        }
        catch (IOError e)
        {
            if (e.getCause() instanceof IOException)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException((Exception)e.getCause(), file);
            }
            else
            {
                throw e;
            }
        }
    }

    public File getFile()
    {
        return file;
    }

    public EncodingStats stats()
    {
        // We could return sstable.header.stats(), but this may not be as accurate than the actual sstable stats (see
        // SerializationHeader.make() for details) so we use the latter instead.
        return sstable.stats();
    }

    public int compareTo(SSTableIdentityIterator o)
    {
        return key.compareTo(o.key);
    }
}
