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

package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import java.util.concurrent.CompletionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;

/**
 * A flow of {@link IndexFileEntry} for the
 * trie index table format.
 */
public class TrieIndexFileFlow extends FlowSource<IndexFileEntry>
{
    private static final Logger logger = LoggerFactory.getLogger(TrieIndexFileFlow.class);

    private final RandomAccessReader dataFileReader;
    private final TrieIndexSSTableReader sstable;
    private final PartitionIndex partitionIndex;
    private final IPartitioner partitioner;
    private final PartitionPosition left;
    private final int inclusiveLeft;
    private final PartitionPosition right;
    private final int exclusiveRight;
    private final FileHandle rowIndexFile;
    private final TPCScheduler onReadyExecutor;

    private FileDataInput rowIndexFileReader;
    private PartitionIndex.IndexPosIterator posIterator;
    private long pos = PartitionIndex.NOT_FOUND;
    private IndexFileEntry next;

    public TrieIndexFileFlow(RandomAccessReader dataFileReader,
                             TrieIndexSSTableReader sstable,
                             PartitionPosition left,
                             int inclusiveLeft,
                             PartitionPosition right,
                             int exclusiveRight)
    {
        this.dataFileReader = dataFileReader;
        this.sstable = sstable;
        this.partitionIndex = sstable.partitionIndex;
        this.partitioner = sstable.metadata().partitioner;
        this.left = left;
        this.inclusiveLeft = inclusiveLeft;
        this.right = right;
        this.exclusiveRight = exclusiveRight;
        this.rowIndexFile = sstable.rowIndexFile;
        this.onReadyExecutor = TPC.bestTPCScheduler();
    }

    public void requestNext()
    {
        readWithRetry(0);
    }

    private void readWithRetry(int retryNum)
    {
        try
        {
           if (TPC.READ_ASYNC_MAX_CACHE_MISSES > 0 && retryNum >= TPC.READ_ASYNC_MAX_CACHE_MISSES)
                throw new IllegalStateException(String.format("Too many NotInCacheExceptions (%d) in trie index flow", retryNum));

            IndexFileEntry current = next == null ? readFirst() : readNext();
            if (current != IndexFileEntry.EMPTY)
                subscriber.onNext(current);
            else
                subscriber.onComplete();
        }
        catch (Rebufferer.NotInCacheException e)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} - NotInCacheException at retry number {}: {}", hashCode(), retryNum, e.getMessage());

            // Retry the request once data is in the cache
            e.accept(this.getClass(),
                     () -> readWithRetry(retryNum + 1),
                     (t) ->
                     {
                         logger.error("Failed to retry due to exception", t);

                         // Calling completeExceptionally() wraps the original exception into a CompletionException even
                         // though the documentation says otherwise
                         if (t instanceof CompletionException && t.getCause() != null)
                             t = t.getCause();

                         subscriber.onError(t);
                         return null;
                     },
                     onReadyExecutor);
        }
        catch (CorruptSSTableException | IOException e)
        {
            sstable.markSuspect();
            subscriber.onError(new CorruptSSTableException(e, sstable.getFilename()));
        }
        catch (Throwable t)
        {
            subscriber.onError(t);
        }
    }

    /**
     * Perform late initialization, that is initialization that is deferred until the first item is requested,
     * so that we don't leak resources if the flow is never subscribed to. This can be called multiple times
     * in case of {@link Rebufferer.NotInCacheException}s.
     */
    private void lateInitialization()
    {
        Rebufferer.ReaderConstraint rc = Rebufferer.ReaderConstraint.ASYNC;

        if (rowIndexFileReader == null)
            rowIndexFileReader = rowIndexFile.createReader(rc);

        if (posIterator == null)
            posIterator = new PartitionIndex.IndexPosIterator(partitionIndex, left, right, rc, FileAccessType.RANDOM); // can throw NotInCacheException
    }

    private IndexFileEntry readFirst() throws IOException
    {
        lateInitialization(); // can throw NotInCacheException

        next = readEntry(); // can throw NotInCacheException
        if (next.key != null && !(next.key.compareTo(left) > inclusiveLeft))
        {
            next = null;
            next = readEntry(); // can throw NotInCacheException
        }

        return readNext(); // can throw NotInCacheException
    }

    private IndexFileEntry readNext() throws IOException
    {
        IndexFileEntry ret = next;

        if (ret == IndexFileEntry.EMPTY)
            return ret; // we are done

        next = readEntry(); // can throw NotInCacheException

        // if next is empty, then ret is the last partition to be published, in this case we check against
        // any right limit and suppress the last partition if it is beyond the limit
        if (next == IndexFileEntry.EMPTY && right != null && ret.key.compareTo(right) > exclusiveRight)
            return IndexFileEntry.EMPTY; // exclude last partition outside range

        return ret;
    }

    /**
     * Returns the next index file entry by reading the position first, then reading either the
     * row index file or the data file depending on where the position points to.
     *
     * This method must be async-read-safe, {@link PartitionIndex.IndexPosIterator#nextIndexPos()} as well as
     * reading from the row index or data files may all throw {@link Rebufferer.NotInCacheException}
     * and we must be ready to re-enter this method in a consistent state.
     */
    private IndexFileEntry readEntry() throws IOException
    {
        if (pos == PartitionIndex.NOT_FOUND)
            pos = posIterator.nextIndexPos();

        IndexFileEntry ret;
        if (pos != PartitionIndex.NOT_FOUND)
        {
            if (pos >= 0)
            {
                if (pos != rowIndexFileReader.getSeekPosition())
                    rowIndexFileReader.seek(pos);

                ret = new IndexFileEntry(partitioner.decorateKey(ByteBufferUtil.readWithShortLength(rowIndexFileReader)),
                        TrieIndexEntry.deserialize(rowIndexFileReader, rowIndexFileReader.getSeekPosition()));
            }
            else
            {
                long dataPos = ~pos;
                if (dataPos != dataFileReader.getSeekPosition())
                    dataFileReader.seek(dataPos);

                ret = new IndexFileEntry(partitioner.decorateKey(ByteBufferUtil.readWithShortLength(dataFileReader)),
                        new RowIndexEntry(dataPos));
            }

            pos = PartitionIndex.NOT_FOUND; // make sure next time we get the next pos
        }
        else
        {
            ret = IndexFileEntry.EMPTY;
        }

        //logger.debug("{} - Read @pos {}: {}", hashCode(), pos, ret);
        return ret;
    }


    public void close() throws Exception
    {
        Throwable err = Throwables.closeNonNull(null, posIterator, rowIndexFileReader);
        Throwables.maybeFail(err);
    }
}
