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

package org.apache.cassandra.io.sstable.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.filter.BloomFilterTracker;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.utils.concurrent.SharedCloseable.sharedCopyOrNull;

public abstract class SSTableReaderWithFilter extends SSTableReader
{
    protected volatile IFilter filter;
    private final AtomicBoolean bfDeserializationStarted = new AtomicBoolean(false);
    private final boolean bloomFilterLazyLoading = BloomFilter.lazyLoading();
    private final int bloomFilterLazyLoadingWindow = BloomFilter.lazyLoadingWindow();
    private final long bloomFilterLazyLoadingThreshold = BloomFilter.lazyLoadingThreshold();

    private final BloomFilterTracker filterTracker;

    protected SSTableReaderWithFilter(Builder<?, ?> builder, Owner owner)
    {
        super(builder, owner);
        this.filter = Objects.requireNonNull(builder.getFilter());
        this.filterTracker = owner().map(Owner::getBloomFilterTracker)
                                    .orElseGet(BloomFilterTracker::createNoopTracker);
    }

    @Override
    protected List<AutoCloseable> setupInstance(boolean trackHotness)
    {
        ArrayList<AutoCloseable> closeables = Lists.newArrayList(filter);
        closeables.addAll(super.setupInstance(trackHotness));
        return closeables;
    }

    protected final <B extends Builder<?, B>> B unbuildTo(B builder, boolean sharedCopy)
    {
        B b = super.unbuildTo(builder, sharedCopy);
        if (builder.getFilter() == null)
            b.setFilter(sharedCopy ? sharedCopyOrNull(filter) : filter);
        return b;
    }

    protected boolean isPresentInFilter(IFilter.FilterKey key)
    {
        return filter.isPresent(key);
    }

    @Override
    public boolean mayContainAssumingKeyIsInRange(DecoratedKey key)
    {
        maybeDeserializeLazyBloomFilter();
        // if we don't have bloom filter(bf_fp_chance=1.0 or filter file is missing),
        // we check index file instead.
        return !filter.isInformative() && getPosition(key, Operator.EQ, false) >= 0 || filter.isPresent(key);
    }

    protected boolean inBloomFilter(DecoratedKey dk)
    {
        maybeDeserializeLazyBloomFilter();
        return filter.isPresent(dk);
    }

    /**
     * Defer BF deserialization when enabled to reduce memory pressure in use case where many sstables are not accessed frequently
     *
     * @return true if BF deserialization is attempted; false otherwise.
     */
    @VisibleForTesting
    boolean maybeDeserializeLazyBloomFilter()
    {
        if (!bloomFilterLazyLoading || filter != FilterFactory.AlwaysPresentForLazyLoading)
            return false;

        Preconditions.checkState(partitionIndexReadMeter.isPresent(), "Read index meter should have been available");

        boolean loadBloomFilter = false;

        // If the threshold was set to zero we always want to deserialize
        if (bloomFilterLazyLoadingThreshold == 0)
            loadBloomFilter = true;
            // otherwise, if window is <= 0 we use the threshold as an absolute count
        else if (bloomFilterLazyLoadingWindow <= 0 && partitionIndexReadMeter.get().count() >= bloomFilterLazyLoadingThreshold)
            loadBloomFilter = true;
            // otherwise we look at the count in the specified window
        else if (bloomFilterLazyLoadingWindow > 0 && partitionIndexReadMeter.get().rate(bloomFilterLazyLoadingWindow) >= bloomFilterLazyLoadingThreshold)
            loadBloomFilter = true;

        if (!loadBloomFilter)
            return false;

        // concurrent reads should only trigger async bloom filter deserialization once
        if (!bfDeserializationStarted.compareAndSet(false, true))
            return false;

        Stage.IO.execute(() ->
                         {
                             logger.debug("Deserializing lazy bloom filter for {}", descriptor.baseFileURI());

                             // hold sstable reference to prevent sstable being released before bloom filter deserialization completes
                             Ref<SSTableReader> ref = tryRef();
                             if (ref == null)
                             {
                                 logger.error("Unable to reference sstable, will use pass-through bloom filter");
                                 filter = FilterFactory.AlwaysPresent;
                             }
                             else
                             {
                                 try
                                 {
                                     // the only recoverable BF deserialization error is remote storage timeout; but it should be
                                     // fine to continue with pass-through filter and wait for compaction to replace current sstable.
                                     IFilter loaded = FilterComponent.load(descriptor);
                                     if (loaded == null)
                                     {
                                         filter = FilterFactory.AlwaysPresent;
                                         logger.error("Failed to deserialize lazy bloom filter, will use pass-through bloom filter");
                                     }
                                     else
                                     {
                                         logger.debug("Successfuly loaded lazy bloom filter for {}", descriptor.baseFileURI());

                                         filter = loaded;
                                         tidy.addCloseable(loaded); // close newly created bloom filter on sstable close
                                     }
                                 }
                                 catch (IOException e)
                                 {
                                     logger.info("Bloom filter for " + descriptor + " could not be deserialized", e);
                                 }
                                 finally
                                 {
                                     ref.release();
                                 }
                             }
                         });

        return true;
    }

    @Override
    protected void notifySelected(SSTableReadsListener.SelectionReason reason, SSTableReadsListener localListener, Operator op, boolean updateStats, AbstractRowIndexEntry entry)
    {
        super.notifySelected(reason, localListener, op, updateStats, entry);

        if (!(updateStats && op == SSTableReader.Operator.EQ))
            return;

        filterTracker.addTruePositive();
    }

    @Override
    protected void notifySkipped(SSTableReadsListener.SkippingReason reason, SSTableReadsListener localListener, Operator op, boolean updateStats)
    {
        super.notifySkipped(reason, localListener, op, updateStats);

        if (!updateStats)
            return;

        switch (reason)
        {
            case BLOOM_FILTER:
                filterTracker.addTrueNegative();
                break;
            case MIN_MAX_KEYS:
                // checking bloom filter against keys outside the sstable range make no sense so collecting
                // statistics on that makes no sense either
                break;
            default:
                if (op == SSTableReader.Operator.EQ)
                    filterTracker.addFalsePositive();
        }
    }

    public BloomFilterTracker getFilterTracker()
    {
        return filterTracker;
    }

    public long getFilterSerializedSize()
    {
        return filter.serializedSize(descriptor.version.hasOldBfFormat());
    }

    public long getFilterOffHeapSize()
    {
        return filter.offHeapSize();
    }
    
    @VisibleForTesting
    public IFilter getFilter()
    {
        return filter;
    }

    public abstract SSTableReaderWithFilter cloneAndReplace(IFilter filter);

    public abstract static class Builder<R extends SSTableReaderWithFilter, B extends Builder<R, B>> extends SSTableReader.Builder<R, B>
    {
        private IFilter filter;

        public Builder(Descriptor descriptor)
        {
            super(descriptor);
        }

        public B setFilter(IFilter filter)
        {
            this.filter = filter;
            return (B) this;
        }

        public IFilter getFilter()
        {
            return this.filter;
        }
    }
}
