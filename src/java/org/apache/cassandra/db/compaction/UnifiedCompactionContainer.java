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

package org.apache.cassandra.db.compaction;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.ScannerList;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.schema.CompactionParams;

public class UnifiedCompactionContainer implements CompactionStrategyContainer
{
    private final CompactionStrategyFactory factory;
    private final CompactionParams params;
    private final CompactionParams metadataParams;
    private final UnifiedCompactionStrategy strategy;
    private final boolean enableAutoCompaction;
    private final boolean hasVector;

    AtomicBoolean enabled;

    UnifiedCompactionContainer(CompactionStrategyFactory factory,
                               BackgroundCompactions backgroundCompactions,
                               CompactionParams params,
                               CompactionParams metadataParams,
                               boolean enabled,
                               boolean enableAutoCompaction)
    {
        this.factory = factory;
        this.params = params;
        this.metadataParams = metadataParams;
        this.strategy = new UnifiedCompactionStrategy(factory, backgroundCompactions, params.options());
        this.enabled = new AtomicBoolean(enabled);
        this.enableAutoCompaction = enableAutoCompaction;
        this.hasVector = strategy.getController().hasVectorType();

        factory.getCompactionLogger().strategyCreated(this.strategy);

        if (this.strategy.getOptions().isLogEnabled())
            factory.getCompactionLogger().enable();
        else
            factory.getCompactionLogger().disable();

        startup();
    }

    @Override
    public void enable()
    {
        this.enabled.set(true);
    }

    @Override
    public void disable()
    {
        this.enabled.set(false);
    }

    @Override
    public boolean isEnabled()
    {
        return enableAutoCompaction && enabled.get() && strategy.isActive;
    }

    @Override
    public boolean isActive()
    {
        return strategy.isActive;
    }

    public static CompactionStrategyContainer create(@Nullable CompactionStrategyContainer previous,
                                                     CompactionStrategyFactory strategyFactory,
                                                     CompactionParams compactionParams,
                                                     CompactionStrategyContainer.ReloadReason reason,
                                                     boolean enableAutoCompaction)
    {
        boolean enabled = CompactionStrategyFactory.enableCompactionOnReload(previous, compactionParams, reason);
        BackgroundCompactions backgroundCompactions;
        // inherit compactions history from previous UCS container
        if (previous instanceof UnifiedCompactionContainer)
            backgroundCompactions = ((UnifiedCompactionContainer) previous).getBackgroundCompactions();

        // for other cases start from scratch
        // We don't inherit from legacy compactions right now because there are multiple strategies and we'd need
        // to merge their BackgroundCompactions to support that. Merging per se is not tricky, but the bigger problem
        // is aggregate cleanup. We'd need to unsubscribe from compaction tasks by legacy strategies and subscribe
        // by the new UCS to remove inherited ongoing compactions when they complete.
        // We might want to revisit this issue later to improve UX.
        else
            backgroundCompactions = new BackgroundCompactions(strategyFactory.getRealm());
        CompactionParams metadataParams = createMetadataParams(previous, compactionParams, reason);

        if (previous != null)
            previous.shutdown();

        return new UnifiedCompactionContainer(strategyFactory,
                                              backgroundCompactions,
                                              compactionParams,
                                              metadataParams,
                                              enabled,
                                              enableAutoCompaction);
    }

    @Override
    public CompactionStrategyContainer reload(@Nonnull CompactionStrategyContainer previous,
                                              CompactionParams compactionParams,
                                              ReloadReason reason)
    {
        return create(previous, factory, compactionParams, reason, enableAutoCompaction);
    }

    @Override
    public boolean shouldReload(CompactionParams params, ReloadReason reason)
    {
        return reason != CompactionStrategyContainer.ReloadReason.METADATA_CHANGE
               || !params.equals(getMetadataCompactionParams())
               || hasVector != factory.getRealm().metadata().hasVectorType();
    }

    private static CompactionParams createMetadataParams(@Nullable CompactionStrategyContainer previous,
                                                         CompactionParams compactionParams,
                                                         ReloadReason reason)
    {
        CompactionParams metadataParams;
        if (reason == CompactionStrategyContainer.ReloadReason.METADATA_CHANGE)
            // metadataParams are aligned with compactionParams. We do not access TableParams.compaction to avoid racing with
            // concurrent ALTER TABLE metadata change.
            metadataParams = compactionParams;
        else if (previous != null)
            metadataParams = previous.getMetadataCompactionParams();
        else
            metadataParams = null;

        return metadataParams;
    }

    @Override
    public CompactionParams getCompactionParams()
    {
        return params;
    }

    @Override
    public CompactionParams getMetadataCompactionParams()
    {
        return metadataParams;
    }

    @Override
    public List<CompactionStrategy> getStrategies()
    {
        return ImmutableList.of(strategy);
    }

    @Override
    public List<CompactionStrategy> getStrategies(boolean isRepaired, @Nullable UUID pendingRepair)
    {
        return getStrategies();
    }

    @Override
    public void repairSessionCompleted(UUID sessionID)
    {
        // We are not tracking SSTables, so nothing to do here.
    }

    /**
     * UCC does not need to use this method with {@link CompactionRealm#mutateRepairedWithLock}
     * @return null
     */
    @Override
    public ReentrantReadWriteLock.WriteLock getWriteLock()
    {
        return null;
    }

    @Override
    public CompactionLogger getCompactionLogger()
    {
        return strategy.compactionLogger;
    }

    @Override
    public void pause()
    {
        strategy.pause();
    }

    @Override
    public void resume()
    {
        strategy.resume();
    }

    @Override
    public void startup()
    {
        strategy.startup();
    }

    @Override
    public void shutdown()
    {
        strategy.shutdown();
    }

    @Override
    public Collection<AbstractCompactionTask> getNextBackgroundTasks(int gcBefore)
    {
        return strategy.getNextBackgroundTasks(gcBefore);
    }

    @Override
    public CompactionTasks getMaximalTasks(int gcBefore, boolean splitOutput, int permittedParallelism)
    {
        return strategy.getMaximalTasks(gcBefore, splitOutput, permittedParallelism);
    }

    @Override
    public CompactionTasks getUserDefinedTasks(Collection<? extends CompactionSSTable> sstables, int gcBefore)
    {
        return strategy.getUserDefinedTasks(sstables, gcBefore);
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        return strategy.getEstimatedRemainingTasks();
    }

    @Override
    public AbstractCompactionTask createCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes)
    {
        return strategy.createCompactionTask(txn, gcBefore, maxSSTableBytes);
    }

    @Override
    public int getTotalCompactions()
    {
        return strategy.getTotalCompactions();
    }

    @Override
    public List<CompactionStrategyStatistics> getStatistics()
    {
        return strategy.getStatistics();
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return strategy.getMaxSSTableBytes();
    }

    @Override
    public int[] getSSTableCountPerLevel()
    {
        return strategy.getSSTableCountPerLevel();
    }

    @Override
    public int getLevelFanoutSize()
    {
        return strategy.getLevelFanoutSize();
    }

    @Override
    public ScannerList getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
    {
        return strategy.getScanners(sstables, ranges);
    }

    @Override
    public String getName()
    {
        return strategy.getName();
    }

    @Override
    public Set<? extends CompactionSSTable> getSSTables()
    {
        return strategy.getSSTables();
    }

    @Override
    public Collection<Collection<CompactionSSTable>> groupSSTablesForAntiCompaction(Collection<? extends CompactionSSTable> sstablesToGroup)
    {
        return strategy.groupSSTablesForAntiCompaction(sstablesToGroup);
    }

    @Override
    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                       long keyCount,
                                                       long repairedAt,
                                                       UUID pendingRepair,
                                                       boolean isTransient,
                                                       IntervalSet<CommitLogPosition> commitLogPositions,
                                                       int sstableLevel,
                                                       SerializationHeader header,
                                                       Collection<Index.Group> indexGroups,
                                                       LifecycleNewTracker lifecycleNewTracker)
    {
        return strategy.createSSTableMultiWriter(descriptor,
                                                 keyCount,
                                                 repairedAt,
                                                 pendingRepair,
                                                 isTransient,
                                                 commitLogPositions,
                                                 sstableLevel,
                                                 header,
                                                 indexGroups,
                                                 lifecycleNewTracker);
    }

    @Override
    public boolean supportsEarlyOpen()
    {
        return strategy.supportsEarlyOpen();
    }

    @Override
    public void periodicReport()
    {
        strategy.periodicReport();
    }

    @Override
    public Map<String, String> getMaxOverlapsMap()
    {
        return strategy.getMaxOverlapsMap();
    }

    BackgroundCompactions getBackgroundCompactions()
    {
        return strategy.backgroundCompactions;
    }

    @Override
    public void onInProgress(CompactionProgress progress)
    {
        strategy.onInProgress(progress);
    }

    @Override
    public void onCompleted(UUID id, Throwable err)
    {
        strategy.onCompleted(id, err);
    }

    @Override
    public void handleNotification(INotification notification, Object sender)
    {
        // TODO - this is a no-op because the strategy is stateless but we could detect here
        // sstables that are added either because of streaming or because of nodetool refresh
    }
}
