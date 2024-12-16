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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.Reservations;
import org.apache.cassandra.db.compaction.unified.ShardedMultiWriter;
import org.apache.cassandra.db.compaction.unified.UnifiedCompactionTask;
import org.apache.cassandra.db.lifecycle.CompositeLifecycleTransaction;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.PartialLifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Overlaps;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.utils.Throwables.perform;

/// The design of the unified compaction strategy is described in [UnifiedCompactionStrategy.md](./UnifiedCompactionStrategy.md).
///
/// See also [CEP-26](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-26%3A+Unified+Compaction+Strategy).
public class UnifiedCompactionStrategy extends AbstractCompactionStrategy
{
    @SuppressWarnings("unused") // accessed via reflection
    public static final Class<? extends CompactionStrategyContainer> CONTAINER_CLASS = UnifiedCompactionContainer.class;

    private static final Logger logger = LoggerFactory.getLogger(UnifiedCompactionStrategy.class);

    public static final int MAX_LEVELS = 32;   // This is enough for a few petabytes of data (with the worst case fan factor
    // at W=0 this leaves room for 2^32 sstables, presumably of at least 1MB each).

    private static final Pattern SCALING_PARAMETER_PATTERN = Pattern.compile("(N)|L(\\d+)|T(\\d+)|([+-]?\\d+)");
    private static final String SCALING_PARAMETER_PATTERN_SIMPLIFIED = SCALING_PARAMETER_PATTERN.pattern()
                                                                                                .replaceAll("[()]", "")
                                                                                                .replace("\\d", "[0-9]");

    /// Special level definition for major compactions.
    static final Level LEVEL_MAXIMAL = new Level(-1, 0, 0, 0, 1, 0, Double.POSITIVE_INFINITY);

    private final Controller controller;

    private volatile ArenaSelector currentArenaSelector;
    private volatile ShardManager currentShardManager;

    private long lastExpiredCheck;

    public UnifiedCompactionStrategy(CompactionStrategyFactory factory, BackgroundCompactions backgroundCompactions, Map<String, String> options)
    {
        this(factory, backgroundCompactions, options, Controller.fromOptions(factory.getRealm(), options));
    }

    public UnifiedCompactionStrategy(CompactionStrategyFactory factory, BackgroundCompactions backgroundCompactions, Controller controller)
    {
        this(factory, backgroundCompactions, new HashMap<>(), controller);
    }

    public UnifiedCompactionStrategy(CompactionStrategyFactory factory, BackgroundCompactions backgroundCompactions, Map<String, String> options, Controller controller)
    {
        super(factory, backgroundCompactions, options);
        this.controller = controller;
    }

    @VisibleForTesting
    public UnifiedCompactionStrategy(CompactionStrategyFactory factory, Controller controller)
    {
        this(factory, new BackgroundCompactions(factory.getRealm()), new HashMap<>(), controller);
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        return Controller.validateOptions(CompactionStrategyOptions.validateOptions(options));
    }

    public void storeControllerConfig()
    {
        getController().storeControllerConfig();
    }

    public static int fanoutFromScalingParameter(int w)
    {
        return w < 0 ? 2 - w : 2 + w; // see formula in design doc
    }

    public static int thresholdFromScalingParameter(int w)
    {
        return w <= 0 ? 2 : 2 + w; // see formula in design doc
    }

    public static int parseScalingParameter(String value)
    {
        Matcher m = SCALING_PARAMETER_PATTERN.matcher(value);
        if (!m.matches())
            throw new ConfigurationException("Scaling parameter " + value + " must match " + SCALING_PARAMETER_PATTERN_SIMPLIFIED);

        if (m.group(1) != null)
            return 0;
        else if (m.group(2) != null)
            return 2 - atLeast2(Integer.parseInt(m.group(2)), value);
        else if (m.group(3) != null)
            return atLeast2(Integer.parseInt(m.group(3)), value) - 2;
        else
            return Integer.parseInt(m.group(4));
    }

    private static int atLeast2(int value, String str)
    {
        if (value < 2)
            throw new ConfigurationException("Fan factor cannot be lower than 2 in " + str);
        return value;
    }

    public static String printScalingParameter(int w)
    {
        if (w < 0)
            return 'L' + Integer.toString(2 - w);
        else if (w > 0)
            return 'T' + Integer.toString(w + 2);
        else
            return "N";
    }

    /// Make a time-based UUID for unified compaction tasks with sequence 0. The reason to do this is to accommodate
    /// parallelized compactions:
    /// - Sequence 0 (visible as `-8000-` in the UUID string) denotes single-task (i.e. non-parallelized) compactions.
    /// - Sequence >0 (`-800n-`) denotes the individual task's index of a parallelized compaction.
    /// - Parallelized compactions use sequence 0 as the transaction id, and sequences from 1 to the number of tasks
    ///   for the ids of individual tasks.
    public static UUID nextTimeUUID()
    {
        return UUIDGen.withSequence(UUIDGen.getTimeUUID(), 0);
    }

    @Override
    public Collection<Collection<CompactionSSTable>> groupSSTablesForAntiCompaction(Collection<? extends CompactionSSTable> sstablesToGroup)
    {
        Collection<Collection<CompactionSSTable>> groups = new ArrayList<>();
        for (Arena arena : getCompactionArenas(sstablesToGroup, (i1, i2) -> true)) // take all sstables
        {
            groups.addAll(super.groupSSTablesForAntiCompaction(arena.sstables));
        }

        return groups;
    }

    @Override
    public synchronized CompactionTasks getUserDefinedTasks(Collection<? extends CompactionSSTable> sstables, int gcBefore)
    {
        // The tasks need to be split by repair status and disk, but otherwise we must assume the user knows what they
        // are doing.
        List<AbstractCompactionTask> tasks = new ArrayList<>();
        for (Arena arena : getCompactionArenas(sstables, UnifiedCompactionStrategy::isSuitableForCompaction))
            tasks.addAll(super.getUserDefinedTasks(arena.sstables, gcBefore));
        return CompactionTasks.create(tasks);
    }

    /// Get a list of maximal aggregates that can be compacted independently in parallel to achieve a major compaction.
    ///
    /// These aggregates split the sstables in each arena into non-overlapping groups where the boundaries between these
    /// groups are also boundaries of the current sharding configuration. Compacting the groups independently has the
    /// same effect as compacting all of the sstables in the arena together in one operation.
    public synchronized List<CompactionAggregate.UnifiedAggregate> getMaximalAggregates()
    {
        maybeUpdateSelector();
        // The aggregates are split into arenas by repair status and disk, as well as in non-overlapping sections to
        // enable some parallelism and efficient use of extra space. The result will be split across shards according to
        // its density.
        // Depending on the parallelism, the operation may require up to 100% extra space to complete.
        List<CompactionAggregate.UnifiedAggregate> aggregates = new ArrayList<>();

        for (Arena arena : getCompactionArenas(realm.getLiveSSTables(), UnifiedCompactionStrategy::isSuitableForCompaction))
        {
            // If possible, we want to issue separate compactions for non-overlapping sets of sstables, to allow
            // for smaller extra space requirements. However, if the sharding configuration has changed, a major
            // compaction should combine non-overlapping sets if they are split on a boundary that is no longer
            // in effect.
            List<Set<CompactionSSTable>> groups =
            getShardManager().splitSSTablesInShards(arena.sstables,
                                                    makeShardingStats(arena.sstables).shardCountForDensity,
                                                    (sstableShard, shardRange) -> Sets.newHashSet(sstableShard));

            // Now combine all of these groups that share an sstable so that we have valid independent transactions.
            groups = Overlaps.combineSetsWithCommonElement(groups);

            for (Set<CompactionSSTable> group : groups)
            {
                aggregates.add(CompactionAggregate.createUnified(group,
                                                                 Overlaps.maxOverlap(group,
                                                                                     CompactionSSTable.startsAfter,
                                                                                     CompactionSSTable.firstKeyComparator,
                                                                                     CompactionSSTable.lastKeyComparator),
                                                                 CompactionPick.create(nextTimeUUID(), LEVEL_MAXIMAL.index, group),
                                                                 Collections.emptyList(),
                                                                 arena,
                                                                 LEVEL_MAXIMAL));
            }
        }
        return aggregates;
    }

    @Override
    public synchronized CompactionTasks getMaximalTasks(int gcBefore, boolean splitOutput, int permittedParallelism)
    {
        if (permittedParallelism <= 0)
            permittedParallelism = Integer.MAX_VALUE;

        List<AbstractCompactionTask> tasks = new ArrayList<>();
        try
        {
            // Split the space into independently compactable groups.
            for (var aggregate : getMaximalAggregates())
            {
                LifecycleTransaction txn = realm.tryModify(aggregate.getSelected().sstables(),
                                                           OperationType.COMPACTION,
                                                           aggregate.getSelected().id());

                // Create (potentially parallelized) tasks for each group.
                if (txn != null)
                    createAndAddTasks(gcBefore, txn, getShardingStats(aggregate), permittedParallelism, tasks);
                // we ignore splitOutput (always split according to the strategy's sharding) and do not need isMaximal

                // Note: major compactions should not end up in the background compactions tracker to avoid wreaking
                // havok in the thread assignment logic.
            }

            // If we have more arenas/non-overlapping sets than the permitted parallelism, we will try to run all the
            // individual tasks in parallel (including as parallelized compactions) so that they finish quickest and release
            // any space they hold, and then reuse the compaction thread to run the next set of tasks.
            return CompactionTasks.create(CompositeCompactionTask.applyParallelismLimit(tasks, permittedParallelism));
        }
        catch (Throwable t)
        {
            throw rejectTasks(tasks, t);
        }
    }

    @Override
    public void startup()
    {
        perform(super::startup,
                () -> controller.startup(this, ScheduledExecutors.scheduledTasks));
    }

    @Override
    public void shutdown()
    {
        perform(super::shutdown,
                controller::shutdown);
    }

    /// Returns a collections of compaction tasks.
    ///
    /// This method is synchornized because task creation is significantly more expensive in UCS; the strategy is
    /// stateless, therefore it has to compute the shard/bucket structure on each call.
    ///
    /// @param gcBefore throw away tombstones older than this
    /// @return collection of AbstractCompactionTask, which could be either a CompactionTask or an UnifiedCompactionTask
    @Override
    public synchronized Collection<AbstractCompactionTask> getNextBackgroundTasks(int gcBefore)
    {
        // TODO - we should perhaps consider executing this code less frequently than legacy strategies
        // since it's more expensive, and we should therefore prevent a second concurrent thread from executing at all

        // Repairs can leave behind sstables in pending repair state if they race with a compaction on those sstables.
        // Both the repair and the compact process can't modify the same sstables set at the same time. So compaction
        // is left to eventually move those sstables from FINALIZED repair sessions away from repair states.
        Collection<AbstractCompactionTask> repairFinalizationTasks = ActiveRepairService
                                                                     .instance
                                                                     .consistent
                                                                     .local
                                                                     .getZombieRepairFinalizationTasks(realm, realm.getLiveSSTables());
        if (!repairFinalizationTasks.isEmpty())
            return repairFinalizationTasks;

        // Expirations have to run before compaction (if run in parallel they may cause overlap tracker to leave
        // unnecessary tombstones in place), so return only them if found.
        Collection<AbstractCompactionTask> expirationTasks = getExpirationTasks(gcBefore);
        if (expirationTasks != null)
            return expirationTasks;

        return getNextBackgroundTasks(getNextCompactionAggregates(), gcBefore);
    }

    /// Check for fully expired sstables and return a collection of expiration tasks if found.
    public Collection<AbstractCompactionTask> getExpirationTasks(int gcBefore)
    {
        long ts = System.currentTimeMillis();
        boolean expiredCheck = ts - lastExpiredCheck > controller.getExpiredSSTableCheckFrequency();
        if (!expiredCheck)
            return null;
        lastExpiredCheck = ts;

        var expired = getFullyExpiredSSTables(gcBefore);
        if (expired.isEmpty())
            return null;

        if (logger.isDebugEnabled())
            logger.debug("Expiration check found {} fully expired SSTables", expired.size());

        return createExpirationTasks(expired);
    }

    /// Create expiration tasks for the given set of expired sstables.
    /// Used by CNDB
    public List<AbstractCompactionTask> createExpirationTasks(Set<CompactionSSTable> expired)
    {
        // if we found sstables to expire, split them to arenas to correctly isolate their repair status.
        var tasks = new ArrayList<AbstractCompactionTask>();
        try
        {
            for (var arena : getCompactionArenas(expired, (i1, i2) -> true))
            {
                LifecycleTransaction txn = realm.tryModify(arena.sstables, OperationType.COMPACTION);
                if (txn != null)
                    tasks.add(createExpirationTask(txn));
                else
                    logger.warn("Failed to submit expiration task because a transaction could not be created. If this happens frequently, it should be reported");
            }
            return tasks;
        }
        catch (Throwable t)
        {
            throw rejectTasks(tasks, t);
        }
    }

    /// Get all expired sstables, regardless of expiration status.
    /// This is simpler and faster than per-arena collection, and will find nothing in most calls.
    /// Used by CNDB
    public Set<CompactionSSTable> getFullyExpiredSSTables(int gcBefore)
    {
        return CompactionController.getFullyExpiredSSTables(realm,
                                                                   getSuitableSSTables(),
                                                                   realm::getOverlappingLiveSSTables,
                                                                   gcBefore,
                                                                   controller.getIgnoreOverlapsInExpirationCheck());
    }

    /// Used by CNDB where compaction aggregates come from etcd rather than the strategy
    /// @return collection of `AbstractCompactionTask`, which could be either a `CompactionTask` or a `UnifiedCompactionTask`
    public synchronized Collection<AbstractCompactionTask> getNextBackgroundTasks(Collection<CompactionAggregate> aggregates, int gcBefore)
    {
        controller.onStrategyBackgroundTaskRequest();
        return createCompactionTasks(aggregates, gcBefore);
    }

    private Collection<AbstractCompactionTask> createCompactionTasks(Collection<CompactionAggregate> aggregates, int gcBefore)
    {
        Collection<AbstractCompactionTask> tasks = new ArrayList<>(aggregates.size());
        try
        {
            for (CompactionAggregate aggregate : aggregates)
                createAndAddTasks(gcBefore, (CompactionAggregate.UnifiedAggregate) aggregate, tasks);

            return tasks;
        }
        catch (Throwable t)
        {
            throw rejectTasks(tasks, t);
        }
    }

    /// Create compaction tasks for the given aggregate and add them to the given tasks list.
    public void createAndAddTasks(int gcBefore, CompactionAggregate.UnifiedAggregate aggregate, Collection<? super UnifiedCompactionTask> tasks)
    {
        CompactionPick selected = aggregate.getSelected();
        int parallelism = aggregate.getPermittedParallelism();
        Preconditions.checkNotNull(selected);
        Preconditions.checkArgument(!selected.isEmpty());

        LifecycleTransaction transaction = realm.tryModify(selected.sstables(),
                                                           OperationType.COMPACTION,
                                                           selected.id());
        if (transaction != null)
        {
            // This will ignore the range of the operation, which is fine.
            backgroundCompactions.setSubmitted(this, transaction.opId(), aggregate);
            createAndAddTasks(gcBefore, transaction, aggregate.operationRange(), aggregate.keepOriginals(), getShardingStats(aggregate), parallelism, tasks);
        }
        else
        {
            // This can happen e.g. due to a race with upgrade tasks
            logger.error("Failed to submit compaction {} because a transaction could not be created. If this happens frequently, it should be reported", aggregate);
        }
    }

    private static RuntimeException rejectTasks(Iterable<? extends AbstractCompactionTask> tasks, Throwable error)
    {
        for (var task : tasks)
            error = task.rejected(error);
        throw Throwables.throwAsUncheckedException(error);
    }

    public static class ShardingStats
    {
        public final PartitionPosition min;
        public final PartitionPosition max;
        public final long totalOnDiskSize;
        public final double uniqueKeyRatio;
        public final double density;
        public final int shardCountForDensity;
        public final int coveredShardCount;

        /// Construct sharding statistics for the given collection of sstables that are to be compacted in full.
        public ShardingStats(Collection<? extends CompactionSSTable> sstables, ShardManager shardManager, Controller controller)
        {
            assert !sstables.isEmpty();
            // the partition count aggregation is costly, so we only perform this once when the aggregate is selected for execution.
            long onDiskLength = 0;
            long partitionCountSum = 0;
            PartitionPosition min = null;
            PartitionPosition max = null;
            boolean hasOnlySSTableReaders = true;
            for (CompactionSSTable sstable : sstables)
            {
                onDiskLength += sstable.onDiskLength();
                partitionCountSum += sstable.estimatedKeys();
                min = min == null || min.compareTo(sstable.getFirst()) > 0 ? sstable.getFirst() : min;
                max = max == null || max.compareTo(sstable.getLast()) < 0 ? sstable.getLast() : max;
                if (!(sstable instanceof SSTableReader)
                    || ((SSTableReader) sstable).descriptor == null)    // for tests
                    hasOnlySSTableReaders = false;
            }
            long estimatedPartitionCount;
            if (hasOnlySSTableReaders)
                estimatedPartitionCount = SSTableReader.getApproximateKeyCount(Iterables.filter(sstables, SSTableReader.class));
            else
                estimatedPartitionCount = partitionCountSum;

            this.totalOnDiskSize = onDiskLength;
            this.uniqueKeyRatio = 1.0 * estimatedPartitionCount / partitionCountSum;
            this.min = min;
            this.max = max;
            this.density = shardManager.density(onDiskLength, min, max, estimatedPartitionCount);
            this.shardCountForDensity = controller.getNumShards(this.density * shardManager.shardSetCoverage());
            this.coveredShardCount = shardManager.coveredShardCount(min, max, shardCountForDensity);
        }

        /// Construct sharding statistics for the given collection of sstables that are to be partially compacted
        /// in the given operation range. Done by adjusting numbers by the fraction of the sstable that is in range.
        public ShardingStats(Collection<? extends CompactionSSTable> sstables, Range<Token> operationRange, ShardManager shardManager, Controller controller)
        {
            assert !sstables.isEmpty();
            assert operationRange != null;
            long onDiskLengthInRange = 0;
            long partitionCountSum = 0;
            long partitionCountSumInRange = 0;
            PartitionPosition min = null;
            PartitionPosition max = null;
            boolean hasOnlySSTableReaders = true;
            for (CompactionSSTable sstable : sstables)
            {
                PartitionPosition left = sstable.getFirst();
                PartitionPosition right = sstable.getLast();
                boolean extendsBefore = left.getToken().compareTo(operationRange.left) <= 0;
                boolean extendsAfter = !operationRange.right.isMinimum() && right.getToken().compareTo(operationRange.right) > 0;
                if (extendsBefore)
                    left = operationRange.left.nextValidToken().minKeyBound();
                if (extendsAfter)
                    right = operationRange.right.maxKeyBound();
                double fractionInRange = extendsBefore || extendsAfter
                                         ? shardManager.rangeSpanned(left, right) / shardManager.rangeSpanned(sstable.getFirst(), sstable.getLast())
                                         : 1;

                onDiskLengthInRange += (long) (sstable.onDiskLength() * fractionInRange);
                partitionCountSumInRange += (long) (sstable.estimatedKeys() * fractionInRange);
                partitionCountSum += sstable.estimatedKeys();
                min = min == null || min.compareTo(left) > 0 ? left : min;
                max = max == null || max.compareTo(right) < 0 ? right : max;
                if (!(sstable instanceof SSTableReader)
                    || ((SSTableReader) sstable).descriptor == null)    // for tests
                    hasOnlySSTableReaders = false;
            }
            long estimatedPartitionCount;
            if (hasOnlySSTableReaders)
                estimatedPartitionCount = SSTableReader.getApproximateKeyCount(Iterables.filter(sstables, SSTableReader.class));
            else
                estimatedPartitionCount = partitionCountSum;

            this.min = min;
            this.max = max;
            this.totalOnDiskSize = onDiskLengthInRange;
            this.uniqueKeyRatio = 1.0 * estimatedPartitionCount / partitionCountSum;
            this.density = shardManager.density(onDiskLengthInRange, min, max, (long) (partitionCountSumInRange * uniqueKeyRatio));
            this.shardCountForDensity = controller.getNumShards(this.density * shardManager.shardSetCoverage());
            this.coveredShardCount = shardManager.coveredShardCount(min, max, shardCountForDensity);
        }

        /// Testing only, use specified values.
        @VisibleForTesting
        ShardingStats(PartitionPosition min, PartitionPosition max, long totalOnDiskSize, double uniqueKeyRatio, double density, int shardCountForDensity, int coveredShardCount)
        {

            this.min = min;
            this.max = max;
            this.totalOnDiskSize = totalOnDiskSize;
            this.uniqueKeyRatio = uniqueKeyRatio;
            this.density = density;
            this.shardCountForDensity = shardCountForDensity;
            this.coveredShardCount = coveredShardCount;
        }
    }

    /// Get and store the sharding stats for a given aggregate
    public ShardingStats getShardingStats(CompactionAggregate.UnifiedAggregate aggregate)
    {
        var shardingStats = aggregate.getShardingStats();
        if (shardingStats == null)
        {
            final Range<Token> operationRange = aggregate.operationRange();
            shardingStats = operationRange != null
                            ? new ShardingStats(aggregate.getSelected().sstables(), operationRange, getShardManager(), controller)
                            : new ShardingStats(aggregate.getSelected().sstables(), getShardManager(), controller);
            aggregate.setShardingStats(shardingStats);
        }
        return shardingStats;
    }

    ShardingStats makeShardingStats(ILifecycleTransaction txn)
    {
        return makeShardingStats(txn.originals());
    }

    ShardingStats makeShardingStats(Collection<? extends CompactionSSTable> sstables)
    {
        return new ShardingStats(sstables, getShardManager(), controller);
    }

    void createAndAddTasks(int gcBefore,
                           LifecycleTransaction transaction,
                           ShardingStats shardingStats,
                           int parallelism,
                           Collection<? super CompactionTask> tasks)
    {
        createAndAddTasks(gcBefore, transaction, null, false, shardingStats, parallelism, tasks);
    }

    @VisibleForTesting
    void createAndAddTasks(int gcBefore,
                           LifecycleTransaction transaction,
                           Range<Token> operationRange,
                           boolean keepOriginals,
                           ShardingStats shardingStats,
                           int parallelism,
                           Collection<? super UnifiedCompactionTask> tasks)
    {
        if (controller.parallelizeOutputShards() && parallelism > 1)
            tasks.addAll(createParallelCompactionTasks(transaction, operationRange, keepOriginals, shardingStats, gcBefore, parallelism));
        else
            tasks.add(createCompactionTask(transaction, operationRange, keepOriginals, shardingStats, gcBefore));
    }

    /// Create the sstable writer used for flushing.
    ///
    /// @return an sstable writer that will split sstables into a number of shards as calculated by the controller for
    ///         the expected flush density.
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
        ShardManager shardManager = getShardManager();
        double flushDensity = realm.metrics().flushSizeOnDisk().get() * shardManager.shardSetCoverage() / shardManager.localSpaceCoverage();
        ShardTracker boundaries = shardManager.boundaries(controller.getFlushShards(flushDensity));
        return new ShardedMultiWriter(realm,
                                      descriptor,
                                      keyCount,
                                      repairedAt,
                                      pendingRepair,
                                      isTransient,
                                      commitLogPositions,
                                      header,
                                      indexGroups,
                                      lifecycleNewTracker,
                                      boundaries);
    }

    /// Create the task that in turns creates the sstable writer used for compaction.
    ///
    /// @return a sharded compaction task that in turn will create a sharded compaction writer.
    private UnifiedCompactionTask createCompactionTask(LifecycleTransaction transaction, ShardingStats shardingStats, int gcBefore)
    {
        return new UnifiedCompactionTask(realm, this, transaction, gcBefore, getShardManager(), shardingStats);
    }

    /// Create the task that in turns creates the sstable writer used for compaction. This version is for a ranged task,
    /// where we produce outputs but cannot delete the input sstables until all components of the operation are complete.
    ///
    /// @return a sharded compaction task that in turn will create a sharded compaction writer.
    private UnifiedCompactionTask createCompactionTask(LifecycleTransaction transaction, Range<Token> operationRange, boolean keepOriginals, ShardingStats shardingStats, int gcBefore)
    {
        return new UnifiedCompactionTask(realm, this, transaction, gcBefore, keepOriginals, getShardManager(), shardingStats, operationRange, transaction.originals(), null, null);
    }

    @Override
    protected UnifiedCompactionTask createCompactionTask(final int gcBefore, LifecycleTransaction txn, boolean isMaximal, boolean splitOutput)
    {
        return createCompactionTask(txn, makeShardingStats(txn), gcBefore);
    }

    @Override
    public UnifiedCompactionTask createCompactionTask(LifecycleTransaction txn, final int gcBefore, long maxSSTableBytes)
    {
        return createCompactionTask(txn, makeShardingStats(txn), gcBefore);
    }

    /// Create a collection of parallelized compaction tasks that perform the compaction in parallel.
    private Collection<UnifiedCompactionTask> createParallelCompactionTasks(LifecycleTransaction transaction,
                                                                            Range<Token> operationRange,
                                                                            boolean keepOriginals,
                                                                            ShardingStats shardingStats,
                                                                            int gcBefore,
                                                                            int parallelism)
    {
        final int coveredShardCount = shardingStats.coveredShardCount;
        assert parallelism > 1;

        Collection<SSTableReader> sstables = transaction.originals();
        ShardManager shardManager = getShardManager();
        CompositeLifecycleTransaction compositeTransaction = new CompositeLifecycleTransaction(transaction);
        SharedCompactionProgress sharedProgress = new SharedCompactionProgress(transaction.opId(), transaction.opType(), TableOperation.Unit.BYTES);
        SharedCompactionObserver sharedObserver = new SharedCompactionObserver(this);
        List<UnifiedCompactionTask> tasks = shardManager.splitSSTablesInShardsLimited(
            sstables,
            operationRange,
            shardingStats.shardCountForDensity,
            shardingStats.coveredShardCount,
            parallelism,
            (rangeSSTables, range) -> new UnifiedCompactionTask(realm,
                                                                this,
                                                                new PartialLifecycleTransaction(compositeTransaction),
                                                                gcBefore,
                                                                keepOriginals,
                                                                shardManager,
                                                                shardingStats,
                                                                range,
                                                                rangeSSTables,
                                                                sharedProgress,
                                                                sharedObserver)
        );
        compositeTransaction.completeInitialization();
        assert tasks.size() <= parallelism;
        assert tasks.size() <= coveredShardCount;

        if (tasks.isEmpty())
            transaction.close(); // this should not be reachable normally, close the transaction for safety

        if (tasks.size() == 1) // if there's just one range, make it a non-ranged task (to apply early open etc.)
        {
            assert tasks.get(0).inputSSTables().equals(sstables);
            return Collections.singletonList(createCompactionTask(transaction, operationRange, keepOriginals, shardingStats, gcBefore));
        }
        else
            return tasks;
    }

    private ExpirationTask createExpirationTask(LifecycleTransaction transaction)
    {
        return new ExpirationTask(realm, transaction);
    }

    private void maybeUpdateSelector()
    {
        if (currentArenaSelector != null && !currentArenaSelector.diskBoundaries.isOutOfDate())
            return; // the disk boundaries (and thus the local ranges too) have not changed since the last time we calculated

        synchronized (this)
        {
            if (currentArenaSelector != null && !currentArenaSelector.diskBoundaries.isOutOfDate())
                return; // another thread beat us to the update

            DiskBoundaries currentBoundaries = realm.getDiskBoundaries();
            var maybeShardManager = realm.buildShardManager();
            currentShardManager = maybeShardManager != null
                           ? maybeShardManager
                           : ShardManager.create(currentBoundaries, realm.getKeyspaceReplicationStrategy(), controller.isReplicaAware());
            currentArenaSelector = new ArenaSelector(controller, currentBoundaries);
            // Note: this can just as well be done without the synchronization (races would be benign, just doing some
            // redundant work). For the current usages of this blocking is fine and expected to perform no worse.
        }
    }

    // used by CNDB
    public ShardManager getShardManager()
    {
        maybeUpdateSelector();
        return currentShardManager;
    }

    ArenaSelector getArenaSelector()
    {
        maybeUpdateSelector();
        return currentArenaSelector;
    }

    private CompactionLimits getCurrentLimits(int maxConcurrentCompactions)
    {
        // Calculate the running compaction limits, i.e. the overall number of compactions permitted, which is either
        // the compaction thread count, or the compaction throughput divided by the compaction rate (to prevent slowing
        // down individual compaction progress).
        String rateLimitLog = "";

        // identify space limit
        long spaceOverheadLimit = controller.maxCompactionSpaceBytes();

        // identify throughput limit
        double throughputLimit = controller.maxThroughput();
        int maxCompactions;
        if (throughputLimit < Double.MAX_VALUE)
        {
            int maxCompactionsForThroughput;

            double compactionRate = backgroundCompactions.compactionRate.get();
            if (compactionRate > 0)
            {
                // Start as many as can saturate the limit, making sure to also account for compactions that have
                // already been started but don't have progress yet.

                // Note: the throughput limit is adjusted here because the limiter won't let compaction proceed at more
                // than the given rate, and small hiccups or rounding errors could cause this to go above the current
                // running count when we are already at capacity.
                // Allow up to 5% variability, or if we are permitted more than 20 concurrent compactions, one/maxcount
                // so that we don't issue less tasks than we should.
                double adjustment = Math.min(0.05, 1.0 / maxConcurrentCompactions);
                maxCompactionsForThroughput = (int) Math.ceil(throughputLimit * (1 - adjustment) / compactionRate);
            }
            else
            {
                // If we don't have running compactions we don't know the effective rate.
                // Allow only one compaction; this will be called again soon enough to recheck.
                maxCompactionsForThroughput = 1;
            }

            rateLimitLog = String.format(" rate-based limit %d (rate %s/%s)",
                                         maxCompactionsForThroughput,
                                         FBUtilities.prettyPrintMemoryPerSecond((long) compactionRate),
                                         FBUtilities.prettyPrintMemoryPerSecond((long) throughputLimit));
            maxCompactions = Math.min(maxConcurrentCompactions, maxCompactionsForThroughput);
        }
        else
            maxCompactions = maxConcurrentCompactions;

        // Now that we have a count, make sure it is spread close to equally among levels. In other words, reserve
        // floor(permitted / levels) compactions for each level and don't permit more than ceil(permitted / levels) on
        // any, to make sure that no level hogs all threads and thus lowest-level ops (which need to run more often but
        // complete quickest) have a chance to run frequently. Also, running compactions can't go above the specified
        // space overhead limit.
        // To do this we count the number and size of already running compactions on each level and make sure any new
        // ones we select satisfy these constraints.
        int[] perLevel = new int[MAX_LEVELS];
        int levelCount = 1; // Start at 1 to avoid division by zero if the aggregates list is empty.
        int runningCompactions = 0;
        long spaceAvailable = spaceOverheadLimit;
        int remainingAdaptiveCompactions = controller.getMaxRecentAdaptiveCompactions(); //limit for number of compactions triggered by new W value
        if (remainingAdaptiveCompactions == -1)
            remainingAdaptiveCompactions = Integer.MAX_VALUE;
        for (CompactionPick compaction : backgroundCompactions.getCompactionsInProgress())
        {
            final int level = levelOf(compaction);
            if (level < 0)  // expire-only compactions are allowed to run outside of the limits
                continue;
            ++perLevel[level];
            ++runningCompactions;
            levelCount = Math.max(levelCount, level + 1);
            spaceAvailable -= controller.getOverheadSizeInBytes(compaction);
            if (controller.isRecentAdaptive(compaction))
                --remainingAdaptiveCompactions;
        }

        CompactionLimits limits = new CompactionLimits(runningCompactions,
                                                       maxCompactions,
                                                       maxConcurrentCompactions,
                                                       perLevel,
                                                       levelCount,
                                                       spaceAvailable,
                                                       rateLimitLog,
                                                       remainingAdaptiveCompactions);
        logger.trace("Selecting up to {} new compactions of up to {}, concurrency limit {}{}",
                     Math.max(0, limits.maxCompactions - limits.runningCompactions),
                     FBUtilities.prettyPrintMemory(limits.spaceAvailable),
                     limits.maxConcurrentCompactions,
                     limits.rateLimitLog);
        return limits;
    }

    private Collection<CompactionAggregate> updateLevelCountWithParentAndGetSelection(final CompactionLimits limits,
                                                                                      List<CompactionAggregate.UnifiedAggregate> pending)
    {
        long totalCompactionLimit = controller.maxCompactionSpaceBytes();
        int levelCount = limits.levelCount;
        for (CompactionAggregate.UnifiedAggregate aggregate : pending)
        {
            warnIfSizeAbove(aggregate, totalCompactionLimit);

            // Make sure the level count includes all levels for which we have sstables (to be ready to compact
            // as soon as the threshold is crossed)...
            levelCount = Math.max(levelCount, aggregate.bucketIndex() + 1);
            CompactionPick selected = aggregate.getSelected();
            if (selected != null)
            {
                // ... and also the levels that a layout-preserving selection would create.
                levelCount = Math.max(levelCount, levelOf(selected) + 1);
            }
        }
        int[] perLevel = limits.perLevel;
        if (levelCount != perLevel.length)
            perLevel = Arrays.copyOf(perLevel, levelCount);

        return getSelection(pending,
                            limits.maxCompactions,
                            perLevel,
                            limits.spaceAvailable,
                            limits.remainingAdaptiveCompactions);
    }

    /// Selects compactions to run next.
    ///
    /// @return a subset of compaction aggregates to run next
    private Collection<CompactionAggregate> getNextCompactionAggregates()
    {
        final CompactionLimits limits = getCurrentLimits(controller.maxConcurrentCompactions());

        List<CompactionAggregate.UnifiedAggregate> pending = getPendingCompactionAggregates(limits.spaceAvailable);
        setPendingCompactionAggregates(pending);

        return updateLevelCountWithParentAndGetSelection(limits, pending);
    }

    /// Selects compactions to run next from the passed aggregates.
    ///
    /// The intention here is to use this method directly from outside processes, to run compactions from a set
    /// of pre-existing aggregates, that have been generated out of process.
    ///
    /// @param aggregates a collection of aggregates from which to select the next compactions
    /// @param maxConcurrentCompactions the maximum number of concurrent compactions
    /// @return a subset of compaction aggregates to run next
    public Collection<CompactionAggregate> getNextCompactionAggregates(Collection<CompactionAggregate.UnifiedAggregate> aggregates,
                                                                       int maxConcurrentCompactions)
    {
        final CompactionLimits limits = getCurrentLimits(maxConcurrentCompactions);
        maybeUpdateSelector();
        return updateLevelCountWithParentAndGetSelection(limits, new ArrayList<>(aggregates));
    }

    /// Returns all pending compaction aggregates.
    ///
    /// This method is used by CNDB to find all pending compactions and put them to etcd.
    ///
    /// @return all pending compaction aggregates
    public Collection<CompactionAggregate.UnifiedAggregate> getPendingCompactionAggregates()
    {
        return getPendingCompactionAggregates(controller.maxCompactionSpaceBytes());
    }

    /// Set the compaction aggregates passed in as pending in [BackgroundCompactions]. This ensures
    /// that the compaction statistics will be accurate.
    ///
    /// This is called by [#getNextCompactionAggregates()]
    /// and externally after calling [#getPendingCompactionAggregates()]
    /// or before submitting tasks.
    ///
    /// Also, note that skipping the call to [#setPending(CompactionStrategy,Collection)]
    /// would result in memory leaks: the aggregates added in [#setSubmitted(CompactionStrategy,UUID,CompactionAggregate)]
    /// would never be removed, and the aggregates hold references to the compaction tasks, so they retain a significant
    /// size of heap memory.
    ///
    /// @param pending the aggregates that should be set as pending compactions
    public void setPendingCompactionAggregates(Collection<? extends CompactionAggregate> pending)
    {
        backgroundCompactions.setPending(this, pending);
    }

    private List<CompactionAggregate.UnifiedAggregate> getPendingCompactionAggregates(long spaceAvailable)
    {
        maybeUpdateSelector();

        List<CompactionAggregate.UnifiedAggregate> pending = new ArrayList<>();

        for (Map.Entry<Arena, List<Level>> entry : getLevels().entrySet())
        {
            Arena arena = entry.getKey();

            for (Level level : entry.getValue())
            {
                Collection<CompactionAggregate.UnifiedAggregate> aggregates = level.getCompactionAggregates(arena, controller, spaceAvailable);
                // Note: We allow empty aggregates into the list of pending compactions. The pending compactions list
                // is for progress tracking only, and it is helpful to see empty levels there.
                pending.addAll(aggregates);
            }
        }

        return pending;
    }

    /// This method logs a warning related to the fact that the space overhead limit also applies when a
    /// single compaction is above that limit. This should prevent running out of space at the expense of ending up
    /// with several extra sstables at the highest-level (compared to the number of sstables that we should have
    /// as per config of the strategy), i.e. slightly higher read amplification. This is a sensible tradeoff but
    /// the operators must be warned if this happens, and that's the purpose of this warning.
    private void warnIfSizeAbove(CompactionAggregate.UnifiedAggregate aggregate, long spaceOverheadLimit)
    {
        if (controller.getOverheadSizeInBytes(aggregate.selected) > spaceOverheadLimit)
            logger.warn("Compaction needs to perform an operation that is bigger than the current space overhead " +
                        "limit - size {} (compacting {} sstables in arena {}/bucket {}); limit {} = {}% of dataset size {}. " +
                        "To honor the limit, this operation will not be performed, which may result in degraded performance.\n" +
                        "Please verify the compaction parameters, specifically {} and {}.",
                        FBUtilities.prettyPrintMemory(controller.getOverheadSizeInBytes(aggregate.selected)),
                        aggregate.selected.sstables().size(),
                        aggregate.getArena().name(),
                        aggregate.bucketIndex(),
                        FBUtilities.prettyPrintMemory(spaceOverheadLimit),
                        controller.getMaxSpaceOverhead() * 100,
                        FBUtilities.prettyPrintMemory(controller.getDataSetSizeBytes()),
                        Controller.DATASET_SIZE_OPTION,
                        Controller.MAX_SPACE_OVERHEAD_OPTION);
    }

    /// Returns a selection of the compactions to be submitted. The selection will be chosen so that the total
    /// number of compactions is at most totalCount, where each level gets a share that is the whole part of the ratio
    /// between the total permitted number of compactions, and the remainder gets distributed among the levels
    /// according to the preferences of the [#prioritize] method. Usually this means preferring
    /// compaction picks with a higher max overlap, with a random selection when multiple picks have the same maximum.
    /// Note that if a level does not have tasks to fill its share, its quota will remain unused in this
    /// allocation.
    ///
    /// The selection also limits the size of the newly scheduled compactions to be below spaceAvailable by not
    /// scheduling compactions if they would push the combined size above that limit.
    ///
    /// @param pending list of all current aggregates with possible selection for each bucket
    /// @param totalCount maximum number of compactions permitted to run
    /// @param perLevel int array with the number of in-progress compactions per level
    /// @param spaceAvailable amount of space in bytes available for the new compactions
    /// @param remainingAdaptiveCompactions number of adaptive compactions (i.e. ones triggered by scaling parameter
    ///                                     change by the adaptive controller) that can still be scheduled
    List<CompactionAggregate> getSelection(List<CompactionAggregate.UnifiedAggregate> pending,
                                           int totalCount,
                                           int[] perLevel,
                                           long spaceAvailable,
                                           int remainingAdaptiveCompactions)
    {
        Controller controller = getController();
        Reservations reservations = Reservations.create(totalCount,
                                                        perLevel,
                                                        controller.getReservedThreads(),
                                                        controller.getReservationsType());
        // If the inclusion method is not transitive, we may have multiple buckets/selections for the same sstable.
        boolean shouldCheckSSTableSelected = controller.overlapInclusionMethod() != Overlaps.InclusionMethod.TRANSITIVE;
        // If so, make sure we only select one such compaction.
        Set<CompactionSSTable> selectedSSTables = shouldCheckSSTableSelected ? new HashSet<>() : null;

        int remaining = totalCount;
        for (int countInLevel : perLevel)
            remaining -= countInLevel;

        // Note: if we are in the middle of changes in the parameters or level count, remainder might become negative.
        // This is okay, some buckets will temporarily not get their rightful share until these tasks complete.

        // Let the controller prioritize the compactions.
        pending = controller.prioritize(pending);
        int proposed = 0;

        // Select the first ones, permitting only the specified number per level.
        List<CompactionAggregate> selected = new ArrayList<>(pending.size());
        for (CompactionAggregate.UnifiedAggregate aggregate : pending)
        {
            if (remaining == 0)
                break; // no threads to allocate from

            final CompactionPick pick = aggregate.getSelected();
            if (pick.isEmpty())
                continue;

            ++proposed;
            long overheadSizeInBytes = controller.getOverheadSizeInBytes(pick);
            if (overheadSizeInBytes > spaceAvailable)
                continue; // compaction is too large for current cycle

            int currentLevel = levelOf(pick);
            boolean isAdaptive = controller.isRecentAdaptive(pick);
            // avoid computing sharding stats if are not going to schedule the compaction at all
            if (!reservations.hasRoom(currentLevel))
                continue;  // honor the reserved thread counts
            if (isAdaptive && remainingAdaptiveCompactions <= 0)
                continue; // do not allow more than remainingAdaptiveCompactions to limit latency spikes upon changing W
            if (shouldCheckSSTableSelected && !Collections.disjoint(selectedSSTables, pick.sstables()))
                continue; // do not allow multiple selections of the same sstable

            int parallelism = controller.parallelizeOutputShards() ? getShardingStats(aggregate).coveredShardCount : 1;
            if (parallelism > remaining)
                parallelism = remaining;
            assert currentLevel >= 0 : "Invalid level in " + pick;

            if (isAdaptive)
            {
                if (parallelism > remainingAdaptiveCompactions)
                {
                    parallelism = remainingAdaptiveCompactions;
                    assert parallelism > 0; // we checked the remainingAdaptiveCompactions in advance
                }
            }

            parallelism = reservations.accept(currentLevel, parallelism);
            assert parallelism > 0; // we checked hasRoom in advance, there must always be at least one thread to use

            // Note: the reservations tracker assumes it is the last check and a pick is accepted if it returns true.

            if (isAdaptive)
                remainingAdaptiveCompactions -= parallelism;
            remaining -= parallelism;
            spaceAvailable -= overheadSizeInBytes;
            aggregate.setPermittedParallelism(parallelism);
            selected.add(aggregate);
            if (shouldCheckSSTableSelected)
                selectedSSTables.addAll(pick.sstables());
        }

        reservations.debugOutput(selected.size(), proposed, remaining);
        return selected;
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        return backgroundCompactions.getEstimatedRemainingTasks();
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public Set<? extends CompactionSSTable> getSSTables()
    {
        return realm.getLiveSSTables();
    }

    @VisibleForTesting
    public int getW(int index)
    {
        return controller.getScalingParameter(index);
    }

    public Controller getController()
    {
        return controller;
    }

    /// Group candidate sstables into compaction arenas.
    /// Each compaction arena is obtained by comparing using a compound comparator for the equivalence classes
    /// configured in the arena selector of this strategy.
    ///
    /// @param sstables a collection of the sstables to be assigned to arenas
    /// @param compactionFilter a bifilter (sstable, isCompacting) to include CompactionSSTables suitable for compaction
    /// @return a list of arenas, where each arena contains sstables that belong to that arena
    public Collection<Arena> getCompactionArenas(Collection<? extends CompactionSSTable> sstables,
                                                 BiPredicate<CompactionSSTable, Boolean> compactionFilter)
    {
        return getCompactionArenas(sstables, compactionFilter, getArenaSelector());
    }

    Collection<Arena> getCompactionArenas(Collection<? extends CompactionSSTable> sstables,
                                          BiPredicate<CompactionSSTable, Boolean> compactionFilter,
                                          ArenaSelector arenaSelector)
    {
        Map<CompactionSSTable, Arena> arenasBySSTables = new TreeMap<>(arenaSelector);
        Set<? extends CompactionSSTable> compacting = realm.getCompactingSSTables();
        for (CompactionSSTable sstable : sstables)
            if (compactionFilter.test(sstable, compacting.contains(sstable)))
                arenasBySSTables.computeIfAbsent(sstable, t -> new Arena(arenaSelector))
                      .add(sstable);

        return arenasBySSTables.values();
    }

    @SuppressWarnings("unused") // used by CNDB to deserialize aggregates
    public Arena getCompactionArena(Collection<? extends CompactionSSTable> sstables)
    {
        Arena arena = new Arena(getArenaSelector());
        for (CompactionSSTable table : sstables)
            arena.add(table);
        return arena;
    }

    @SuppressWarnings("unused") // used by CNDB to deserialize aggregates
    public Level getLevel(int index, double min, double max)
    {
        return new Level(controller, index, min, max);
    }

    /// @return a LinkedHashMap of arenas with buckets where order of arenas are preserved
    @VisibleForTesting
    Map<Arena, List<Level>> getLevels()
    {
        return getLevels(realm.getLiveSSTables(), UnifiedCompactionStrategy::isSuitableForCompaction);
    }

    private static boolean isSuitableForCompaction(CompactionSSTable sstable, boolean isCompacting)
    {
        return sstable.isSuitableForCompaction() && !isCompacting;
    }

    Iterable<? extends CompactionSSTable> getSuitableSSTables()
    {
        return getFilteredSSTables(UnifiedCompactionStrategy::isSuitableForCompaction);
    }

    Iterable<? extends CompactionSSTable> getFilteredSSTables(BiPredicate<CompactionSSTable, Boolean> predicate)
    {
        Set<? extends CompactionSSTable> compacting = realm.getCompactingSSTables();
        return Iterables.filter(realm.getLiveSSTables(), s -> predicate.test(s, compacting.contains(s)));
    }

    /// Groups the sstables passed in into arenas and buckets. This is used by the strategy to determine
    /// new compactions, and by external tools in CNDB to analyze the strategy decisions.
    ///
    /// @param sstables a collection of the sstables to be assigned to arenas
    /// @param compactionFilter a bifilter(sstable, isCompacting) to include CompactionSSTables,
    ///                         e.g., [#isSuitableForCompaction()]
    ///
    /// @return a map of arenas to their buckets
    public Map<Arena, List<Level>> getLevels(Collection<? extends CompactionSSTable> sstables,
                                             BiPredicate<CompactionSSTable, Boolean> compactionFilter)
    {
        // Copy to avoid race condition
        var currentShardManager = getShardManager();
        Collection<Arena> arenas = getCompactionArenas(sstables, compactionFilter);
        Map<Arena, List<Level>> ret = new LinkedHashMap<>(); // should preserve the order of arenas

        for (Arena arena : arenas)
        {
            List<Level> levels = new ArrayList<>(MAX_LEVELS);

            // Precompute the density, then sort.
            List<SSTableWithDensity> ssTableWithDensityList = new ArrayList<>(arena.sstables.size());
            for (CompactionSSTable sstable : arena.sstables)
                ssTableWithDensityList.add(new SSTableWithDensity(sstable, currentShardManager.density(sstable)));
            Collections.sort(ssTableWithDensityList);

            double maxSize = controller.getMaxLevelDensity(0, controller.getBaseSstableSize(controller.getFanout(0)) / currentShardManager.localSpaceCoverage());
            int index = 0;
            Level level = new Level(controller, index, 0, maxSize);
            for (SSTableWithDensity candidateWithDensity : ssTableWithDensityList)
            {
                final CompactionSSTable candidate = candidateWithDensity.sstable;
                final double size = candidateWithDensity.density;
                if (size < level.max)
                {
                    level.add(candidate);
                    continue;
                }

                level.complete();
                levels.add(level); // add even if empty

                while (true)
                {
                    ++index;
                    double minSize = maxSize;
                    maxSize = controller.getMaxLevelDensity(index, minSize);
                    level = new Level(controller, index, minSize, maxSize);
                    if (size < level.max)
                    {
                        level.add(candidate);
                        break;
                    }
                    else
                    {
                        levels.add(level); // add the empty level
                    }
                }
            }

            if (!level.sstables.isEmpty())
            {
                level.complete();
                levels.add(level);
            }

            if (!levels.isEmpty())
                ret.put(arena, levels);

            if (logger.isTraceEnabled())
                logger.trace("Arena {} has {} levels", arena, levels.size());
        }

        logger.trace("Found {} arenas with buckets for {}.{}", ret.size(), realm.getKeyspaceName(), realm.getTableName());
        return ret;
    }

    private static int levelOf(CompactionPick pick)
    {
        return (int) pick.parent();
    }

    public TableMetadata getMetadata()
    {
        return realm.metadata();
    }

    /// A compaction arena contains the list of sstables that belong to this arena as well as the arena
    /// selector used for comparison.
    public static class Arena implements Comparable<Arena>
    {
        final List<CompactionSSTable> sstables;
        final ArenaSelector selector;

        Arena(ArenaSelector selector)
        {
            this.sstables = new ArrayList<>();
            this.selector = selector;
        }

        void add(CompactionSSTable ssTableReader)
        {
            sstables.add(ssTableReader);
        }

        public String name()
        {
            CompactionSSTable t = sstables.get(0);
            return selector.name(t);
        }

        @Override
        public int compareTo(Arena o)
        {
            return selector.compare(this.sstables.get(0), o.sstables.get(0));
        }

        @Override
        public String toString()
        {
            return String.format("%s, %d sstables", name(), sstables.size());
        }

        @VisibleForTesting
        public List<CompactionSSTable> getSSTables()
        {
            return sstables;
        }
    }

    @Override
    public String toString()
    {
        return String.format("Unified strategy %s", getMetadata());
    }

    /// A level: index, sstables and some properties.
    public static class Level
    {
        final List<CompactionSSTable> sstables;
        final int index;
        final double survivalFactor;
        final int scalingParameter; // scaling parameter used to calculate fanout and threshold
        final int fanout; // fanout factor between levels
        final int threshold; // number of SSTables that trigger a compaction
        final double min; // min density of sstables for this level
        final double max; // max density of sstables for this level
        double avg = 0; // avg size of sstables in this level
        int maxOverlap = -1; // maximum number of overlapping sstables

        Level(int index, int scalingParameter, int fanout, int threshold, double survivalFactor, double min, double max)
        {
            this.index = index;
            this.scalingParameter = scalingParameter;
            this.fanout = fanout;
            this.threshold = threshold;
            this.survivalFactor = survivalFactor;
            this.min = min;
            this.max = max;
            this.sstables = new ArrayList<>(threshold);
        }

        Level(Controller controller, int index, double min, double max)
        {
            this(index,
                 controller.getScalingParameter(index),
                 controller.getFanout(index),
                 controller.getThreshold(index),
                 controller.getSurvivalFactor(index),
                 min,
                 max);
        }

        public Collection<CompactionSSTable> getSSTables()
        {
            return sstables;
        }

        public int getIndex()
        {
            return index;
        }

        void add(CompactionSSTable sstable)
        {
            this.sstables.add(sstable);
            // consider size of all components to reduce chance of out-of-disk
            long size = CassandraRelevantProperties.UCS_COMPACTION_INCLUDE_NON_DATA_FILES_SIZE.getBoolean()
                        ? sstable.onDiskComponentsSize() : sstable.onDiskLength();
            this.avg += (size - avg) / sstables.size();
        }

        void complete()
        {
            if (logger.isTraceEnabled())
                logger.trace("Level: {}", this);
        }

        /// Return the compaction aggregate
        Collection<CompactionAggregate.UnifiedAggregate> getCompactionAggregates(Arena arena,
                                                                                 Controller controller,
                                                                                 long spaceAvailable)
        {
            if (logger.isTraceEnabled())
                logger.trace("Creating compaction aggregate with sstable set {}", sstables);


            // Note that adjacent overlap sets may include deduplicated sstable
            List<Set<CompactionSSTable>> overlaps = Overlaps.constructOverlapSets(sstables,
                                                                                  CompactionSSTable.startsAfter,
                                                                                  CompactionSSTable.firstKeyComparator,
                                                                                  CompactionSSTable.lastKeyComparator);
            for (Set<CompactionSSTable> overlap : overlaps)
                maxOverlap = Math.max(maxOverlap, overlap.size());
            List<CompactionSSTable> unbucketed = new ArrayList<>();

            List<Bucket> buckets = Overlaps.assignOverlapsIntoBuckets(threshold,
                                                                      controller.overlapInclusionMethod(),
                                                                      overlaps,
                                                                      this::makeBucket,
                                                                      unbucketed::addAll);

            List<CompactionAggregate.UnifiedAggregate> aggregates = new ArrayList<>();
            for (Bucket bucket : buckets)
                aggregates.add(bucket.constructAggregate(controller, spaceAvailable, arena));

            // Add all unbucketed sstables separately. Note that this will list the level (with its set of sstables)
            // even if it does not need compaction.
            if (!unbucketed.isEmpty())
                aggregates.add(CompactionAggregate.createUnified(unbucketed,
                                                                 maxOverlap,
                                                                 CompactionPick.EMPTY,
                                                                 Collections.emptySet(),
                                                                 arena,
                                                                 this));

            if (logger.isTraceEnabled())
                logger.trace("Returning compaction aggregates {} for level {} of arena {}",
                             aggregates, this, arena);
            return aggregates;
        }

        private Bucket makeBucket(List<Set<CompactionSSTable>> overlaps, int startIndex, int endIndex)
        {
            return endIndex == startIndex + 1
                   ? new SimpleBucket(this, overlaps.get(startIndex))
                   : new MultiSetBucket(this, overlaps.subList(startIndex, endIndex));
        }

        @Override
        public String toString()
        {
            return String.format("W: %d, T: %d, F: %d, index: %d, min: %s, max %s, %d sstables, overlap %s",
                                 scalingParameter,
                                 threshold,
                                 fanout,
                                 index,
                                 densityAsString(min),
                                 densityAsString(max),
                                 sstables.size(),
                                 maxOverlap);
        }

        private String densityAsString(double density)
        {
            return FBUtilities.prettyPrintBinary(density, "B", " ");
        }
    }

    /// A compaction bucket, i.e. a selection of overlapping sstables from which a compaction should be selected.
    static abstract class Bucket
    {
        final Level level;
        final List<CompactionSSTable> allSSTablesSorted;
        final int maxOverlap;

        Bucket(Level level, Collection<CompactionSSTable> allSSTablesSorted, int maxOverlap)
        {
            // single section
            this.level = level;
            this.allSSTablesSorted = new ArrayList<>(allSSTablesSorted);
            this.allSSTablesSorted.sort(CompactionSSTable.maxTimestampDescending);  // we remove entries from the back
            this.maxOverlap = maxOverlap;
        }

        Bucket(Level level, List<Set<CompactionSSTable>> overlapSections)
        {
            // multiple sections
            this.level = level;
            int maxOverlap = 0;
            Set<CompactionSSTable> all = new HashSet<>();
            for (Set<CompactionSSTable> section : overlapSections)
            {
                maxOverlap = Math.max(maxOverlap, section.size());
                all.addAll(section);
            }
            this.allSSTablesSorted = new ArrayList<>(all);
            this.allSSTablesSorted.sort(CompactionSSTable.maxTimestampDescending);  // we remove entries from the back
            this.maxOverlap = maxOverlap;
        }

        /// Select compactions from this bucket. Normally this would form a compaction out of all sstables in the
        /// bucket, but if compaction is very late we may prefer to act more carefully:
        /// - we should not use more inputs than the permitted maximum
        /// - we should not select a compaction whose execution will use more temporary space than is available
        /// - we should select SSTables in a way that preserves the structure of the compaction hierarchy
        /// These impose a limit on the size of a compaction; to make sure we always reduce the read amplification by
        /// this much, we treat this number as a limit on overlapping sstables, i.e. if A and B don't overlap with each
        /// other but both overlap with C and D, all four will be selected to form a limit-three compaction. A limit-two
        /// one may choose CD, ABC or ABD.
        /// Also, the subset is selected by max timestamp order, oldest first, to avoid violating sstable time order. In
        /// the example above, if B is oldest and C is older than D, the limit-two choice would be ABC (if A is older
        /// than D) or BC (if A is younger, avoiding combining C with A skipping D).
        ///
        /// @param controller The compaction controller.
        /// @param spaceAvailable The amount of space available for compaction, limits the maximum number of sstables
        ///                       that can be selected.
        /// @return A compaction pick to execute next.
        CompactionAggregate.UnifiedAggregate constructAggregate(Controller controller, long spaceAvailable, Arena arena)
        {
            int count = maxOverlap;
            int threshold = level.threshold;
            int fanout = level.fanout;
            int index = level.index;
            int maxSSTablesToCompact = Math.max(fanout, (int) Math.min(spaceAvailable / level.avg, controller.maxSSTablesToCompact()));

            assert count >= threshold;
            if (count <= fanout)
            {

                // Happy path. We are not late or (for levelled) we are only so late that a compaction now will
                // have the same effect as doing levelled compactions one by one. Compact all. We do not cap
                // this pick at maxSSTablesToCompact due to an assumption that maxSSTablesToCompact is much
                // greater than F. See {@link Controller#MAX_SSTABLES_TO_COMPACT_OPTION} for more details.
                return CompactionAggregate.createUnified(allSSTablesSorted,
                                                         maxOverlap,
                                                         CompactionPick.create(nextTimeUUID(), index, allSSTablesSorted),
                                                         Collections.emptySet(),
                                                         arena,
                                                         level);
            }
            // The choices below assume that pulling the oldest sstables will reduce maxOverlap by the selected
            // number of sstables. This is not always true (we may, e.g. select alternately from different overlap
            // sections if the structure is complex enough), but is good enough heuristic that results in usable
            // compaction sets.
            else if (count <= fanout * controller.getFanout(index + 1) || maxSSTablesToCompact == fanout)
            {
                // Compaction is a bit late, but not enough to jump levels via layout compactions. We need a special
                // case to cap compaction pick at maxSSTablesToCompact.
                if (count <= maxSSTablesToCompact)
                    return CompactionAggregate.createUnified(allSSTablesSorted,
                                                             maxOverlap,
                                                             CompactionPick.create(nextTimeUUID(), index, allSSTablesSorted),
                                                             Collections.emptySet(),
                                                             arena,
                                                             level);

                CompactionPick pick = CompactionPick.create(nextTimeUUID(), index, pullOldestSSTables(maxSSTablesToCompact));
                count -= maxSSTablesToCompact;
                List<CompactionPick> pending = new ArrayList<>();
                while (count >= threshold)
                {
                    pending.add(CompactionPick.create(nextTimeUUID(), index, pullOldestSSTables(maxSSTablesToCompact)));
                    count -= maxSSTablesToCompact;
                }

                return CompactionAggregate.createUnified(allSSTablesSorted, maxOverlap, pick, pending, arena, level);
            }
            // We may, however, have accumulated a lot more than T if compaction is very late, or a set of small
            // tables was dumped on us (e.g. when converting from legacy LCS or for tests).
            else
            {
                // We need to pick the compactions in such a way that the result of doing them all spreads the data in
                // a similar way to how compaction would lay them if it was able to keep up. This means:
                // - for tiered compaction (w >= 0), compact in sets of as many as required to get to a level.
                //   for example, for w=2 and 55 sstables, do 3 compactions of 16 sstables, 1 of 4, and leave the other 3 alone
                // - for levelled compaction (w < 0), compact all that would reach a level.
                //   for w=-2 and 55, this means one compaction of 48, one of 4, and one of 3 sstables.
                List<CompactionPick> picks = layoutCompactions(controller, maxSSTablesToCompact);
                // Out of the set of necessary compactions, choose the one to run randomly. This gives a better
                // distribution among levels and should result in more compactions running in parallel in a big data
                // dump.
                assert !picks.isEmpty();  // we only enter this if count > F: layoutCompactions must have selected something to run
                CompactionPick selected = picks.remove(controller.random().nextInt(picks.size()));
                return CompactionAggregate.createUnified(allSSTablesSorted, maxOverlap, selected, picks, arena, level);
            }
        }

        private List<CompactionPick> layoutCompactions(Controller controller, int maxSSTablesToCompact)
        {
            List<CompactionPick> pending = new ArrayList<>();
            int pos = layoutCompactions(controller, level.index + 1, level.fanout, maxSSTablesToCompact, pending);
            int size = maxOverlap;
            if (size - pos >= level.threshold) // can only happen in the levelled case.
            {
                assert size - pos < maxSSTablesToCompact; // otherwise it should have already been picked
                pending.add(CompactionPick.create(nextTimeUUID(), level.index, allSSTablesSorted));
            }
            return pending;
        }

        /// Collects in {@param list} compactions of {@param sstables} such that they land in {@param level} and higher.
        ///
        /// Recursively combines SSTables into [CompactionPick]s in way that up to {@param maxSSTablesToCompact}
        /// SSTables are combined to reach the highest possible level, then the rest is combined for the level before,
        /// etc up to {@param level}.
        ///
        /// To agree with what compaction normally does, the first sstables from the list are placed in the picks that
        /// combine to reach the highest levels.
        ///
        /// @param level minimum target level for compactions to land
        /// @param step - number of source SSTables required to reach level
        /// @param maxSSTablesToCompact limit on the number of sstables per compaction
        /// @param list - result list of layout-preserving compaction picks
        /// @return index of the last used SSTable from {@param sstables}; the number of remaining sstables will be lower
        ///         than step
        private int layoutCompactions(Controller controller,
                                      int level,
                                      int step,
                                      int maxSSTablesToCompact,
                                      List<CompactionPick> list)
        {
            if (step > maxOverlap || step > maxSSTablesToCompact)
                return 0;

            int w = controller.getScalingParameter(level);
            int f = controller.getFanout(level);
            int pos = layoutCompactions(controller,
                                        level + 1,
                                        step * f,
                                        maxSSTablesToCompact,
                                        list);

            int total = maxOverlap;
            // step defines the number of source sstables that are needed to reach this level (ignoring overwrites
            // and deletions).
            // For tiered compaction we will select batches of this many.
            int pickSize = step;
            if (w < 0)
            {
                // For levelled compaction all the sstables that would reach this level need to be compacted to one,
                // so select the highest multiple of step that is available, but make sure we don't do a compaction
                // bigger than the limit.
                pickSize *= Math.min(total - pos, maxSSTablesToCompact) / pickSize;

                if (pickSize == 0)  // Not enough sstables to reach this level, we can skip the processing below.
                    return pos;     // Note: this cannot happen on the top level, but can on lower ones.
            }

            while (pos + pickSize <= total)
            {
                // Note that we assign these compactions to the level that would normally produce them, which means that
                // they won't be taking up threads dedicated to the busy level.
                // Normally sstables end up on a level when a compaction on the previous brings their size to the
                // threshold (which corresponds to pickSize == step, always the case for tiered); in the case of
                // levelled compaction, when we compact more than 1 but less than F sstables on a level (which
                // corresponds to pickSize > step), it is an operation that is triggered on the same level.
                list.add(CompactionPick.create(nextTimeUUID(),
                                               pickSize > step ? level : level - 1,
                                               pullOldestSSTables(pickSize)));
                pos += pickSize;
            }

            // In the levelled case, if we had to adjust pickSize due to maxSSTablesToCompact, there may
            // still be enough sstables to reach this level (e.g. if max was enough for 2*step, but we had 3*step).
            if (pos + step <= total)
            {
                pickSize = ((total - pos) / step) * step;
                list.add(CompactionPick.create(nextTimeUUID(),
                                               pickSize > step ? level : level - 1,
                                               pullOldestSSTables(pickSize)));
                pos += pickSize;
            }
            return pos;
        }

        static <T> List<T> pullLast(List<T> source, int limit)
        {
            List<T> result = new ArrayList<>(limit);
            while (--limit >= 0)
                result.add(source.remove(source.size() - 1));
            return result;
        }

        /**
         * Pull the oldest sstables to get at most limit-many overlapping sstables to compact in each overlap section.
         */
        abstract Collection<CompactionSSTable> pullOldestSSTables(int overlapLimit);
    }

    public static class SimpleBucket extends Bucket
    {
        public SimpleBucket(Level level, Collection<CompactionSSTable> sstables)
        {
            super(level, sstables, sstables.size());
        }

        Collection<CompactionSSTable> pullOldestSSTables(int overlapLimit)
        {
            if (allSSTablesSorted.size() <= overlapLimit)
                return allSSTablesSorted;
            return pullLast(allSSTablesSorted, overlapLimit);
        }
    }

    public static class MultiSetBucket extends Bucket
    {
        final List<Set<CompactionSSTable>> overlapSets;

        public MultiSetBucket(Level level, List<Set<CompactionSSTable>> overlapSets)
        {
            super(level, overlapSets);
            this.overlapSets = overlapSets;
        }

        Collection<CompactionSSTable> pullOldestSSTables(int overlapLimit)
        {
            return Overlaps.pullLastWithOverlapLimit(allSSTablesSorted, overlapSets, overlapLimit);
        }
    }

    static class CompactionLimits
    {
        final int runningCompactions;
        final int maxConcurrentCompactions;
        final int maxCompactions;
        final int[] perLevel;
        int levelCount;
        final long spaceAvailable;
        final String rateLimitLog;
        final int remainingAdaptiveCompactions;

        public CompactionLimits(int runningCompactions,
                                int maxCompactions,
                                int maxConcurrentCompactions,
                                int[] perLevel,
                                int levelCount,
                                long spaceAvailable,
                                String rateLimitLog,
                                int remainingAdaptiveCompactions)
        {
            this.runningCompactions = runningCompactions;
            this.maxCompactions = maxCompactions;
            this.maxConcurrentCompactions = maxConcurrentCompactions;
            this.perLevel = perLevel;
            this.levelCount = levelCount;
            this.spaceAvailable = spaceAvailable;
            this.rateLimitLog = rateLimitLog;
            this.remainingAdaptiveCompactions = remainingAdaptiveCompactions;
        }

        @Override
        public String toString()
        {
            return String.format("Current limits: running=%d, max=%d, maxConcurrent=%d, perLevel=%s, levelCount=%d, spaceAvailable=%s, rateLimitLog=%s, remainingAdaptiveCompactions=%d",
                                 runningCompactions, maxCompactions, maxConcurrentCompactions, Arrays.toString(perLevel), levelCount,
                                 FBUtilities.prettyPrintMemory(spaceAvailable), rateLimitLog, remainingAdaptiveCompactions);
        }
    }

    /**
     * Utility wrapper to efficiently store the density of an SSTable with the SSTable itself.
     */
    private static class SSTableWithDensity implements Comparable<SSTableWithDensity>
    {
        final CompactionSSTable sstable;
        final double density;

        SSTableWithDensity(CompactionSSTable sstable, double density)
        {
            this.sstable = sstable;
            this.density = density;
        }

        @Override
        public int compareTo(SSTableWithDensity o)
        {
            return Double.compare(density, o.density);
        }
    }
}
