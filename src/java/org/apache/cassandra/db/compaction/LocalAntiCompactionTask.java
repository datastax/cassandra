package org.apache.cassandra.db.compaction;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.db.compaction.CompactionManager.getDefaultGcBefore;

/**
 * Executes the final anticompaction by actually anticompacting those local sstables not fully contained into repaired
 * ranges, and repairing (via the repairedAt attribute) fully contained local sstables and remote sstables.
 */
public class LocalAntiCompactionTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(LocalAntiCompactionTask.class);
    private final CompactionManager compactionManager = CompactionManager.instance;
    private final ColumnFamilyStore cfs;
    private final Collection<Range<Token>> ranges;
    private final long repairedAt;
    private final UUID parentSessionId;
    private final LifecycleTransaction localSSTablesTxn;
    private final Set<LifecycleTransaction> remoteSSTablesTxns;

    private final Set<SSTableReader> unrepairedSet;

    @VisibleForTesting
    public LocalAntiCompactionTask(ColumnFamilyStore cfs, Collection<Range<Token>> ranges,
                                   UUID parentSessionId, long repairedAt,
                                   LifecycleTransaction localSSTablesTxn)

    {
        this(cfs, ranges, parentSessionId, repairedAt, localSSTablesTxn, Collections.emptySet(), Collections.emptySet());
    }

    public LocalAntiCompactionTask(ColumnFamilyStore cfs, Collection<Range<Token>> ranges,
                                   UUID parentSessionId, long repairedAt,
                                   LifecycleTransaction localSSTablesTxn,
                                   Set<LifecycleTransaction> remoteSSTablesTxns,
                                   Set<SSTableReader> unrepairedSet)
    {
        this.cfs = cfs;
        this.ranges = ranges;
        this.parentSessionId = parentSessionId;
        this.repairedAt = repairedAt;
        this.localSSTablesTxn = localSSTablesTxn;
        this.remoteSSTablesTxns = remoteSSTablesTxns;
        this.unrepairedSet = unrepairedSet;
        assert unrepairedSet.stream().allMatch(u -> Sets.union(Collections.singleton(localSSTablesTxn), remoteSSTablesTxns)
                                                        .stream().anyMatch(t -> t.originals().contains(u))) :
                                              "SSTables on repaired set must be contained in at least one transaction.";
    }

    @VisibleForTesting
    public LifecycleTransaction getLocalSSTablesTransaction()
    {
        return localSSTablesTxn;
    }

    @VisibleForTesting
    public Set<LifecycleTransaction> getRemoteSSTablesTransactions()
    {
        return remoteSSTablesTxns;
    }

    public final void run()
    {
        try
        {
            //Release all SSTables in the unrepaired set
            if (!unrepairedSet.isEmpty())
            {
                logger.debug("[repair {}] Not marking {} live SSTable(s) as repaired because they " +
                            "potentially shadow rows compacted during repair.", parentSessionId,
                             unrepairedSet.size(), cfs.keyspace.getName(), cfs.name);

                unrepairedSet.forEach(u ->
                                      Sets.union(Collections.singleton(localSSTablesTxn), remoteSSTablesTxns)
                                          .stream()
                                          .filter(t -> t.originals().contains(u))
                                          .forEach(t -> t.cancel(u)));
            }

            // Remove any SSTables not contained in repaired range
            List<Range<Token>> normalizedRanges = Range.normalize(ranges);
            Set<SSTableReader> localNonIntersecting = localSSTablesTxn.originals().stream()
                                                                 .filter(s -> normalizedRanges.stream().noneMatch(r -> r.intersects(s.getRange())))
                                                                 .collect(Collectors.toSet());
            // A remote SSTable may be received but repair of that range failed, so we must not mark these SSTables repaired
            Set<SSTableReader> remoteNonIntersecting = remoteSSTablesTxns.stream().flatMap(t -> t.originals().stream())
                                                                                  .filter(s -> normalizedRanges.stream().noneMatch(r -> r.intersects(s.getRange())))
                                                                                  .collect(Collectors.toSet());
            if (!localNonIntersecting.isEmpty() || !remoteNonIntersecting.isEmpty())
            {
                logger.info("[repair #{}] {} SSTable(s) do not intersect repaired ranges {}, removing from anti-compaction.", parentSessionId,
                            localNonIntersecting.size() + remoteNonIntersecting.size(), normalizedRanges);
                if (!localNonIntersecting.isEmpty())
                    localSSTablesTxn.cancel(localNonIntersecting);
                if (!remoteNonIntersecting.isEmpty())
                    remoteNonIntersecting.forEach(s -> remoteSSTablesTxns.stream().filter(t -> t.originals().contains(s)).forEach(t -> t.cancel(s)));
            }

            // Then add to the set of sstables to repair all the local sstables fully contained in the repaired ranges:
            Set<SSTableReader> localSSTablesFullyContainedInRepairedRange = localSSTablesTxn.originals().stream()
                                                                               .filter(s -> normalizedRanges.stream().anyMatch(r -> r.equals(s.getRange()) ||
                                                                                                                                    r.contains(s.getRange())))
                                                                               .collect(Collectors.toSet());

            Set<SSTableReader> sstablesToAntiCompact = Sets.newHashSet(Sets.difference(localSSTablesTxn.originals(), localSSTablesFullyContainedInRepairedRange));

            Set<SSTableReader> remoteToMarkRepaired = remoteSSTablesTxns.stream().flatMap(r -> r.originals().stream()).collect(Collectors.toSet());

            // Mark as repaired remote SSTables or local SSTables fully contained in repaired range
            Set<SSTableReader> markRepairedSet = Sets.union(localSSTablesFullyContainedInRepairedRange, remoteToMarkRepaired);

            logger.info("[repair #{}] Starting anticompaction for {}.{} on {}/{} sstables on {} ranges.", parentSessionId, cfs.keyspace.getName(),
                        cfs.getTableName(), sstablesToAntiCompact.size() + markRepairedSet.size(), cfs.getLiveSSTables().size(), ranges.size());

            int antiCompactedSSTableCount = 0;
            if (!sstablesToAntiCompact.isEmpty())
            {
                // Repairs can take place on both unrepaired (incremental + full) and repaired (full) data.
                // Although anti-compaction could work on repaired sstables as well and would result in having more accurate
                // repairedAt values for these, we still avoid anti-compacting already repaired sstables, as we currently don't
                // make use of any actual repairedAt value and splitting up sstables just for that is not worth it at this point.
                Set<SSTableReader> unrepairedSSTables = sstablesToAntiCompact.stream()
                                                                             .filter((s) -> !s.isRepaired())
                                                                             .collect(Collectors.toSet());

                // Iterate over sstables to check if the repaired / unrepaired ranges intersect them.
                Collection<Collection<SSTableReader>> groupedSSTables = cfs.getCompactionStrategyManager().groupSSTablesForAntiCompaction(unrepairedSSTables);
                for (Collection<SSTableReader> sstableGroup : groupedSSTables)
                {
                    try (LifecycleTransaction splittedTxn = localSSTablesTxn.split(sstableGroup))
                    {
                        int antiCompacted = antiCompactGroup(splittedTxn);
                        antiCompactedSSTableCount += antiCompacted;
                    }
                }
                logger.info("[repair #{}] Anticompaction completed successfully, anticompacted from {} to {} sstable(s).",
                            parentSessionId, sstablesToAntiCompact.size(), antiCompactedSSTableCount);
            }

            if (!markRepairedSet.isEmpty())
            {
                logger.info("[repair #{}] {} SSTable(s) fully contained in repaired range, mutating repairedAt instead of anticompacting.",
                            parentSessionId, markRepairedSet.size());
                cfs.mutateRepairedAt(markRepairedSet, repairedAt);
                antiCompactedSSTableCount += markRepairedSet.size();
            }

            if (sstablesToAntiCompact.isEmpty() && markRepairedSet.isEmpty())
            {
                logger.info("[repair #{}] No SSTables to anti-compact.", parentSessionId);
            }
            else
            {
                logger.info("[repair #{}] Anticompaction completed successfully, anticompacted from {} to {} sstable(s).",
                            parentSessionId, sstablesToAntiCompact.size() + markRepairedSet.size(), antiCompactedSSTableCount);
            }

            // Release local and remote SSTables sstables fully contained in repaired range and finish local transaction
            localSSTablesTxn.cancel(localSSTablesFullyContainedInRepairedRange);
            remoteSSTablesTxns.stream().forEach(t -> t.abort()); //This is aborted otherwise the SSTables are removed from the tracker
            localSSTablesTxn.finish();
        }
        catch (Throwable t)
        {
            Throwable accumulate = t;
            for (LifecycleTransaction txn : Sets.union(Collections.singleton(localSSTablesTxn), remoteSSTablesTxns))
            {
                try
                {
                    txn.close();
                }
                catch (Throwable t2)
                {
                    accumulate.addSuppressed(t2);
                }
            }
            logger.error("Error during anti-compaction.", accumulate);
            throw Throwables.propagate(accumulate);
        }
    }

    @VisibleForTesting
    protected int antiCompactGroup(LifecycleTransaction groupTxn)
    {
        long groupMaxDataAge = -1;

        for (Iterator<SSTableReader> i = groupTxn.originals().iterator(); i.hasNext();)
        {
            SSTableReader sstable = i.next();
            if (groupMaxDataAge < sstable.maxDataAge)
                groupMaxDataAge = sstable.maxDataAge;
        }

        if (groupTxn.originals().size() == 0)
        {
            logger.info("No valid anticompactions for this group, All sstables were compacted and are no longer available");
            return 0;
        }

        logger.info("Anticompacting {}", groupTxn);
        Set<SSTableReader> sstableAsSet = groupTxn.originals();

        File destination = CompactionManager.getWriterLocation(cfs, sstableAsSet, OperationType.ANTICOMPACTION);
        long repairedKeyCount = 0;
        long unrepairedKeyCount = 0;
        int nowInSec = FBUtilities.nowInSeconds();

        CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();
        try (SSTableRewriter repairedSSTableWriter = new SSTableRewriter(groupTxn, groupMaxDataAge, false, false);
             SSTableRewriter unRepairedSSTableWriter = new SSTableRewriter(groupTxn, groupMaxDataAge, false, false);
             AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(groupTxn.originals());
             CompactionController controller = new CompactionController(cfs, sstableAsSet, getDefaultGcBefore(cfs, nowInSec));
             CompactionIterator ci = new CompactionIterator(OperationType.ANTICOMPACTION, scanners.scanners, controller, nowInSec, UUIDGen.getTimeUUID(), compactionManager.getMetrics()))
        {
            int expectedBloomFilterSize = Math.max(cfs.metadata.params.minIndexInterval, (int)(SSTableReader.getApproximateKeyCount(sstableAsSet)));

            repairedSSTableWriter.switchWriter(CompactionManager.createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, repairedAt, sstableAsSet, groupTxn));
            unRepairedSSTableWriter.switchWriter(CompactionManager.createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, ActiveRepairService.UNREPAIRED_SSTABLE, sstableAsSet, groupTxn));
            Range.OrderedRangeContainmentChecker containmentChecker = new Range.OrderedRangeContainmentChecker(ranges);
            while (ci.hasNext())
            {
                try (UnfilteredRowIterator partition = ci.next())
                {
                    // if current range from sstable is repaired, save it into the new repaired sstable
                    if (containmentChecker.contains(partition.partitionKey().getToken()))
                    {
                        repairedSSTableWriter.append(partition);
                        repairedKeyCount++;
                    }
                    // otherwise save into the new 'non-repaired' table
                    else
                    {
                        unRepairedSSTableWriter.append(partition);
                        unrepairedKeyCount++;
                    }
                }
            }

            List<SSTableReader> anticompactedSSTables = new ArrayList<>();
            // since both writers are operating over the same Transaction, we cannot use the convenience Transactional.finish() method,
            // as on the second finish() we would prepareToCommit() on a Transaction that has already been committed, which is forbidden by the API
            // (since it indicates misuse). We call permitRedundantTransitions so that calls that transition to a state already occupied are permitted.
            groupTxn.permitRedundantTransitions();
            repairedSSTableWriter.setRepairedAt(repairedAt).prepareToCommit();
            unRepairedSSTableWriter.prepareToCommit();
            anticompactedSSTables.addAll(repairedSSTableWriter.finished());
            anticompactedSSTables.addAll(unRepairedSSTableWriter.finished());
            repairedSSTableWriter.commit();
            unRepairedSSTableWriter.commit();

            logger.trace("Repaired {} keys out of {} for {}/{} in {}", repairedKeyCount,
                         repairedKeyCount + unrepairedKeyCount,
                         cfs.keyspace.getName(),
                         cfs.getColumnFamilyName(),
                         groupTxn);
            return anticompactedSSTables.size();
        }
        catch (Throwable e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            logger.error("Error anticompacting " + groupTxn, e);
        }
        return 0;
    }

    @VisibleForTesting
    public Set<SSTableReader> getUnrepairedSet()
    {
        return unrepairedSet;
    }

    @VisibleForTesting
    public Collection<Range<Token>> getSuccessfulRanges()
    {
        return ranges;
    }
}
