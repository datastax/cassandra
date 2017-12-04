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

package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class DiskBoundaryManager
{
    /**
     * Whether partitioning sstables by token range is enabled when there are multiple disk
     */
    private static final boolean SPLIT_SSTABLES_BY_TOKEN_RANGE = Boolean.parseBoolean(System.getProperty("cassandra.split_sstables_by_token_range", "true"));

    private static final Logger logger = LoggerFactory.getLogger(DiskBoundaryManager.class);
    private ConcurrentHashMap<Directories, DiskBoundaries> diskBoundaries = new ConcurrentHashMap<>();

    public DiskBoundaries getDiskBoundaries(ColumnFamilyStore cfs, Directories directories)
    {
        if (!cfs.getPartitioner().splitter().isPresent() || !SPLIT_SSTABLES_BY_TOKEN_RANGE)
            return new DiskBoundaries(directories.getWriteableLocations(), null, -1, -1);
        // copy the reference to avoid getting nulled out by invalidate() below
        // - it is ok to race, compaction will move any incorrect tokens to their correct places, but
        // returning null would be bad
        DiskBoundaries db = diskBoundaries.get(directories);
        if (isOutOfDate(db))
        {
            synchronized (this)
            {
                db = diskBoundaries.get(directories);
                if (isOutOfDate(db))
                {
                    logger.debug("Refreshing disk boundary cache of {} for {}.{}", directories, cfs.keyspace.getName(), cfs.getTableName());
                    DiskBoundaries oldBoundaries = db;
                    db = getDiskBoundaryValue(cfs, directories);
                    diskBoundaries.put(directories, db);
                    logger.debug("Updating boundaries of {} from {} to {} for {}.{}", directories, oldBoundaries, diskBoundaries, cfs.keyspace.getName(), cfs.getTableName());
                }
            }
        }
        return db;
    }

    /**
     * check if the given disk boundaries are out of date due not being set or to having too old diskVersion/ringVersion
     */
    private boolean isOutOfDate(DiskBoundaries db)
    {
        if (db == null)
            return true;
        long currentRingVersion = StorageService.instance.getTokenMetadata().getRingVersion();
        int currentDiskVersion = BlacklistedDirectories.getDirectoriesVersion();
        return currentRingVersion != db.ringVersion || currentDiskVersion != db.directoriesVersion;
    }

    private static DiskBoundaries getDiskBoundaryValue(ColumnFamilyStore cfs, Directories directories)
    {
        Collection<Range<Token>> localRanges;

        long ringVersion;
        TokenMetadata tmd;
        do
        {
            tmd = StorageService.instance.getTokenMetadata();
            ringVersion = tmd.getRingVersion();
            if (StorageService.instance.isBootstrapMode())
            {
                localRanges = tmd.getPendingRanges(cfs.keyspace.getName(), FBUtilities.getBroadcastAddress());
            }
            else
            {
                // Reason we use use the future settled TMD is that if we decommission a node, we want to stream
                // from that node to the correct location on disk, if we didn't, we would put new files in the wrong places.
                // We do this to minimize the amount of data we need to move in rebalancedisks once everything settled
                localRanges = cfs.keyspace.getReplicationStrategy().getAddressRanges(tmd.cloneAfterAllSettled()).get(FBUtilities.getBroadcastAddress());
            }
            logger.debug("Got local ranges {} (ringVersion = {})", localRanges, ringVersion);
        }
        while (ringVersion != tmd.getRingVersion()); // if ringVersion is different here it means that
                                                     // it might have changed before we calculated localRanges - recalculate

        int directoriesVersion;
        Directories.DataDirectory[] dirs;
        do
        {
            directoriesVersion = BlacklistedDirectories.getDirectoriesVersion();
            dirs = directories.getWriteableLocations();
        }
        while (directoriesVersion != BlacklistedDirectories.getDirectoriesVersion()); // if directoriesVersion has changed we need to recalculate

        if (localRanges == null || localRanges.isEmpty())
            return new DiskBoundaries(dirs, null, ringVersion, directoriesVersion);

        List<Range<Token>> sortedLocalRanges = Range.sort(localRanges);

        List<PartitionPosition> positions = getDiskBoundaries(sortedLocalRanges, cfs.getPartitioner(), dirs);
        return new DiskBoundaries(dirs, positions, ringVersion, directoriesVersion);
    }

    /**
     * Returns a list of disk boundaries, the result will differ depending on whether vnodes are enabled or not.
     *
     * What is returned are upper bounds for the disks, meaning everything from partitioner.minToken up to
     * getDiskBoundaries(..).get(0) should be on the first disk, everything between 0 to 1 should be on the second disk
     * etc.
     *
     * The final entry in the returned list will always be the partitioner maximum tokens upper key bound
     */
    private static List<PartitionPosition> getDiskBoundaries(List<Range<Token>> sortedLocalRanges, IPartitioner partitioner, Directories.DataDirectory[] dataDirectories)
    {
        assert partitioner.splitter().isPresent();
        Splitter splitter = partitioner.splitter().get();
        boolean dontSplitRanges = DatabaseDescriptor.getNumTokens() > 1;
        List<Token> boundaries = splitter.splitOwnedRanges(dataDirectories.length, sortedLocalRanges, dontSplitRanges);
        // If we can't split by ranges, split evenly to ensure utilisation of all disks
        if (dontSplitRanges && boundaries.size() < dataDirectories.length)
            boundaries = splitter.splitOwnedRanges(dataDirectories.length, sortedLocalRanges, false);

        List<PartitionPosition> diskBoundaries = new ArrayList<>();
        for (int i = 0; i < boundaries.size() - 1; i++)
            diskBoundaries.add(boundaries.get(i).maxKeyBound());
        diskBoundaries.add(partitioner.getMaximumToken().maxKeyBound());
        return diskBoundaries;
    }

    public void invalidate()
    {
        diskBoundaries.clear();
    }
}
