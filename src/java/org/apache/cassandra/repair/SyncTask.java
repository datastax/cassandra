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
package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import com.google.common.util.concurrent.AbstractFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.RangeHash;

/**
 * SyncTask will calculate the difference of MerkleTree between two nodes
 * and perform necessary operation to repair replica.
 */
public abstract class SyncTask extends AbstractFuture<SyncStat> implements Runnable
{
    private static Logger logger = LoggerFactory.getLogger(SyncTask.class);

    protected final RepairJobDesc desc;
    private TreeResponse r1;
    private TreeResponse r2;
    protected final InetAddress endpoint1;
    protected final InetAddress endpoint2;

    private final Executor taskExecutor;
    private final SyncTask next;
    private final Map<InetAddress, Set<RangeHash>> receivedRangeCache;

    protected volatile SyncStat stat;

    public SyncTask(RepairJobDesc desc, TreeResponse r1, TreeResponse r2, Executor taskExecutor, SyncTask next,
                    Map<InetAddress, Set<RangeHash>> receivedRangeCache)
    {
        this.desc = desc;
        this.r1 = r1;
        this.r2 = r2;
        this.endpoint1 = r1.endpoint;
        this.endpoint2 = r2.endpoint;
        this.taskExecutor = taskExecutor;
        this.next = next;
        this.receivedRangeCache = receivedRangeCache;
    }

    /**
     * Compares trees, and triggers repairs for any ranges that mismatch.
     */
    public void run()
    {
        try
        {
            // compare trees, and collect differences
            List<MerkleTree.TreeDifference> diffs = MerkleTrees.diff(r1.trees, r2.trees);

            // Finished with trees, now help java free them
            r1 = null;
            r2 = null;


            stat = new SyncStat(new NodePair(endpoint1, endpoint2), diffs.size());

            // choose a repair method based on the significance of the difference
            String format = String.format("[repair #%s] Endpoints %s and %s %%s for %s", desc.sessionId, endpoint1, endpoint2, desc.columnFamily);
            if (diffs.isEmpty())
            {
                logger.info(String.format(format, "are consistent"));
                Tracing.traceRepair("Endpoint {} is consistent with {} for {}.", endpoint1, endpoint2, desc.columnFamily);
                set(stat);
                return;
            }

            List<Range<Token>> transferToLeft = new ArrayList<>(diffs.size());
            List<Range<Token>> transferToRight = new ArrayList<>(diffs.size());

            for (MerkleTree.TreeDifference treeDiff : diffs)
            {
                RangeHash rightRangeHash = treeDiff.getRightRangeHash();
                RangeHash leftRangeHash = treeDiff.getLeftRangeHash();
                Set<RangeHash> leftReceived = receivedRangeCache.computeIfAbsent(endpoint1, i -> new HashSet<>());
                Set<RangeHash> rightReceived = receivedRangeCache.computeIfAbsent(endpoint2, i -> new HashSet<>());
                if (leftReceived.contains(rightRangeHash))
                {
                    logger.trace("Skipping transfer of already transferred range {} to {}.", treeDiff, endpoint1);
                }
                else
                {
                    transferToLeft.add(treeDiff);
                    leftReceived.add(rightRangeHash);
                }
                if (rightReceived.contains(leftRangeHash))
                {
                    logger.trace("Skipping transfer of already transferred range {} to {}.", treeDiff, endpoint2);
                }
                else
                {
                    transferToRight.add(treeDiff);
                    rightReceived.add(leftRangeHash);
                }
            }

            // non-0 difference: perform streaming repair
            int skippedLeft = diffs.size() - transferToLeft.size();
            int skippedRight = diffs.size() - transferToRight.size();
            String skippedMsg = transferToLeft.size() != diffs.size() || transferToRight.size() != diffs.size()?
                                String.format(" (%d and %d ranges skipped respectively).", skippedLeft, skippedRight) : "";
            logger.info(String.format(format, "have " + diffs.size() + " range(s) out of sync") + skippedMsg);
            Tracing.traceRepair("Endpoint {} has {} range(s) out of sync with {} for {}{}.",
                                endpoint1, diffs.size(), endpoint2, desc.columnFamily, skippedMsg);

            if (transferToLeft.isEmpty() && transferToRight.isEmpty())
            {
                logger.info("[repair #{}] All differences between {} and {} already transferred for {}.", desc.sessionId, endpoint1, endpoint2, desc.columnFamily);
                set(stat);
                return;
            }

            startSync(transferToLeft, transferToRight);
        }
        catch (Throwable t)
        {
            logger.info("[repair #{}] Error while calculating differences between {} and {}.", desc.sessionId, endpoint1, endpoint2, t);
            setException(t);
        }
        finally
        {
            if (next != null)
                this.taskExecutor.execute(next);
        }
    }

    public SyncStat getCurrentStat()
    {
        return stat;
    }

    protected abstract void startSync(List<Range<Token>> transferToLeft, List<Range<Token>> transferToRight);
}
