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

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import com.google.common.util.concurrent.AbstractFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.MerkleTrees;

/**
 * SyncTask will calculate the difference of MerkleTree between two nodes
 * and perform necessary operation to repair replica.
 */
public abstract class SyncTask extends AbstractFuture<SyncStat> implements Runnable
{
    private static Logger logger = LoggerFactory.getLogger(SyncTask.class);

    protected final RepairJobDesc desc;
    protected final TreeResponse r1;
    protected final TreeResponse r2;
    private final Executor taskExecutor;
    private final SyncTask next;

    protected volatile SyncStat stat;

    public SyncTask(RepairJobDesc desc, TreeResponse r1, TreeResponse r2, Executor taskExecutor, SyncTask next)
    {
        this.desc = desc;
        this.r1 = r1;
        this.r2 = r2;
        this.taskExecutor = taskExecutor;
        this.next = next;
    }

    /**
     * Compares trees, and triggers repairs for any ranges that mismatch.
     */
    public void run()
    {
        try
        {
            // compare trees, and collect differences
            List<Range<Token>> differences = MerkleTrees.difference(r1.trees, r2.trees);

            stat = new SyncStat(new NodePair(r1.endpoint, r2.endpoint), differences.size());

            // choose a repair method based on the significance of the difference
            String format = String.format("[repair #%s] Endpoints %s and %s %%s for %s", desc.sessionId, r1.endpoint, r2.endpoint, desc.columnFamily);
            if (differences.isEmpty())
            {
                logger.info(String.format(format, "are consistent"));
                Tracing.traceRepair("Endpoint {} is consistent with {} for {}", r1.endpoint, r2.endpoint, desc.columnFamily);
                set(stat);
                return;
            }

            // non-0 difference: perform streaming repair
            logger.info(String.format(format, "have " + differences.size() + " range(s) out of sync"));
            Tracing.traceRepair("Endpoint {} has {} range(s) out of sync with {} for {}", r1.endpoint, differences.size(), r2.endpoint, desc.columnFamily);
            startSync(differences);
        }
        catch (Throwable t)
        {
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

    protected abstract void startSync(List<Range<Token>> differences);
}
