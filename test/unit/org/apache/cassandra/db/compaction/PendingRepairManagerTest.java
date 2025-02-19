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

package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.repair.consistent.LocalSessionAccessor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class PendingRepairManagerTest extends AbstractPendingRepairTest
{
    @Override
    public String createTableCql()
    {
        // Note: This test is tightly coupled to the LegacyAbstractCompactionStrategy so cannot use the default UCS
        // UCS is tested in UnifiedCompactionContainerPendingRepairTest
        return String.format("CREATE TABLE %s.%s (k INT PRIMARY KEY, v INT) WITH COMPACTION={'class':'SizeTieredCompactionStrategy'}",
                             ks, tbl);
    }

    /**
     * If a local session is ongoing, it should not be cleaned up
     */
    @Test
    public void needsCleanupInProgress()
    {
        PendingRepairManager prm = new PendingRepairManager(cfs, strategyFactory, cfs.getCompactionParams(), false);

        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));

        Assert.assertFalse(prm.canCleanup(repairID));
    }

    /**
     * If a local session is finalized, it should be cleaned up
     */
    @Test
    public void needsCleanupFinalized()
    {
        PendingRepairManager prm = new PendingRepairManager(cfs, strategyFactory, cfs.getCompactionParams(), false);

        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));
        LocalSessionAccessor.finalizeUnsafe(repairID);

        Assert.assertTrue(prm.canCleanup(repairID));
    }

    /**
     * If a local session has failed, it should be cleaned up
     */
    @Test
    public void needsCleanupFailed()
    {
        PendingRepairManager prm = new PendingRepairManager(cfs, strategyFactory, cfs.getCompactionParams(), false);

        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));
        LocalSessionAccessor.failUnsafe(repairID);

        Assert.assertTrue(prm.canCleanup(repairID));
    }

    @Test
    public void needsCleanupNoSession()
    {
        UUID fakeID = UUIDGen.getTimeUUID();
        PendingRepairManager prm = new PendingRepairManager(cfs, strategyFactory, null, false);
        Assert.assertTrue(prm.canCleanup(fakeID));
    }

    @Test
    public void estimateRemainingTasksInProgress()
    {
        PendingRepairManager prm = new PendingRepairManager(cfs, strategyFactory, cfs.getCompactionParams(), false);

        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));

        Assert.assertEquals(0, prm.getEstimatedRemainingTasks());
        Assert.assertEquals(0, prm.getNumPendingRepairFinishedTasks());
    }

    @Test
    public void estimateRemainingFinishedRepairTasks()
    {
        PendingRepairManager prm = new PendingRepairManager(cfs, strategyFactory, cfs.getCompactionParams(), false);

        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));
        Assert.assertNotNull(prm.get(repairID));
        LocalSessionAccessor.finalizeUnsafe(repairID);

        Assert.assertEquals(0, prm.getEstimatedRemainingTasks());
        Assert.assertEquals(1, prm.getNumPendingRepairFinishedTasks());
    }

    @Test
    public void getNextBackgroundTask()
    {
        PendingRepairManager prm = new PendingRepairManager(cfs, strategyFactory, cfs.getCompactionParams(), false);

        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        prm.addSSTable(sstable);

        repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        prm.addSSTable(sstable);
        LocalSessionAccessor.finalizeUnsafe(repairID);

        Assert.assertEquals(2, prm.getSessions().size());
        Assert.assertTrue(prm.getNextBackgroundTasks(FBUtilities.nowInSeconds()).isEmpty());
        Collection<AbstractCompactionTask> compactionTasks = prm.getNextRepairFinishedTasks();
        Assert.assertEquals(1, compactionTasks.size());
        AbstractCompactionTask compactionTask = compactionTasks.iterator().next();
        try
        {
            Assert.assertNotNull(compactionTask);
            Assert.assertSame(RepairFinishedCompactionTask.class, compactionTask.getClass());
            RepairFinishedCompactionTask cleanupTask = (RepairFinishedCompactionTask) compactionTask;
            Assert.assertEquals(repairID, cleanupTask.getSessionID());
        }
        finally
        {
            compactionTask.transaction.abort();
        }
    }

    @Test
    public void getNextBackgroundTaskNoSessions()
    {
        PendingRepairManager prm = new PendingRepairManager(cfs, strategyFactory, cfs.getCompactionParams(), false);
        Assert.assertTrue(prm.getNextBackgroundTasks(FBUtilities.nowInSeconds()).isEmpty());
    }

    /**
     * If all sessions should be cleaned up, getNextBackgroundTask should return null
     */
    @Test
    public void getNextBackgroundTaskAllCleanup()
    {
        PendingRepairManager prm = new PendingRepairManager(cfs, strategyFactory, cfs.getCompactionParams(), false);
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));
        Assert.assertNotNull(prm.get(repairID));
        LocalSessionAccessor.finalizeUnsafe(repairID);

        Assert.assertTrue(prm.getNextBackgroundTasks(FBUtilities.nowInSeconds()).isEmpty());

    }

    @Test
    public void maximalTaskNeedsCleanup()
    {
        PendingRepairManager prm = new PendingRepairManager(cfs, strategyFactory, cfs.getCompactionParams(), false);

        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));
        Assert.assertNotNull(prm.get(repairID));
        LocalSessionAccessor.finalizeUnsafe(repairID);

        Collection<AbstractCompactionTask> tasks = prm.getMaximalTasks(FBUtilities.nowInSeconds(), false, 0);
        try
        {
            Assert.assertEquals(1, tasks.size());
        }
        finally
        {
            tasks.stream().forEach(t -> t.transaction.abort());
        }
    }

    @Test
    public void userDefinedTaskTest()
    {
        PendingRepairManager prm = new PendingRepairManager(cfs, strategyFactory, cfs.getCompactionParams(), false);
        UUID repairId = registerSession(cfs, true, true);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairId, false);
        prm.addSSTable(sstable);

        Collection<AbstractCompactionTask> tasks = prm.createUserDefinedTasks(Collections.singleton(sstable), 100);
        Assert.assertEquals(1, tasks.size());
    }

    @Test
    public void mixedPendingSessionsTest()
    {
        PendingRepairManager prm = new PendingRepairManager(cfs, strategyFactory, cfs.getCompactionParams(), false);
        UUID repairId = registerSession(cfs, true, true);
        UUID repairId2 = registerSession(cfs, true, true);
        SSTableReader sstable = makeSSTable(true);
        SSTableReader sstable2 = makeSSTable(true);

        mutateRepaired(sstable, repairId, false);
        mutateRepaired(sstable2, repairId2, false);
        prm.addSSTable(sstable);
        prm.addSSTable(sstable2);
        Collection<AbstractCompactionTask> tasks = prm.createUserDefinedTasks(Lists.newArrayList(sstable, sstable2), 100);
        Assert.assertEquals(2, tasks.size());
    }

    /**
     * Tests that a IllegalSSTableArgumentException is thrown if we try to get
     * scanners for an sstable that isn't pending repair
     */
    @Test(expected = PendingRepairManager.IllegalSSTableArgumentException.class)
    public void getScannersInvalidSSTable() throws Exception
    {
        PendingRepairManager prm = new PendingRepairManager(cfs, strategyFactory, cfs.getCompactionParams(), false);
        SSTableReader sstable = makeSSTable(true);
        prm.getScanners(Collections.singleton(sstable), Collections.singleton(RANGE1));
    }

    /**
     * Tests that a IllegalSSTableArgumentException is thrown if we try to get
     * scanners for an sstable that isn't pending repair
     */
    @Test(expected = PendingRepairManager.IllegalSSTableArgumentException.class)
    public void getOrCreateInvalidSSTable() throws Exception
    {
        PendingRepairManager prm = new PendingRepairManager(cfs, strategyFactory, cfs.getCompactionParams(), false);
        SSTableReader sstable = makeSSTable(true);
        prm.getOrCreate(sstable);
    }

    @Test
    public void sessionHasData()
    {
        PendingRepairManager prm = new PendingRepairManager(cfs, strategyFactory, cfs.getCompactionParams(), false);

        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        Assert.assertFalse(prm.hasDataForSession(repairID));
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        prm.addSSTable(sstable);
        Assert.assertTrue(prm.hasDataForSession(repairID));
    }

    @Test
    public void noEmptyCompactionTask()
    {
        PendingRepairManager prm = new PendingRepairManager(cfs, strategyFactory, cfs.getCompactionParams(), false);
        SSTableReader sstable = makeSSTable(false);
        UUID id = UUID.randomUUID();
        mutateRepaired(sstable, id, false);
        prm.getOrCreate(sstable);
        cfs.truncateBlocking();
        Assert.assertFalse(cfs.getSSTables(SSTableSet.LIVE).iterator().hasNext());
        Assert.assertTrue(cfs.getCompactionStrategy().getNextBackgroundTasks(0).isEmpty());

    }
}
