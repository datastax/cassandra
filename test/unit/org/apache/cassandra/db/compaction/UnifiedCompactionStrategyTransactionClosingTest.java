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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.when;

@RunWith(BMUnitRunner.class)
public class UnifiedCompactionStrategyTransactionClosingTest extends BaseCompactionStrategyTest
{
    public static AtomicInteger txClosures = new AtomicInteger(0);

    @BeforeClass
    public static void setUpClass()
    {
        BaseCompactionStrategyTest.setUpClass();
    }

    @Before
    public void setUp()
    {
        txClosures.set(0);
        super.setUp();
    }

    @Test
    @BMRules(rules = {
    @BMRule(name = "Throw exception to force tx closure",
    targetClass = "org.apache.cassandra.db.compaction.UnifiedCompactionStrategy",
    targetMethod = "createAndAddTasks",
    action = "throw new org.apache.cassandra.db.compaction.CompactionInterruptedException" +
             "(null, org.apache.cassandra.db.compaction.TableOperation$StopTrigger.UNIT_TESTS);"),
    @BMRule(name = "Capture tx closure",
    targetClass = "org.apache.cassandra.utils.concurrent.Transactional$AbstractTransactional",
    targetMethod = "close",
    action = "org.apache.cassandra.db.compaction.UnifiedCompactionStrategyTransactionClosingTest.txClosures.incrementAndGet()")
    })
    public void testTransactionClosesGetMaximalTasks()
    {
        Set<SSTableReader> allSSTables = new HashSet<>();
        allSSTables.addAll(mockNonOverlappingSSTables(12, 0, 100 << 20));
        dataTracker.addInitialSSTables(allSSTables);

        Controller controller = Mockito.mock(Controller.class);
        when(controller.getNumShards(anyDouble())).thenReturn(10);
        when(controller.parallelizeOutputShards()).thenReturn(true);
        when(controller.maxConcurrentCompactions()).thenReturn(1000);
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        try
        {
            strategy.getMaximalTasks(0, false, 20);
        }
        catch (RuntimeException e)
        {
        }
        assertTrue("The expected count of transaction close operations cannot be zero", txClosures.get() > 0);
    }

    @Test
    @BMRules(rules = {
    @BMRule(name = "Throw exception to force tx closure",
    targetClass = "org.apache.cassandra.db.compaction.BackgroundCompactions",
    targetMethod = "setSubmitted",
    action = "throw new org.apache.cassandra.db.compaction.CompactionInterruptedException" +
             "(null, org.apache.cassandra.db.compaction.TableOperation$StopTrigger.UNIT_TESTS);"),
    @BMRule(name = "Capture tx closure",
    targetClass = "org.apache.cassandra.utils.concurrent.Transactional$AbstractTransactional",
    targetMethod = "close",
    action = "org.apache.cassandra.db.compaction.UnifiedCompactionStrategyTransactionClosingTest.txClosures.incrementAndGet()")
    })
    public void testTransactionClosesCreateAndAddTasks()
    {
        Set<SSTableReader> allSSTables = new HashSet<>(mockNonOverlappingSSTables(12, 0, 100 << 20));
        dataTracker.addInitialSSTables(allSSTables);

        Controller controller = Mockito.mock(Controller.class);
        when(controller.getNumShards(anyDouble())).thenReturn(10);
        when(controller.parallelizeOutputShards()).thenReturn(true);
        when(controller.maxConcurrentCompactions()).thenReturn(1000);

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);
        Collection<CompactionAggregate.UnifiedAggregate> maximals = strategy.getMaximalAggregates();

        try
        {
            strategy.createAndAddTasks(0, maximals.iterator().next(), new ArrayList<>(), strategy);
        }
        catch (RuntimeException e)
        {
        }

        assertTrue("The expected count of transaction close operations cannot be zero", txClosures.get() > 0);
    }
}