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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.apache.cassandra.db.compaction.CompactionTaskTest.mockStrategy;
import static org.apache.cassandra.db.compaction.CompactionTaskTest.mutateRepaired;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DisabledRepairStateCheckingTest
{
    private static TableMetadata cfm;
    private static ColumnFamilyStore cfs;

    private final CompactionStrategy mockStrategy;

    public DisabledRepairStateCheckingTest()
    {
        this.mockStrategy = mockStrategy(cfs, true);
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        CassandraRelevantProperties.COMPACTION_SKIP_REPAIR_STATE_CHECKING.setBoolean(true);
        SchemaLoader.prepareServer();
        cfm = CreateTableStatement.parse("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)", "ks").build();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1), cfm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.id);
    }

    @Before
    public void setUp() throws Exception
    {
        cfs.getCompactionStrategyContainer().enable();
        cfs.truncateBlocking();
    }

    /**
     * Duplicate of {@link CompactionTaskTest#mixedSSTableFailure()} with disabled repair state checking. Creating the
     * task should succeed.
     */
    @Test
    public void mixedSSTableFailure() throws Exception
    {
        cfs.getCompactionStrategyContainer().disable();
        for (int m = 1; m < 16; ++m) // test all combinations of two or more sstables with different repair marking
        {
            if (Integer.bitCount(m) <= 1)
                continue;
            for (int order = 0; order < Integer.bitCount(m); order++)
            {

                cfs.truncateBlocking();
                QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (1, 1);");
                cfs.forceBlockingFlush(UNIT_TESTS);
                QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (2, 2);");
                cfs.forceBlockingFlush(UNIT_TESTS);
                QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (3, 3);");
                cfs.forceBlockingFlush(UNIT_TESTS);
                QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (4, 4);");
                cfs.forceBlockingFlush(UNIT_TESTS);

                List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
                Assert.assertEquals(4, sstables.size());

                SSTableReader unrepaired = sstables.get(0);
                SSTableReader repaired = sstables.get(1);
                SSTableReader pending1 = sstables.get(2);
                SSTableReader pending2 = sstables.get(3);

                mutateRepaired(repaired, FBUtilities.nowInSeconds(), ActiveRepairService.NO_PENDING_REPAIR, false);
                mutateRepaired(pending1, ActiveRepairService.UNREPAIRED_SSTABLE, UUIDGen.getTimeUUID(), false);
                mutateRepaired(pending2, ActiveRepairService.UNREPAIRED_SSTABLE, UUIDGen.getTimeUUID(), false);

                for (int i = 3; i >= 0; i--)
                {
                    if ((m & (1 << i)) == 0)
                        sstables.remove(i);
                }
                Collections.rotate(sstables, order);

                LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
                assertNotNull(txn);
                CompactionTask task = new CompactionTask(cfs, txn, 0, false, mockStrategy);
                assertNotNull(task); // task must be successfully created
                task.executeInternal(); // and run
                for (SSTableReader s : txn.current())
                {
                    assertFalse(s.isRepaired()); // and the resulting files must be marked unrepaired
                    assertNull(s.getPendingRepair());
                }
            }
        }
    }
}
