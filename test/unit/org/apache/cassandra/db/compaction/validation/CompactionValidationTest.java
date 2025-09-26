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
package org.apache.cassandra.db.compaction.validation;

import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class CompactionValidationTest extends CQLTester
{
    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s (pk int, ck1 int, ck2 int, v1 int, v2 int, primary key(pk, ck1, ck2))";

    private static final Injection SIMULATE_NOT_FULLY_EXPIRED = Injections.newCustom("simulate_not_fully_expired")
                                                                         .add(InvokePointBuilder.newInvokePoint().onClass("org.apache.cassandra.db.compaction.validation.CompactionValidationTask")
                                                                                                .onMethod("isFullyExpired"))
                                                                         .add(ActionBuilder.newActionBuilder().actions().doReturn(false))
                                                                         .build();

    @BeforeClass
    public static void setupClass()
    {
        CQLTester.setUpClass();

        DatabaseDescriptor.createAllDirectories();

        requireNetwork();
    }

    @Before
    public void setup()
    {
        CassandraRelevantProperties.COMPACTION_VALIDATION_MODE.reset();
        CassandraRelevantProperties.COMPACTION_VALIDATION_MODE.setString("WARN");
    }

    @After
    public void removeInjections()
    {
        Injections.deleteAll();
    }

    @Test
    public void testValidation() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);

        populateSSTable(1, 10, 5, 5);
        populateSSTable(13, 20, 5, 5);
        populateSSTable(23, 30, 5, 5);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(cfs.getLiveSSTables()).hasSize(3);

        Stats initial = Stats.fetch();

        cfs.forceMajorCompaction();

        assertThat(cfs.getLiveSSTables()).hasSize(1);
        assertSuccessfulValidationWithoutAbsentKeys(initial);
    }

    @Test
    public void testValidationWithRowTombstone() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);

        populateSSTable(1, 1, 5, 5);
        populateRowDeletionSSTable(1, 1, 5, 5);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(cfs.getLiveSSTables()).hasSize(2);

        Stats initial = Stats.fetch();

        cfs.forceMajorCompaction();

        assertThat(cfs.getLiveSSTables()).hasSize(1);
        assertSuccessfulValidationWithoutAbsentKeys(initial);
    }

    @Test
    public void testValidationWithExpiredRowTombstone() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE + " WITH gc_grace_seconds = 1");

        populateSSTable(0, 0, 4, 4);
        populateSSTable(1, 1, 4, 4);
        populateSSTable(2, 2, 4, 4);
        populateRowDeletionSSTable(0, 1, 4, 4); // delete all rows in key 0 and key 1

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(cfs.getLiveSSTables()).hasSize(4);

        // sleep 3 seconds to pass gc_grace_seconds
        FBUtilities.sleepQuietly(3000);

        Stats initial = Stats.fetch();

        cfs.forceMajorCompaction();
        assertThat(cfs.getLiveSSTables()).hasSize(1);

        assertSuccessfulValidationWithAbsentKeys(initial, 2); // key 0 and key 1 are removed
    }

    @Test
    public void testValidationWithExpiredRowTombstoneWithStatic() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v1 int, v2 int, st int static, primary key(pk, ck1, ck2)) WITH gc_grace_seconds = 1");

        populateSSTable(1, 1, 5, 5);
        populateSSTable(2, 2, 5, 5);
        execute("INSERT INTO %s (pk, st) VALUES (?, ?)", 2, 2);
        flush();

        populateRowDeletionSSTable(1, 1, 5, 5); // delete key 1 rows
        populatePartitionDeletionSSTable(2, 2); // delete key 2 rows and static rows
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(cfs.getLiveSSTables()).hasSize(5);

        // sleep 3 seconds to pass gc_grace_seconds
        FBUtilities.sleepQuietly(3000);

        Stats initial = Stats.fetch();

        cfs.forceMajorCompaction();
        assertThat(cfs.getLiveSSTables()).isEmpty();

        assertSuccessfulValidationWithAbsentKeys(initial, 2); // key 1 are removed
    }

    @Test
    public void testValidationWithExpiredPartitionTombstone() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE + " WITH gc_grace_seconds = 1");

        populateSSTable(0, 0, 5, 5);
        populateSSTable(1, 1, 5, 5);
        populateSSTable(2, 2, 5, 5);
        populatePartitionDeletionSSTable(0, 1);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(cfs.getLiveSSTables()).hasSize(4);

        // sleep 3 seconds to pass gc_grace_seconds
        FBUtilities.sleepQuietly(3000);

        Stats initial = Stats.fetch();

        cfs.forceMajorCompaction();
        assertThat(cfs.getLiveSSTables()).hasSize(1);

        assertSuccessfulValidationWithAbsentKeys(initial, 2); // key 0 and key 1 are removed
    }

    @Test
    public void testValidationWithExpiredRangeTombstone() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE + " WITH gc_grace_seconds = 1");

        populateSSTable(0, 0, 5, 5);
        populateSSTable(1, 1, 5, 5);
        populateSSTable(2, 2, 5, 5);
        populateRangeDeletionSSTable(0, 1, 5); // populate 5 range deletion for key 0 and 1

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(cfs.getLiveSSTables()).hasSize(4);

        // sleep 3 seconds to pass gc_grace_seconds
        FBUtilities.sleepQuietly(3000);

        Stats initial = Stats.fetch();

        cfs.forceMajorCompaction();
        assertThat(cfs.getLiveSSTables()).hasSize(1);

        assertSuccessfulValidationWithAbsentKeys(initial, 2); // key 0 and key 1 are removed
    }

    @Test
    public void testValidationWithTTLRowTombstone() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE + " WITH default_time_to_live = 1");

        populateSSTable(1, 1, 5, 5);
        populateSSTable(2, 2, 5, 5);
        populateSSTableWithTTL(3, 3, 4, 4, 100);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(cfs.getLiveSSTables()).hasSize(3);

        Stats initial = Stats.fetch();

        cfs.forceMajorCompaction();

        assertThat(cfs.getLiveSSTables()).hasSize(1);
        assertSuccessfulValidationWithoutAbsentKeys(initial);
    }

    @Test
    public void testValidationWithExpiredTTLRowTombstone() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE + " WITH default_time_to_live = 1 and gc_grace_seconds = 1");

        populateSSTable(1, 3, 5, 5);
        populateSSTableWithTTL(4, 4, 5, 5, 1000); // ttl of 1000 is not expiring

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(cfs.getLiveSSTables()).hasSize(2);

        // sleep 4 seconds to pass ttl and gc_grace_seconds
        FBUtilities.sleepQuietly(4000);

        Stats initial = Stats.fetch();

        cfs.forceMajorCompaction();

        assertThat(cfs.getLiveSSTables()).hasSize(1);
        assertSuccessfulValidationWithAbsentKeys(initial, 2); // 2 boundary keys from 1st sstable are absent
    }

    @Test
    public void testValidationWithoutOutputSSTable() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE + " WITH gc_grace_seconds = 1");

        populateSSTable(1, 1, 5, 5);
        populateSSTable(2, 2, 5, 5);
        populateRowDeletionSSTable(1, 2, 5, 5);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(cfs.getLiveSSTables()).hasSize(3);

        // sleep 3 seconds to pass gc_grace_seconds
        FBUtilities.sleepQuietly(3_000);

        Stats initial = Stats.fetch();

        cfs.forceMajorCompaction();

        // all sstables are removed
        assertThat(cfs.getLiveSSTables()).hasSize(0);
        assertSuccessfulValidationWithAbsentKeys(initial, 2);
    }

    @Test
    public void testAbortOnDataLoss() throws Throwable
    {
        CassandraRelevantProperties.COMPACTION_VALIDATION_MODE.setString("ABORT");
        Injections.inject(SIMULATE_NOT_FULLY_EXPIRED);

        createTable(CREATE_TABLE_TEMPLATE + " WITH gc_grace_seconds = 1");

        populateSSTable(1, 1, 5, 5);
        populateRowDeletionSSTable(1, 1, 5, 5);
        FBUtilities.sleepQuietly(2000);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> before = cfs.getLiveSSTables();
        assertThat(before).hasSize(2);

        Stats initial = Stats.fetch();

        assertThatThrownBy(() -> cfs.forceMajorCompaction()).hasMessageContaining("POTENTIAL DATA LOSS");

        assertThat(cfs.getLiveSSTables()).hasSize(2).isEqualTo(before);
        assertDataLossWithAbsentKeys(initial, 1); // key 1 missing
    }

    private void populateSSTable(int startPartition, int endPartition, int ck1PerPartition, int ck2PerClustering) throws Throwable
    {
        for (int partition = startPartition; partition <= endPartition; partition++)
        {
            for (int ck1 = 0; ck1 < ck1PerPartition; ck1++)
            {
                for (int ck2 = 0; ck2 < ck2PerClustering; ck2++)
                {
                    execute("INSERT INTO %s (pk, ck1, ck2, v1, v2) VALUES (?, ?, ?, ?, ?)", partition, ck1, ck2, 0, 0);
                }
            }
        }
        flush();
    }

    private void populateSSTableWithTTL(int startPartition, int endPartition, int ck1PerPartition, int ck2PerClustering, int ttl) throws Throwable
    {
        for (int partition = startPartition; partition <= endPartition; partition++)
        {
            for (int ck1 = 0; ck1 < ck1PerPartition; ck1++)
            {
                for (int ck2 = 0; ck2 < ck2PerClustering; ck2++)
                {
                    execute("INSERT INTO %s (pk, ck1, ck2, v1, v2) VALUES (?, ?, ?, ?, ?) USING TTL ?", partition, ck1, ck2, 0, 0, ttl);
                }
            }
        }
        flush();
    }

    private void populateRowDeletionSSTable(int startKey, int endKey, int ck1PerPartition, int ck2PerClustering) throws Throwable
    {
        for (int partition = startKey; partition <= endKey; partition++)
        {
            for (int ck1 = 0; ck1 < ck1PerPartition; ck1++)
            {
                for (int ck2 = 0; ck2 < ck2PerClustering; ck2++)
                {
                    execute("DELETE FROM %s where pk = ? and ck1 = ? and ck2 = ?", partition, ck1, ck2);
                }
            }
        }
        flush();
    }

    private void populatePartitionDeletionSSTable(int startKey, int endKey) throws Throwable
    {
        for (int partition = startKey; partition <= endKey; partition++)
        {
            execute("DELETE FROM %s where pk = ?", partition);
        }
        flush();
    }

    private void populateRangeDeletionSSTable(int startKey, int endKey, int ck1PerPartition) throws Throwable
    {
        for (int partition = startKey; partition <= endKey; partition++)
        {
            for (int ck1 = 0; ck1 < ck1PerPartition; ck1++)
            {
                execute("DELETE FROM %s where pk = ? and ck1 = ?", partition, ck1);
            }
        }
        flush();
    }

    private void assertSuccessfulValidationWithAbsentKeys(Stats initialStats, int absentKeys)
    {
        initialStats.assertStats(1, 0, absentKeys, 0);
    }

    private void assertSuccessfulValidationWithoutAbsentKeys(Stats initialStats)
    {
        initialStats.assertStats(1, 1, 0, 0);
    }

    private void assertDataLossWithAbsentKeys(Stats initialStats, int absentKeys)
    {
        initialStats.assertStats(1, 0, absentKeys, 1);
    }

    private static class Stats
    {
        private final int validations;
        private final int validationsWithoutAbsentKeys;
        private final int absentKeys;
        private final int potentialDataLosses;

        private Stats(int validations, int validationsWithoutAbsentKeys, int absentKeys, int potentialDataLosses)
        {
            this.validations = validations;
            this.validationsWithoutAbsentKeys = validationsWithoutAbsentKeys;
            this.absentKeys = absentKeys;
            this.potentialDataLosses = potentialDataLosses;
        }

        public static Stats fetch()
        {
            CompactionValidationMetrics metrics = CompactionValidationMetrics.INSTANCE;
            return new Stats((int) metrics.validationCount.count(),
                    (int) metrics.validationWithoutAbsentKeys.count(),
                    (int) metrics.absentKeys.count(),
                    (int) metrics.potentialDataLosses.count());
        }

        public void assertStats(int validationDiff, int validationsWithoutAbsentKeysDiff, int absentKeysDiff, int potentialDataLossesDiff)
        {
            Stats current = Stats.fetch();
            assertEquals(this.validations + validationDiff, current.validations);
            assertEquals(this.validationsWithoutAbsentKeys + validationsWithoutAbsentKeysDiff, current.validationsWithoutAbsentKeys);
            assertEquals(this.absentKeys + absentKeysDiff, current.absentKeys);
            assertEquals(this.potentialDataLosses + potentialDataLossesDiff, current.potentialDataLosses);
        }
    }
}
