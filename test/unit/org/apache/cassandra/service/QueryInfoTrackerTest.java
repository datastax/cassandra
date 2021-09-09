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

package org.apache.cassandra.service;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Session;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;

/**
 * Tests that the methods of the {@link QueryInfoTracker} interface are correctly called.
 *
 * <p>The tests below use "drivers" sessions so as to ensure that queries go through {@link StorageProxy}, where
 * {@link QueryInfoTracker} is setup.
 */
public class QueryInfoTrackerTest extends CQLTester
{
    private static final String KEYSPACE = "test_ks";
    private volatile TestQueryInfoTracker tracker;
    private volatile Session session;

    @Before
    public void setupTest()
    {
        tracker = new TestQueryInfoTracker();
        StorageProxy.instance.register(tracker);
        requireNetwork();
        session = sessionNet();
        // Just in case the teardown didn't run for some reason.
        session.execute("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
                        " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
    }

    @After
    public void teardownTest()
    {
        session.execute("DROP KEYSPACE IF EXISTS " + KEYSPACE);
    }

    @Test
    public void testSimpleQueryTracing()
    {
        int KEYS = 4;
        int CLUSTERING = 4;
        String TABLE = KEYSPACE + ".simple";
        session.execute("CREATE TABLE " + TABLE + "(k int, c int, v int, PRIMARY KEY (k, c))");
        for (int k = 0; k < KEYS; k++)
        {
            for (int v = 0; v < CLUSTERING; v++)
            {
                session.execute("INSERT INTO " + TABLE + "(k, c) values (?, ?)", k, v);
            }
        }

        int expectedWrites = KEYS * CLUSTERING;
        int expectedRows = KEYS * CLUSTERING;
        assertEquals(expectedWrites, tracker.writes.get());
        assertEquals(expectedRows, tracker.writtenRows.get());
        assertEquals(0, tracker.loggedWrites.get());

        session.execute("SELECT * FROM " + TABLE + " WHERE k = ?", 0);

        session.execute("UPDATE " + TABLE + " SET v = ? WHERE k = ? AND c IN ?", 42, 0, Arrays.asList(0, 2, 3));
        expectedWrites += 1; // We only did one more write ...
        expectedRows += 3;   // ... but it updates 3 rows.
        assertEquals(expectedWrites, tracker.writes.get());
        assertEquals(expectedRows, tracker.writtenRows.get());
        assertEquals(0, tracker.loggedWrites.get());
    }

    @Test
    public void testLoggedBatchQueryTracing()
    {
        String TABLE = KEYSPACE + ".logged_batch";
        session.execute("CREATE TABLE " + TABLE + "(k int, c int, v int, PRIMARY KEY (k, c))");

        session.execute("BEGIN BATCH "
                        + "INSERT INTO " + TABLE + "(k, c, v) VALUES (0, 0, 0);"
                        + "INSERT INTO " + TABLE + "(k, c, v) VALUES (1, 1, 1);"
                        + "INSERT INTO " + TABLE + "(k, c, v) VALUES (2, 2, 2);"
                        + "INSERT INTO " + TABLE + "(k, c, v) VALUES (3, 3, 3);"
                        + "APPLY BATCH");

        assertEquals(1, tracker.writes.get());
        assertEquals(1, tracker.loggedWrites.get());
        assertEquals(4, tracker.writtenRows.get());

        session.execute("BEGIN BATCH "
                        + "INSERT INTO " + TABLE + "(k, c, v) VALUES (4, 4, 4);"
                        + "INSERT INTO " + TABLE + "(k, c, v) VALUES (5, 5, 5);"
                        + "APPLY BATCH");

        assertEquals(2, tracker.writes.get());
        assertEquals(2, tracker.loggedWrites.get());
        assertEquals(6, tracker.writtenRows.get());
    }

    private static class TestQueryInfoTracker implements QueryInfoTracker
    {
        public final AtomicInteger writes = new AtomicInteger();
        public final AtomicInteger loggedWrites = new AtomicInteger();
        public final AtomicInteger writtenRows = new AtomicInteger();
        public final AtomicInteger errorWrites = new AtomicInteger();

        private boolean shouldIgnore(TableMetadata table)
        {
            // We exclude anything that isn't on our test keyspace to be sure no "system" query interferes.
            return !table.keyspace.equals(KEYSPACE);
        }

        private TableMetadata extractTable(Collection<? extends IMutation> mutations)
        {
            return Iterables.getLast(mutations).getPartitionUpdates().iterator().next().metadata();
        }

        @Override
        public WriteTracker onWrite(ClientState state,
                                    boolean isLogged,
                                    Collection<? extends IMutation> mutations,
                                    ConsistencyLevel consistencyLevel)
        {
            if (shouldIgnore(extractTable(mutations)))
                return WriteTracker.NOOP;

            return new WriteTracker() {
                @Override
                public void onDone()
                {
                    writes.incrementAndGet();
                    if (isLogged)
                        loggedWrites.incrementAndGet();
                    for (IMutation mutation : mutations)
                    {
                        for (PartitionUpdate update : mutation.getPartitionUpdates())
                        {
                            writtenRows.addAndGet(update.rowCount());
                        }
                    }
                }

                @Override
                public void onError(Throwable exception)
                {
                    errorWrites.incrementAndGet();
                }
            };
        }
    }
}
