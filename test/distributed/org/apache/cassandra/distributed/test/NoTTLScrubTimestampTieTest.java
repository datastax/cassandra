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

package org.apache.cassandra.distributed.test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertTrue;

/**
 * Shows what a {@code nodetool scrub -o NO_TTL [--bump-ttl-timestamp]} on every replica does when other copies of a cell
 * carry the same timestamp. At equal timestamps a tombstone or an expiring cell beats a non-expiring cell (CASSANDRA-14592),
 * so a permanent cell rewritten at the original timestamp loses to them unless {@code --bump-ttl-timestamp} is specified.
 * <p>
 * Every test scrubs all the replicas, with no client write in the whole test: this is the cloned database scenario.
 * k=0 is written with INSERT, so the row also has primary key liveness, and k=1 with UPDATE, so it has none.
 */
public class NoTTLScrubTimestampTieTest extends TestBaseImpl
{
    private static final long WRITE_TIMESTAMP = 1000;
    private static final String CREATE = "CREATE TABLE %s.%s (k int PRIMARY KEY, v int) " +
                                         "WITH gc_grace_seconds = 864000 " +
                                         "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'}";
    private static final String SELECT_ALL = "SELECT k, v FROM %s.%s";
    private static final String SELECT_DETAILS = "SELECT k, v, ttl(v), writetime(v) FROM %s.%s";

    /**
     * Data still live when the clone is scrubbed: no tombstone exists yet and no expiring copy survives the scrub,
     * so the outcome doesn't depend on the timestamp.
     */
    @Test
    public void unexpiredDataOnIdleClone() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            String table = "unexpired";
            create(cluster, table);
            write(cluster, table, 3600);
            cluster.forEach(node -> node.flush(KEYSPACE));

            cluster.forEach(node -> scrubNoTTL(node, table, false));

            assertRows(cluster.coordinator(1).execute(String.format(SELECT_ALL, KEYSPACE, table), ConsistencyLevel.ALL),
                       row(1, 1), row(0, 0));
        }
    }

    /**
     * The TTL elapsed before the clone was taken, and only one replica had compacted the data, so that replica holds
     * a tombstone at the write timestamp where the other one still holds the expired cell. Scrub keeps the tombstone
     * and makes the expired cell permanent: with {@code --bump-ttl-timestamp}, the data the scrub brings back supersedes
     * the tombstone.
     */
    @Test
    public void expiredDataCompactedOnOneReplicaOnly() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            String table = "expired";
            create(cluster, table);
            int ttl = 2;
            write(cluster, table, ttl);
            cluster.forEach(node -> node.flush(KEYSPACE));
            TimeUnit.SECONDS.sleep(ttl + 1);
            cluster.get(1).forceCompact(KEYSPACE, table);

            cluster.forEach(node -> scrubNoTTL(node, table, true));

            StringBuilder timeline = new StringBuilder();
            describe(timeline, "after scrubbing both replicas", cluster, table);

            String read = String.format(SELECT_ALL, KEYSPACE, table);
            Object[][] result = cluster.coordinator(1).execute(read, ConsistencyLevel.ALL);
            timeline.append("read at ALL: ").append(Arrays.deepToString(result)).append('\n');
            describe(timeline, "after that read, with its read repair", cluster, table);

            assertTrue(timeline.toString(), Arrays.deepEquals(new Object[][]{ row(1, 1), row(0, 0) }, result));
        }
    }

    /**
     * One replica at a time, while the TTL is still running and with a read between the two scrubs. The read repair
     * brings the expiring copy back to the replica already scrubbed, in its memtable, where no scrub can reach it.
     * With {@code --bump-ttl-timestamp}, the bumped permanent cell supersedes the expiring copy.
     */
    @Test
    public void readDuringRollingScrub() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            String table = "rolling";
            create(cluster, table);
            int ttl = 10;
            long expiresAtMillis = Clock.Global.currentTimeMillis() + TimeUnit.SECONDS.toMillis(ttl);
            write(cluster, table, ttl);
            cluster.forEach(node -> node.flush(KEYSPACE));

            StringBuilder timeline = new StringBuilder();
            String read = String.format(SELECT_ALL, KEYSPACE, table);

            scrubNoTTL(cluster.get(1), table, true);
            describe(timeline, "node1 scrubbed, node2 not yet", cluster, table);
            cluster.coordinator(1).execute(read, ConsistencyLevel.ALL);
            describe(timeline, "after a read at ALL, with its read repair", cluster, table);
            scrubNoTTL(cluster.get(2), table, true);
            describe(timeline, "both replicas scrubbed", cluster, table);

            TimeUnit.MILLISECONDS.sleep(Math.max(0, expiresAtMillis - Clock.Global.currentTimeMillis()) + 1000);
            describe(timeline, "original TTL elapsed", cluster, table);

            Object[][] result = cluster.coordinator(1).execute(read, ConsistencyLevel.ALL);
            timeline.append("read at ALL: ").append(Arrays.deepToString(result)).append('\n');

            assertTrue(timeline.toString(), Arrays.deepEquals(new Object[][]{ row(1, 1), row(0, 0) }, result));
        }
    }

    private static void create(Cluster cluster, String table)
    {
        cluster.schemaChange(String.format(CREATE, KEYSPACE, table));
    }

    private static void write(Cluster cluster, String table, int ttl)
    {
        cluster.coordinator(1).execute(String.format("INSERT INTO %s.%s (k, v) VALUES (0, 0) USING TIMESTAMP %d AND TTL %d",
                                                     KEYSPACE, table, WRITE_TIMESTAMP, ttl),
                                       ConsistencyLevel.ALL);
        cluster.coordinator(1).execute(String.format("UPDATE %s.%s USING TIMESTAMP %d AND TTL %d SET v = 1 WHERE k = 1",
                                                     KEYSPACE, table, WRITE_TIMESTAMP, ttl),
                                       ConsistencyLevel.ALL);
    }

    private static void scrubNoTTL(IInvokableInstance node, String table, boolean bumpTimestamp)
    {
        NodeToolResult result;
        if (bumpTimestamp)
            result = node.nodetoolResult("scrub", "--no-snapshot", "--overwrite-ttl", "NO_TTL", "--bump-ttl-timestamp", KEYSPACE, table);
        else
            result = node.nodetoolResult("scrub", "--no-snapshot", "--overwrite-ttl", "NO_TTL", KEYSPACE, table);
        result.asserts().success();
    }

    private static void describe(StringBuilder timeline, String step, Cluster cluster, String table)
    {
        String select = String.format(SELECT_DETAILS, KEYSPACE, table);
        timeline.append(step).append(":\n");
        cluster.forEach(node -> timeline.append("  node").append(node.config().num())
                                        .append(" [k, v, ttl(v), writetime(v)] = ")
                                        .append(Arrays.deepToString(node.executeInternal(select)))
                                        .append('\n'));
    }
}
