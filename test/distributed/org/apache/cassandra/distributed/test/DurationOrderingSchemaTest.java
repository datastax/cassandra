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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.awaitility.Awaitility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tables with a duration where values are sorted, which CREATE TABLE rejects here but Apache Cassandra or nodes before
 * CNDB-9753 accepted, reach a node as a schema push from a peer and as a schema pull, get data, and survive a restart
 * of both nodes. The keyspace and the tables can still be
 * altered afterwards.
 */
public class DurationOrderingSchemaTest extends TestBaseImpl
{
    private static final List<String> TABLES = Arrays.asList(
    // Apache accepts these
    "CREATE TABLE %s.set_of_tuples (pk int PRIMARY KEY, s set<frozen<tuple<duration, int>>>, m map<frozen<list<duration>>, int>)",
    "CREATE TABLE %s.ck_set_of_lists (pk int, ck frozen<set<frozen<list<duration>>>>, v int, PRIMARY KEY (pk, ck))",
    "CREATE TABLE %s.pk_set_of_tuples (pk frozen<set<frozen<tuple<duration, int>>>> PRIMARY KEY, v int)",
    // Nodes before CNDB-9753 accepted this
    "CREATE TABLE %s.legacy_map_key (pk frozen<map<duration, text>>, ck frozen<map<duration, text>>, v int, PRIMARY KEY (pk, ck))"
    );

    private static final String[] TABLE_NAMES = { "set_of_tuples", "ck_set_of_lists", "pk_set_of_tuples", "legacy_map_key" };


    /** Durations in the order they are written. */
    private static final String[] DURATIONS = { "1mo", "40d", "1d", "-1d", "1h" };

    /** Indexes into {@link #DURATIONS} in the order of their serialized bytes, which is how they sort. */
    private static final List<Integer> SORTED = Arrays.asList(4, 3, 2, 1, 0);

    @Test
    public void testPeerSchemaDataAndRestart() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)).start()))
        {
            for (String ddl : TABLES)
                assertThatThrownBy(() -> cluster.schemaChange(String.format(ddl, KEYSPACE)))
                .hasMessageContaining("duration types are not supported");

            // Node 2 pushes the tables to node 1, as a peer that accepted the CREATE TABLE would, without applying them
            // itself: it gets them back by pulling the schema from node 1.
            pushTablesWithoutApplying(cluster.get(2));

            Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
                for (IInvokableInstance instance : cluster)
                    assertThat(tables(instance)).contains(TABLE_NAMES);
                assertThat(schemaVersion(cluster.get(1))).isEqualTo(schemaVersion(cluster.get(2)));
            });

            for (int i = 0; i < DURATIONS.length; i++)
            {
                String d = DURATIONS[i];
                int coordinator = 1 + i % 2;
                execute(cluster, coordinator, "UPDATE %s.set_of_tuples SET s = s + {(%s, 0)}, m = m + {[%s]: 1} WHERE pk = 0", d, d);
                execute(cluster, coordinator, "INSERT INTO %s.ck_set_of_lists (pk, ck, v) VALUES (0, {[%s]}, %d)", d, i);
                execute(cluster, coordinator, "INSERT INTO %s.pk_set_of_tuples (pk, v) VALUES ({(%s, 0)}, %d)", d, i);
                execute(cluster, coordinator, "INSERT INTO %s.legacy_map_key (pk, ck, v) VALUES ({1d: 'a'}, {%s: 'x'}, %d)", d, i);
                // Some data in sstables, the rest only in the commit log
                if (i == 2)
                    cluster.forEach(instance -> instance.flush(KEYSPACE));
            }
            checkData(cluster);

            long[] restartMarks = new long[cluster.size()];
            for (IInvokableInstance instance : cluster)
            {
                long mark = instance.logs().mark();
                restartMarks[instance.config().num() - 1] = mark;
                instance.shutdown().get();
                instance.startup();
                assertThat(instance.logs().grep(mark, "InvalidColumnTypeException").getResult()).isEmpty();
            }
            Awaitility.await()
                      .atMost(60, TimeUnit.SECONDS)
                      .untilAsserted(() -> assertThat(schemaVersion(cluster.get(1))).isEqualTo(schemaVersion(cluster.get(2))));
            for (IInvokableInstance instance : cluster)
                assertThat(tables(instance)).contains(TABLE_NAMES);
            checkData(cluster);

            cluster.forEach(instance -> instance.flush(KEYSPACE));
            for (String table : TABLE_NAMES)
                cluster.forEach(instance -> instance.forceCompact(KEYSPACE, table));
            checkData(cluster);

            // The keyspace and these tables can still be altered, but not to give a new column such a type
            cluster.schemaChange(String.format("ALTER KEYSPACE %s WITH replication = " +
                                               "{'class': 'NetworkTopologyStrategy', 'datacenter0': 2}", KEYSPACE));
            for (String table : TABLE_NAMES)
                cluster.schemaChange(String.format("ALTER TABLE %s.%s ADD added int", KEYSPACE, table));
            assertThatThrownBy(() -> cluster.schemaChange(String.format("ALTER TABLE %s.set_of_tuples ADD s2 set<frozen<tuple<duration, int>>>",
                                                                        KEYSPACE)))
            .hasMessageContaining("duration types are not supported");
            Awaitility.await()
                      .atMost(60, TimeUnit.SECONDS)
                      .untilAsserted(() -> assertThat(schemaVersion(cluster.get(1))).isEqualTo(schemaVersion(cluster.get(2))));
            checkData(cluster);

            // Loading the schema warns about such a column once, whatever the number of loads since the restart (the
            // ALTERs above reload it from system_schema on the peer)
            for (IInvokableInstance instance : cluster)
                assertThat(instance.logs().grep(restartMarks[instance.config().num() - 1],
                                                "Column " + KEYSPACE + ".set_of_tuples.s of type .* has a duration in a sorted position").getResult())
                .hasSize(1);

            // New tables with these types are still rejected after all this
            for (String ddl : TABLES)
                assertThatThrownBy(() -> cluster.schemaChange(String.format(ddl.replace(".", ".new_"), KEYSPACE)))
                .hasMessageContaining("duration types are not supported");
        }
    }

    private static void pushTablesWithoutApplying(IInvokableInstance instance)
    {
        List<String> ddls = TABLES;
        String keyspaceName = KEYSPACE;
        instance.runOnInstance(() -> {
            KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
            List<Mutation> mutations = new ArrayList<>();
            for (String ddl : ddls)
            {
                TableMetadata table = CreateTableStatement.parse(String.format(ddl, keyspaceName), keyspaceName, keyspace.types).build();
                mutations.add(SchemaTestUtil.createTableMutation(keyspace, table, FBUtilities.timestampMicros()));
            }
            InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
            for (InetAddressAndPort peer : Gossiper.instance.getLiveMembers())
            {
                if (!peer.equals(self))
                    MessagingService.instance().send(Message.out(Verb.SCHEMA_PUSH_REQ, (Collection<Mutation>) mutations), peer);
            }
        });
    }

    private static void checkData(Cluster cluster)
    {
        for (int node = 1; node <= cluster.size(); node++)
        {
            String describe = "node" + node;
            assertThat(column(cluster, node, "SELECT v FROM %s.ck_set_of_lists WHERE pk = 0"))
            .describedAs(describe).isEqualTo(SORTED);
            assertThat(column(cluster, node, "SELECT v FROM %s.legacy_map_key WHERE pk = {1d: 'a'}"))
            .describedAs(describe).isEqualTo(SORTED);

            assertThat(column(cluster, node, "SELECT count(*) FROM %s.pk_set_of_tuples")).describedAs(describe).containsExactly(5L);
            for (String d : DURATIONS)
                assertThat(column(cluster, node, "SELECT v FROM %s.pk_set_of_tuples WHERE pk = {(" + d + ", 0)}"))
                .describedAs(describe + ": partition " + d).hasSize(1);

            Object[][] rows = cluster.coordinator(node).execute(String.format("SELECT s, m FROM %s.set_of_tuples WHERE pk = 0", KEYSPACE),
                                                                ConsistencyLevel.ALL);
            assertThat(rows.length).describedAs(describe).isEqualTo(1);
            assertThat((Collection<?>) rows[0][0]).describedAs(describe).hasSize(DURATIONS.length);
            assertThat((Map<?, ?>) rows[0][1]).describedAs(describe).hasSize(DURATIONS.length);
        }
    }

    private static List<Object> column(Cluster cluster, int node, String query)
    {
        List<Object> values = new ArrayList<>();
        for (Object[] row : cluster.coordinator(node).execute(String.format(query, KEYSPACE), ConsistencyLevel.ALL))
            values.add(row[0]);
        return values;
    }

    private static void execute(Cluster cluster, int node, String query, Object... args)
    {
        Object[] formatArgs = new Object[args.length + 1];
        formatArgs[0] = KEYSPACE;
        System.arraycopy(args, 0, formatArgs, 1, args.length);
        cluster.coordinator(node).execute(String.format(query, formatArgs), ConsistencyLevel.ALL);
    }

    private static UUID schemaVersion(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> Schema.instance.getVersion());
    }

    private static List<String> tables(IInvokableInstance instance)
    {
        String keyspaceName = KEYSPACE;
        return instance.callOnInstance(() -> {
            List<String> names = new ArrayList<>();
            KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
            if (keyspace != null)
                keyspace.tables.forEach(t -> names.add(t.name));
            return names;
        });
    }
}
