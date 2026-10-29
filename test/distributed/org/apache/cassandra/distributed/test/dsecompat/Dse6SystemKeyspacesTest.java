/*
 * Copyright IBM Corp.
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

package org.apache.cassandra.distributed.test.dsecompat;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.schema.DefaultSchemaUpdateHandler;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An upgraded node in {@code DSE_6} mode leaves the replicated system keyspaces ({@code system_auth},
 * {@code system_distributed}, {@code system_traces}) as the old nodes defined them, and the HCD 1.2 peer's schema does
 * not gain this software's tables (HCD 1.2 is a 4.0 line with system tables of its own, so the lists are compared
 * before and after the upgrade).
 * <p>
 * The failure observed with DSE 6.8.65 came from DSE pulling the upgraded node's schema and merging it. HCD 1.2 does not
 * pull the schema of a peer of another major version by itself, and the harness runs without gossip (see
 * {@code Dse6GossipTest}), so the test makes the HCD 1.2 node do what DSE did: send a schema pull to the upgraded node at
 * the DSE dialect and merge the answer through the path of its migration coordinator, and push a schema change to the
 * upgraded node, which ignores it. At the end the upgraded node restarts with {@code internode_compatibility_mode: NONE}
 * and makes the additions it deferred.
 */
public class Dse6SystemKeyspacesTest extends DseDialectTestBase
{
    private static final String REPLICATED_SYSTEM_KEYSPACES = "'system_auth', 'system_distributed', 'system_traces'";
    private static final int HCD = 2;
    /** The keyspace the HCD 1.2 node pushes to the upgraded node. */
    private static final String PUSHED_KEYSPACE = "pushed_by_hcd12";
    /** A table of this software that HCD 1.2's system_distributed does not have. */
    private static final String DEFERRED_TABLE = "system_distributed.partition_denylist";

    private static volatile Set<String> tablesBefore;
    private static volatile Set<String> columnsBefore;
    private static volatile UUID hcdSchemaBefore;

    @Test
    public void replicatedSystemKeyspacesKeepTheOldDefinitions() throws Throwable
    {
        dse6TestCase(2)
        .nodesToUpgrade(1)
        .setup(cluster -> {
            tablesBefore = tables(cluster.get(HCD));
            columnsBefore = columns(cluster.get(HCD));
            hcdSchemaBefore = hcdSchemaVersion(cluster.get(HCD));
            // both HCD 1.2 nodes have the same definitions to begin with
            assertThat(tables(cluster.get(1))).isEqualTo(tablesBefore);
            assertThat(tablesBefore).contains("system_auth.roles", "system_distributed.repair_history", "system_traces.sessions");
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            // the upgraded node kept the definitions it loaded, and says what it defers to the NONE start
            assertThat(tables(cluster.get(node))).isEqualTo(tablesBefore);
            assertThat(columns(cluster.get(node))).isEqualTo(columnsBefore);
            for (String keyspace : new String[]{ "system_auth", "system_distributed", "system_traces" })
                assertThat(cluster.get(node).logs().grep("internode_compatibility_mode DSE_6: " + keyspace + " keeps the definition it was loaded with").getResult())
                .as(keyspace).isNotEmpty();
            assertThat(tablesBefore).doesNotContain(DEFERRED_TABLE);
            assertThat(cluster.get(node).logs().grep("internode_compatibility_mode DSE_6: system_distributed keeps the definition .*: \\[.*" +
                                                     DEFERRED_TABLE.replace(".", "\\.") + "[,\\]]").getResult())
            .as("the deferred additions of system_distributed").isNotEmpty();

            // the HCD 1.2 node pulls the upgraded node's schema at the DSE dialect, as DSE does, and merges the answer:
            // there is nothing in it, and its schema is unchanged
            assertThat(hcdPullsAndMergesSchemaOf(cluster.get(node).broadcastAddress())).isZero();
            assertThat(tables(cluster.get(HCD))).isEqualTo(tablesBefore);
            assertThat(columns(cluster.get(HCD))).isEqualTo(columnsBefore);
            assertThat(hcdSchemaVersion(cluster.get(HCD))).isEqualTo(hcdSchemaBefore);
            assertThat(compatStatus(cluster, node)).contains(DefaultSchemaUpdateHandler.SCHEMA_PULL_WITHOUT_SCHEMA);

            // a schema change pushed by the HCD 1.2 node at the DSE dialect is ignored, and counted
            hcdPushesKeyspaceTo(cluster.get(node).broadcastAddress());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (!compatStatus(cluster, node).contains(DefaultSchemaUpdateHandler.SCHEMA_PUSH_IGNORED) && System.nanoTime() < deadline)
                TimeUnit.MILLISECONDS.sleep(200);
            assertThat(compatStatus(cluster, node)).contains(DefaultSchemaUpdateHandler.SCHEMA_PUSH_IGNORED);
            assertThat(cluster.get(node).executeInternal("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?", PUSHED_KEYSPACE))
            .as("the keyspace pushed by the HCD 1.2 node").isEmpty();

            // the first start with NONE makes the additions deferred in DSE_6 mode. The mode is dropped once no old node
            // is left: the HCD 1.2 node stops first (a node without the mode cannot speak its dialect), and the upgrade
            // marker and the record of DSE peers, which would refuse that start, are passed by the property
            cluster.get(HCD).shutdown().get();
            cluster.get(node).shutdown().get();
            cluster.get(node).config().set("internode_compatibility_mode", InternodeCompatibilityMode.NONE.name());
            try (WithProperties properties = new WithProperties().set(CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS, true))
            {
                cluster.get(node).startup();
            }
            assertThat(tables(cluster.get(node))).containsAll(tablesBefore).contains(DEFERRED_TABLE);
            // the one addition HCD 1.2 lacks is that table (the lists logged in DSE_6 mode): no column is deferred
            assertThat(columns(cluster.get(node))).containsAll(columnsBefore);
        })
        .run();
    }

    /**
     * Runs inside the HCD 1.2 node, by reflection: sends the upgraded node, at the DSE dialect, the schema push of a
     * new keyspace, as a DSE node pushes a DDL to a peer it considers schema-compatible.
     */
    private static void hcdPushesKeyspaceTo(InetSocketAddress upgraded) throws Exception
    {
        ClassLoader classLoader = instanceClassLoader(HCD);
        Class<?> addressClass = Class.forName("org.apache.cassandra.locator.InetAddressAndPort", true, classLoader);
        Object address = addressClass.getMethod("getByAddressOverrideDefaults", InetAddress.class, Integer.class)
                                     .invoke(null, upgraded.getAddress(), upgraded.getPort());
        Class<?> paramsClass = Class.forName("org.apache.cassandra.schema.KeyspaceParams", true, classLoader);
        Class<?> keyspaceClass = Class.forName("org.apache.cassandra.schema.KeyspaceMetadata", true, classLoader);
        Object keyspace = keyspaceClass.getMethod("create", String.class, paramsClass)
                                       .invoke(null, PUSHED_KEYSPACE, paramsClass.getMethod("simple", int.class).invoke(null, 1));
        Method makeCreate = Class.forName("org.apache.cassandra.schema.SchemaKeyspace", true, classLoader)
                                 .getDeclaredMethod("makeCreateKeyspaceMutation", keyspaceClass, long.class);
        makeCreate.setAccessible(true);
        long timestamp = (long) Class.forName("org.apache.cassandra.utils.FBUtilities", true, classLoader).getMethod("timestampMicros").invoke(null);
        Object builder = makeCreate.invoke(null, keyspace, timestamp);
        Object mutation = Class.forName("org.apache.cassandra.db.Mutation$SimpleBuilder", true, classLoader).getMethod("build").invoke(builder);

        Class<?> verbClass = Class.forName("org.apache.cassandra.net.Verb", true, classLoader);
        Class<?> messageClass = Class.forName("org.apache.cassandra.net.Message", true, classLoader);
        Object push = messageClass.getMethod("out", verbClass, Object.class)
                                  .invoke(null, verbClass.getField("SCHEMA_PUSH_REQ").get(null), Collections.singletonList(mutation));
        Object messaging = Class.forName("org.apache.cassandra.net.MessagingService", true, classLoader).getMethod("instance").invoke(null);
        messaging.getClass().getMethod("send", messageClass, addressClass).invoke(messaging, push, address);
    }

    /**
     * Runs inside the HCD 1.2 node, by reflection (test classes are not shared with the instances of this harness): sends a
     * schema pull to the upgraded node, waits for the answer and merges it as the HCD 1.2 migration coordinator merges the
     * answer to its own pulls. Returns the number of schema mutations the answer carried.
     */
    private static int hcdPullsAndMergesSchemaOf(InetSocketAddress upgraded) throws Exception
    {
        ClassLoader classLoader = instanceClassLoader(HCD);
        Class<?> addressClass = Class.forName("org.apache.cassandra.locator.InetAddressAndPort", true, classLoader);
        Object address = addressClass.getMethod("getByAddressOverrideDefaults", InetAddress.class, Integer.class)
                                     .invoke(null, upgraded.getAddress(), upgraded.getPort());
        Class<?> verbClass = Class.forName("org.apache.cassandra.net.Verb", true, classLoader);
        Class<?> messageClass = Class.forName("org.apache.cassandra.net.Message", true, classLoader);
        Object request = messageClass.getMethod("out", verbClass, Object.class)
                                     .invoke(null, verbClass.getField("SCHEMA_PULL_REQ").get(null),
                                             Class.forName("org.apache.cassandra.net.NoPayload", true, classLoader).getField("noPayload").get(null));

        CompletableFuture<Collection<?>> answer = new CompletableFuture<>();
        Class<?> callbackClass = Class.forName("org.apache.cassandra.net.RequestCallback", true, classLoader);
        InvocationHandler handler = (proxy, method, args) -> {
            switch (method.getName())
            {
                case "onResponse":
                    answer.complete((Collection<?>) messageClass.getField("payload").get(args[0]));
                    return null;
                case "onFailure":
                    answer.completeExceptionally(new AssertionError("the schema pull failed: " + args[1]));
                    return null;
                case "invokeOnFailure":
                    return true;
                case "toString":
                    return "schema pull callback";
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "equals":
                    return proxy == args[0];
                // the other default methods of HCD 1.2's RequestCallback, answered as their defaults do
                case "trackLatencyForSnitch":
                    return false;
                case "getRequestSensors":
                    return null;
                default:
                    throw new AssertionError("unexpected call to the schema pull callback: " + method);
            }
        };
        Object callback = Proxy.newProxyInstance(classLoader, new Class<?>[]{ callbackClass }, handler);
        Object messaging = Class.forName("org.apache.cassandra.net.MessagingService", true, classLoader).getMethod("instance").invoke(null);
        messaging.getClass().getMethod("sendWithCallback", messageClass, addressClass, callbackClass).invoke(messaging, request, address, callback);
        Collection<?> mutations = answer.get(30, TimeUnit.SECONDS);

        Class<?> schemaClass = Class.forName("org.apache.cassandra.schema.Schema", true, classLoader);
        Object updateHandler = schemaClass.getField("updateHandler").get(schemaClass.getField("instance").get(null));
        Method merge = updateHandler.getClass().getDeclaredMethod("applyMutationsFromCoordinator", addressClass, Collection.class);
        merge.setAccessible(true);
        merge.invoke(updateHandler, address, mutations);
        return mutations.size();
    }

    private static Set<String> tables(IInstance instance)
    {
        Set<String> tables = new TreeSet<>();
        for (Object[] row : instance.executeInternal("SELECT keyspace_name, table_name FROM system_schema.tables WHERE keyspace_name IN (" + REPLICATED_SYSTEM_KEYSPACES + ')'))
            tables.add(row[0] + "." + row[1]);
        return tables;
    }

    private static Set<String> columns(IInstance instance)
    {
        Set<String> columns = new TreeSet<>();
        for (Object[] row : instance.executeInternal("SELECT keyspace_name, table_name, column_name FROM system_schema.columns WHERE keyspace_name IN (" + REPLICATED_SYSTEM_KEYSPACES + ')'))
            columns.add(row[0] + "." + row[1] + '.' + row[2]);
        return columns;
    }

    /** The schema version of an HCD 1.2 node, from its {@code system.local} table. */
    private static UUID hcdSchemaVersion(IInstance instance)
    {
        return (UUID) instance.executeInternal("SELECT schema_version FROM system.local")[0][0];
    }
}
