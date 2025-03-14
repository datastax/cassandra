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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.DistributedTestBase;
import org.apache.cassandra.distributed.util.ColumnTypeUtil;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SCHEMA_DELAY_MS;
import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.awaitility.Awaitility.await;

public class TestBaseImpl extends DistributedTestBase
{
    public static final Object[][] EMPTY_ROWS = new Object[0][];
    public static final boolean[] BOOLEANS = new boolean[]{ false, true };

    @After
    public void afterEach() {
        super.afterEach();
    }

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        Files.createDirectories(FileUtils.getTempDir().toPath());
        ICluster.setup();
    }

    @Override
    public Cluster.Builder builder() {
        // This is definitely not the smartest solution, but given the complexity of the alternatives and low risk, we can just rely on the
        // fact that this code is going to work accross _all_ versions.
        return Cluster.build();
    }

    public static Object[][] rows(Object[]...rows)
    {
        Object[][] r = new Object[rows.length][];
        System.arraycopy(rows, 0, r, 0, rows.length);
        return r;
    }

    public static Object list(Object...values)
    {
        return Arrays.asList(values);
    }

    public static Object set(Object...values)
    {
        return ImmutableSet.copyOf(values);
    }

    public static Object map(Object...values)
    {
        if (values.length % 2 != 0)
            throw new IllegalArgumentException("Invalid number of arguments, got " + values.length);

        int size = values.length / 2;
        Map<Object, Object> m = new LinkedHashMap<>(size);
        for (int i = 0; i < size; i++)
            m.put(values[2 * i], values[(2 * i) + 1]);
        return m;
    }

    public static ByteBuffer tuple(Object... values)
    {
        ByteBuffer[] bbs = new ByteBuffer[values.length];
        for (int i = 0; i < values.length; i++)
            bbs[i] = ColumnTypeUtil.makeByteBuffer(values[i]);
        return TupleType.buildValue(bbs);
    }

    protected void bootstrapAndJoinNode(Cluster cluster)
    {
        cluster.stream().forEach(node -> {
            assert node.config().has(Feature.NETWORK) : "Network feature must be enabled on the cluster";
            assert node.config().has(Feature.GOSSIP) : "Gossip feature must be enabled on the cluster";
        });

        IInstanceConfig config = cluster.newInstanceConfig();
        config.set("auto_bootstrap", true);
        IInvokableInstance newInstance = cluster.bootstrap(config);
        withProperty(BOOTSTRAP_SCHEMA_DELAY_MS.getKey(), Integer.toString(90 * 1000),
                     () -> withProperty("cassandra.join_ring", false, () -> newInstance.startup(cluster)));
        newInstance.nodetoolResult("join").asserts().success();

        // Wait until all the other live nodes on the cluster see this node as NORMAL.
        // The old nodes will update their tokens only after the new node announces its NORMAL state through gossip.
        // This is to avoid disagreements about ring ownership between nodes and sudden ownership changes
        // while running the tests.
        InetAddressAndPort address = nodeAddress(newInstance.broadcastAddress());
        await()
            .atMost(90, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                assert cluster.stream().allMatch(node -> node.isShutdown() || node.callOnInstance(() -> {
                    EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(address);
                    return state != null && state.isNormalState();
                })) : "New node should be seen in NORMAL state by the other nodes in the cluster";
        });
    }

    private static InetAddressAndPort nodeAddress(InetSocketAddress address)
    {
        return InetAddressAndPort.getByAddressOverrideDefaults(address.getAddress(), address.getPort());
    }

    public static void fixDistributedSchemas(Cluster cluster)
    {
        // These keyspaces are under replicated by default, so must be updated when doing a mulit-node cluster;
        // else bootstrap will fail with 'Unable to find sufficient sources for streaming range <range> in keyspace <name>'
        for (String ks : Arrays.asList("system_auth", "system_traces"))
        {
            cluster.schemaChange("ALTER KEYSPACE " + ks + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': " + Math.min(cluster.size(), 3) + "}");
        }

        // in real live repair is needed in this case, but in the test case it doesn't matter if the tables loose
        // anything, so ignoring repair to speed up the tests.
    }

    /* Provide the cluster cannot start with the configured options */
    void assertCannotStartDueToConfigurationException(Cluster cluster)
    {
        Throwable tr = null;
        try
        {
            cluster.startup();
        }
        catch (Throwable maybeConfigException)
        {
            tr = maybeConfigException;
        }

        if (tr == null)
        {
            Assert.fail("Expected a ConfigurationException");
        }
        else
        {
            Assert.assertEquals(ConfigurationException.class.getName(), tr.getClass().getName());
        }
    }

    /**
     * Runs the given function before and after a flush of sstables.  This is useful for checking that behavior is
     * the same whether data is in memtables or sstables.
     *
     * @param cluster the tested cluster
     * @param keyspace the keyspace to flush
     * @param runnable the test to run
     */
    public static void beforeAndAfterFlush(Cluster cluster, String keyspace, CQLTester.CheckedFunction runnable) throws Throwable
    {
        try
        {
            runnable.apply();
        }
        catch (Throwable t)
        {
            throw new AssertionError("Test failed before flush:\n" + t, t);
        }

        for (int i = 1; i <= cluster.size(); i++)
        {
            cluster.get(i).flush(keyspace);

            try
            {
                runnable.apply();
            }
            catch (Throwable t)
            {
                throw new AssertionError("Test failed after flushing node " + i + ":\n" + t, t);
            }
        }
    }
}
