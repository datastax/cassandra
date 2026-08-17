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

package org.apache.cassandra.distributed.test.sensors;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.sensors.ActiveSensorsFactory;
import org.apache.cassandra.sensors.SensorsRegistry;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.assertj.core.api.Assertions;

/**
 * Verifies that after a distributed request completes, sensor values accumulated by
 * {@link org.apache.cassandra.net.ResponseVerbHandler} from replica responses are visible in the
 * coordinator's {@link org.apache.cassandra.sensors.SensorsRegistry}.
 *
 * <p>Uses {@value NODES_COUNT} nodes with {@link ConsistencyLevel#ALL}. Node 1 is always the
 * coordinator; nodes 2 through {@value NODES_COUNT} are remote replicas. Node 2 is used as the
 * per-replica reference in assertions — all remote replicas process the same data so their
 * sensor values are identical.
 *
 * <p>Each test uses a dedicated table so registry values do not bleed across tests (the cluster
 * and its registries are shared for the lifetime of the test class).
 *
 * <p>Queries are executed via {@code acceptsOnInstance} using an {@link AtomicReference} to
 * ferry registry values back across the in-process classloader boundary.
 */
public class CoordinatorSensorRegistrySyncTest extends TestBaseImpl
{
    private static final int NODES_COUNT = 3;
    private static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ALL;

    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());
        cluster = init(Cluster.build(NODES_COUNT).start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    /**
     * Verifies write sensor propagation via {@code StorageProxy.mutate()}.
     *
     * <p>The local replica on node 1 calls {@code handler.onResponse(null)}, bypassing
     * {@link org.apache.cassandra.net.ResponseVerbHandler}, so only the {@code (NODES_COUNT - 1)}
     * remote replicas contribute UCU to the coordinator's registry via internode response headers.
     * {@code WRITE_BYTES} from all remote replicas is identical (same row), so the coordinator
     * total must divide evenly by the per-replica value.
     */
    @Test
    public void testWriteSensorsSyncedToRegistry() throws Exception
    {
        String tbl = "tbl_write_sync";
        cluster.schemaChange(withKeyspace("CREATE TABLE IF NOT EXISTS %s." + tbl + " (pk int PRIMARY KEY, v1 text)"));

        AtomicReference<double[]> coordinatorRef = new AtomicReference<>();
        cluster.get(1).acceptsOnInstance(
            (IIsolatedExecutor.SerializableConsumer<AtomicReference<double[]>>) r -> {
                executeWithResult(withKeyspace("INSERT INTO %s." + tbl + " (pk, v1) VALUES (1, 'hello')"));
                r.set(new double[]{ registrySumOnNode(tbl, Type.WRITE_BYTES),
                                    registrySumOnNode(tbl, Type.UCU) });
            }).accept(coordinatorRef);

        AtomicReference<double[]> replicaRef = new AtomicReference<>();
        cluster.get(2).acceptsOnInstance(
            (IIsolatedExecutor.SerializableConsumer<AtomicReference<double[]>>) r -> {
                r.set(new double[]{ registrySumOnNode(tbl, Type.WRITE_BYTES),
                                    registrySumOnNode(tbl, Type.UCU) });
            }).accept(replicaRef);

        double[] coordinator = coordinatorRef.get();
        double[] replica     = replicaRef.get();
        double perReplicaWb  = replica[0];
        double perReplicaUcu = replica[1];
        int remoteReplicas   = NODES_COUNT - 1;

        Assertions.assertThat(perReplicaWb)
                  .describedAs("per-replica WRITE_BYTES on node 2 must be positive")
                  .isGreaterThan(0D);
        Assertions.assertThat(perReplicaUcu)
                  .describedAs("per-replica UCU on node 2 must be positive")
                  .isGreaterThan(0D);
        Assertions.assertThat(coordinator[0])
                  .describedAs("coordinator WRITE_BYTES must be positive")
                  .isGreaterThan(0D);
        Assertions.assertThat(coordinator[0] % perReplicaWb)
                  .describedAs("coordinator WRITE_BYTES must divide evenly by per-replica value")
                  .isEqualTo(0D);
        Assertions.assertThat(coordinator[1])
                  .describedAs("coordinator UCU must equal (NODES_COUNT-1) × per-replica UCU")
                  .isEqualTo(remoteReplicas * perReplicaUcu);
    }

    /**
     * Verifies read sensor propagation via {@code StorageProxy.read()} — non-paging path.
     *
     * <p>The local read on node 1 calls {@code handler.response(response)} directly, bypassing
     * {@link org.apache.cassandra.net.ResponseVerbHandler}. Only the {@code (NODES_COUNT - 1)}
     * remote replicas contribute UCU to the coordinator's registry via internode response headers.
     * {@code READ_BYTES} from all remote replicas is identical (same row), so the coordinator
     * total must divide evenly by the per-replica value.
     */
    @Test
    public void testReadSensorsSyncedToRegistry_NoPaging() throws Exception
    {
        String tbl = "tbl_read_nopaging_sync";
        cluster.schemaChange(withKeyspace("CREATE TABLE IF NOT EXISTS %s." + tbl + " (pk int PRIMARY KEY, v1 text)"));
        cluster.get(1).runOnInstance(() ->
            executeWithResult(withKeyspace("INSERT INTO %s." + tbl + " (pk, v1) VALUES (1, 'hello')")));

        AtomicReference<double[]> coordinatorRef = new AtomicReference<>();
        cluster.get(1).acceptsOnInstance(
            (IIsolatedExecutor.SerializableConsumer<AtomicReference<double[]>>) r -> {
                executeWithResultNoPaging(withKeyspace("SELECT * FROM %s." + tbl + " WHERE pk = 1"));
                r.set(new double[]{ registrySumOnNode(tbl, Type.READ_BYTES),
                                    registrySumOnNode(tbl, Type.UCU) });
            }).accept(coordinatorRef);

        AtomicReference<double[]> replicaRef = new AtomicReference<>();
        cluster.get(2).acceptsOnInstance(
            (IIsolatedExecutor.SerializableConsumer<AtomicReference<double[]>>) r -> {
                r.set(new double[]{ registrySumOnNode(tbl, Type.READ_BYTES),
                                    registrySumOnNode(tbl, Type.UCU) });
            }).accept(replicaRef);

        double[] coordinator = coordinatorRef.get();
        double[] replica     = replicaRef.get();
        double perReplicaRb  = replica[0];
        double perReplicaUcu = replica[1];
        int remoteReplicas   = NODES_COUNT - 1;

        Assertions.assertThat(perReplicaRb)
                  .describedAs("per-replica READ_BYTES on node 2 must be positive")
                  .isGreaterThan(0D);
        Assertions.assertThat(perReplicaUcu)
                  .describedAs("per-replica UCU on node 2 must be positive")
                  .isGreaterThan(0D);
        Assertions.assertThat(coordinator[0])
                  .describedAs("coordinator READ_BYTES must be positive")
                  .isGreaterThan(0D);
        Assertions.assertThat(coordinator[0] % perReplicaRb)
                  .describedAs("coordinator READ_BYTES must divide evenly by per-replica value")
                  .isEqualTo(0D);
        Assertions.assertThat(coordinator[1])
                  .describedAs("coordinator UCU must equal (NODES_COUNT-1) × per-replica UCU")
                  .isEqualTo(remoteReplicas * perReplicaUcu);
    }

    /**
     * Verifies read sensor propagation via {@code StorageProxy.read()} — paging path (page size 512).
     * Same assertions as {@link #testReadSensorsSyncedToRegistry_NoPaging()}.
     */
    @Test
    public void testReadSensorsSyncedToRegistry_Paging() throws Exception
    {
        String tbl = "tbl_read_paging_sync";
        cluster.schemaChange(withKeyspace("CREATE TABLE IF NOT EXISTS %s." + tbl + " (pk int PRIMARY KEY, v1 text)"));
        cluster.get(1).runOnInstance(() ->
            executeWithResult(withKeyspace("INSERT INTO %s." + tbl + " (pk, v1) VALUES (1, 'hello')")));

        AtomicReference<double[]> coordinatorRef = new AtomicReference<>();
        cluster.get(1).acceptsOnInstance(
            (IIsolatedExecutor.SerializableConsumer<AtomicReference<double[]>>) r -> {
                executeWithResult(withKeyspace("SELECT * FROM %s." + tbl + " WHERE pk = 1"));
                r.set(new double[]{ registrySumOnNode(tbl, Type.READ_BYTES),
                                    registrySumOnNode(tbl, Type.UCU) });
            }).accept(coordinatorRef);

        AtomicReference<double[]> replicaRef = new AtomicReference<>();
        cluster.get(2).acceptsOnInstance(
            (IIsolatedExecutor.SerializableConsumer<AtomicReference<double[]>>) r -> {
                r.set(new double[]{ registrySumOnNode(tbl, Type.READ_BYTES),
                                    registrySumOnNode(tbl, Type.UCU) });
            }).accept(replicaRef);

        double[] coordinator = coordinatorRef.get();
        double[] replica     = replicaRef.get();
        double perReplicaRb  = replica[0];
        double perReplicaUcu = replica[1];
        int remoteReplicas   = NODES_COUNT - 1;

        Assertions.assertThat(perReplicaRb)
                  .describedAs("per-replica READ_BYTES on node 2 must be positive")
                  .isGreaterThan(0D);
        Assertions.assertThat(perReplicaUcu)
                  .describedAs("per-replica UCU on node 2 must be positive")
                  .isGreaterThan(0D);
        Assertions.assertThat(coordinator[0])
                  .describedAs("coordinator READ_BYTES must be positive")
                  .isGreaterThan(0D);
        Assertions.assertThat(coordinator[0] % perReplicaRb)
                  .describedAs("coordinator READ_BYTES must divide evenly by per-replica value")
                  .isEqualTo(0D);
        Assertions.assertThat(coordinator[1])
                  .describedAs("coordinator UCU must equal (NODES_COUNT-1) × per-replica UCU")
                  .isEqualTo(remoteReplicas * perReplicaUcu);
    }

    /**
     * Verifies CAS sensor propagation via {@code StorageProxy.cas()}.
     *
     * <p>For each Paxos phase, the local node (node 1) bypasses the verb handler:
     * {@code preparePaxos} calls {@code doPrepare()} directly, {@code proposePaxos} calls
     * {@code doPropose()} directly, and {@code commitPaxosLocal} calls {@code PaxosState.commit()}
     * then {@code responseHandler.onResponse(null)}. None of these paths run through
     * {@link org.apache.cassandra.net.ResponseVerbHandler}, so the local node contributes no
     * UCU to the coordinator.
     *
     * <p>Unlike simple writes and reads, UCU cannot be asserted as an exact multiple of node 2's
     * value: each Paxos phase encodes {@code INTERNODE_BYTES} into UCU, and the inbound message
     * payload size (e.g. the {@code PrepareResponse} carrying the most-recent commit) can differ
     * per replica depending on local paxos state. So remote replicas may produce different UCU
     * values per phase. The assertions verify positivity for all three sensor types — confirming
     * that sensors were synced for each.
     */
    @Test
    public void testCasSensorsSyncedToRegistry() throws Exception
    {
        String tbl = "tbl_cas_sync";
        cluster.schemaChange(withKeyspace("CREATE TABLE IF NOT EXISTS %s." + tbl + " (pk int PRIMARY KEY, v1 text)"));
        // seed a row so the IF condition matches and a real Commit is issued
        cluster.get(1).runOnInstance(() ->
            executeWithResult(withKeyspace("INSERT INTO %s." + tbl + " (pk, v1) VALUES (1, 'before')")));

        AtomicReference<double[]> coordinatorRef = new AtomicReference<>();
        cluster.get(1).acceptsOnInstance(
            (IIsolatedExecutor.SerializableConsumer<AtomicReference<double[]>>) r -> {
                executeWithResult(withKeyspace("UPDATE %s." + tbl + " SET v1 = 'after' WHERE pk = 1 IF v1 = 'before'"));
                r.set(new double[]{ registrySumOnNode(tbl, Type.WRITE_BYTES),
                                    registrySumOnNode(tbl, Type.READ_BYTES),
                                    registrySumOnNode(tbl, Type.UCU) });
            }).accept(coordinatorRef);

        AtomicReference<double[]> replicaRef = new AtomicReference<>();
        cluster.get(2).acceptsOnInstance(
            (IIsolatedExecutor.SerializableConsumer<AtomicReference<double[]>>) r -> {
                r.set(new double[]{ registrySumOnNode(tbl, Type.WRITE_BYTES),
                                    registrySumOnNode(tbl, Type.READ_BYTES),
                                    registrySumOnNode(tbl, Type.UCU) });
            }).accept(replicaRef);

        double[] coordinator = coordinatorRef.get();
        double[] replica     = replicaRef.get();
        double perReplicaWb  = replica[0];
        double perReplicaRb  = replica[1];
        double perReplicaUcu = replica[2];

        Assertions.assertThat(perReplicaWb)
                  .describedAs("per-replica WRITE_BYTES on node 2 must be positive")
                  .isGreaterThan(0D);
        Assertions.assertThat(perReplicaRb)
                  .describedAs("per-replica READ_BYTES on node 2 must be positive")
                  .isGreaterThan(0D);
        Assertions.assertThat(perReplicaUcu)
                  .describedAs("per-replica UCU on node 2 must be positive")
                  .isGreaterThan(0D);
        Assertions.assertThat(coordinator[0])
                  .describedAs("coordinator WRITE_BYTES must be positive")
                  .isGreaterThan(0D);
        Assertions.assertThat(coordinator[1])
                  .describedAs("coordinator READ_BYTES must be positive")
                  .isGreaterThan(0D);
        Assertions.assertThat(coordinator[2])
                  .describedAs("coordinator UCU must be positive")
                  .isGreaterThan(0D);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Sums registry values of the given {@code type} for {@code table} on the calling node.
     * Must be {@code static} to be serializable across the in-process classloader boundary.
     */
    private static double registrySumOnNode(String table, Type type)
    {
        return SensorsRegistry.instance
                   .getSensorsByType(type)
                   .stream()
                   .filter(s -> s.getContext().getTable().equals(table))
                   .mapToDouble(org.apache.cassandra.sensors.Sensor::getValue)
                   .sum();
    }

    /** Executes {@code query} with a page size of 512 rows. */
    private static ResultMessage<?> executeWithResult(String query)
    {
        return executeWithResult(query, PageSize.inRows(512));
    }

    /** Executes {@code query} without paging ({@link PageSize#NONE}). */
    private static ResultMessage<?> executeWithResultNoPaging(String query)
    {
        return executeWithResult(query, PageSize.NONE);
    }

    private static ResultMessage<?> executeWithResult(String query, PageSize pageSize)
    {
        long nanoTime = System.nanoTime();
        QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(query);
        org.apache.cassandra.db.ConsistencyLevel cl = org.apache.cassandra.db.ConsistencyLevel
                .fromCode(ConsistencyLevel.valueOf(CONSISTENCY_LEVEL.name()).ordinal());
        QueryOptions options = QueryOptions.create(cl, null, false, pageSize, null, null,
                                                   ProtocolVersion.CURRENT, prepared.keyspace);
        return prepared.statement.execute(QueryProcessor.internalQueryState(), options, nanoTime);
    }
}
