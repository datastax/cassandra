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
package org.apache.cassandra.sensors;

import java.nio.ByteBuffer;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.schema.TableMetadata;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies sensor behaviour for batches targeting virtual tables.
 *
 * <p>Only UNLOGGED batches are valid for virtual tables — LOGGED batches, conditional batches,
 * and mixed batches (virtual + regular tables) are all rejected at validation time before
 * execution begins.
 *
 * <p>Virtual table writes bypass the storage engine and replica round-trips entirely, so no
 * {@link RequestSensors} are installed on the thread-local and the response carries no sensor
 * custom-payload headers.
 *
 * <p>Tests use the native-protocol stack (not the internal query path) so that the full
 * batch execution path is exercised, including the virtual-table branch that applies mutations
 * directly without going through {@link org.apache.cassandra.service.StorageProxy}.
 */
public class SensorsVirtualTableBatchTest extends CQLTester
{
    // All lowercase so CQL unquoted identifiers (lowercased by QualifiedName) match the registry key.
    private static final String KS_NAME = "sensors_virtual_table_batch_test_ks";
    private static final String VT_NAME = "vt";

    /** Minimal writable virtual table that accepts writes and discards them. */
    private static class WritableVirtualTable extends AbstractVirtualTable
    {
        WritableVirtualTable(String keyspaceName, String tableName)
        {
            super(TableMetadata.builder(keyspaceName, tableName)
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .addPartitionKeyColumn("key", UTF8Type.instance)
                               .addRegularColumn("value", Int32Type.instance)
                               .build());
        }

        @Override
        public DataSet data()
        {
            return new SimpleDataSet(metadata());
        }

        @Override
        public void apply(PartitionUpdate update)
        {
            // Accept writes but discard — we only care that the path is exercised.
        }
    }

    @BeforeClass
    public static void setUpClass()
    {
        // Enable active sensors so any accidental registration would be detectable.
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());

        CQLTester.setUpClass();

        // requireNetwork() must be called before registering the virtual keyspace:
        // CQLTester.startServices() (called inside requireNetwork) also registers built-in virtual
        // keyspaces, so our keyspace must be added after that call or it may be lost.
        requireNetwork();

        VirtualKeyspaceRegistry.instance.register(
                new VirtualKeyspace(KS_NAME, ImmutableList.of(new WritableVirtualTable(KS_NAME, VT_NAME))));
    }

    @After
    public void afterTest()
    {
        // Clear thread-local sensor state between tests.
        RequestTracker.instance.set(null);
    }

    /**
     * An UNLOGGED batch on a virtual table applies mutations directly without going through the
     * storage engine or any replica round-trip. Because no storage activity occurs, no sensor
     * values are produced and the response must carry no sensor custom-payload headers.
     */
    @Test
    public void testUnloggedBatchOnVirtualTableProducesNoSensors() throws Throwable
    {
        com.datastax.driver.core.ResultSet rs =
                executeNet("BEGIN UNLOGGED BATCH " +
                           "UPDATE " + KS_NAME + '.' + VT_NAME + " SET value = 1 WHERE key = 'pk1';" +
                           "UPDATE " + KS_NAME + '.' + VT_NAME + " SET value = 2 WHERE key = 'pk2';" +
                           "APPLY BATCH");

        // Virtual table writes produce no storage activity, so no sensor values are accumulated.
        // The thread-local sensor reference is null (never initialised for this path).
        assertThat(RequestTracker.instance.get())
                .as("No RequestSensors should be initialised for a virtual table batch — no storage occurs")
                .isNull();

        // The response custom payload must also be empty: no storage means no sensor headers to report.
        Map<String, ByteBuffer> payload = rs.getExecutionInfo().getIncomingPayload();
        assertThat(payload)
                .as("Response custom payload must contain no sensor headers for a virtual table batch")
                .isNullOrEmpty();
    }
}
