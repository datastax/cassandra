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

package org.apache.cassandra.distributed.test.index;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/**
 * Base class for distributed index tests providing common functionality for index status management
 * and verification. Can be used by any secondary index implementation (SAI, SASI, etc.).
 */
public class IndexTestBase extends TestBaseImpl
{
    protected static void markIndexBuilding(IInvokableInstance node,
                                            String keyspace,
                                            String table,
                                            String indexName,
                                            boolean isInitialBuild,
                                            boolean isNewCF)
    {
        node.runOnInstance(() -> {
            SecondaryIndexManager sim = Objects.requireNonNull(Schema.instance.getKeyspaceInstance(keyspace))
                                               .getColumnFamilyStore(table).indexManager;
            Index index = sim.getIndexByName(indexName);
            sim.markIndexesBuilding(Collections.singleton(index), true, isNewCF, isInitialBuild);
        });
    }

    public static void waitForIndexingStatus(IInvokableInstance node, String keyspace, String index, IInvokableInstance replica, Index.Status status)
    {
        InetAddressAndPort replicaAddressAndPort = getFullAddress(replica);
        await().atMost(5, TimeUnit.SECONDS)
               .until(() -> node.callOnInstance(() -> getIndexStatus(keyspace, index, replicaAddressAndPort) == status));
    }

    protected static InetAddressAndPort getFullAddress(IInvokableInstance node)
    {
        InetAddress address = node.broadcastAddress().getAddress();
        int port = node.callOnInstance(() -> FBUtilities.getBroadcastAddressAndPort().port);
        return InetAddressAndPort.getByAddressOverrideDefaults(address, port);
    }

    protected static Index.Status getIndexStatus(String keyspaceName, String indexName, InetAddressAndPort replica)
    {
        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
        if (keyspace == null)
            return Index.Status.UNKNOWN;

        TableMetadata table = keyspace.findIndexedTable(indexName).orElse(null);
        if (table == null)
            return Index.Status.UNKNOWN;

        SecondaryIndexManager indexManager = Keyspace.openAndGetStore(table).indexManager;

        return indexManager.getIndexStatus(replica, keyspaceName, indexName);
    }
}
