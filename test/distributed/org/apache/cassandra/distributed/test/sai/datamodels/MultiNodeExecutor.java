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

package org.apache.cassandra.distributed.test.sai.datamodels;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.distributed.util.ColumnTypeUtil;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.cql.datamodels.DataModel;
import org.apache.cassandra.index.sai.disk.format.Version;

public class MultiNodeExecutor implements DataModel.Executor
{
    private final Cluster cluster;

    public MultiNodeExecutor(Cluster cluster)
    {
        this.cluster = cluster;
    }

    @Override
    public void createTable(String statement)
    {
        cluster.schemaChange(statement);
    }

    @Override
    public void flush(String keyspace, String table)
    {
        cluster.forEach(node -> node.flush(keyspace));
    }

    @Override
    public void compact(String keyspace, String table)
    {
        cluster.forEach(node -> node.forceCompact(keyspace, table));
    }

    @Override
    public void setCurrentVersion(Version version)
    {
        // need to pass version as String, because Version is not serializable and cannot be easily made to be so
        String versionName = version.toString();
        cluster.forEach(node ->
                        node.runOnInstance(() -> org.apache.cassandra.index.sai.SAIUtil.setCurrentVersion(Version.parse(versionName))));
    }

    @Override
    public void disableCompaction(String keyspace, String table)
    {
        cluster.forEach((node) -> node.runOnInstance(() -> Keyspace.open(keyspace).getColumnFamilyStore(table).disableAutoCompaction()));
    }

    @Override
    public void waitForTableIndexesQueryable(String keyspace, String table)
    {
        SAIUtil.waitForIndexQueryableOnFirstNode(cluster, keyspace);
    }

    @Override
    public void executeLocal(String query, Object... values)
    {
        Object[] buffers = ColumnTypeUtil.transformValues(values);
        cluster.coordinator(1).execute(query, ConsistencyLevel.QUORUM, buffers);
    }

    @Override
    public List<Object> executeRemote(String query, int fetchSize, Object... values)
    {
        Object[] buffers = ColumnTypeUtil.transformValues(values);
        Iterator<Object> iterator = cluster.coordinator(1).executeWithPagingWithResult(query, ConsistencyLevel.QUORUM, fetchSize, buffers).map(row -> row.get(0));

        List<Object> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);

        return result;
    }

    @Override
    public void counterReset()
    {
        MultiNodeQueryTester.Counter.reset();
    }

    @Override
    public long getCounter()
    {
        return MultiNodeQueryTester.Counter.get();
    }

    @Override
    public Set<Version> getSSTableIndexVersions(String keyspace, String indexedTable)
    {
        // Need to pass version as String, because Version is not serializable
        var versions = new HashSet<String>();
        cluster.forEach(node ->
                        versions.addAll(
                            node.callsOnInstance(() ->
                                SAITester.getSSTableIndexVersions(keyspace, indexedTable)
                                         .stream()
                                         .map(Version::toString)
                                         .collect(Collectors.toSet())
                            ).call()
            )
        );
        return versions.stream().map(Version::parse).collect(Collectors.toSet());
    }
}
