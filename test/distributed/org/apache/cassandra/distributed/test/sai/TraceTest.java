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

package org.apache.cassandra.distributed.test.sai;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.TracingUtil;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.awaitility.Awaitility.await;

public class TraceTest extends TestBaseImpl
{
    private final static int ROWS = 100;
    private final static int MATCHED_ROWS = 30;

    private final static Pattern NUMBER_PATTERN = Pattern.compile("\\d+");

    @Test
    public void testMultiIndexTracing() throws Throwable
    {
        String originalTraceTimeout = TracingUtil.setWaitForTracingEventTimeoutSecs("1");

        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(config -> config.with(GOSSIP, NETWORK))
                                           .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                           .start()))
        {
            cluster.schemaChange("CREATE KEYSPACE trace_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
            cluster.schemaChange("CREATE TABLE trace_ks.tbl (pk int primary key, v1 int)");
            cluster.schemaChange("CREATE CUSTOM INDEX tbl_v1_idx ON trace_ks.tbl(v1) USING 'StorageAttachedIndex'");

            for (int row = 0; row < ROWS; row++)
            {
                cluster.coordinator(1).execute(String.format("INSERT INTO trace_ks.tbl (pk, v1) VALUES (%s, %s)", row, row), ConsistencyLevel.ONE);
            }

            cluster.forEach(c -> c.flush(KEYSPACE));

            SAIUtil.waitForIndexQueryableOnFirstNode(cluster, "trace_ks");

            UUID sessionId = nextTimeUUID().asUUID();
            cluster.coordinator(1).executeWithTracingWithResult(sessionId, "SELECT * from trace_ks.tbl WHERE v1 < " + MATCHED_ROWS, ConsistencyLevel.ONE);

            // TODO We can improve the asserts for this when we have improved tracing and multi-node support
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                List<TracingUtil.TraceEntry> traceEntries = TracingUtil.getTrace(cluster, sessionId, ConsistencyLevel.ONE);
                return traceEntries.stream().map(traceEntry -> traceEntry.activity)
                                   .filter(activity -> activity.contains("post-filtered"))
                                   .mapToLong(this::fetchPartitionCount).sum() == MATCHED_ROWS;
            });
        }
        finally
        {
            TracingUtil.setWaitForTracingEventTimeoutSecs(originalTraceTimeout);
        }
    }

    private long fetchPartitionCount(String activity)
    {
        List<Long> values = new ArrayList<>();
        Matcher matcher = NUMBER_PATTERN.matcher(activity);
        while (matcher.find())
            values.add(Long.parseLong(matcher.group()));
        return values.get(3);
    }
}
