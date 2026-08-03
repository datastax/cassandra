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

package org.apache.cassandra.tracing;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.metrics.ClientRequestsMetrics;
import org.apache.cassandra.metrics.ClientRequestsMetricsProvider;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.WrappedRunnable;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Trace storage that pushes trace information to cassandra tables within a keyspace
 */
public class KeyspaceTraceStorage implements TraceStorage
{
    private final TableMetadata eventsTable;
    private final TableMetadata sessionsTable;

    public KeyspaceTraceStorage(TableMetadata eventsTable, TableMetadata sessionsTable)
    {
        this.eventsTable = eventsTable;
        this.sessionsTable = sessionsTable;
    }

    @Override
    public Future<Void> startSession(TraceState state,
                                     InetAddress client,
                                     String request,
                                     Map<String, String> parameters,
                                     long startedAt)
    {

        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(sessionsTable, state.sessionId);
        Row.SimpleBuilder rb = builder.row();
        rb.ttl(state.ttl)
          .add("client", client)
          .add("coordinator", FBUtilities.getBroadcastAddressAndPort().address);
        if (!Gossiper.instance.hasMajorVersion3Nodes())
            rb.add("coordinator_port", FBUtilities.getBroadcastAddressAndPort().port);
        rb.add("request", request)
          .add("started_at", new Date(startedAt))
          .add("command", state.traceType.toString())
          .appendAll("parameters", parameters);
        addExtraClientData(state.clientState, rb);

        Mutation mutation = builder.buildAsMutation();

        return submit(state.clientState, mutation);
    }

    /**
     * By default, this does nothing, but provides a hook if additional trace data needs to be appended that has
     * to do with the client.
     *
     * @param traceRow the row being traced
     */
    protected void addExtraClientData(@Nonnull ClientState clientState, @Nonnull Row.SimpleBuilder traceRow)
    {

    }

    @Override
    public Future<Void> stopSession(TraceState state)
    {

        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(sessionsTable, state.sessionId);
        Row.SimpleBuilder rb = builder.row()
                                      .ttl(state.ttl)
                                      .add("duration", state.elapsed());
        addExtraClientData(state.clientState, rb);
        Mutation mutation = builder.buildAsMutation();
        return submit(state.clientState, mutation);
    }

    @Override
    public Future<Void> recordEvent(TraceState state, String threadName, String message)
    {
        return submit(state.clientState, getEventMutation(state.clientState, state.sessionIdBytes, message, state.elapsed(), threadName, state.ttl));
    }

    @Override
    public Future<Void> recordNonLocalEvent(ClientState clientState,
                                            ByteBuffer sessionId,
                                            String message,
                                            int elapsedTime,
                                            String threadName,
                                            int ttl)
    {
        return submit(clientState, getEventMutation(clientState, sessionId, message, elapsedTime, threadName, ttl));
    }

    private Mutation getEventMutation(ClientState clientState,
                                      ByteBuffer sessionId,
                                          String message,
                                          int elapsedTime,
                                          String threadName,
                                          int ttl)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(eventsTable, sessionId);
        Row.SimpleBuilder rowBuilder = builder.row(UUIDGen.getTimeUUID())
                                              .ttl(ttl);

        rowBuilder.add("activity", message)
                  .add("source", FBUtilities.getBroadcastAddressAndPort().address);
        if (!Gossiper.instance.hasMajorVersion3Nodes())
            rowBuilder.add("source_port", FBUtilities.getBroadcastAddressAndPort().port);
        rowBuilder.add("thread", threadName);

        if (elapsedTime >= 0)
            rowBuilder.add("source_elapsed", elapsedTime);

        addExtraClientData(clientState, rowBuilder);
        return builder.buildAsMutation();
    }

    protected static @NonNull CompletableFuture<Void> submit(ClientState state, Mutation mutation)
    {
//        return Stage.TRACING.submit(() -> mutateWithCatch(state, mutation));
        return Stage.TRACING.submit(new WrappedRunnable()
        {
            @Override
            protected void runMayThrow() throws Exception
            {
                mutateWithCatch(state, mutation);
            }
        });
    }

    private static void mutateWithCatch(ClientState state, Mutation mutation)
    {
        try
        {
            Tracing.logger.info("Applying trace mutation {}", mutation);
            ClientRequestsMetrics metrics = ClientRequestsMetricsProvider.instance.metrics(mutation.getKeyspaceName());
            StorageProxy.mutate(Collections.singletonList(mutation), ConsistencyLevel.ANY, System.nanoTime(), metrics, state);
            Tracing.logger.info("Trace mutation complete (mutation={})", mutation);
        }
        catch (OverloadedException e)
        {
            Tracing.logger.warn("Too many nodes are overloaded to save trace events");
        }
        catch(WriteFailureException wfe)
        {
            Tracing.logger.error("Write failure: ", wfe);
        }
    }
}
