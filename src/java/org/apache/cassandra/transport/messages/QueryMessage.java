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
package org.apache.cassandra.transport.messages;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.ImmutableMap;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryEvents;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * A CQL query
 */
public class QueryMessage extends Message.Request
{
    public static final Message.Codec<QueryMessage> codec = new Message.Codec<QueryMessage>()
    {
        public QueryMessage decode(ByteBuf body, ProtocolVersion version)
        {
            String query = CBUtil.readLongString(body);
            return new QueryMessage(query, QueryOptions.codec.decode(body, version));
        }

        public void encode(QueryMessage msg, ByteBuf dest, ProtocolVersion version)
        {
            CBUtil.writeLongString(msg.query, dest);
            if (version == ProtocolVersion.V1)
                CBUtil.writeConsistencyLevel(msg.options.getConsistency(), dest);
            else
                QueryOptions.codec.encode(msg.options, dest, version);
        }

        public int encodedSize(QueryMessage msg, ProtocolVersion version)
        {
            int size = CBUtil.sizeOfLongString(msg.query);

            if (version == ProtocolVersion.V1)
            {
                size += CBUtil.sizeOfConsistencyLevel(msg.options.getConsistency());
            }
            else
            {
                size += QueryOptions.codec.encodedSize(msg.options, version);
            }
            return size;
        }
    };

    public final String query;
    public final QueryOptions options;

    public QueryMessage(String query, QueryOptions options)
    {
        super(Type.QUERY);
        this.query = query;
        this.options = options;
    }

    @Override
    protected boolean isTraceable()
    {
        return true;
    }

    @Override
    protected CompletableFuture<Response> maybeExecuteAsync(QueryState state, long queryStartNanoTime, boolean traceRequest)
    {
        CQLStatement statement = null;
        try
        {
            if (options.getPageSize().getSize() == 0)
                throw new ProtocolException("The page size cannot be 0");

            if (traceRequest)
                traceQuery(state);

            long requestStartMillisTime = System.currentTimeMillis();
            Tracing.trace("Executing query started");

            QueryHandler queryHandler = ClientState.getCQLQueryHandler();
            statement = queryHandler.parse(query, state, options);

            Optional<Stage> asyncStage = Stage.fromStatement(statement);
            if (asyncStage.isPresent())
            {
                CQLStatement finalStatement = statement;
                return asyncStage.get().submit(() -> handleRequest(state, queryHandler, queryStartNanoTime, finalStatement, requestStartMillisTime));
            }
            else
                return CompletableFuture.completedFuture(handleRequest(state, queryHandler, queryStartNanoTime, statement, requestStartMillisTime));
        }
        catch (Exception exception)
        {
            return CompletableFuture.completedFuture(handleException(state, statement, exception));
        }
    }

    private Response handleRequest(QueryState queryState, QueryHandler queryHandler, long queryStartNanoTime, CQLStatement statement, long requestStartMillisTime)
    {
        try
        {
            Response response = queryHandler.process(statement, queryState, options, getCustomPayload(), queryStartNanoTime);
            QueryEvents.instance.notifyQuerySuccess(statement, query, options, queryState, requestStartMillisTime, response);

            if (options.skipMetadata() && response instanceof ResultMessage.Rows)
                ((ResultMessage.Rows) response).result.metadata.setSkipMetadata();

            return response;
        }
        catch (Exception ex)
        {
            return handleException(queryState, statement, ex);
        }
        finally
        {
            Tracing.trace("Executing query completed");
        }
    }

    private ErrorMessage handleException(QueryState queryState, CQLStatement statement, Exception exception)
    {
        QueryEvents.instance.notifyQueryFailure(statement, query, options, queryState, exception);
        JVMStabilityInspector.inspectThrowable(exception);
        if (!((exception instanceof RequestValidationException) || (exception instanceof RequestExecutionException)))
            logger.error("Unexpected error during query", exception);

        return ErrorMessage.fromException(exception);
    }

    private void traceQuery(QueryState state)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.put("query", query);
        if (options.getPageSize().isDefined())
        {
            builder.put("page_size", Integer.toString(options.getPageSize().getSize()));
            builder.put("page_size_unit", options.getPageSize().getUnit().name());
        }
        if (options.getConsistency() != null)
            builder.put("consistency_level", options.getConsistency().name());
        if (options.getSerialConsistency(state) != null)
            builder.put("serial_consistency_level", options.getSerialConsistency(state).name());

        Tracing.instance.begin("Execute CQL3 query", state.getClientAddress(), builder.build());
    }

    @Override
    public String toString()
    {
        return String.format("QUERY %s [pageSize = %s]", query, options.getPageSize());
    }
}
