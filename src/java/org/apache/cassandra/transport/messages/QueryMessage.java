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
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryEvents;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.reads.thresholds.CoordinatorWarnings;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

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
    protected boolean isTrackable()
    {
        return true;
    }

    @Override
    protected Future<Response> maybeExecuteAsync(QueryState state, Dispatcher.RequestTime requestTime, boolean traceRequest)
    {
        CQLStatement statement = null;
        try
        {
            if (options.getPageSize().getSize() == 0)
                throw new ProtocolException("The page size cannot be 0");

            if (traceRequest)
                traceQuery(state);

            long requestStartMillisTime = Clock.Global.currentTimeMillis();
            Tracing.trace("Executing query started");

            QueryHandler queryHandler = ClientState.getCQLQueryHandler();
            statement = queryHandler.parse(query, state, options);

            Optional<Stage> asyncStage = Stage.fromStatement(statement);
            if (asyncStage.isPresent())
            {
                // Execution will continue on a new executor, but Dispatcher already called CoordinatorWarnings.init
                // on the current thread; the Dispatcher.processRequest request.execute() callback must call
                // CoordinatorWarnings.done() on the same thread that called init(). Reset CoordinatorWarnings on the
                // current thread, and init on the new thread. See CNDB-13432 and CNDB-10759.
                CoordinatorWarnings.reset();
                // Capture ExecutorLocals (including ClientWarn.State) to propagate to the async stage thread
                // so that warnings generated during query execution are properly captured.
                ExecutorLocals executorLocals = ExecutorLocals.current();
                CQLStatement finalStatement = statement;
                return asyncStage.get().submit(() ->
                                               {
                                                   // Restore ExecutorLocals on the async stage thread
                                                   try (Closeable ignored = executorLocals.get())
                                                   {
                                                       try
                                                       {
                                                           if (isTrackable())
                                                               CoordinatorWarnings.init();

                                                           // at the time of the check, this includes the time spent in the NTR queue, basic query parsing/set up,
                                                           // and any time spent in the queue for the async stage
                                                           long elapsedTime = elapsedTimeSinceCreation(TimeUnit.NANOSECONDS);
                                                           ClientMetrics.instance.recordAsyncQueueTime(elapsedTime, TimeUnit.NANOSECONDS);
                                                           if (elapsedTime > DatabaseDescriptor.getNativeTransportTimeout(TimeUnit.NANOSECONDS))
                                                           {
                                                               ClientMetrics.instance.markTimedOutBeforeAsyncProcessing();
                                                               throw new OverloadedException("Query timed out before it could start");
                                                           }
                                                       }
                                                       catch (Exception e)
                                                       {
                                                           return handleException(state, finalStatement, e);
                                                       }
                                                       return handleRequest(state, queryHandler, requestTime, finalStatement, requestStartMillisTime);
                                                   }
                                               });
            }
            else
                return ImmediateFuture.success(handleRequest(state, queryHandler, requestTime, statement, requestStartMillisTime));
        }
        catch (Exception exception)
        {
            return ImmediateFuture.success(handleException(state, statement, exception));
        }
    }

    private Response handleRequest(QueryState queryState, QueryHandler queryHandler, Dispatcher.RequestTime requestTime, CQLStatement statement, long requestStartMillisTime)
    {
        try
        {
            Response response = queryHandler.process(statement, queryState, options, getCustomPayload(), requestTime);
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
        return String.format("QUERY %s [pageSize = %s] at consistency %s",
                             query, options.getPageSize(), options.getConsistency());
    }
}
