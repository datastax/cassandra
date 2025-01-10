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
package org.apache.cassandra.transport;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.BatchMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionEvaluationLogger;
import org.awaitility.core.TimeoutEvent;

public class TransportTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        System.setProperty("cassandra.custom_query_handler_class", "org.apache.cassandra.transport.TransportTest$TestQueryHandler");
        CQLTester.setUpClass();
        CQLTester.requireNetwork();
    }

    @After
    public void dropCreatedTable()
    {
        try
        {
            QueryProcessor.executeOnceInternal("DROP TABLE " + KEYSPACE + ".atable");
        }
        catch (Throwable t)
        {
            // ignore
        }
    }

    @Test
    public void testAsyncTransport() throws Throwable
    {
        Assert.assertSame(TransportTest.TestQueryHandler.class, ClientState.getCQLQueryHandler().getClass());

        CassandraRelevantProperties.NATIVE_TRANSPORT_ASYNC_READ_WRITE_ENABLED.setBoolean(true);
        try
        {
            doTestTransport();
        }
        finally
        {
            CassandraRelevantProperties.NATIVE_TRANSPORT_ASYNC_READ_WRITE_ENABLED.setBoolean(false);
        }
    }

    private void doTestTransport() throws Throwable
    {
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort))
        {
            client.connect(false);

            // Async native transport causes native transport requests to be executed asynchronously on the coordinator read and coordinator write
            // stages: this means the expected number of tasks on those stages starts at 1 for the async request.
            // The read and write stages are used for the actual execution.
            // Please don't tell me we're not using var. Making final copies is worse. Hardcoding the values is also worse.
            var ref = new Object()
            {
                long expectedCoordinateReadTasks = 1;
                long expectedExecuteReadTasks = 1;
                int expectedCoordinateMutationTasks = 1;
                int expectedExecuteMutationTasks = 1;
            };

            QueryMessage createMessage = new QueryMessage("CREATE TABLE " + KEYSPACE + ".atable (pk int PRIMARY KEY, v text)", QueryOptions.DEFAULT);
            PrepareMessage prepareMessage = new PrepareMessage("SELECT * FROM " + KEYSPACE + ".atable", null);

            client.execute(createMessage);
            ResultMessage.Prepared prepareResponse = (ResultMessage.Prepared) client.execute(prepareMessage);

            ExecuteMessage executeMessage = new ExecuteMessage(prepareResponse.statementId, prepareResponse.resultMetadataId, QueryOptions.DEFAULT);
            Message.Response executeResponse = client.execute(executeMessage);
            Assert.assertEquals(1, executeResponse.getWarnings().size());
            Assert.assertEquals("async-prepared", executeResponse.getWarnings().get(0));
            awaitUntil(() -> Stage.COORDINATE_READ.getCompletedTaskCount() == ref.expectedCoordinateReadTasks);
            awaitUntil(() -> Stage.READ.getCompletedTaskCount() == ref.expectedExecuteReadTasks);

            // we now expect two more tasks
            ref.expectedCoordinateReadTasks++;
            ref.expectedExecuteReadTasks++;
            QueryMessage readMessage = new QueryMessage("SELECT * FROM " + KEYSPACE + ".atable", QueryOptions.DEFAULT);
            Message.Response readResponse = client.execute(readMessage);
            Assert.assertEquals(1, executeResponse.getWarnings().size());
            Assert.assertEquals("async-process", readResponse.getWarnings().get(0));
            awaitUntil(() -> Stage.COORDINATE_READ.getCompletedTaskCount() == ref.expectedCoordinateReadTasks);
            awaitUntil(() -> Stage.READ.getCompletedTaskCount() == ref.expectedExecuteReadTasks);

            BatchMessage batchMessage = new BatchMessage(BatchStatement.Type.UNLOGGED,
                                                         Collections.singletonList("INSERT INTO " + KEYSPACE + ".atable (pk,v) VALUES (1, 'foo')"),
                                                         Collections.singletonList(Collections.<ByteBuffer>emptyList()),
                                                         QueryOptions.DEFAULT);
            Message.Response batchResponse = client.execute(batchMessage);
            Assert.assertEquals(1, executeResponse.getWarnings().size());
            Assert.assertEquals("async-batch", batchResponse.getWarnings().get(0));
            awaitUntil(() -> Stage.COORDINATE_READ.getCompletedTaskCount() == ref.expectedCoordinateReadTasks);
            awaitUntil(() -> Stage.READ.getCompletedTaskCount() == ref.expectedExecuteReadTasks);

            // we now expect two more tasks
            ref.expectedCoordinateMutationTasks++;
            ref.expectedExecuteMutationTasks++;
            QueryMessage insertMessage = new QueryMessage("INSERT INTO " + KEYSPACE + ".atable (pk,v) VALUES (1, 'foo')", QueryOptions.DEFAULT);
            Message.Response insertResponse = client.execute(insertMessage);
            Assert.assertEquals(1, executeResponse.getWarnings().size());
            Assert.assertEquals("async-process", insertResponse.getWarnings().get(0));
            awaitUntil(() -> Stage.COORDINATE_READ.getCompletedTaskCount() == ref.expectedCoordinateReadTasks);
            awaitUntil(() -> Stage.READ.getCompletedTaskCount() == ref.expectedExecuteReadTasks);
        }
    }

    private void awaitUntil(Callable<Boolean> condition)
    {
        Awaitility.await("await until stages have completed the required number of tasks; on timeout check the ERROR logs for the actual numbers completed")
                  .conditionEvaluationListener(new DumpStageInfoOnTimeout())
                  .until(condition);
    }

    public static class TestQueryHandler implements QueryHandler
    {
        @Override
        public QueryProcessor.Prepared getPrepared(MD5Digest id)
        {
            return QueryProcessor.instance.getPrepared(id);
        }

        @Override
        public CQLStatement parse(String query, QueryState state, QueryOptions options)
        {
            return QueryProcessor.instance.parse(query, state, options);
        }

        @Override
        public ResultMessage.Prepared prepare(String query,
                                              ClientState clientState,
                                              Map<String, ByteBuffer> customPayload)
        throws RequestValidationException
        {
            return QueryProcessor.instance.prepare(query, clientState, customPayload);
        }

        @Override
        public ResultMessage process(CQLStatement statement,
                                     QueryState state,
                                     QueryOptions options,
                                     Map<String, ByteBuffer> customPayload,
                                     long queryStartNanoTime)
        throws RequestExecutionException, RequestValidationException
        {
            ClientWarn.instance.warn("async-process");
            return QueryProcessor.instance.process(statement, state, options, customPayload, queryStartNanoTime);
        }

        @Override
        public ResultMessage processBatch(BatchStatement statement,
                                          QueryState state,
                                          BatchQueryOptions options,
                                          Map<String, ByteBuffer> customPayload,
                                          long queryStartNanoTime)
        throws RequestExecutionException, RequestValidationException
        {
            ClientWarn.instance.warn("async-batch");
            return QueryProcessor.instance.processBatch(statement, state, options, customPayload, queryStartNanoTime);
        }

        @Override
        public ResultMessage processPrepared(CQLStatement statement,
                                             QueryState state,
                                             QueryOptions options,
                                             Map<String, ByteBuffer> customPayload,
                                             long queryStartNanoTime)
        throws RequestExecutionException, RequestValidationException
        {
            ClientWarn.instance.warn("async-prepared");
            return QueryProcessor.instance.processPrepared(statement, state, options, customPayload, queryStartNanoTime);
        }
    }

    private static class DumpStageInfoOnTimeout extends ConditionEvaluationLogger
    {
        public DumpStageInfoOnTimeout()
        {
            super(logger::warn);
        }

        @Override
        public void onTimeout(TimeoutEvent timeoutEvent)
        {
            super.onTimeout(timeoutEvent);
            logger.error("The number of tasks each stage had completed when timed out:");
            Arrays.stream(Stage.values())
                  .forEach(stage -> logger.error("{}: {}", stage, stage.getCompletedTaskCount()));
        }
    }
}
