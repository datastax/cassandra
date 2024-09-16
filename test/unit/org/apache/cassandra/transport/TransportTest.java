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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
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

public class TransportTest extends CQLTester
{
    private static Field cqlQueryHandlerField;
    private static boolean modifiersAccessible;

    @BeforeClass
    public static void makeCqlQueryHandlerAccessible()
    {
        try
        {
            cqlQueryHandlerField = ClientState.class.getDeclaredField("cqlQueryHandler");
            cqlQueryHandlerField.setAccessible(true);

            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersAccessible = modifiersField.isAccessible();
            modifiersField.setAccessible(true);
            modifiersField.setInt(cqlQueryHandlerField, cqlQueryHandlerField.getModifiers() & ~Modifier.FINAL);
        }
        catch (IllegalAccessException | NoSuchFieldException e)
        {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void resetCqlQueryHandlerField()
    {
        if (cqlQueryHandlerField == null)
            return;
        try
        {
            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(cqlQueryHandlerField, cqlQueryHandlerField.getModifiers() | Modifier.FINAL);

            cqlQueryHandlerField.setAccessible(false);

            modifiersField.setAccessible(modifiersAccessible);
        }
        catch (IllegalAccessException | NoSuchFieldException e)
        {
            throw new RuntimeException(e);
        }
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
        SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort);
        QueryHandler queryHandler = (QueryHandler) cqlQueryHandlerField.get(null);
        cqlQueryHandlerField.set(null, new TransportTest.TestQueryHandler());
        try
        {
            requireNetwork();

            client.connect(false);

            // Async native transport causes native transport requests to be executed asynchronously on the read and write
            // stages: this means the expected number of tasks on those stages starts at 2, one for the async request,
            // one for the actual execution.
            int expectedReadTasks = 2;
            int expectedMutationTasks = 2;

            QueryMessage createMessage = new QueryMessage("CREATE TABLE " + KEYSPACE + ".atable (pk int PRIMARY KEY, v text)", QueryOptions.DEFAULT);
            PrepareMessage prepareMessage = new PrepareMessage("SELECT * FROM " + KEYSPACE + ".atable", null);

            Message.Response createResponse = client.execute(createMessage);
            ResultMessage.Prepared prepareResponse = (ResultMessage.Prepared) client.execute(prepareMessage);

            ExecuteMessage executeMessage = new ExecuteMessage(prepareResponse.statementId, prepareResponse.resultMetadataId, QueryOptions.DEFAULT);
            Message.Response executeResponse = client.execute(executeMessage);
            Assert.assertEquals(1, executeResponse.getWarnings().size());
            Assert.assertEquals("async-prepared", executeResponse.getWarnings().get(0));
            Assert.assertEquals(expectedReadTasks, Stage.READ.executor().getCompletedTaskCount());

            expectedReadTasks += Stage.READ.executor().getCompletedTaskCount(); // we now expect two more tasks
            QueryMessage readMessage = new QueryMessage("SELECT * FROM " + KEYSPACE + ".atable", QueryOptions.DEFAULT);
            Message.Response readResponse = client.execute(readMessage);
            Assert.assertEquals(1, executeResponse.getWarnings().size());
            Assert.assertEquals("async-process", readResponse.getWarnings().get(0));
            Assert.assertEquals(expectedReadTasks, Stage.READ.executor().getCompletedTaskCount());

            BatchMessage batchMessage = new BatchMessage(BatchStatement.Type.UNLOGGED,
                                                         Collections.<Object>singletonList("INSERT INTO " + KEYSPACE + ".atable (pk,v) VALUES (1, 'foo')"),
                                                         Collections.singletonList(Collections.<ByteBuffer>emptyList()),
                                                         QueryOptions.DEFAULT);
            Message.Response batchResponse = client.execute(batchMessage);
            Assert.assertEquals(1, executeResponse.getWarnings().size());
            Assert.assertEquals("async-batch", batchResponse.getWarnings().get(0));
            Assert.assertEquals(expectedMutationTasks, Stage.MUTATION.executor().getCompletedTaskCount());

            expectedMutationTasks += Stage.MUTATION.executor().getCompletedTaskCount(); // we now expect two more tasks
            QueryMessage insertMessage = new QueryMessage("INSERT INTO " + KEYSPACE + ".atable (pk,v) VALUES (1, 'foo')", QueryOptions.DEFAULT);
            Message.Response insertResponse = client.execute(insertMessage);
            Assert.assertEquals(1, executeResponse.getWarnings().size());
            Assert.assertEquals("async-process", insertResponse.getWarnings().get(0));
            Assert.assertEquals(expectedMutationTasks, Stage.MUTATION.executor().getCompletedTaskCount());
        }
        finally
        {
            client.close();
            cqlQueryHandlerField.set(null, queryHandler);
        }
    }

    public static class TestQueryHandler implements QueryHandler
    {
        public QueryProcessor.Prepared getPrepared(MD5Digest id)
        {
            return QueryProcessor.instance.getPrepared(id);
        }

        public CQLStatement parse(String query, QueryState state, QueryOptions options)
        {
            return QueryProcessor.instance.parse(query, state, options);
        }

        public ResultMessage.Prepared prepare(String query,
                                              ClientState clientState,
                                              Map<String, ByteBuffer> customPayload)
        throws RequestValidationException
        {
            return QueryProcessor.instance.prepare(query, clientState, customPayload);
        }

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
}
