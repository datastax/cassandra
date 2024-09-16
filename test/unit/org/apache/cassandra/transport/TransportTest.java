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
import java.util.Collections;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.transport.messages.BatchMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;

public class TransportTest extends CQLTester
{
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
            // Async native transport doubles the number of read/write/batch tasks
            doTestTransport(2);
        }
        finally
        {
            CassandraRelevantProperties.NATIVE_TRANSPORT_ASYNC_READ_WRITE_ENABLED.setBoolean(false);
        }
    }

    private void doTestTransport(long expectedTasks) throws Throwable
    {
        SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort);
        try
        {
            requireNetwork();

            client.connect(false);

            QueryMessage createMessage = new QueryMessage("CREATE TABLE " + KEYSPACE + ".atable (pk int PRIMARY KEY, v text)", QueryOptions.DEFAULT);
            PrepareMessage prepareMessage = new PrepareMessage("SELECT * FROM " + KEYSPACE + ".atable", null);

            Message.Response createResponse = client.execute(createMessage);
            ResultMessage.Prepared prepareResponse = (ResultMessage.Prepared) client.execute(prepareMessage);

            ExecuteMessage executeMessage = new ExecuteMessage(prepareResponse.statementId, prepareResponse.resultMetadataId, QueryOptions.DEFAULT);
            Message.Response executeResponse = client.execute(executeMessage);
            Assert.assertEquals(expectedTasks, Stage.READ.executor().getCompletedTaskCount());

            QueryMessage readMessage = new QueryMessage("SELECT * FROM " + KEYSPACE + ".atable", QueryOptions.DEFAULT);
            Message.Response readResponse = client.execute(readMessage);
            Assert.assertEquals(expectedTasks * 2, Stage.READ.executor().getCompletedTaskCount());

            BatchMessage batchMessage = new BatchMessage(BatchStatement.Type.UNLOGGED,
                                                         Collections.<Object>singletonList("INSERT INTO " + KEYSPACE + ".atable (pk,v) VALUES (1, 'foo')"),
                                                         Collections.singletonList(Collections.<ByteBuffer>emptyList()),
                                                         QueryOptions.DEFAULT);
            Message.Response batchResponse = client.execute(batchMessage);
            Assert.assertEquals(expectedTasks, Stage.MUTATION.executor().getCompletedTaskCount());

            QueryMessage insertMessage = new QueryMessage("INSERT INTO " + KEYSPACE + ".atable (pk,v) VALUES (1, 'foo')", QueryOptions.DEFAULT);
            Message.Response insertResponse = client.execute(insertMessage);
            Assert.assertEquals(expectedTasks * 2, Stage.MUTATION.executor().getCompletedTaskCount());
        }
        finally
        {
            client.close();
        }
    }
}
