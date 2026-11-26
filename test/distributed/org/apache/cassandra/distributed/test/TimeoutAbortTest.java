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

package org.apache.cassandra.distributed.test;

import java.util.List;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TimeoutAbortTest extends TestBaseImpl
{
    private static final int ITERATION_DELAY_MS = 2000;
    private static final int READ_TIMEOUT_MS = ITERATION_DELAY_MS / 2;

    @Test
    public void timeoutTest() throws Throwable
    {
        // Every row and partition read takes longer than the read timeout, so the read times out as soon as it
        // reaches the first row inside the range tombstone.
        System.setProperty("cassandra.test.read_iteration_delay_ms", Integer.toString(ITERATION_DELAY_MS));
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(config -> config.set("read_request_timeout_in_ms", READ_TIMEOUT_MS))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int, ck1 int, ck2 int, d int, primary key (id, ck1, ck2))"));
            cluster.coordinator(1).execute(withKeyspace("delete from %s.tbl using timestamp 5 where id = 1 and ck1 = 77 "), ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);
            Thread.sleep(1000);
            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id, ck1, ck2, d) values (1,77,?,1) using timestamp 10"), ConsistencyLevel.ALL, i);
            cluster.get(1).flush(KEYSPACE);

            long logPosition = cluster.get(1).logs().mark();
            try
            {
                cluster.coordinator(1).execute(withKeyspace("select * from %s.tbl where id=1 and ck1 = 77"), ConsistencyLevel.ALL);
                fail("Read was expected to time out");
            }
            catch (Exception e)
            {
                assertTrue(e.getClass().getName(), e.getClass().getName().contains("ReadTimeoutException"));
            }

            // The coordinator gives up before the replica notices the timeout, give the replica time to stop.
            Thread.sleep(2L * ITERATION_DELAY_MS);
            List<String> errors = cluster.get(1).logs().grepForErrors(logPosition).getResult();
            assertTrue(errors.toString(), errors.isEmpty());
        }
        finally
        {
            System.clearProperty("cassandra.test.read_iteration_delay_ms");
        }
    }
}
