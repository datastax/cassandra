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

package org.apache.cassandra.db.memtable;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(BMUnitRunner.class)
@BMUnitConfig(debug = true)
public class FailedFlushTest extends CQLTester
{
    AtomicInteger flushes = new AtomicInteger();

    @Test
    public void failedFlushAndWritesTest() throws InterruptedException {
        DebuggableScheduledThreadPoolExecutor scheduledExecutor = new DebuggableScheduledThreadPoolExecutor("forced flush");
        scheduledExecutor.scheduleAtFixedRate(() -> doFlush(), 60, 60, java.util.concurrent.TimeUnit.SECONDS);

        createTable(KEYSPACE, "CREATE TABLE %s (pk int PRIMARY KEY, value int)", "failedflushtest");

        int idx = 1;
        while (idx < 10_000_000 && flushes.get() < 3)
        {
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", idx, idx);

            if (idx % 10000 == 0)
            {
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
                logger.info("idx={}, memtable-size={}", idx, memtable.getLiveDataSize());
            }
            idx++;
        }
        Thread.sleep(10000);
    }

    private void doFlush() {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.getAllMemtables().forEach(m -> logger.info("pre flush {} memtable-size={}", m, m.getLiveDataSize()));
        try {
            getCurrentColumnFamilyStore().forceFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS).get();
        } catch (InterruptedException e) {
            logger.error("DUPA Interrupted ", e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.error("DUPA Failed ", e);
            throw new RuntimeException(e);
        }
        finally {
            cfs.getAllMemtables().forEach(m -> logger.info("post flush {} memtable-size={}", m, m.getLiveDataSize()));
            flushes.incrementAndGet();
        }
    }
}
