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

package org.apache.cassandra.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.KeyspaceParams;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Functional test for StorageService drain() method.
 * Tests the actual drain behavior including state transitions, component shutdown,
 * and data flushing. This test class is separate because drain() shuts down the system
 * globally and cannot be run alongside other StorageService tests.
 */
public class StorageServiceDrainTest
{
    private static final String KEYSPACE = "StorageServiceDrainTest";
    private static final String TABLE = "Standard1";

    static EmbeddedCassandraService service;
    private static ListAppender<ILoggingEvent> logAppender;
    private static ColumnFamilyStore cfs;

    @BeforeClass
    public static void startup() throws IOException
    {
        service = ServerTestUtils.startEmbeddedCassandraService();

        // Create a test keyspace and table
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE, 0, AsciiType.instance, BytesType.instance));

        cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);

        // Set up log capturing for StorageService
        logAppender = new ListAppender<>();
        logAppender.start();
        ((Logger) LoggerFactory.getLogger(StorageService.class)).addAppender(logAppender);
    }

    @AfterClass
    public static void tearDown()
    {
        if (logAppender != null)
        {
            ((Logger) LoggerFactory.getLogger(StorageService.class)).detachAppender(logAppender);
            logAppender.stop();
        }
        if (service != null)
            service.stop();
    }

    @Test
    public void testDrain() throws IOException, InterruptedException, ExecutionException
    {
        // Verify initial state - service should be in NORMAL mode
        assertEquals("NORMAL", StorageService.instance.getOperationMode());
        assertFalse("Service should not be drained initially", StorageService.instance.isDrained());
        assertFalse("Service should not be draining initially", StorageService.instance.isDraining());
        assertTrue("Gossiper should be enabled initially", Gossiper.instance.isEnabled());
        assertFalse("Mutation executors should not be terminated initially", Stage.areMutationExecutorsTerminated());

        // Write some data to ensure there's something to flush
        Mutation mutation = new RowUpdateBuilder(cfs.metadata.get(), 0, "testkey")
                           .clustering("testcol")
                           .add("val", ByteBuffer.wrap("testvalue".getBytes()))
                           .build();
        mutation.apply();

        // Verify there's data in the memtable before drain
        long memtableSizeBefore = cfs.metric.memtableLiveDataSize.getValue();
        assertTrue("Memtable should have data before drain", memtableSizeBefore > 0);

        logAppender.list.clear();

        StorageService.instance.drain();

        // Verify final state - service should be DRAINED
        assertEquals("DRAINED", StorageService.instance.getOperationMode());
        assertTrue("Service should be drained", StorageService.instance.isDrained());
        assertFalse("Service should not be draining after drain completes", StorageService.instance.isDraining());

        // Verify components are properly shut down
        assertFalse("Gossiper should be disabled after drain", Gossiper.instance.isEnabled());
        assertTrue("Mutation executors should be terminated after drain", Stage.areMutationExecutorsTerminated());

        // Verify data was flushed - memtable should be empty
        long memtableSizeAfter = cfs.metric.memtableLiveDataSize.getValue();
        assertEquals("Memtable should be empty after drain", 0, memtableSizeAfter);

        // Collect all INFO level log messages
        List<String> infoMessages = logAppender.list.stream()
                .filter(event -> Level.INFO == event.getLevel())
                .map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.toList());

        assertTrue("Should log 'DRAINED'",
                  infoMessages.stream().anyMatch(msg -> msg.contains("DRAINED")));
    }
}
