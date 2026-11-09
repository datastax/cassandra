/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.repair;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.PrepareMessage;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.StorageCompatibilityMode;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TokenRangeTestUtil.MessageDelivery;
import static org.apache.cassandra.utils.TokenRangeTestUtil.generateRanges;
import static org.apache.cassandra.utils.TokenRangeTestUtil.node1;
import static org.apache.cassandra.utils.TokenRangeTestUtil.randomInt;
import static org.apache.cassandra.utils.TokenRangeTestUtil.registerOutgoingMessageSink;
import static org.apache.cassandra.utils.TokenRangeTestUtil.setLocalTokens;
import static org.apache.cassandra.utils.TokenRangeTestUtil.token;
import static org.apache.cassandra.utils.TokenRangeTestUtil.uuid;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A node in {@code storage_compatibility_mode: DSE_5_1}, whose big {@code me} sstables cannot be marked pending
 * repair, refuses to take part in an incremental repair, whatever node coordinates it, and takes part in a full or a
 * preview one; outside that storage mode an incremental repair is prepared as before.
 */
public class RepairMessageVerbHandlerDse51Test
{
    private static final String TEST_NAME = "repair_message_vh_dse51_test_";
    private static final String KEYSPACE = TEST_NAME + "cql_keyspace";
    private static final String TABLE = "table1";
    private static List<TableId> tableIds;

    private StorageCompatibilityMode storageModeBefore;

    @BeforeClass
    public static void init() throws Exception
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition(TEST_NAME);
        StorageService.instance.initServer(0);
        tableIds = Collections.singletonList(Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata().id);
    }

    @Before
    public void setup()
    {
        storageModeBefore = DatabaseDescriptor.getStorageCompatibilityMode();
        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadata().updateNormalTokens(Lists.newArrayList(token(0), token(200),
                                                                                         token(400)),
                                                                      node1);
        setLocalTokens(100);
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(storageModeBefore);
    }

    @Test
    public void anIncrementalRepairIsRefusedInTheDse51StorageMode() throws Exception
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.DSE_5_1);
        long before = DseCompatRefusals.count("repair");
        Logger logger = (Logger) LoggerFactory.getLogger(RepairMessageVerbHandler.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        TimeUUID session = uuid();
        MessageDelivery response;
        try
        {
            response = prepare(session, true, PreviewKind.NONE);
        }
        finally
        {
            logger.detachAppender(appender);
            appender.stop();
        }
        assertThat(response.message.verb()).isEqualTo(Verb.FAILURE_RSP);
        assertThat(response.message.payload).isInstanceOf(RequestFailureReason.class);
        assertThat(DseCompatRefusals.count("repair")).isEqualTo(before + 1);
        // no parent session is left, and the reason is logged here, with the session and the coordinator
        assertThatThrownBy(() -> ActiveRepairService.instance().getParentRepairSession(session))
        .isInstanceOf(NoSuchRepairSessionException.class);
        assertThat(appender.list).filteredOn(event -> event.getLevel().isGreaterOrEqual(Level.WARN))
                                 .extracting(ILoggingEvent::getFormattedMessage)
                                 .containsExactly("Refused the prepare of the repair session " + session + " from " +
                                                  node1 + ": incremental repair is refused in " +
                                                  "storage_compatibility_mode DSE_5_1: the big me sstables this node " +
                                                  "writes for DSE 5.1 cannot be marked pending repair, so they " +
                                                  "cannot be anticompacted; use a full repair (nodetool repair " +
                                                  "-full, coordinated by a node without the mode, with " +
                                                  "stream_entire_sstables: false on every node without the " +
                                                  "mode) until the flag is dropped and the big me sstables of this " +
                                                  "node are rewritten (nodetool upgradesstables)");

        // a full repair and a preview of an incremental one take place
        assertPrepared(false, PreviewKind.NONE);
        assertPrepared(true, PreviewKind.UNREPAIRED);
        assertThat(DseCompatRefusals.count("repair")).isEqualTo(before + 1);
    }

    /** The refusal comes before the check of the pending compactions, which would give another reason. */
    @Test
    public void theRefusalComesBeforeThePendingCompactionsCheck() throws Exception
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.DSE_5_1);
        int threshold = DatabaseDescriptor.getRepairPendingCompactionRejectThreshold();
        DatabaseDescriptor.setRepairPendingCompactionRejectThreshold(-1);
        try
        {
            Logger logger = (Logger) LoggerFactory.getLogger(RepairMessageVerbHandler.class);
            ListAppender<ILoggingEvent> appender = new ListAppender<>();
            appender.start();
            logger.addAppender(appender);
            TimeUUID session = uuid();
            MessageDelivery response;
            try
            {
                response = prepare(session, true, PreviewKind.NONE);
            }
            finally
            {
                logger.detachAppender(appender);
                appender.stop();
            }
            assertThat(response.message.verb()).isEqualTo(Verb.FAILURE_RSP);
            assertThat(appender.list).filteredOn(event -> event.getLevel().isGreaterOrEqual(Level.WARN))
                                     .extracting(ILoggingEvent::getFormattedMessage)
                                     .singleElement()
                                     .satisfies(text -> assertThat(text).startsWith("Refused the prepare of the " +
                                                                                    "repair session " + session));
            // without the storage mode the same prepare is refused for the pending compactions, not by the mode
            DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
            long refusals = DseCompatRefusals.count("repair");
            assertThat(prepare(uuid(), true, PreviewKind.NONE).message.verb()).isEqualTo(Verb.FAILURE_RSP);
            assertThat(DseCompatRefusals.count("repair")).isEqualTo(refusals);
        }
        finally
        {
            DatabaseDescriptor.setRepairPendingCompactionRejectThreshold(threshold);
        }
    }

    @Test
    public void anIncrementalRepairIsPreparedInTheOtherStorageModes() throws Exception
    {
        long before = DseCompatRefusals.count("repair");
        for (StorageCompatibilityMode mode : StorageCompatibilityMode.values())
        {
            if (mode == StorageCompatibilityMode.DSE_5_1)
                continue;
            DatabaseDescriptor.setStorageCompatibilityMode(mode);
            assertPrepared(true, PreviewKind.NONE);
        }
        assertThat(DseCompatRefusals.count("repair")).isEqualTo(before);
    }

    /** Prepares a repair session, asserts that it is prepared, and removes it. */
    private static void assertPrepared(boolean incremental, PreviewKind previewKind) throws Exception
    {
        TimeUUID session = uuid();
        MessageDelivery response = prepare(session, incremental, previewKind);
        assertThat(response.message.verb()).as("%s", DatabaseDescriptor.getStorageCompatibilityMode())
                                           .isEqualTo(Verb.REPAIR_RSP);
        assertThat(response.message.payload).isNotInstanceOf(RequestFailureReason.class);
        assertThat(ActiveRepairService.instance().removeParentRepairSession(session)).isNotNull();
    }

    private static MessageDelivery prepare(TimeUUID session, boolean incremental, PreviewKind previewKind)
    throws Exception
    {
        MessagingService.instance().outboundSink.clear();
        MessagingService.instance().inboundSink.clear();
        ListenableFuture<MessageDelivery> sink = registerOutgoingMessageSink(false);
        PrepareMessage prepare = new PrepareMessage(session, tableIds, generateRanges(10, 20), incremental,
                                                    ActiveRepairService.UNREPAIRED_SSTABLE, true, previewKind);
        Message<RepairMessage> message = Message.builder(Verb.PREPARE_MSG, (RepairMessage) prepare).from(node1)
                                                .withId(randomInt()).build();
        new RepairMessageVerbHandler().doVerb(message);
        return sink.get(1, TimeUnit.SECONDS);
    }
}
