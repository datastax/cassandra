/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.net.InetAddresses;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Timer;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.MessagingMetrics;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.caffinitas.ohc.histo.EstimatedHistogram;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MessagingServiceTest
{
    private final static long[] bucketOffsets = new EstimatedHistogram(160).getBucketOffsets();
    public static final IInternodeAuthenticator ALLOW_NOTHING_AUTHENTICATOR = new IInternodeAuthenticator()
    {
        public boolean authenticate(InetAddress remoteAddress, int remotePort)
        {
            return false;
        }

        public void validateConfiguration() throws ConfigurationException
        {

        }
    };
    private static IInternodeAuthenticator originalAuthenticator;
    private static ServerEncryptionOptions originalServerEncryptionOptions;
    private static InetAddressAndPort originalListenAddress;

    private final MessagingService messagingService = new MessagingService(true);

    @BeforeClass
    public static void beforeClass() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.0.0.1"));
        originalAuthenticator = DatabaseDescriptor.getInternodeAuthenticator();
        originalServerEncryptionOptions = DatabaseDescriptor.getInternodeMessagingEncyptionOptions();
        originalListenAddress = InetAddressAndPort.getByAddressOverrideDefaults(DatabaseDescriptor.getListenAddress(), DatabaseDescriptor.getStoragePort());
    }

    @Before
    public void before() throws UnknownHostException
    {
        messagingService.metrics.resetDroppedMessages();
        messagingService.closeOutbound(InetAddressAndPort.getByName("127.0.0.2"));
        messagingService.closeOutbound(InetAddressAndPort.getByName("127.0.0.3"));
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setInternodeAuthenticator(originalAuthenticator);
        DatabaseDescriptor.setInternodeMessagingEncyptionOptions(originalServerEncryptionOptions);
        DatabaseDescriptor.setShouldListenOnBroadcastAddress(false);
        DatabaseDescriptor.setListenAddress(originalListenAddress.address);
        FBUtilities.reset();
    }

    @Test
    public void testDroppedMessages()
    {
        Verb verb = Verb.READ_REQ;

        for (int i = 1; i <= 5000; i++)
            messagingService.metrics.recordDroppedMessage(verb, i, MILLISECONDS, i % 2 == 0);

        List<String> logs = new ArrayList<>();
        messagingService.metrics.resetAndConsumeDroppedErrors(logs::add);
        assertEquals(1, logs.size());
        Pattern regexp = Pattern.compile("READ_REQ messages were dropped in last 5000 ms: (\\d+) internal and (\\d+) cross node. Mean internal dropped latency: (\\d+) ms and Mean cross-node dropped latency: (\\d+) ms");
        Matcher matcher = regexp.matcher(logs.get(0));
        assertTrue(matcher.find());
        assertEquals(2500, Integer.parseInt(matcher.group(1)));
        assertEquals(2500, Integer.parseInt(matcher.group(2)));
        assertTrue(Integer.parseInt(matcher.group(3)) > 0);
        assertTrue(Integer.parseInt(matcher.group(4)) > 0);
        assertEquals(5000, (int) messagingService.metrics.getDroppedMessages().get(verb.toString()));

        logs.clear();
        messagingService.metrics.resetAndConsumeDroppedErrors(logs::add);
        assertEquals(0, logs.size());

        for (int i = 0; i < 2500; i++)
            messagingService.metrics.recordDroppedMessage(verb, i, MILLISECONDS, i % 2 == 0);

        logs.clear();
        messagingService.metrics.resetAndConsumeDroppedErrors(logs::add);
        assertEquals(1, logs.size());
        matcher = regexp.matcher(logs.get(0));
        assertTrue(matcher.find());
        assertEquals(1250, Integer.parseInt(matcher.group(1)));
        assertEquals(1250, Integer.parseInt(matcher.group(2)));
        assertTrue(Integer.parseInt(matcher.group(3)) > 0);
        assertTrue(Integer.parseInt(matcher.group(4)) > 0);
        assertEquals(7500, (int) messagingService.metrics.getDroppedMessages().get(verb.toString()));
    }

    @Test
    public void testDCLatency()
    {
        int latency = 100;
        Map<String, MessagingMetrics.DCLatencyRecorder> dcLatency = MessagingService.instance().metrics.dcLatency;
        dcLatency.clear();

        long now = System.currentTimeMillis();
        long sentAt = now - latency;
        assertNull(dcLatency.get("datacenter1"));
        addDCLatency(sentAt, now);
        assertNotNull(dcLatency.get("datacenter1"));
        assertEquals(1, dcLatency.get("datacenter1").dcLatency.getCount());
        long expectedBucket = bucketOffsets[Math.abs(Arrays.binarySearch(bucketOffsets, MILLISECONDS.toNanos(latency))) - 1];
        assertEquals(expectedBucket, dcLatency.get("datacenter1").dcLatency.getSnapshot().getMax());
    }

    @Test
    public void testNegativeDCLatency()
    {
        MessagingMetrics.DCLatencyRecorder updater =
        (MessagingMetrics.DCLatencyRecorder) MessagingService.instance().metrics.internodeLatencyRecorder(InetAddressAndPort.getLocalHost());

        // if clocks are off should just not track anything
        int latency = -100;

        long now = System.currentTimeMillis();
        long sentAt = now - latency;

        long count = updater.dcLatency.getCount();
        updater.accept(Verb.READ_REQ, now - sentAt, MILLISECONDS);
        // negative value shoudln't be recorded
        assertEquals(count, updater.dcLatency.getCount());
    }

    @Test
    public void testQueueWaitLatency()
    {
        int latency = 100;
        Verb verb = Verb.MUTATION_REQ;

        Map<Verb, Timer> queueWaitLatency = MessagingService.instance().metrics.internalLatency;
        MessagingService.instance().metrics.recordInternalLatency(verb, InetAddressAndPort.getLocalHost(), latency, MILLISECONDS);
        assertEquals(1, queueWaitLatency.get(verb).getCount());
        long expectedBucket = bucketOffsets[Math.abs(Arrays.binarySearch(bucketOffsets, MILLISECONDS.toNanos(latency))) - 1];
        assertEquals(expectedBucket, queueWaitLatency.get(verb).getSnapshot().getMax());
    }

    @Test
    public void testNegativeQueueWaitLatency()
    {
        int latency = -100;
        Verb verb = Verb.MUTATION_REQ;

        Map<Verb, Timer> queueWaitLatency = MessagingService.instance().metrics.internalLatency;
        queueWaitLatency.clear();

        assertNull(queueWaitLatency.get(verb));
        MessagingService.instance().metrics.recordInternalLatency(verb, InetAddressAndPort.getLocalHost(), latency, MILLISECONDS);
        assertNull(queueWaitLatency.get(verb));
    }

    private static void addDCLatency(long sentAt, long nowTime)
    {
        MessagingService.instance().metrics.internodeLatencyRecorder(InetAddressAndPort.getLocalHost()).accept(Verb.READ_REQ, nowTime - sentAt, MILLISECONDS);
    }

    /**
     * Make sure that if internode authenticatino fails for an outbound connection that all the code that relies
     * on getting the connection pool handles the null return
     *
     * @throws Exception
     */
    @Test
    public void testFailedInternodeAuth() throws Exception
    {
        MessagingService ms = MessagingService.instance();
        DatabaseDescriptor.setInternodeAuthenticator(ALLOW_NOTHING_AUTHENTICATOR);
        InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.250");

        //Should return null
        Message messageOut = Message.out(Verb.ECHO_REQ, noPayload);
        assertFalse(ms.isConnected(address, messageOut));

        //Should tolerate null
        ms.closeOutbound(address);
        ms.send(messageOut, address);
    }

//    @Test
//    public void reconnectWithNewIp() throws Exception
//    {
//        InetAddressAndPort publicIp = InetAddressAndPort.getByName("127.0.0.2");
//        InetAddressAndPort privateIp = InetAddressAndPort.getByName("127.0.0.3");
//
//        // reset the preferred IP value, for good test hygene
//        SystemKeyspace.updatePreferredIP(publicIp, publicIp);
//
//        // create pool/conn with public addr
//        Assert.assertEquals(publicIp, messagingService.getCurrentEndpoint(publicIp));
//        messagingService.maybeReconnectWithNewIp(publicIp, privateIp).await(1L, TimeUnit.SECONDS);
//        Assert.assertEquals(privateIp, messagingService.getCurrentEndpoint(publicIp));
//
//        messagingService.closeOutbound(publicIp);
//
//        // recreate the pool/conn, and make sure the preferred ip addr is used
//        Assert.assertEquals(privateIp, messagingService.getCurrentEndpoint(publicIp));
//    }

    @Test
    public void listenPlainConnection() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.none);
        listen(serverEncryptionOptions, false);
    }

    @Test
    public void listenPlainConnectionWithBroadcastAddr() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.none);
        listen(serverEncryptionOptions, true);
    }

    @Test
    public void listenRequiredSecureConnection() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withOptional(false)
                                                          .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all)
                                                          .withLegacySslStoragePort(false);
        listen(serverEncryptionOptions, false);
    }

    @Test
    public void listenRequiredSecureConnectionWithBroadcastAddr() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withOptional(false)
                                                          .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all)
                                                          .withLegacySslStoragePort(false);
        listen(serverEncryptionOptions, true);
    }

    @Test
    public void listenRequiredSecureConnectionWithLegacyPort() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all)
                                                          .withOptional(false)
                                                          .withLegacySslStoragePort(true);
        listen(serverEncryptionOptions, false);
    }

    @Test
    public void listenRequiredSecureConnectionWithBroadcastAddrAndLegacyPort() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all)
                                                          .withOptional(false)
                                                          .withLegacySslStoragePort(true);
        listen(serverEncryptionOptions, true);
    }

    @Test
    public void listenOptionalSecureConnection() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withOptional(true);
        listen(serverEncryptionOptions, false);
    }

    @Test
    public void listenOptionalSecureConnectionWithBroadcastAddr() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withOptional(true);
        listen(serverEncryptionOptions, true);
    }

    private void listen(ServerEncryptionOptions serverEncryptionOptions, boolean listenOnBroadcastAddr) throws InterruptedException
    {
        InetAddress listenAddress = FBUtilities.getJustLocalAddress();
        if (listenOnBroadcastAddr)
        {
            DatabaseDescriptor.setShouldListenOnBroadcastAddress(true);
            listenAddress = InetAddresses.increment(FBUtilities.getBroadcastAddressAndPort().address);
            DatabaseDescriptor.setListenAddress(listenAddress);
            FBUtilities.reset();
        }

        InboundConnectionSettings settings = new InboundConnectionSettings()
                                             .withEncryption(serverEncryptionOptions);
        InboundSockets connections = new InboundSockets(settings);
        try
        {
            connections.open().await();
            Assert.assertTrue(connections.isListening());

            Set<InetAddressAndPort> expect = new HashSet<>();
            expect.add(InetAddressAndPort.getByAddressOverrideDefaults(listenAddress, DatabaseDescriptor.getStoragePort()));
            if (settings.encryption.enable_legacy_ssl_storage_port)
                expect.add(InetAddressAndPort.getByAddressOverrideDefaults(listenAddress, DatabaseDescriptor.getSSLStoragePort()));
            if (listenOnBroadcastAddr)
            {
                expect.add(InetAddressAndPort.getByAddressOverrideDefaults(FBUtilities.getBroadcastAddressAndPort().address, DatabaseDescriptor.getStoragePort()));
                if (settings.encryption.enable_legacy_ssl_storage_port)
                    expect.add(InetAddressAndPort.getByAddressOverrideDefaults(FBUtilities.getBroadcastAddressAndPort().address, DatabaseDescriptor.getSSLStoragePort()));
            }

            Assert.assertEquals(expect.size(), connections.sockets().size());

            final int legacySslPort = DatabaseDescriptor.getSSLStoragePort();
            for (InboundSockets.InboundSocket socket : connections.sockets())
            {
                Assert.assertEquals(serverEncryptionOptions.isEnabled(), socket.settings.encryption.isEnabled());
                Assert.assertEquals(serverEncryptionOptions.isOptional(), socket.settings.encryption.isOptional());
                if (!serverEncryptionOptions.isEnabled())
                    assertNotEquals(legacySslPort, socket.settings.bindAddress.port);
                if (legacySslPort == socket.settings.bindAddress.port)
                    Assert.assertFalse(socket.settings.encryption.isOptional());
                Assert.assertTrue(socket.settings.bindAddress.toString(), expect.remove(socket.settings.bindAddress));
            }
        }
        finally
        {
            connections.close().await();
            Assert.assertFalse(connections.isListening());
        }
    }


//    @Test
//    public void getPreferredRemoteAddrUsesPrivateIp() throws UnknownHostException
//    {
//        MessagingService ms = MessagingService.instance();
//        InetAddressAndPort remote = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.151", 7000);
//        InetAddressAndPort privateIp = InetAddressAndPort.getByName("127.0.0.6");
//
//        OutboundConnectionSettings template = new OutboundConnectionSettings(remote)
//                                              .withConnectTo(privateIp)
//                                              .withAuthenticator(ALLOW_NOTHING_AUTHENTICATOR);
//        OutboundConnections pool = new OutboundConnections(template, new MockBackPressureStrategy(null).newState(remote));
//        ms.channelManagers.put(remote, pool);
//
//        Assert.assertEquals(privateIp, ms.getPreferredRemoteAddr(remote));
//    }
//
//    @Test
//    public void getPreferredRemoteAddrUsesPreferredIp() throws UnknownHostException
//    {
//        MessagingService ms = MessagingService.instance();
//        InetAddressAndPort remote = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.115", 7000);
//
//        InetAddressAndPort preferredIp = InetAddressAndPort.getByName("127.0.0.16");
//        SystemKeyspace.updatePreferredIP(remote, preferredIp);
//
//        Assert.assertEquals(preferredIp, ms.getPreferredRemoteAddr(remote));
//    }
//
//    @Test
//    public void getPreferredRemoteAddrUsesPrivateIpOverridesPreferredIp() throws UnknownHostException
//    {
//        MessagingService ms = MessagingService.instance();
//        InetAddressAndPort local = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.4", 7000);
//        InetAddressAndPort remote = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.105", 7000);
//        InetAddressAndPort privateIp = InetAddressAndPort.getByName("127.0.0.6");
//
//        OutboundConnectionSettings template = new OutboundConnectionSettings(remote)
//                                              .withConnectTo(privateIp)
//                                              .withAuthenticator(ALLOW_NOTHING_AUTHENTICATOR);
//
//        OutboundConnections pool = new OutboundConnections(template, new MockBackPressureStrategy(null).newState(remote));
//        ms.channelManagers.put(remote, pool);
//
//        InetAddressAndPort preferredIp = InetAddressAndPort.getByName("127.0.0.16");
//        SystemKeyspace.updatePreferredIP(remote, preferredIp);
//
//        Assert.assertEquals(privateIp, ms.getPreferredRemoteAddr(remote));
//    }

    private static class PostSinkFilter
    {
        private Verb verb;
        public int count;

        PostSinkFilter(Verb verb)
        {
            this.verb = verb;
        }

        public void accept(Message<?> message, InetAddressAndPort to)
        {
            // Count all the messages seen for our verb
            if (message.verb() == verb)
            {
                count++;
            }
        }
    }

    @Test
    public void runPostSinkHookVerbFilter() throws UnknownHostException
    {
        PostSinkFilter echoSink = new PostSinkFilter(Verb.ECHO_REQ);
        MessagingService.instance().outboundSink.addPost((message, to) -> echoSink.accept(message, to));

        int numOfMessages = 3;
        // echoRecorder should see all ECHO_REQ messages
        sendMessages(numOfMessages, Verb.ECHO_REQ);
        assertEquals(numOfMessages, echoSink.count);

        PostSinkFilter hintSink = new PostSinkFilter(Verb.HINT_REQ);
        MessagingService.instance().outboundSink.addPost((message, to) -> hintSink.accept(message, to));

        // hintRecorder should not see any ECHO_REQ messages
        sendMessages(numOfMessages, Verb.ECHO_REQ);
        assertEquals(0, hintSink.count);
    }

    public static class TestMessagingMetrics extends MessagingMetrics {}

    @Test
    public void testCreatingCustomMessagingMetrics()
    {
        String originalValue = CassandraRelevantProperties.CUSTOM_MESSAGING_METRICS_PROVIDER_PROPERTY.getString();
        try
        {
            CassandraRelevantProperties.CUSTOM_MESSAGING_METRICS_PROVIDER_PROPERTY.setString(TestMessagingMetrics.class.getName());
            MessagingService testMessagingService = new MessagingService(true);
            assertTrue(testMessagingService.metrics instanceof TestMessagingMetrics);
        }
        finally
        {
            if (originalValue == null)
                System.clearProperty(CassandraRelevantProperties.CUSTOM_MESSAGING_METRICS_PROVIDER_PROPERTY.getKey());
            else
                CassandraRelevantProperties.CUSTOM_MESSAGING_METRICS_PROVIDER_PROPERTY.setString(originalValue);
        }
    }

    private static void sendMessages(int numOfMessages, Verb verb) throws UnknownHostException
    {
        InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.253");

        for (int i = 0; i < numOfMessages; i++)
        {
            MessagingService.instance().send(Message.out(verb, noPayload), address);
        }
    }

    @Test
    public void testDroppedMutationsTrackedByTable()
    {
        // Reset metrics to ensure clean state
        messagingService.metrics.resetDroppedMessages();

        // Create table metadata for a test table with unique name
        TableMetadata metadata = MockSchema.newTableMetadata("test_ks_drops", "test_table_drops");
        String tableKey = metadata.keyspace + "." + metadata.name;

        // Create a mutation for the table
        Mutation mutation = new RowUpdateBuilder(metadata, 0, "key1")
                            .clustering("col1")
                            .add("value", "test_value")
                            .build();

        // Create a message with the mutation
        Message<Mutation> message = Message.builder(Verb.MUTATION_REQ, mutation).build();

        // Record the mutation as dropped
        messagingService.metrics.recordDroppedMessage(message, 100, MILLISECONDS);

        // Verify the table-specific metric was updated
        Map<String, Long> droppedByTable = messagingService.metrics.getDroppedMutationsByTable();
        assertNotNull(droppedByTable);
        assertEquals(Long.valueOf(1L), droppedByTable.get(tableKey));

        // Drop another mutation for the same table
        messagingService.metrics.recordDroppedMessage(message, 200, MILLISECONDS);

        // Verify the counter accumulated
        droppedByTable = messagingService.metrics.getDroppedMutationsByTable();
        assertEquals(Long.valueOf(2L), droppedByTable.get(tableKey));
    }

    @Test
    public void testMultipleTablesTrackedIndependently()
    {
        // Reset metrics to ensure clean state
        messagingService.metrics.resetDroppedMessages();

        // Create metadata for multiple tables
        TableMetadata table1 = MockSchema.newTableMetadata("ks1", "table1");
        TableMetadata table2 = MockSchema.newTableMetadata("ks1", "table2");
        TableMetadata table3 = MockSchema.newTableMetadata("ks2", "table1");

        String tableKey1 = table1.keyspace + "." + table1.name;
        String tableKey2 = table2.keyspace + "." + table2.name;
        String tableKey3 = table3.keyspace + "." + table3.name;

        // Create mutations for each table
        Mutation mutation1 = new RowUpdateBuilder(table1, 0, "key1").clustering("col1").add("value", "val1").build();
        Mutation mutation2 = new RowUpdateBuilder(table2, 0, "key2").clustering("col1").add("value", "val2").build();
        Mutation mutation3 = new RowUpdateBuilder(table3, 0, "key3").clustering("col1").add("value", "val3").build();

        // Drop mutations for different tables
        messagingService.metrics.recordDroppedMessage(Message.builder(Verb.MUTATION_REQ, mutation1).build(), 100, MILLISECONDS);
        messagingService.metrics.recordDroppedMessage(Message.builder(Verb.MUTATION_REQ, mutation1).build(), 100, MILLISECONDS);
        messagingService.metrics.recordDroppedMessage(Message.builder(Verb.MUTATION_REQ, mutation2).build(), 100, MILLISECONDS);
        messagingService.metrics.recordDroppedMessage(Message.builder(Verb.MUTATION_REQ, mutation3).build(), 100, MILLISECONDS);
        messagingService.metrics.recordDroppedMessage(Message.builder(Verb.MUTATION_REQ, mutation3).build(), 100, MILLISECONDS);
        messagingService.metrics.recordDroppedMessage(Message.builder(Verb.MUTATION_REQ, mutation3).build(), 100, MILLISECONDS);

        // Verify each table tracked independently
        Map<String, Long> droppedByTable = messagingService.metrics.getDroppedMutationsByTable();
        assertEquals(Long.valueOf(2L), droppedByTable.get(tableKey1));
        assertEquals(Long.valueOf(1L), droppedByTable.get(tableKey2));
        assertEquals(Long.valueOf(3L), droppedByTable.get(tableKey3));
    }

    @Test
    public void testCrossNodeVsInternalLatencyPerTable()
    {
        // Reset metrics to ensure clean state
        messagingService.metrics.resetDroppedMessages();

        TableMetadata metadata = MockSchema.newTableMetadata("test_ks", "test_table");
        String tableKey = metadata.keyspace + "." + metadata.name;

        Mutation mutation = new RowUpdateBuilder(metadata, 0, "key1")
                            .clustering("col1")
                            .add("value", "test_value")
                            .build();

        Message<Mutation> crossNodeMessage = Message.builder(Verb.MUTATION_REQ, mutation)
                                                     .from(InetAddressAndPort.getLocalHost())
                                                     .build();
        Message<Mutation> internalMessage = Message.builder(Verb.MUTATION_REQ, mutation).build();

        // Record cross-node dropped mutations
        messagingService.metrics.recordDroppedMessage(crossNodeMessage, 100, MILLISECONDS);
        messagingService.metrics.recordDroppedMessage(crossNodeMessage, 150, MILLISECONDS);

        // Record internal dropped mutations
        messagingService.metrics.recordDroppedMessage(internalMessage, 50, MILLISECONDS);

        // Verify the table-specific metric was updated for both types
        Map<String, Long> droppedByTable = messagingService.metrics.getDroppedMutationsByTable();
        assertEquals(Long.valueOf(3L), droppedByTable.get(tableKey));
    }

    @Test
    public void testMultiplePartitionUpdatesInSingleMutation()
    {
        // Reset metrics to ensure clean state
        messagingService.metrics.resetDroppedMessages();

        // Create two different tables in the same keyspace with unique names
        TableMetadata table1 = MockSchema.newTableMetadata("ks_multi_part", "table1_multi");
        TableMetadata table2 = MockSchema.newTableMetadata("ks_multi_part", "table2_multi");

        String tableKey1 = table1.keyspace + "." + table1.name;
        String tableKey2 = table2.keyspace + "." + table2.name;

        // Create mutations for each table with the same partition key
        Mutation mutation1 = new RowUpdateBuilder(table1, 0, "key1").clustering("col1").add("value", "val1").build();
        Mutation mutation2 = new RowUpdateBuilder(table2, 0, "key1").clustering("col1").add("value", "val2").build();

        // Merge mutations into a single batch mutation (requires same keyspace and key)
        Mutation batchMutation = Mutation.merge(Arrays.asList(mutation1, mutation2));

        // Drop the batch mutation
        Message<Mutation> message = Message.builder(Verb.MUTATION_REQ, batchMutation).build();
        messagingService.metrics.recordDroppedMessage(message, 100, MILLISECONDS);

        // Verify both tables are tracked (each partition update counted)
        Map<String, Long> droppedByTable = messagingService.metrics.getDroppedMutationsByTable();
        assertEquals(Long.valueOf(1L), droppedByTable.get(tableKey1));
        assertEquals(Long.valueOf(1L), droppedByTable.get(tableKey2));
    }

    @Test
    public void testMBeanExposesDroppedMutationsByTable()
    {
        // Reset metrics to ensure clean state
        messagingService.metrics.resetDroppedMessages();

        // Create table metadata with unique name
        TableMetadata metadata = MockSchema.newTableMetadata("test_ks_mbean", "test_table_mbean");
        String tableKey = metadata.keyspace + "." + metadata.name;

        // Create and drop a mutation
        Mutation mutation = new RowUpdateBuilder(metadata, 0, "key1")
                            .clustering("col1")
                            .add("value", "test_value")
                            .build();
        Message<Mutation> message = Message.builder(Verb.MUTATION_REQ, mutation).build();
        messagingService.metrics.recordDroppedMessage(message, 100, MILLISECONDS);

        // Access via MBean interface
        MessagingServiceMBean mbean = new MessagingServiceMBeanImpl(true, messagingService.versions, messagingService.metrics);
        Map<String, Long> droppedByTable = mbean.getDroppedMutationsByTable();

        // Verify the data is accessible through MBean
        assertNotNull(droppedByTable);
        assertEquals(Long.valueOf(1L), droppedByTable.get(tableKey));
    }

    @Test
    public void testEmptyDroppedMutationsByTableMap()
    {
        // Reset metrics to ensure clean state
        messagingService.metrics.resetDroppedMessages();

        // Get dropped mutations by table when no mutations have been dropped
        Map<String, Long> droppedByTable = messagingService.metrics.getDroppedMutationsByTable();

        // Verify it returns an empty map (not null)
        assertNotNull(droppedByTable);
        assertEquals(0, droppedByTable.size());
    }
}
