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

package org.apache.cassandra.gms;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.VersionAndType;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EndpointStateTest
{

    // the following are dse-6.x legacy applicationState ordinals
    private static final ApplicationState DSE__STATUS = ApplicationState.values()[0];
    private static final ApplicationState DSE__INTERNAL_IP = ApplicationState.values()[7];
    private static final ApplicationState DSE__NATIVE_TRANSPORT_PORT = ApplicationState.values()[15];
    private static final ApplicationState DSE__NATIVE_TRANSPORT_PORT_SSL = ApplicationState.values()[16];
    private static final ApplicationState DSE__STORAGE_PORT = ApplicationState.values()[17];
    private static final ApplicationState DSE__SCHEMA_COMPATIBILITY_VERSION = ApplicationState.values()[20];
    private static final ApplicationState DSE__DISK_USAGE = ApplicationState.values()[21];

    public volatile VersionedValue.VersionedValueFactory valueFactory =
        new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner());

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testMultiThreadedReadConsistency() throws InterruptedException
    {
        for (int i = 0; i < 500; i++)
            innerTestMultiThreadedReadConsistency();
    }

    /**
     * Test that a thread reading values whilst they are updated by another thread will
     * not see an entry unless it sees the entry previously added as well, even though
     * we are accessing the map via an iterator backed by the underlying map. This
     * works because EndpointState copies the map each time values are added.
     */
    private void innerTestMultiThreadedReadConsistency() throws InterruptedException
    {
        final Token token = DatabaseDescriptor.getPartitioner().getRandomToken();
        final List<Token> tokens = Collections.singletonList(token);
        final HeartBeatState hb = new HeartBeatState(0);
        final EndpointState state = new EndpointState(hb);
        final AtomicInteger numFailures = new AtomicInteger();

        Thread t1 = new Thread(new Runnable()
        {
            public void run()
            {
                state.addApplicationState(ApplicationState.TOKENS, valueFactory.tokens(tokens));
                state.addApplicationState(ApplicationState.STATUS_WITH_PORT, valueFactory.normal(tokens));
            }
        });

        Thread t2 = new Thread(new Runnable()
        {
            public void run()
            {
                for (int i = 0; i < 50; i++)
                {
                    Map<ApplicationState, VersionedValue> values = new EnumMap<>(ApplicationState.class);
                    for (Map.Entry<ApplicationState, VersionedValue> entry : state.states())
                        values.put(entry.getKey(), entry.getValue());

                    if (values.containsKey(ApplicationState.STATUS_WITH_PORT) && !values.containsKey(ApplicationState.TOKENS))
                    {
                        numFailures.incrementAndGet();
                        System.out.println(String.format("Failed: %s", values));
                    }
                }
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        assertTrue(numFailures.get() == 0);
    }

    @Test
    public void testMultiThreadWriteConsistency() throws InterruptedException, UnknownHostException
    {
        for (int i = 0; i < 500; i++)
            innerTestMultiThreadWriteConsistency();
    }

    /**
     * Test that two threads can update the state map concurrently.
     */
    private void innerTestMultiThreadWriteConsistency() throws InterruptedException, UnknownHostException
    {
        final Token token = DatabaseDescriptor.getPartitioner().getRandomToken();
        final List<Token> tokens = Collections.singletonList(token);
        final InetAddress ip = InetAddress.getByAddress(null, new byte[] { 127, 0, 0, 1});
        final UUID hostId = UUID.randomUUID();
        final HeartBeatState hb = new HeartBeatState(0);
        final EndpointState state = new EndpointState(hb);

        Thread t1 = new Thread(new Runnable()
        {
            public void run()
            {
                Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
                states.put(ApplicationState.TOKENS, valueFactory.tokens(tokens));
                states.put(ApplicationState.STATUS_WITH_PORT, valueFactory.normal(tokens));
                state.addApplicationStates(states);
            }
        });

        Thread t2 = new Thread(new Runnable()
        {
            public void run()
            {
                Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
                states.put(ApplicationState.INTERNAL_IP, valueFactory.internalIP(ip));
                states.put(ApplicationState.HOST_ID, valueFactory.hostId(hostId));
                state.addApplicationStates(states);
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        Set<Map.Entry<ApplicationState, VersionedValue>> states = state.states();
        assertEquals(4, states.size());

        Map<ApplicationState, VersionedValue> values = new EnumMap<>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> entry : states)
            values.put(entry.getKey(), entry.getValue());

        assertTrue(values.containsKey(ApplicationState.STATUS_WITH_PORT));
        assertTrue(values.containsKey(ApplicationState.TOKENS));
        assertTrue(values.containsKey(ApplicationState.INTERNAL_IP));
        assertTrue(values.containsKey(ApplicationState.HOST_ID));
    }

    @Test
    public void testCCReleaseVersion() throws IOException
    {
        String versionString = "4.0.11.0-0b982c438bfc";
        String c3safeVersionString = "4.0.11.0";
        VersionedValue releaseVersion = valueFactory.releaseVersion(versionString);

        Map.Entry<ApplicationState, VersionedValue> entry = Map.entry(ApplicationState.RELEASE_VERSION, releaseVersion);
        Set<Map.Entry<ApplicationState, VersionedValue>> filtered40 = EndpointStateSerializer.filterOutgoingStates(Set.of(entry), MessagingService.VERSION_40);
        assertEquals(ApplicationState.RELEASE_VERSION, filtered40.stream().findFirst().get().getKey());
        assertEquals(versionString, filtered40.stream().findFirst().get().getValue().value);
        Set<Map.Entry<ApplicationState, VersionedValue>> filtered3014 = EndpointStateSerializer.filterOutgoingStates(Set.of(entry), MessagingService.VERSION_3014);
        assertEquals(ApplicationState.RELEASE_VERSION, filtered3014.stream().findFirst().get().getKey());
        assertEquals(c3safeVersionString, filtered3014.stream().findFirst().get().getValue().value);

        HeartBeatState hb = new HeartBeatState(0);
        EndpointState state = new EndpointState(hb);
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.RELEASE_VERSION, releaseVersion);
        state.addApplicationStates(states);
        assertEquals(versionString, state.getReleaseVersion().toString());

        DataOutputBuffer buffer = new DataOutputBuffer();
        EndpointState.serializer.serialize(state, buffer, MessagingService.VERSION_40);
        DataInputBuffer input = new DataInputBuffer(buffer.buffer(), false);
        EndpointState deserializedCurrent = EndpointState.serializer.deserialize(input, MessagingService.VERSION_40);
        assertEquals(versionString, deserializedCurrent.getApplicationState(ApplicationState.RELEASE_VERSION).value);

        buffer = new DataOutputBuffer();
        EndpointState.serializer.serialize(state, buffer, MessagingService.VERSION_3014);
        input = new DataInputBuffer(buffer.buffer(), false);
        EndpointState deserializedOld = EndpointState.serializer.deserialize(input, MessagingService.VERSION_3014);
        assertEquals(c3safeVersionString, deserializedOld.getApplicationState(ApplicationState.RELEASE_VERSION).value);
    }

    @Test
    public void testFilterOutgoingStates_30() throws UnknownHostException
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.INTERNAL_ADDRESS_AND_PORT, valueFactory.internalAddressAndPort(InetAddressAndPort.getByName("10.0.0.1:7000")));
        states.put(ApplicationState.NATIVE_ADDRESS_AND_PORT, valueFactory.nativeaddressAndPort(InetAddressAndPort.getByName("127.0.0.1:9042")));
        Token token = DatabaseDescriptor.getPartitioner().getTokenFactory().fromString("1");
        states.put(ApplicationState.STATUS_WITH_PORT, valueFactory.normal(Collections.singleton(token)));
        states.put(ApplicationState.SSTABLE_VERSIONS, valueFactory.sstableVersions(Collections.singleton(VersionAndType.fromString("bti-na"))));
        states.put(ApplicationState.INDEX_STATUS, valueFactory.indexStatus("test_index_status"));
        states.put(ApplicationState.DC, valueFactory.datacenter("datacenter1"));
        states.put(ApplicationState.DISK_USAGE, valueFactory.diskUsage("u:0.5"));

        Map<ApplicationState, VersionedValue> filtered = new EnumMap<>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> entry : EndpointStateSerializer.filterOutgoingStates(states.entrySet(), MessagingService.VERSION_30))
            filtered.put(entry.getKey(), entry.getValue());

        assertEquals("10.0.0.1", filtered.get(DSE__INTERNAL_IP).value);
        assertEquals("7000", filtered.get(DSE__STORAGE_PORT).value);
        assertEquals("9042", filtered.get(DSE__NATIVE_TRANSPORT_PORT).value);
        assertEquals("NORMAL", filtered.get(DSE__STATUS).value);
        assertFalse(filtered.containsKey(ApplicationState.SSTABLE_VERSIONS));
        assertFalse(filtered.containsKey(ApplicationState.INDEX_STATUS));
        assertEquals("datacenter1", filtered.get(ApplicationState.DC).value);
        assertEquals("u:0.5", filtered.get(DSE__DISK_USAGE).value);
        assertEquals(6, filtered.size());
    }

    @Test
    public void testFilterOutgoingStates_40() throws UnknownHostException
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.INTERNAL_ADDRESS_AND_PORT, valueFactory.internalAddressAndPort(InetAddressAndPort.getByName("10.0.0.1:7000")));

        Map<ApplicationState, VersionedValue> filtered = new EnumMap<>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> entry : EndpointStateSerializer.filterOutgoingStates(states.entrySet(), MessagingService.VERSION_40))
            filtered.put(entry.getKey(), entry.getValue());

        assertEquals("10.0.0.1:7000", filtered.get(ApplicationState.INTERNAL_ADDRESS_AND_PORT).value);
        assertEquals(1, filtered.size());
    }

    @Test
    public void testFilterIncomingStates_30()
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(DSE__NATIVE_TRANSPORT_PORT, VersionedValue.unsafeMakeVersionedValue("9042", 1));
        states.put(DSE__STORAGE_PORT, VersionedValue.unsafeMakeVersionedValue("7000", 1));
        states.put(DSE__DISK_USAGE, VersionedValue.unsafeMakeVersionedValue("u:0.5", 1));
        states.put(DSE__SCHEMA_COMPATIBILITY_VERSION, VersionedValue.unsafeMakeVersionedValue("SCHEMA_COMPATIBILITY_VERSION", 1));
        states.put(DSE__NATIVE_TRANSPORT_PORT_SSL, VersionedValue.unsafeMakeVersionedValue("NATIVE_TRANSPORT_PORT_SSL", 1));

        Map<ApplicationState, VersionedValue> filtered = EndpointStateSerializer.filterIncomingStates(states, MessagingService.VERSION_30);

        assertEquals("9042", filtered.get(ApplicationState.NATIVE_ADDRESS_AND_PORT).value);
        assertEquals("7000", filtered.get(ApplicationState.INTERNAL_ADDRESS_AND_PORT).value);
        assertEquals("u:0.5", filtered.get(ApplicationState.DISK_USAGE).value);
        assertEquals(3, filtered.size());
    }

    @Test
    public void testFilterIncomingStates_40()
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.RACK, valueFactory.rack("rack1"));

        Map<ApplicationState, VersionedValue> filtered = new EnumMap<>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> entry : EndpointStateSerializer.filterOutgoingStates(states.entrySet(), MessagingService.VERSION_40))
            filtered.put(entry.getKey(), entry.getValue());

        assertEquals("rack1", filtered.get(ApplicationState.RACK).value);
    }
}
