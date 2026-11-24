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

package org.apache.cassandra.net;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.DsePeersRecord;

import static org.apache.cassandra.net.ConnectionType.LARGE_MESSAGES;
import static org.apache.cassandra.net.ConnectionType.SMALL_MESSAGES;
import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * DSE 6 compatibility mode: a request whose verb does not exist in the pre-4.0 dialect (repair, Paxos v2, ping, custom
 * verbs...) never reaches a DSE 6 peer. {@code MessagingService} refuses it before it is queued, failing its callback at
 * once; one queued before the peer's version was known is refused when it is serialized, and dropped cleanly: the
 * connection stays up, its reserved capacity is released once, the messages behind it are delivered. Every refusal is
 * counted for {@code nodetool compatstatus}.
 */
public class Dse6RefusedVerbTest
{
    private static final SocketFactory factory = new SocketFactory();
    private static final String PING_REFUSAL = "verb PING_REQ to a DSE 6 peer";

    private InternodeCompatibilityMode modeBefore;

    @BeforeClass
    public static void startup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @AfterClass
    public static void cleanup() throws InterruptedException
    {
        factory.shutdownNow();
    }

    @Before
    public void dse6Mode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        DseCompatRefusals.reset();
    }

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        DseCompatRefusals.reset();
    }

    /** Refused before it is queued: no connection is even opened, and the callback fails at once instead of timing out. */
    @Test
    public void refusedBeforeItIsQueued() throws Throwable
    {
        InetAddressAndPort dsePeer = InetAddressAndPort.getByName("127.0.0.77:7012");
        MessagingService messaging = MessagingService.instance();
        int minClusterVersionBefore = messaging.versions.minClusterVersion;
        messaging.versions.set(dsePeer, VERSION_3014);
        assertTrue("a peer at the dialect is recorded as a DSE peer", DsePeersRecord.peers().contains(dsePeer));
        try
        {
            CompletableFuture<RequestFailureReason> failure = new CompletableFuture<>();
            Message<PingRequest> ping = Message.out(Verb.PING_REQ, PingRequest.forSmall);
            messaging.sendWithCallback(ping, dsePeer, failureCallback(failure));
            assertSame(RequestFailureReason.UNKNOWN, failure.get(10, TimeUnit.SECONDS));
            assertNull(messaging.callbacks.get(ping.id(), dsePeer));
            assertFalse("no connection is opened for the refused message", messaging.channelManagers.containsKey(dsePeer));
            assertEquals(1, DseCompatRefusals.count(PING_REFUSAL));

            // a one-way send of such a verb is refused too
            messaging.send(Message.out(Verb.PING_REQ, PingRequest.forSmall), dsePeer);
            assertEquals(2, DseCompatRefusals.count(PING_REFUSAL));
            assertFalse(messaging.channelManagers.containsKey(dsePeer));
        }
        finally
        {
            forget(messaging, dsePeer, minClusterVersionBefore);
        }
    }

    /**
     * A refused message already queued on a connection that turns out to speak the dialect: it fails to serialize, is
     * dropped once with its capacity released, and the message behind it is delivered on the same connection.
     */
    @Test
    public void refusedWhenSerializedOnAConnectionAtVersion11() throws Throwable
    {
        for (ConnectionType type : new ConnectionType[]{ SMALL_MESSAGES, LARGE_MESSAGES })
            refusedWhenSerialized(type);
    }

    /**
     * With the mode off nothing is refused, as before the mode existed: a message to a peer recorded below 4.0 (which no
     * handshake records without the mode) is queued on a connection like any other, its callback left to its timeout.
     */
    @Test
    public void notRefusedWithTheModeOff() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.78:7012");
        MessagingService messaging = MessagingService.instance();
        int minClusterVersionBefore = messaging.versions.minClusterVersion;
        messaging.versions.set(peer, VERSION_3014);
        assertFalse("without the mode no DSE peer is recorded", DsePeersRecord.peers().contains(peer));
        try
        {
            CompletableFuture<RequestFailureReason> failure = new CompletableFuture<>();
            Message<PingRequest> ping = Message.out(Verb.PING_REQ, PingRequest.forSmall);
            messaging.sendWithCallback(ping, peer, failureCallback(failure));
            assertTrue("the message is queued on a connection", messaging.channelManagers.containsKey(peer));
            assertNotNull("its callback waits for its timeout", messaging.callbacks.get(ping.id(), peer));
            assertFalse(failure.isDone());
            assertEquals(0, DseCompatRefusals.count(PING_REFUSAL));
        }
        finally
        {
            messaging.closeOutboundNow(peer);
            forget(messaging, peer, minClusterVersionBefore);
        }
    }

    /**
     * Forgets the version of the peer, which in DSE 6 compatibility mode also drops it from the record of DSE peers once
     * the record is written, and restores the lowest version of the cluster, which forgetting the last remaining version
     * leaves as it was.
     */
    private static void forget(MessagingService messaging, InetAddressAndPort peer, int minClusterVersionBefore) throws Exception
    {
        messaging.versions.reset(peer);
        DsePeersRecord.awaitPersisted();
        assertFalse(DsePeersRecord.peers().contains(peer));
        messaging.versions.minClusterVersion = minClusterVersionBefore;
    }

    private static RequestCallback<Object> failureCallback(CompletableFuture<RequestFailureReason> failure)
    {
        return new RequestCallback<Object>()
        {
            public void onResponse(Message<Object> msg)
            {
                failure.completeExceptionally(new AssertionError("no response expected"));
            }

            public boolean invokeOnFailure()
            {
                return true;
            }

            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
                failure.complete(failureReason);
            }
        };
    }

    private void refusedWhenSerialized(ConnectionType type) throws Throwable
    {
        InboundSockets inbound = new InboundSockets(new InboundConnectionSettings().withAcceptMessaging(null));
        inbound.open().sync();
        InetAddressAndPort endpoint = inbound.sockets().stream().map(s -> s.settings.bindAddress).findFirst().get();
        MessagingService.instance().versions.set(endpoint, VERSION_3014);
        CountDownLatch delivered = new CountDownLatch(1);
        Supplier<? extends IVerbHandler<?>> echoHandler = Verb.ECHO_REQ.unsafeSetHandler(() -> message -> delivered.countDown());
        OutboundConnectionSettings template = new OutboundConnectionSettings(endpoint).withDefaultReserveLimits()
                                                                                      .withSocketFactory(factory)
                                                                                      .withDefaults(ConnectionCategory.MESSAGING);
        ResourceLimits.EndpointAndGlobal reserve = new ResourceLimits.EndpointAndGlobal(new ResourceLimits.Concurrent(template.applicationSendQueueReserveEndpointCapacityInBytes),
                                                                                         template.applicationSendQueueReserveGlobalCapacityInBytes);
        OutboundConnection outbound = new OutboundConnection(type, template, reserve);
        try
        {
            // queued straight on the connection, bypassing the check of MessagingService, as a message queued before
            // the peer's version was known is: the connection is established at 11 when it is serialized
            CompletableFuture<RequestFailureReason> failure = new CompletableFuture<>();
            Message<PingRequest> ping = Message.out(Verb.PING_REQ, PingRequest.forSmall);
            MessagingService.instance().callbacks.addWithExpiration(failureCallback(failure), ping, endpoint);
            outbound.enqueue(ping);
            outbound.enqueue(Message.out(Verb.ECHO_REQ, noPayload));

            assertTrue(type + ": the message behind the refused one is delivered", delivered.await(30, TimeUnit.SECONDS));
            // its callback fails at once, as for a message refused before it is queued
            assertSame(type.toString(), RequestFailureReason.UNKNOWN, failure.get(10, TimeUnit.SECONDS));
            assertNull(MessagingService.instance().callbacks.get(ping.id(), endpoint));
            Util.spinAssertEquals(0, outbound::pendingCount, 10);
            assertEquals(type + " connection at the dialect", VERSION_3014, outbound.messagingVersion());
            assertEquals(type + " refused message dropped once", 1, outbound.errorCount());
            assertEquals(type + " sent", 1, outbound.sentCount());
            assertEquals(type + " a single connection, no reconnect", 1, outbound.successfulConnections());
            assertTrue(outbound.isConnected());
            Util.spinAssertEquals(0L, () -> reserve.endpoint().using(), 10);
            assertEquals(1, DseCompatRefusals.count(PING_REFUSAL));
        }
        finally
        {
            outbound.close(false).get(30, TimeUnit.SECONDS);
            inbound.close().get(30, TimeUnit.SECONDS);
            Verb.ECHO_REQ.unsafeSetHandler(echoHandler);
            MessagingService.instance().versions.reset(endpoint);
            DseCompatRefusals.reset();
        }
    }
}
