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
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.DsePeersRecord;

import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * DSE 6 compatibility mode: a request whose verb does not exist in the pre-4.0 dialect (repair, Paxos v2, ping, custom
 * verbs...) never reaches a DSE 6 peer. {@code MessagingService} refuses it before it is queued, failing its callback at
 * once. Every refusal is counted for {@code nodetool compatstatus}.
 */
public class Dse6RefusedVerbTest
{
    private static final String PING_REFUSAL = "verb PING_REQ to a DSE 6 peer";

    private InternodeCompatibilityMode modeBefore;

    @BeforeClass
    public static void startup()
    {
        DatabaseDescriptor.daemonInitialization();
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
}
