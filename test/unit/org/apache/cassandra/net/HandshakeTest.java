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

package org.apache.cassandra.net;

import java.nio.channels.ClosedChannelException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.OutboundConnectionInitiator.Result;
import org.apache.cassandra.net.OutboundConnectionInitiator.Result.MessagingSuccess;

import static org.apache.cassandra.net.MessagingService.VERSION_30;
import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.MessagingService.VERSION_DS_11;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.MessagingService.minimum_version;
import static org.apache.cassandra.net.ConnectionType.SMALL_MESSAGES;
import static org.apache.cassandra.net.OutboundConnectionInitiator.*;

// TODO: test failure due to exception, timeout, etc
public class HandshakeTest
{
    private static final SocketFactory factory = new SocketFactory();
    private static final int SUPPORTED_DSE_VERSION = 4;
    @BeforeClass
    public static void startup()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @AfterClass
    public static void cleanup() throws InterruptedException
    {
        factory.shutdownNow();
    }

    private Result handshake(int req, int outMin, int outMax) throws ExecutionException, InterruptedException
    {
        return handshake(req, new AcceptVersions(outMin, outMax, SUPPORTED_DSE_VERSION), null);
    }
    private Result handshake(int req, int outMin, int outMax, int inMin, int inMax) throws ExecutionException, InterruptedException
    {
        return handshake(req, new AcceptVersions(outMin, outMax, SUPPORTED_DSE_VERSION), new AcceptVersions(inMin, inMax, SUPPORTED_DSE_VERSION));
    }
    private Result handshake(int req, AcceptVersions acceptOutbound, AcceptVersions acceptInbound) throws ExecutionException, InterruptedException
    {
        InboundSockets inbound = new InboundSockets(new InboundConnectionSettings().withAcceptMessaging(acceptInbound));
        try
        {
            inbound.open();
            InetAddressAndPort endpoint = inbound.sockets().stream().map(s -> s.settings.bindAddress).findFirst().get();
            EventLoop eventLoop = factory.defaultGroup().next();
            Future<Result<MessagingSuccess>> future =
            initiateMessaging(eventLoop,
                              SMALL_MESSAGES,
                              new OutboundConnectionSettings(endpoint)
                                                    .withAcceptVersions(acceptOutbound)
                                                    .withDefaults(ConnectionCategory.MESSAGING),
                              req, new AsyncPromise<>(eventLoop));
            return future.get(20, TimeUnit.SECONDS);
        }
        catch (TimeoutException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            inbound.close().await(1L, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testBothCurrentVersion() throws InterruptedException, ExecutionException
    {
        Result result = handshake(current_version, minimum_version, current_version);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        result.success().channel.close();
    }

    @Test
    public void testSendCompatibleOldVersion() throws InterruptedException, ExecutionException
    {
        Result result = handshake(current_version, current_version, current_version + 1, current_version +1, current_version + 2);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        Assert.assertEquals(current_version + 1, result.success().messagingVersion);
        result.success().channel.close();
    }

    @Test
    public void testSendCompatibleFutureVersion() throws InterruptedException, ExecutionException
    {
        Result result = handshake(current_version + 1, current_version - 1, current_version + 1);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        Assert.assertEquals(current_version, result.success().messagingVersion);
        result.success().channel.close();
    }

    @Test
    public void testSendIncompatibleFutureVersion() throws InterruptedException, ExecutionException
    {
        Result result = handshake(current_version + 1, current_version + 1, current_version + 1);
        Assert.assertEquals(Result.Outcome.INCOMPATIBLE, result.outcome);
        Assert.assertEquals(current_version, result.incompatible().closestSupportedVersion);
        Assert.assertEquals(current_version, result.incompatible().maxMessagingVersion);
    }

    @Test
    public void testSendIncompatibleOldVersion() throws InterruptedException, ExecutionException
    {
        Result result = handshake(current_version + 1, current_version + 1, current_version + 1, current_version + 2, current_version + 3);
        Assert.assertEquals(Result.Outcome.INCOMPATIBLE, result.outcome);
        Assert.assertEquals(current_version + 2, result.incompatible().closestSupportedVersion);
        Assert.assertEquals(current_version + 3, result.incompatible().maxMessagingVersion);
    }

    @Test
    public void testSendCompatibleMinVersionPre40() throws InterruptedException, ExecutionException
    {
        Result result = handshake(VERSION_30, VERSION_30, VERSION_3014, VERSION_30, VERSION_3014);
        Assert.assertEquals(Result.Outcome.RETRY, result.outcome);
        // special case where VERSION_30 gets bumped to 3014 (so to avoid the column filter bug)
        Assert.assertEquals(VERSION_3014, result.retry().withMessagingVersion);
    }

    @Test
    public void testSendCompatibleMaxVersionPre40() throws InterruptedException, ExecutionException
    {
        Result result = handshake(VERSION_3014, VERSION_30, VERSION_3014, VERSION_30, VERSION_3014);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        Assert.assertEquals(VERSION_3014, result.success().messagingVersion);
        result.success().channel.close();
    }

    @Test
    public void testSendCompatibleFutureVersionPre40() throws InterruptedException, ExecutionException
    {
        Result result = handshake(VERSION_3014, VERSION_30, VERSION_3014, VERSION_30, VERSION_30);
        Assert.assertEquals(Result.Outcome.RETRY, result.outcome);
        Assert.assertEquals(VERSION_30, result.retry().withMessagingVersion);
    }

    @Test
    public void testSendIncompatibleFutureVersionPre40() throws InterruptedException, ExecutionException
    {
        Result result = handshake(VERSION_3014, VERSION_3014, VERSION_3014, VERSION_30, VERSION_30);
        Assert.assertEquals(Result.Outcome.INCOMPATIBLE, result.outcome);
        Assert.assertEquals(-1, result.incompatible().closestSupportedVersion);
        Assert.assertEquals(VERSION_30, result.incompatible().maxMessagingVersion);
    }

    @Test
    public void testDSEToCCInitialHandshake() throws InterruptedException, ExecutionException
    {
        // This is how DSE 255 (0,4) intiaties the connection to CC (10,100)
        Result result = handshake(255, 0, 4, VERSION_30, 100);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        Assert.assertEquals(255, result.success().messagingVersion);
        result.success().channel.close();
    }


    @Test
    public void testDSE6ToCCInitialHandshake() throws InterruptedException, ExecutionException
    {
        // This is how DSE 257 (0,12) intiaties the connection to CC (10,101)
        Result result = handshake(257, 0, 257, VERSION_30, VERSION_DS_11);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        Assert.assertEquals(VERSION_30, result.success().messagingVersion);
        result.success().channel.close();
    }

    @Test
    public void testC30ToDSE6InitialHandshake() throws InterruptedException, ExecutionException
    {
        // This is how  C* 3.0.13 (0,10) intiaties the connection to DSE 257 (10,257)
        Result result = handshake(VERSION_30, 0, VERSION_30, VERSION_30, 257);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        Assert.assertEquals(VERSION_30, result.success().messagingVersion);
    }

    @Test
    public void testC3014ToDSE6InitialHandshake() throws InterruptedException, ExecutionException
    {
        // This is how  C* 3.0.14 (0,101) intiaties the connection to DSE 257 (10,257)
        Result result = handshake(VERSION_3014, 0, VERSION_3014, VERSION_30, 257);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        Assert.assertEquals(VERSION_3014, result.success().messagingVersion);
    }

    @Test
    public void testCCToDSE6InitialHandshake() throws InterruptedException, ExecutionException
    {
        // This is how  CC (10,100) intiaties the connection to DSE 257 (0,4)
        Result result = handshake(VERSION_DS_11, 0, VERSION_DS_11, VERSION_30, 257);
        Assert.assertEquals(Result.Outcome.RETRY, result.outcome);
        Assert.assertEquals(VERSION_3014, result.retry().withMessagingVersion);
    }

    @Test
    public void testCCToDSE6InitialHandshakeOverride() throws InterruptedException, ExecutionException
    {
        try
        {
            System.setProperty(CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.getKey(), "11");
            // This is how  CC (10,100) intiaties the connection to DSE 257 (0,4)
            Result result = handshake(VERSION_DS_11, 0, VERSION_DS_11, VERSION_30, 257);
            Assert.assertEquals(Result.Outcome.RETRY, result.outcome);
            Assert.assertEquals(VERSION_3014, result.retry().withMessagingVersion);
        }
        finally
        {
            System.clearProperty(CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.getKey());
        }
    }

    @Test
    public void testSendCompatibleOldVersionPre40() throws InterruptedException
    {
        try
        {
            handshake(VERSION_30, VERSION_30, VERSION_3014, VERSION_3014, VERSION_3014);
            Assert.fail("Should have thrown");
        }
        catch (ExecutionException e)
        {
            Assert.assertTrue(e.getCause() instanceof ClosedChannelException);
        }
    }

    @Test
    public void testSendIncompatibleOldVersionPre40() throws InterruptedException
    {
        try
        {
            handshake(VERSION_30, VERSION_30, VERSION_30, VERSION_3014, VERSION_3014);
            Assert.fail("Should have thrown");
        }
        catch (ExecutionException e)
        {
            Assert.assertTrue(e.getCause() instanceof ClosedChannelException);
        }
    }

    @Test
    public void testSendCompatibleOldVersion40() throws InterruptedException, ExecutionException
    {
        Result result = handshake(VERSION_30, VERSION_30, VERSION_30, VERSION_30, current_version);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        Assert.assertEquals(VERSION_30, result.success().messagingVersion);
    }

    @Test
    public void testSendIncompatibleOldVersion40() throws InterruptedException
    {
        try
        {
            Assert.fail(Objects.toString(handshake(VERSION_30, VERSION_30, VERSION_30, current_version, current_version)));
        }
        catch (ExecutionException e)
        {
            Assert.assertTrue(e.getCause() instanceof ClosedChannelException);
        }
    }

    @Test // fairly contrived case, but since we introduced logic for testing we need to be careful it doesn't make us worse
    public void testSendToFuturePost40BelievedToBePre40() throws InterruptedException, ExecutionException
    {
        Result result = handshake(VERSION_30, VERSION_30, current_version, VERSION_30, current_version + 1);
        Assert.assertEquals(Result.Outcome.RETRY, result.outcome);
        // special case where VERSION_30 gets bumped to 3014 (so to avoid the column filter bug)
        Assert.assertEquals(VERSION_3014, result.retry().withMessagingVersion);
    }

    @Test
    public void testHandshakeAcceptsDseNodesWithForced3014Version() throws Crc.InvalidCrc
    {
        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(257); // Simulate DSE 6.8+ version encoding

        HandshakeProtocol.Accept accept = HandshakeProtocol.Accept.maybeDecode(buf, MessagingService.VERSION_40);
        Assert.assertNotNull(accept);
        Assert.assertEquals(0, accept.useMessagingVersion);
        Assert.assertEquals(MessagingService.VERSION_3014, accept.maxMessagingVersion);

        buf = Unpooled.buffer();
        buf.writeInt(257);
        accept = HandshakeProtocol.Accept.maybeDecode(buf, MessagingService.VERSION_3014);
        Assert.assertNotNull(accept);
        Assert.assertEquals(0, accept.useMessagingVersion);
        Assert.assertEquals(MessagingService.VERSION_3014, accept.maxMessagingVersion);
    }

    @Test
    public void testHandshakeVersionNegotiationWithPre40Peer() throws Crc.InvalidCrc
    {
        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(MessagingService.VERSION_30); // Pre-4.0 version

        HandshakeProtocol.Accept accept = HandshakeProtocol.Accept.maybeDecode(buf, MessagingService.VERSION_40);
        Assert.assertNotNull(accept);
        Assert.assertEquals(0, accept.useMessagingVersion);
        Assert.assertEquals(MessagingService.VERSION_30, accept.maxMessagingVersion);

        buf = Unpooled.buffer();
        buf.writeInt(MessagingService.VERSION_30);
        accept = HandshakeProtocol.Accept.maybeDecode(buf, MessagingService.VERSION_40);
        Assert.assertNotNull(accept);
        Assert.assertEquals(0, accept.useMessagingVersion);
        Assert.assertEquals(MessagingService.VERSION_30, accept.maxMessagingVersion);
    }
}
