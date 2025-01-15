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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;
import org.awaitility.Awaitility;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.net.MessagingService.VERSION_30;
import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.ConnectionUtils.*;
import static org.apache.cassandra.net.ConnectionType.LARGE_MESSAGES;
import static org.apache.cassandra.net.ConnectionType.SMALL_MESSAGES;
import static org.apache.cassandra.net.OutboundConnectionSettings.Framing.LZ4;
import static org.apache.cassandra.net.OutboundConnections.LARGE_MESSAGE_THRESHOLD;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;

public class ConnectionTest
{
    private static final Logger logger = LoggerFactory.getLogger(ConnectionTest.class);
    private static final SocketFactory factory = new SocketFactory();

    private final Map<Verb, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>>> serializers = new HashMap<>();
    private final Map<Verb, Supplier<? extends IVerbHandler<?>>> handlers = new HashMap<>();
    private final Map<Verb, ToLongFunction<TimeUnit>> timeouts = new HashMap<>();

    private void unsafeSetSerializer(Verb verb, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> supplier) throws Throwable
    {
        serializers.putIfAbsent(verb, verb.unsafeSetSerializer(supplier));
    }

    private void unsafeSetHandler(Verb verb, Supplier<? extends IVerbHandler<?>> supplier) throws Throwable
    {
        handlers.putIfAbsent(verb, verb.unsafeSetHandler(supplier));
    }

    private void unsafeSetExpiration(Verb verb, ToLongFunction<TimeUnit> expiration) throws Throwable
    {
        timeouts.putIfAbsent(verb, verb.unsafeSetExpiration(expiration));
    }

    @After
    public void resetVerbs() throws Throwable
    {
        for (Map.Entry<Verb, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>>> e : serializers.entrySet())
            e.getKey().unsafeSetSerializer(e.getValue());
        serializers.clear();
        for (Map.Entry<Verb, Supplier<? extends IVerbHandler<?>>> e : handlers.entrySet())
            e.getKey().unsafeSetHandler(e.getValue());
        handlers.clear();
        for (Map.Entry<Verb, ToLongFunction<TimeUnit>> e : timeouts.entrySet())
            e.getKey().unsafeSetExpiration(e.getValue());
        timeouts.clear();
    }

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

    interface SendTest
    {
        void accept(InboundMessageHandlers inbound, OutboundConnection outbound, InetAddressAndPort endpoint) throws Throwable;
    }

    interface ManualSendTest
    {
        void accept(Settings settings, InboundSockets inbound, OutboundConnection outbound, InetAddressAndPort endpoint) throws Throwable;
    }

    static class Settings
    {
        static final Settings SMALL = new Settings(SMALL_MESSAGES);
        static final Settings LARGE = new Settings(LARGE_MESSAGES);
        final ConnectionType type;
        final Function<OutboundConnectionSettings, OutboundConnectionSettings> outbound;
        final Function<InboundConnectionSettings, InboundConnectionSettings> inbound;
        Settings(ConnectionType type)
        {
            this(type, Function.identity(), Function.identity());
        }
        Settings(ConnectionType type, Function<OutboundConnectionSettings, OutboundConnectionSettings> outbound,
                 Function<InboundConnectionSettings, InboundConnectionSettings> inbound)
        {
            this.type = type;
            this.outbound = outbound;
            this.inbound = inbound;
        }
        Settings outbound(Function<OutboundConnectionSettings, OutboundConnectionSettings> outbound)
        {
            return new Settings(type, this.outbound.andThen(outbound), inbound);
        }
        Settings inbound(Function<InboundConnectionSettings, InboundConnectionSettings> inbound)
        {
            return new Settings(type, outbound, this.inbound.andThen(inbound));
        }
        Settings override(Settings settings)
        {
            return new Settings(settings.type != null ? settings.type : type,
                                outbound.andThen(settings.outbound),
                                inbound.andThen(settings.inbound));
        }
    }

    static final EncryptionOptions.ServerEncryptionOptions encryptionOptions =
            new EncryptionOptions.ServerEncryptionOptions()
            .withLegacySslStoragePort(true)
            .withOptional(true)
            .withInternodeEncryption(EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.all)
            .withKeyStore("test/conf/cassandra_ssl_test.keystore")
            .withKeyStorePassword("cassandra")
            .withTrustStore("test/conf/cassandra_ssl_test.truststore")
            .withTrustStorePassword("cassandra")
            .withRequireClientAuth(false)
            .withCipherSuites("TLS_RSA_WITH_AES_128_CBC_SHA");

    // 30 is used for CNDB compatibility
    static final AcceptVersions legacy = new AcceptVersions(VERSION_3014, VERSION_3014);

    static final List<Function<Settings, Settings>> MODIFIERS = ImmutableList.of(
        settings -> settings.outbound(outbound -> outbound.withAcceptVersions(legacy))
                            .inbound(inbound -> inbound.withAcceptMessaging(legacy)),
        settings -> settings.outbound(outbound -> outbound.withEncryption(encryptionOptions))
                            .inbound(inbound -> inbound.withEncryption(encryptionOptions)),
        settings -> settings.outbound(outbound -> outbound.withFraming(LZ4))
    );

    static final List<Settings> SETTINGS = applyPowerSet(
        ImmutableList.of(Settings.SMALL, Settings.LARGE),
        MODIFIERS
    );

    private static <T> List<T> applyPowerSet(List<T> settings, List<Function<T, T>> modifiers)
    {
        List<T> result = new ArrayList<>();
        for (Set<Function<T, T>> set : Sets.powerSet(new HashSet<>(modifiers)))
        {
            for (T s : settings)
            {
                for (Function<T, T> f : set)
                    s = f.apply(s);
                result.add(s);
            }
        }
        return result;
    }

    private void test(Settings extraSettings, SendTest test) throws Throwable
    {
        for (Settings s : SETTINGS)
            doTest(s.override(extraSettings), test);
    }
    private void test(SendTest test) throws Throwable
    {
        for (Settings s : SETTINGS)
            doTest(s, test);
    }

    private void testManual(ManualSendTest test) throws Throwable
    {
        for (Settings s : SETTINGS)
            doTestManual(s, test);
    }

    private void doTest(Settings settings, SendTest test) throws Throwable
    {
        doTestManual(settings, (ignore, inbound, outbound, endpoint) -> {
            inbound.open().sync();
            test.accept(MessagingService.instance().getInbound(endpoint), outbound, endpoint);
        });
    }

    private void doTestManual(Settings settings, ManualSendTest test) throws Throwable
    {
        InetAddressAndPort endpoint = FBUtilities.getBroadcastAddressAndPort();
        InboundConnectionSettings inboundSettings = settings.inbound.apply(new InboundConnectionSettings())
                                                                    .withBindAddress(endpoint)
                                                                    .withSocketFactory(factory);
        InboundSockets inbound = new InboundSockets(Collections.singletonList(inboundSettings));
        OutboundConnectionSettings outboundTemplate = settings.outbound.apply(new OutboundConnectionSettings(endpoint))
                                                                       .withDefaultReserveLimits()
                                                                       .withSocketFactory(factory)
                                                                       .withDefaults(ConnectionCategory.MESSAGING);
        ResourceLimits.EndpointAndGlobal reserveCapacityInBytes = new ResourceLimits.EndpointAndGlobal(new ResourceLimits.Concurrent(outboundTemplate.applicationSendQueueReserveEndpointCapacityInBytes), outboundTemplate.applicationSendQueueReserveGlobalCapacityInBytes);
        OutboundConnection outbound = new OutboundConnection(settings.type, outboundTemplate, reserveCapacityInBytes);
        try
        {
            logger.info("Running {} {} -> {}", outbound.messagingVersion(), outbound.settings(), inboundSettings);
            test.accept(settings, inbound, outbound, endpoint);
        }
        finally
        {
            outbound.close(false);
            inbound.close().get(30L, SECONDS);
            outbound.close(false).get(30L, SECONDS);
            resetVerbs();
            MessagingService.instance().messageHandlers.clear();
        }
    }

    @Test
    public void testMessageDeliveryOnReconnect() throws Throwable
    {
        testManual((settings, inbound, outbound, endpoint) -> {
            try
            {
                logger.info("Open 1");
                inbound.open().sync();
                CountDownLatch done = new CountDownLatch(1);
                unsafeSetHandler(Verb._TEST_1, () -> msg -> {
                    logger.info("Handler 1");
                    done.countDown();
                });
                logger.info("Enqueue 1");
                outbound.enqueue(Message.out(Verb._TEST_1, noPayload));
                Assert.assertTrue(done.await(10, SECONDS));
                Assert.assertEquals(done.getCount(), 0);

                // Simulate disconnect
                logger.info("Close 1");
                inbound.close().get(10, SECONDS);
                MessagingService.instance().removeInbound(endpoint);
                inbound = new InboundSockets(settings.inbound.apply(new InboundConnectionSettings()));
                logger.info("Open 2");
                inbound.open().sync();
                Awaitility.await().timeout(10, SECONDS).until(() -> !outbound.isConnected());

                CountDownLatch latch2 = new CountDownLatch(1);
                unsafeSetHandler(Verb._TEST_1, () -> msg ->
                {
                    logger.info("Handler 2");
                    latch2.countDown();
                });
                logger.info("Enqueue 2");
                outbound.enqueue(Message.out(Verb._TEST_1, noPayload));

                latch2.await(10, SECONDS);
                Assert.assertEquals(latch2.getCount(), 0);
            }
            finally
            {
                inbound.close().get(10, SECONDS);
                outbound.close(false).get(10, SECONDS);
            }
        });
    }

    private static <V> void randomize(List<V> list)
    {
        long seed = ThreadLocalRandom.current().nextLong();
        logger.info("Seed used for randomize: " + seed);
        Random random = new Random(seed);
        switch (random.nextInt(3))
        {
            case 0:
                Collections.shuffle(list, random);
                break;
            case 1:
                Collections.reverse(list);
                break;
            case 2:
                // leave as is
        }
    }

    private void connect(OutboundConnection outbound) throws Throwable
    {
        CountDownLatch latch = new CountDownLatch(1);
        unsafeSetHandler(Verb._TEST_1, () -> message -> latch.countDown());
        outbound.enqueue(Message.out(Verb._TEST_1, 0xffffffff));
        latch.await(10, SECONDS);
        Assert.assertEquals(0, latch.getCount());
        Assert.assertTrue(outbound.isConnected());
    }

}
