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

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.net.InetAddresses;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.OutboundConnectionInitiator.SslFallbackConnectionType;
import org.apache.cassandra.security.DefaultSslContextFactory;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.streaming.async.NettyStreamingChannel;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;

import static org.apache.cassandra.config.CassandraRelevantProperties.GOSSIP_DISABLE_THREAD_VALIDATION;
import static org.apache.cassandra.net.MessagingService.VERSION_30;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.MessagingService.minimum_version;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * No plain-text fallback towards DSE 6 peers in the DSE 6 compatibility mode ({@link DseTlsPorts#sslFallback}). With
 * optional internode encryption, which {@code internode_encryption: dc} and {@code rack} imply whatever
 * {@code optional} says, {@link OutboundConnection} retries a failed TLS connection with each SSL fallback strategy in
 * turn, the last one without TLS, and the streaming connection factory tries them in the same order. In {@code DSE_6}
 * mode, when the configuration encrypts the connection and the peer is not known at messaging version 4.0 or later,
 * that last strategy is replaced by the configured TLS one for messaging and not tried for streaming, with a WARN once
 * per peer; a peer known at 4.0 or later, an unencrypted connection and mode {@code NONE} keep the upstream fallback.
 * <p>
 * The real-socket tests bind addresses 127.0.0.41 to 127.0.0.47 and 127.0.0.51 to 127.0.0.55 on the configured
 * storage port: every address of 127.0.0.0/8 is local on Linux (elsewhere they need loopback aliases), and like
 * {@code ConnectionTest} they must not run next to another test that binds that port.
 */
public class DseTlsFallbackTest
{
    private static final int DSE_DIALECT = DseCompat.DSE_DIALECT;
    /** A raw DSE messaging version, as a DSE 6.8.1+ node gossips it: the dialect, not a version above the current one. */
    private static final int DSE_GOSSIPED = DseCompat.DSE_681_VERSION;
    /** The values of {@code optional}: unset, false and true. */
    private static final Boolean[] OPTIONAL = { null, false, true };
    private static final String WARN_PREFIX = "DSE 6 compatibility mode: refusing the unencrypted fallback of the internode connections to ";
    private static final String FALLING_BACK_TO_NO_SSL = "is falling back to NO_SSL reconnect strategy for retry";

    private InternodeCompatibilityMode modeBefore;

    @BeforeClass
    public static void startup()
    {
        // the gossiper checks that state changes run on the gossip stage; the property is read when the class initialises
        try (WithProperties properties = new WithProperties().set(GOSSIP_DISABLE_THREAD_VALIDATION, true))
        {
            SchemaLoader.prepareServer();
            Gossiper.instance.getClass();
        }
    }

    @Before
    public void saveMode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DseTlsPorts.resetPlainFallbackRefusalLog();
    }

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    private static void mode(InternodeCompatibilityMode mode)
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(mode);
    }

    /** With {@code optional} null the setting is left unset (then true for dc and rack). */
    private static ServerEncryptionOptions encryption(InternodeEncryption internodeEncryption, @Nullable Boolean optional)
    {
        ServerEncryptionOptions options = new ServerEncryptionOptions().withInternodeEncryption(internodeEncryption);
        return optional == null ? options : options.withOptional(optional.booleanValue());
    }

    /**
     * Sets the node's messaging version of {@code peer}, which it does not know yet, without writing the record of DSE
     * peers the mode keeps.
     */
    private static void nodeVersion(InetAddressAndPort peer, int version)
    {
        assertThat(MessagingService.instance().versions.assume(peer, version)).as(peer + " was not known").isTrue();
    }

    /**
     * The decision in both modes, for every internode_encryption and optional setting, towards a peer in the local
     * rack, in another rack of the local data center and in another data center, through the defaults of an outbound
     * connection (the node's encryption options, which the snitch applies or not), for every way the connection's and
     * the node's versions can know the peer.
     */
    @Test
    public void theDecisionInBothModesForEveryEncryptionSetting() throws Exception
    {
        int storage = DatabaseDescriptor.getStoragePort();
        InetAddressAndPort sameRack = InetAddressAndPort.getByName("127.0.0.31:" + storage);
        InetAddressAndPort otherRack = InetAddressAndPort.getByName("127.0.0.32:" + storage);
        InetAddressAndPort otherDc = InetAddressAndPort.getByName("127.0.0.33:" + storage);
        IEndpointSnitch snitchBefore = DatabaseDescriptor.getEndpointSnitch();
        ServerEncryptionOptions encryptionBefore = DatabaseDescriptor.getInternodeMessagingEncyptionOptions();
        try
        {
            DatabaseDescriptor.setEndpointSnitch(new AbstractNetworkTopologySnitch()
            {
                @Override
                public String getRack(InetAddressAndPort endpoint)
                {
                    return endpoint.equals(otherRack) ? "rack2" : "rack1";
                }

                @Override
                public String getDatacenter(InetAddressAndPort endpoint)
                {
                    return endpoint.equals(otherDc) ? "dc2" : "dc1";
                }
            });
            for (InternodeCompatibilityMode mode : new InternodeCompatibilityMode[]{ InternodeCompatibilityMode.NONE, InternodeCompatibilityMode.DSE_6 })
            {
                mode(mode);
                for (InternodeEncryption internodeEncryption : InternodeEncryption.values())
                {
                    for (Boolean optional : OPTIONAL)
                    {
                        DatabaseDescriptor.setInternodeMessagingEncyptionOptions(encryption(internodeEncryption, optional));
                        for (InetAddressAndPort peer : List.of(sameRack, otherRack, otherDc))
                        {
                            boolean encrypted = internodeEncryption == InternodeEncryption.all
                                                || (internodeEncryption == InternodeEncryption.dc && peer.equals(otherDc))
                                                || (internodeEncryption == InternodeEncryption.rack && !peer.equals(sameRack));
                            OutboundConnectionSettings settings = new OutboundConnectionSettings(peer).withConnectTo(peer).withDefaults(ConnectionCategory.MESSAGING);
                            String as = mode + ", internode_encryption " + internodeEncryption + ", optional " + optional + ", peer " + peer;
                            assertThat(settings.withEncryption()).as(as).isEqualTo(encrypted);
                            if (encrypted)
                            {
                                // dc and rack are optional whatever optional says: the fallback is reachable
                                boolean expectOptional = internodeEncryption != InternodeEncryption.all || Boolean.TRUE.equals(optional);
                                assertThat(settings.encryption.getOptional()).as(as).isEqualTo(expectOptional);
                            }
                            boolean refused = mode == InternodeCompatibilityMode.DSE_6 && encrypted;
                            // known nowhere
                            assertDecision(settings, current_version, null, null, refused, as + ", unknown version");
                            // known at 4.0 or later by the connection or by the node (a copy taken before the node learnt it)
                            assertDecision(settings, current_version, current_version, null, false, as + ", connection at current");
                            assertDecision(settings, current_version, null, current_version, false, as + ", node at current");
                            assertDecision(settings, VERSION_40, VERSION_40, VERSION_40, false, as + ", at 4.0");
                            // a DSE version gossiped to the node, while the connection still has the current version
                            assertDecision(settings, current_version, current_version, DSE_GOSSIPED, refused, as + ", node at a DSE version");
                            assertDecision(settings, current_version, DSE_GOSSIPED, null, refused, as + ", connection at a DSE version");
                            // the dialect, known by the connection, by the node, or the version the attempt connects at
                            assertDecision(settings, DSE_DIALECT, DSE_DIALECT, null, refused, as + ", connection at the dialect");
                            assertDecision(settings, current_version, null, DSE_DIALECT, refused, as + ", node at the dialect");
                            assertDecision(settings, DSE_DIALECT, current_version, current_version, refused, as + ", attempt at the dialect");
                            assertDecision(settings, VERSION_30, VERSION_30, VERSION_30, refused, as + ", at 10");
                        }
                    }
                }
            }
        }
        finally
        {
            DatabaseDescriptor.setEndpointSnitch(snitchBefore);
            DatabaseDescriptor.setInternodeMessagingEncyptionOptions(encryptionBefore);
        }
    }

    /** The decision with the connection's and the node's versions of the peer (null: not known). */
    private static void assertDecision(OutboundConnectionSettings settings, int messagingVersion, @Nullable Integer connectionVersion,
                                       @Nullable Integer nodeVersion, boolean refused, String as)
    {
        EndpointMessagingVersions connectionVersions = new EndpointMessagingVersions();
        if (connectionVersion != null)
            connectionVersions.set(settings.to, connectionVersion);
        EndpointMessagingVersions nodeVersions = new EndpointMessagingVersions();
        if (nodeVersion != null)
            nodeVersions.set(settings.to, nodeVersion);
        String refusal = DseTlsPorts.plainFallbackRefusal(settings, messagingVersion, connectionVersions, nodeVersions);
        assertThat(refusal != null).as(as).isEqualTo(refused);
    }

    /**
     * Only NO_SSL changes, to SERVER_CONFIG, and only when refused; the refusal is logged at WARN once per peer, saying
     * why; nothing is logged when the fallback is not refused. Here the connection's versions are the node's.
     */
    @Test
    public void onlyNoSslIsReplacedAndTheRefusalIsLoggedOncePerPeer() throws Exception
    {
        int storage = DatabaseDescriptor.getStoragePort();
        InetAddressAndPort unknown = InetAddressAndPort.getByName("127.0.0.34:" + storage);
        InetAddressAndPort atTheDialect = InetAddressAndPort.getByName("127.0.0.35:" + storage);
        InetAddressAndPort upgraded = InetAddressAndPort.getByName("127.0.0.36:" + storage);
        InetAddressAndPort dseGossiped = InetAddressAndPort.getByName("127.0.0.37:" + storage);
        Logger logger = (Logger) LoggerFactory.getLogger(DseTlsPorts.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            nodeVersion(atTheDialect, DSE_DIALECT);
            nodeVersion(upgraded, current_version);
            nodeVersion(dseGossiped, DSE_GOSSIPED);
            ServerEncryptionOptions all = encryption(InternodeEncryption.all, true);
            mode(InternodeCompatibilityMode.NONE);
            for (InetAddressAndPort peer : List.of(unknown, atTheDialect, upgraded, dseGossiped))
                for (SslFallbackConnectionType strategy : SslFallbackConnectionType.values())
                    assertThat(DseTlsPorts.sslFallback(settings(peer, all), DSE_DIALECT, strategy)).isEqualTo(strategy);
            assertThat(warnings(appender)).isEmpty();

            mode(InternodeCompatibilityMode.DSE_6);
            // the encrypted strategies, a peer at 4.0 or later and an unencrypted connection are left alone
            for (InetAddressAndPort peer : List.of(unknown, atTheDialect, upgraded, dseGossiped))
                for (SslFallbackConnectionType strategy : List.of(SslFallbackConnectionType.SERVER_CONFIG, SslFallbackConnectionType.MTLS, SslFallbackConnectionType.SSL))
                    assertThat(DseTlsPorts.sslFallback(settings(peer, all), current_version, strategy)).isEqualTo(strategy);
            assertThat(DseTlsPorts.sslFallback(settings(upgraded, all), current_version, SslFallbackConnectionType.NO_SSL)).isEqualTo(SslFallbackConnectionType.NO_SSL);
            assertThat(DseTlsPorts.sslFallback(new OutboundConnectionSettings(unknown).withConnectTo(unknown), current_version, SslFallbackConnectionType.NO_SSL))
            .isEqualTo(SslFallbackConnectionType.NO_SSL);
            assertThat(warnings(appender)).isEmpty();

            for (int i = 0; i < 3; i++)
            {
                assertThat(DseTlsPorts.sslFallback(settings(unknown, all), current_version, SslFallbackConnectionType.NO_SSL)).isEqualTo(SslFallbackConnectionType.SERVER_CONFIG);
                assertThat(DseTlsPorts.sslFallback(settings(atTheDialect, all), DSE_DIALECT, SslFallbackConnectionType.NO_SSL)).isEqualTo(SslFallbackConnectionType.SERVER_CONFIG);
                assertThat(DseTlsPorts.sslFallback(settings(dseGossiped, all), current_version, SslFallbackConnectionType.NO_SSL)).isEqualTo(SslFallbackConnectionType.SERVER_CONFIG);
            }
            List<ILoggingEvent> logged = warnings(appender);
            assertThat(logged).hasSize(3);
            assertThat(logged).allMatch(e -> e.getLevel() == Level.WARN);
            assertThat(logged.get(0).getFormattedMessage()).isEqualTo(warning(unknown, "its messaging version is not known yet (it may be a DSE 6 node)"));
            assertThat(logged.get(1).getFormattedMessage()).isEqualTo(warning(atTheDialect, "it is at messaging version 11, below 4.0 (DSE 6 nodes are at 11)"));
            assertThat(logged.get(2).getFormattedMessage()).isEqualTo(warning(dseGossiped, "it is at messaging version 11, below 4.0 (DSE 6 nodes are at 11)"));
        }
        finally
        {
            logger.detachAppender(appender);
            for (InetAddressAndPort peer : List.of(atTheDialect, upgraded, dseGossiped))
                MessagingService.instance().versions.reset(peer);
        }
    }

    private static String warning(InetAddressAndPort peer, String reason)
    {
        return WARN_PREFIX + peer + ": internode_encryption (all) encrypts the connections to this peer and " + reason + ", so a failed TLS " +
               "connection is not retried in plain text: the connections to it keep failing while the peer does not accept TLS on port " +
               peer.getPort() + "; check the peer's internode_encryption and ssl_storage_port, and the certificates and truststores of both nodes";
    }

    /**
     * Through real sockets, against a node that accepts only plain text (a misconfigured peer), from optional TLS
     * ({@code internode_encryption: all}, {@code optional: true}): in {@code DSE_6} mode a peer the node does not know,
     * or knows at the dialect or at a gossiped DSE version, is never connected to: the fallback reaches NO_SSL twice
     * and is refused, with one WARN. A peer the node knows at 4.0 or later, even when it learns the version only after
     * the connection was created, and any peer with {@code NONE}, is connected to in plain text after the NO_SSL
     * fallback, as upstream.
     */
    @Test
    public void aPlainOnlyPeerIsNotReachedWithoutTls() throws Throwable
    {
        try
        {
            mode(InternodeCompatibilityMode.DSE_6);
            connect(peer(41), null, false, false);
            connect(peer(42), DSE_DIALECT, false, false);
            connect(peer(43), DSE_GOSSIPED, false, false);
            connect(peer(44), current_version, false, true);
            connect(peer(45), VERSION_40, false, true);
            connect(peer(46), current_version, true, true);

            mode(InternodeCompatibilityMode.NONE);
            connect(peer(47), null, false, true);
        }
        finally
        {
            for (int i = 41; i <= 47; i++)
                MessagingService.instance().versions.reset(peer(i));
        }
    }

    /**
     * The same for streaming connections, which try each strategy in turn (up to three attempts each) and give up: in
     * {@code DSE_6} mode a peer the node does not know, or knows at the dialect or at a gossiped DSE version, gets no
     * streaming connection (the last TLS failure is thrown, NO_SSL is not tried) and one WARN; a peer the node knows
     * at 4.0 or later, and any peer with {@code NONE}, gets a plain-text streaming connection, as upstream.
     */
    @Test
    public void aPlainOnlyPeerGetsNoStreamingConnectionWithoutTls() throws Throwable
    {
        try
        {
            mode(InternodeCompatibilityMode.DSE_6);
            stream(peer(51), null, false);
            stream(peer(52), DSE_GOSSIPED, false);
            stream(peer(53), DSE_DIALECT, false);
            stream(peer(54), current_version, true);

            mode(InternodeCompatibilityMode.NONE);
            stream(peer(55), null, true);
        }
        finally
        {
            for (int i = 51; i <= 55; i++)
                MessagingService.instance().versions.reset(peer(i));
        }
    }

    private void stream(InetAddressAndPort peer, @Nullable Integer version, boolean reached) throws Throwable
    {
        Logger dseLogger = (Logger) LoggerFactory.getLogger(DseTlsPorts.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        dseLogger.addAppender(appender);
        InboundSockets inbound = plainInbound(peer);
        try
        {
            inbound.open().sync();
            if (version != null)
                nodeVersion(peer, version);
            String as = "peer " + peer + " at version " + version;
            OutboundConnectionSettings template = new OutboundConnectionSettings(peer).withEncryption(optionalTls());
            if (reached)
            {
                NettyStreamingChannel channel = NettyStreamingConnectionFactory.connect(template, current_version, StreamingChannel.Kind.CONTROL);
                try
                {
                    assertThat(channel.connected()).as(as).isTrue();
                }
                finally
                {
                    channel.close().await(10, TimeUnit.SECONDS);
                }
                assertThat(warnings(appender)).as(as).isEmpty();
            }
            else
            {
                assertThatThrownBy(() -> NettyStreamingConnectionFactory.connect(template, current_version, StreamingChannel.Kind.CONTROL))
                .as(as).isInstanceOf(IOException.class).hasMessage("failed to connect to " + peer + " for streaming data");
                assertThat(warnings(appender)).as(as).hasSize(1);
                assertThat(warnings(appender).get(0).getFormattedMessage()).as(as).startsWith(WARN_PREFIX + peer + ": ");
            }
        }
        finally
        {
            dseLogger.detachAppender(appender);
            inbound.close().await(10L, TimeUnit.SECONDS);
        }
    }

    /**
     * Connects with optional TLS to a plain-only node at {@code peer}, which the node knows at {@code version} (not at
     * all when null) before the connection is created, or only after when {@code learntAfter}, and checks that it is
     * reached in plain text after the NO_SSL fallback, or never reached, with the refusal logged once.
     */
    private void connect(InetAddressAndPort peer, @Nullable Integer version, boolean learntAfter, boolean reached) throws Throwable
    {
        Logger dseLogger = (Logger) LoggerFactory.getLogger(DseTlsPorts.class);
        Logger connectionLogger = (Logger) LoggerFactory.getLogger(OutboundConnection.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        dseLogger.addAppender(appender);
        connectionLogger.addAppender(appender);
        InboundSockets inbound = plainInbound(peer);
        try
        {
            inbound.open().sync();
            if (version != null && !learntAfter)
                nodeVersion(peer, version);
            OutboundConnection connection = initiateOutbound(peer);
            if (version != null && learntAfter)
                nodeVersion(peer, version);
            try
            {
                // NO_SSL is the fourth strategy: two rounds of the four reach it twice
                int attempts = 2 * SslFallbackConnectionType.values().length + 1;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
                while ((reached ? !connection.isConnected() : connection.connectionAttempts() < attempts) && System.nanoTime() < deadline)
                    Thread.sleep(100);
                String as = "peer " + peer + " at version " + version + (learntAfter ? " learnt after" : "");
                List<String> warnings = appender.list.stream().map(ILoggingEvent::getFormattedMessage)
                                                     .filter(m -> m.startsWith(WARN_PREFIX + peer + ": ")).collect(Collectors.toList());
                if (reached)
                {
                    assertThat(connection.isConnected()).as(as).isTrue();
                    assertThat(appender.list).as(as).anyMatch(e -> e.getFormattedMessage().contains(FALLING_BACK_TO_NO_SSL));
                    assertThat(warnings).as(as).isEmpty();
                }
                else
                {
                    assertThat(connection.connectionAttempts()).as(as).isGreaterThanOrEqualTo(attempts);
                    assertThat(connection.isConnected()).as(as).isFalse();
                    assertThat(warnings).as(as).hasSize(1);
                }
            }
            finally
            {
                connection.close(false).get(10, TimeUnit.SECONDS);
            }
        }
        finally
        {
            dseLogger.detachAppender(appender);
            connectionLogger.detachAppender(appender);
            inbound.close().await(10L, TimeUnit.SECONDS);
        }
    }

    private static InetAddressAndPort peer(int lastByte)
    {
        return InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0." + lastByte), DatabaseDescriptor.getStoragePort());
    }

    /** A node that accepts only plain-text internode connections. */
    private static InboundSockets plainInbound(InetAddressAndPort address)
    {
        List<InboundConnectionSettings> settings = new ArrayList<>();
        settings.add(new InboundConnectionSettings().withAcceptMessaging(new AcceptVersions(minimum_version, current_version))
                                                    .withEncryption(new ServerEncryptionOptions().withInternodeEncryption(InternodeEncryption.none))
                                                    .withBindAddress(address));
        return new InboundSockets(settings);
    }

    /**
     * An outbound connection with optional TLS to {@code peer}, with a message queued so that it connects, which does
     * not expire while the test waits (a connection without pending messages stops retrying).
     */
    private static OutboundConnection initiateOutbound(InetAddressAndPort peer) throws ClosedChannelException
    {
        OutboundConnectionSettings settings = new OutboundConnectionSettings(peer)
                                              .withAcceptVersions(new AcceptVersions(minimum_version, current_version))
                                              .withDefaults(ConnectionCategory.MESSAGING)
                                              .withEncryption(optionalTls());
        OutboundConnections connections = OutboundConnections.tryRegister(new ConcurrentHashMap<>(), peer, settings);
        Message<GossipDigestSyn> message = Message.out(Verb.GOSSIP_DIGEST_SYN, new GossipDigestSyn("cluster", "partitioner", new ArrayList<>(0)),
                                                       approxTime.now() + TimeUnit.MINUTES.toNanos(10));
        OutboundConnection connection = connections.connectionFor(message);
        connection.enqueue(message);
        return connection;
    }

    /** Optional TLS: {@code internode_encryption: all}, {@code optional: true}. */
    private static ServerEncryptionOptions optionalTls()
    {
        return new ServerEncryptionOptions().withOptional(true)
                                            .withKeyStore("test/conf/cassandra_ssl_test.keystore")
                                            .withKeyStorePassword("cassandra")
                                            .withOutboundKeystore("test/conf/cassandra_ssl_test_outbound.keystore")
                                            .withOutboundKeystorePassword("cassandra")
                                            .withTrustStore("test/conf/cassandra_ssl_test.truststore")
                                            .withTrustStorePassword("cassandra")
                                            .withSslContextFactory(new ParameterizedClass(DefaultSslContextFactory.class.getName(), new HashMap<>()))
                                            .withInternodeEncryption(InternodeEncryption.all)
                                            .withRequireClientAuth(false);
    }

    /** Settings whose messaging versions are the node's. */
    private static OutboundConnectionSettings settings(InetAddressAndPort peer, ServerEncryptionOptions encryption)
    {
        return new OutboundConnectionSettings(peer).withConnectTo(peer).withEncryption(encryption);
    }

    private static List<ILoggingEvent> warnings(ListAppender<ILoggingEvent> appender)
    {
        return appender.list.stream().filter(e -> e.getFormattedMessage().startsWith(WARN_PREFIX)).collect(Collectors.toList());
    }
}
