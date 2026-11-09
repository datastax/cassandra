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

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

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
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.GOSSIP_DISABLE_THREAD_VALIDATION;
import static org.apache.cassandra.config.CassandraRelevantProperties.SSL_STORAGE_PORT;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.MessagingService.VERSION_DS_11;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The internode TLS ports of the DSE 6 compatibility mode ({@link DseTlsPorts}): with internode encryption on, a node in
 * {@code DSE_6} mode also listens, TLS only, on {@code ssl_storage_port} when {@code legacy_ssl_storage_port_enabled}
 * is false, because DSE 6 nodes dial an upgraded node there for the connections they encrypt (DSE 6.8 and 6.9 before
 * 6.9.7 always, DSE 6.9.7+ once it has stored a messaging version below DSE 6.9.7's for the node); it dials a DSE 6
 * peer on {@code ssl_storage_port} below DSE 6.9.7 and on {@code storage_port} from 6.9.7 on, by the DSE version gossip
 * or the stored release gives, and by {@code legacy_ssl_storage_port_enabled} when neither does. With {@code NONE} the
 * sockets and the ports are the upstream ones.
 */
public class DseTlsPortsTest
{
    private static final int DSE_DIALECT = DseCompat.DSE_DIALECT;
    /** The values of {@code optional} the socket tests run with: unset, false and true. */
    private static final Boolean[] OPTIONAL = { null, false, true };
    private static final String INFO_PREFIX = "DSE 6 compatibility mode: also listening for TLS-only internode connections on ";

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

    private static ServerEncryptionOptions encryption(InternodeEncryption internodeEncryption, boolean legacyPort)
    {
        return encryption(internodeEncryption, legacyPort, null);
    }

    /** With {@code optional} null the setting is left unset (then true for dc and rack). */
    private static ServerEncryptionOptions encryption(InternodeEncryption internodeEncryption, boolean legacyPort, @Nullable Boolean optional)
    {
        ServerEncryptionOptions options = new ServerEncryptionOptions().withInternodeEncryption(internodeEncryption).withLegacySslStoragePort(legacyPort);
        return optional == null ? options : options.withOptional(optional.booleanValue());
    }

    /** The ports the sockets of a node with these options bind (none is opened). */
    private static List<Integer> boundPorts(ServerEncryptionOptions encryption)
    {
        InboundSockets sockets = new InboundSockets(new InboundConnectionSettings().withEncryption(encryption));
        return sockets.sockets().stream().map(s -> s.settings.bindAddress.getPort()).sorted().collect(Collectors.toList());
    }

    /** The extra socket is bound in DSE_6 mode only, with internode encryption on and the legacy port setting false. */
    @Test
    public void theExtraSocketIsBoundOnlyInDse6ModeWithEncryptionAndNoLegacyPort()
    {
        int storage = DatabaseDescriptor.getStoragePort();
        int ssl = DatabaseDescriptor.getSSLStoragePort();
        assertThat(ssl).isNotEqualTo(storage);
        for (InternodeCompatibilityMode mode : new InternodeCompatibilityMode[]{ InternodeCompatibilityMode.NONE, InternodeCompatibilityMode.DSE_6 })
        {
            mode(mode);
            for (InternodeEncryption internodeEncryption : InternodeEncryption.values())
            {
                for (boolean legacyPort : new boolean[]{ false, true })
                {
                    for (Boolean optional : OPTIONAL)
                    {
                        ServerEncryptionOptions options = encryption(internodeEncryption, legacyPort, optional);
                        boolean forDse = mode == InternodeCompatibilityMode.DSE_6 && internodeEncryption != InternodeEncryption.none && !legacyPort;
                        String as = mode + ", internode_encryption " + internodeEncryption + ", legacy_ssl_storage_port_enabled " + legacyPort +
                                    ", optional " + optional;
                        assertThat(DseTlsPorts.listensOnSslStoragePortForDse(options)).as(as).isEqualTo(forDse);
                        // upstream: storage_port, plus ssl_storage_port with the legacy setting
                        List<Integer> expected = legacyPort || forDse ? List.of(storage, ssl) : List.of(storage);
                        assertThat(boundPorts(options)).as(as).isEqualTo(expected);
                    }
                }
            }
        }
        mode(InternodeCompatibilityMode.DSE_6);
        assertThat(DseTlsPorts.listensOnSslStoragePortForDse(null)).isFalse();
    }

    /**
     * The extra socket requires TLS whatever internode_encryption and optional, as the legacy socket of upstream does
     * (the in-JVM {@code Dse6InternodeTlsPortTest} checks that it refuses a plain connection).
     */
    @Test
    public void theExtraSocketRequiresTls()
    {
        mode(InternodeCompatibilityMode.DSE_6);
        for (InternodeEncryption internodeEncryption : new InternodeEncryption[]{ InternodeEncryption.all, InternodeEncryption.dc, InternodeEncryption.rack })
        {
            for (Boolean optional : OPTIONAL)
            {
                String as = internodeEncryption + ", optional " + optional;
                InboundSockets sockets = new InboundSockets(new InboundConnectionSettings().withEncryption(encryption(internodeEncryption, false, optional)));
                List<InboundSockets.InboundSocket> ssl = sockets.sockets().stream()
                                                                .filter(s -> s.settings.bindAddress.getPort() == DatabaseDescriptor.getSSLStoragePort())
                                                                .collect(Collectors.toList());
                assertThat(ssl).as(as).hasSize(1);
                assertThat(ssl.get(0).settings.encryption.internode_encryption).as(as).isEqualTo(InternodeEncryption.all);
                assertThat(ssl.get(0).settings.encryption.getOptional()).as(as).isFalse();
            }
        }
    }

    /** Both sockets of a DSE_6 node with internode encryption and no legacy port setting open and listen. */
    @Test
    public void bothSocketsOpen() throws Exception
    {
        mode(InternodeCompatibilityMode.DSE_6);
        ServerEncryptionOptions options = encryption(InternodeEncryption.all, false).withOptional(false);
        InboundSockets sockets = new InboundSockets(new InboundConnectionSettings().withEncryption(options));
        try
        {
            sockets.open().sync();
            assertThat(sockets.isListening()).isTrue();
            assertThat(sockets.sockets().stream().map(s -> s.settings.bindAddress.getPort()))
            .containsExactlyInAnyOrder(DatabaseDescriptor.getStoragePort(), DatabaseDescriptor.getSSLStoragePort());
        }
        finally
        {
            sockets.close().await();
        }
        assertThat(sockets.isListening()).isFalse();
    }

    /** With ssl_storage_port equal to storage_port the socket on storage_port is the one DSE dials: nothing is added. */
    @Test
    public void noExtraSocketWhenBothPortsAreEqual()
    {
        mode(InternodeCompatibilityMode.DSE_6);
        try (WithProperties properties = new WithProperties().set(SSL_STORAGE_PORT, DatabaseDescriptor.getStoragePort()))
        {
            assertThat(DatabaseDescriptor.getSSLStoragePort()).isEqualTo(DatabaseDescriptor.getStoragePort());
            ServerEncryptionOptions options = encryption(InternodeEncryption.all, false);
            assertThat(DseTlsPorts.listensOnSslStoragePortForDse(options)).isFalse();
            assertThat(boundPorts(options)).containsExactly(DatabaseDescriptor.getStoragePort());
        }
    }

    /** The extra socket is logged at INFO with its address and the reason; nothing is logged with NONE. */
    @Test
    public void theExtraSocketIsLogged()
    {
        Logger logger = (Logger) LoggerFactory.getLogger(InboundSockets.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            mode(InternodeCompatibilityMode.NONE);
            boundPorts(encryption(InternodeEncryption.all, false));
            assertThat(appender.list).noneMatch(e -> e.getFormattedMessage().startsWith(INFO_PREFIX));
            // with the legacy port setting the socket is the upstream one: nothing to explain
            mode(InternodeCompatibilityMode.DSE_6);
            boundPorts(encryption(InternodeEncryption.all, true));
            assertThat(appender.list).noneMatch(e -> e.getFormattedMessage().startsWith(INFO_PREFIX));

            mode(InternodeCompatibilityMode.DSE_6);
            boundPorts(encryption(InternodeEncryption.dc, false));
            List<ILoggingEvent> logged = appender.list.stream().filter(e -> e.getFormattedMessage().startsWith(INFO_PREFIX)).collect(Collectors.toList());
            assertThat(logged).hasSize(1);
            assertThat(logged.get(0).getLevel()).isEqualTo(Level.INFO);
            assertThat(logged.get(0).getFormattedMessage())
            .isEqualTo(INFO_PREFIX + FBUtilities.getLocalAddressAndPort().withPort(DatabaseDescriptor.getSSLStoragePort()) + " (ssl_storage_port) although " +
                       "legacy_ssl_storage_port_enabled is false, because DSE 6 nodes connect to this node on ssl_storage_port for the " +
                       "connections internode_encryption (dc) encrypts: DSE 6.8 and 6.9 before 6.9.7 always, DSE 6.9.7+ once it has " +
                       "stored a messaging version below DSE 6.9.7's for this node");
        }
        finally
        {
            logger.detachAppender(appender);
        }
    }

    /** DSE 6.9.7+ is dialled on storage_port, earlier DSE 6 releases on ssl_storage_port, an unknown one by the setting. */
    @Test
    public void thePortFollowsTheDseVersionThenTheSetting()
    {
        for (boolean legacyPort : new boolean[]{ false, true })
        {
            assertThat(DseTlsPorts.dialsSslStoragePort(DseCompat.DSE_697_VERSION, legacyPort)).isFalse();
            assertThat(DseTlsPorts.dialsSslStoragePort(7 << 8, legacyPort)).isFalse();
            for (int below : new int[]{ DseCompat.DSE_60_VERSION, DseCompat.DSE_603_VERSION, DseCompat.DSE_68_VERSION,
                                        DseCompat.DSE_681_VERSION, DseCompat.DSE_69_VERSION })
                assertThat(DseTlsPorts.dialsSslStoragePort(below, legacyPort)).as("version " + below).isTrue();
            assertThat(DseTlsPorts.dialsSslStoragePort(DseTlsPorts.UNKNOWN, legacyPort)).isEqualTo(legacyPort);
        }
    }

    /** The DSE messaging version a stored DSE 6 release maps to; any other release is unknown. */
    @Test
    public void theDseVersionOfAStoredRelease()
    {
        assertThat(versionOf(DseVersion.DSE_6_8_65.releaseVersion)).isEqualTo(DseVersion.DSE_6_8_65.relativeMessagingVersion << 8);
        assertThat(versionOf(DseVersion.DSE_6_9_26.releaseVersion)).isEqualTo(DseVersion.DSE_6_9_26.relativeMessagingVersion << 8);
        assertThat(versionOf("4.0.0.680")).isEqualTo(DseCompat.DSE_68_VERSION);
        assertThat(versionOf("4.0.0.681")).isEqualTo(DseCompat.DSE_681_VERSION);
        assertThat(versionOf("4.0.0.690")).isEqualTo(DseCompat.DSE_69_VERSION);
        assertThat(versionOf("4.0.0.696")).isEqualTo(DseCompat.DSE_69_VERSION);
        assertThat(versionOf("4.0.0.697")).isEqualTo(DseCompat.DSE_697_VERSION);
        assertThat(versionOf("4.0.0.6910")).isEqualTo(DseCompat.DSE_697_VERSION);
        assertThat(versionOf("4.0.0.6012")).isEqualTo(DseCompat.DSE_60_VERSION);
        assertThat(versionOf("4.0.0.678")).isEqualTo(DseCompat.DSE_60_VERSION);
        for (String notDse : new String[]{ "4.0.0", "4.0.0.42", "4.0.0.7012", "4.0.0.68650", "4.0.11.0", "5.0.7.0", "3.11.17" })
            assertThat(versionOf(notDse)).as(notDse).isEqualTo(DseTlsPorts.UNKNOWN);
        assertThat(DseTlsPorts.dseMessagingVersionOfRelease(null)).isEqualTo(DseTlsPorts.UNKNOWN);
    }

    private static int versionOf(String release)
    {
        return DseTlsPorts.dseMessagingVersionOfRelease(new CassandraVersion(release));
    }

    /**
     * The outbound port in DSE_6 mode: gossip's NET_VERSION first (a DSE one, or one below 256 that is no DSE node's),
     * then the stored release (no gossip state, or one without NET_VERSION, as right after a restart), then the setting;
     * never for an unencrypted connection or a peer at 4.0 or later, never with NONE.
     */
    @Test
    public void theOutboundPortOfADsePeer() throws Exception
    {
        int storage = DatabaseDescriptor.getStoragePort();
        int ssl = DatabaseDescriptor.getSSLStoragePort();
        InetAddressAndPort dse697Gossiped = InetAddressAndPort.getByName("127.0.0.231:" + storage);
        InetAddressAndPort dse68Gossiped = InetAddressAndPort.getByName("127.0.0.232:" + storage);
        InetAddressAndPort dse69Stored = InetAddressAndPort.getByName("127.0.0.233:" + storage);
        InetAddressAndPort dse68Stored = InetAddressAndPort.getByName("127.0.0.234:" + storage);
        InetAddressAndPort upgradedGossiped = InetAddressAndPort.getByName("127.0.0.235:" + storage);
        InetAddressAndPort unknown = InetAddressAndPort.getByName("127.0.0.236:" + storage);
        InetAddressAndPort dse68SavedState = InetAddressAndPort.getByName("127.0.0.237:" + storage);
        InetAddressAndPort hcd12AtTheDialect = InetAddressAndPort.getByName("127.0.0.238:" + storage);
        try
        {
            Gossiper.instance.initializeNodeUnsafe(dse697Gossiped, UUID.randomUUID(), DseCompat.DSE_697_VERSION, 1);
            // a saved endpoint after a restart: a gossip state with no NET_VERSION yet
            Gossiper.instance.addSavedEndpoint(dse68SavedState);
            // HCD 1.2 gossips its own current version, 101 by default, whatever version it speaks with this node: no
            // DSE node, whatever a stale stored release says
            Gossiper.instance.initializeNodeUnsafe(hcd12AtTheDialect, UUID.randomUUID(), VERSION_DS_11, 1);
            Nodes.peers().update(dse68SavedState, p -> p.setReleaseVersion(new CassandraVersion(DseVersion.DSE_6_8_65.releaseVersion)), true);
            Nodes.peers().update(hcd12AtTheDialect, p -> p.setReleaseVersion(new CassandraVersion(DseVersion.DSE_6_8_65.releaseVersion)), true);
            Gossiper.instance.initializeNodeUnsafe(dse68Gossiped, UUID.randomUUID(), DseCompat.DSE_681_VERSION, 1);
            Gossiper.instance.initializeNodeUnsafe(upgradedGossiped, UUID.randomUUID(), VERSION_40, 1);
            Nodes.peers().update(dse69Stored, p -> p.setReleaseVersion(new CassandraVersion(DseVersion.DSE_6_9_26.releaseVersion)), true);
            Nodes.peers().update(dse68Stored, p -> p.setReleaseVersion(new CassandraVersion(DseVersion.DSE_6_8_65.releaseVersion)), true);
            // gossip has the last word: a stale stored release of an upgraded node is not read
            Nodes.peers().update(upgradedGossiped, p -> p.setReleaseVersion(new CassandraVersion(DseVersion.DSE_6_8_65.releaseVersion)), true);

            mode(InternodeCompatibilityMode.DSE_6);
            assertThat(DseTlsPorts.dseMessagingVersion(dse697Gossiped)).isEqualTo(DseCompat.DSE_697_VERSION);
            assertThat(DseTlsPorts.dseMessagingVersion(dse68Gossiped)).isEqualTo(DseCompat.DSE_681_VERSION);
            assertThat(DseTlsPorts.dseMessagingVersion(dse69Stored)).isEqualTo(DseCompat.DSE_697_VERSION);
            assertThat(DseTlsPorts.dseMessagingVersion(dse68Stored)).isEqualTo(DseCompat.DSE_681_VERSION);
            assertThat(DseTlsPorts.dseMessagingVersion(upgradedGossiped)).isEqualTo(DseTlsPorts.UNKNOWN);
            assertThat(DseTlsPorts.dseMessagingVersion(unknown)).isEqualTo(DseTlsPorts.UNKNOWN);
            assertThat(Gossiper.instance.getEndpointStateForEndpoint(dse68SavedState)).isNotNull();
            assertThat(DseTlsPorts.dseMessagingVersion(dse68SavedState)).isEqualTo(DseCompat.DSE_681_VERSION);
            assertThat(DseTlsPorts.dseMessagingVersion(hcd12AtTheDialect)).isEqualTo(DseTlsPorts.UNKNOWN);

            for (boolean legacyPort : new boolean[]{ false, true })
            {
                ServerEncryptionOptions options = encryption(InternodeEncryption.all, legacyPort);
                assertThat(port(dse697Gossiped, options, DSE_DIALECT)).isEqualTo(storage);
                assertThat(port(dse69Stored, options, DSE_DIALECT)).isEqualTo(storage);
                assertThat(port(dse68Gossiped, options, DSE_DIALECT)).isEqualTo(ssl);
                assertThat(port(dse68Stored, options, DSE_DIALECT)).isEqualTo(ssl);
                assertThat(port(upgradedGossiped, options, DSE_DIALECT)).isEqualTo(legacyPort ? ssl : storage);
                assertThat(port(unknown, options, DSE_DIALECT)).isEqualTo(legacyPort ? ssl : storage);
                assertThat(port(dse68SavedState, options, DSE_DIALECT)).isEqualTo(ssl);
                assertThat(port(hcd12AtTheDialect, options, DSE_DIALECT)).isEqualTo(legacyPort ? ssl : storage);
                // a peer at 4.0 or later is always dialled on storage_port
                assertThat(port(dse68Gossiped, options, current_version)).isEqualTo(storage);
                assertThat(port(dse68Gossiped, options, VERSION_40)).isEqualTo(storage);
            }
            // an unencrypted connection (internode_encryption none, or dc/rack within the data center: see
            // theOutboundPortFollowsTheEncryptionOfTheDataCenter) is left alone
            assertThat(port(dse68Gossiped, null, DSE_DIALECT)).isEqualTo(storage);

            mode(InternodeCompatibilityMode.NONE);
            for (boolean legacyPort : new boolean[]{ false, true })
            {
                OutboundConnectionSettings settings = settings(dse68Gossiped, encryption(InternodeEncryption.all, legacyPort));
                assertThat(settings.withLegacyPortIfNecessary(DSE_DIALECT)).isSameAs(settings);
                // no pre-4.0 port choice upstream, whatever the peer's DSE version
                assertThat(DseTlsPorts.dialsSslStoragePort(dse68Gossiped, legacyPort)).isFalse();
                assertThat(DseTlsPorts.dialsSslStoragePort(unknown, legacyPort)).isFalse();
            }
        }
        finally
        {
            for (InetAddressAndPort peer : Set.of(dse697Gossiped, dse68Gossiped, upgradedGossiped, dse68SavedState, hcd12AtTheDialect))
                Gossiper.instance.unsafeAnnulEndpoint(peer);
            for (InetAddressAndPort peer : Set.of(dse69Stored, dse68Stored, upgradedGossiped, dse68SavedState, hcd12AtTheDialect))
                Nodes.peers().remove(peer, true, true);
        }
    }

    /**
     * With internode_encryption dc, through the defaults of an outbound connection (the node's encryption options, which
     * the snitch applies or not): a DSE 6.8 peer in the local data center is dialled without encryption on
     * storage_port, one in another data center with encryption on ssl_storage_port.
     */
    @Test
    public void theOutboundPortFollowsTheEncryptionOfTheDataCenter() throws Exception
    {
        int storage = DatabaseDescriptor.getStoragePort();
        int ssl = DatabaseDescriptor.getSSLStoragePort();
        InetAddressAndPort sameDc = InetAddressAndPort.getByName("127.0.0.241:" + storage);
        InetAddressAndPort otherDc = InetAddressAndPort.getByName("127.0.0.242:" + storage);
        IEndpointSnitch snitchBefore = DatabaseDescriptor.getEndpointSnitch();
        ServerEncryptionOptions encryptionBefore = DatabaseDescriptor.getInternodeMessagingEncyptionOptions();
        try
        {
            DatabaseDescriptor.setEndpointSnitch(new AbstractNetworkTopologySnitch()
            {
                @Override
                public String getRack(InetAddressAndPort endpoint)
                {
                    return "rack1";
                }

                @Override
                public String getDatacenter(InetAddressAndPort endpoint)
                {
                    return endpoint.equals(otherDc) ? "dc2" : "dc1";
                }
            });
            DatabaseDescriptor.setInternodeMessagingEncyptionOptions(encryption(InternodeEncryption.dc, false));
            for (InetAddressAndPort peer : List.of(sameDc, otherDc))
                Nodes.peers().update(peer, p -> p.setReleaseVersion(new CassandraVersion(DseVersion.DSE_6_8_65.releaseVersion)), true);
            mode(InternodeCompatibilityMode.DSE_6);

            OutboundConnectionSettings local = new OutboundConnectionSettings(sameDc).withConnectTo(sameDc).withDefaults(ConnectionCategory.MESSAGING);
            assertThat(local.encryption).isNull();
            assertThat(local.withLegacyPortIfNecessary(DSE_DIALECT).connectTo().getPort()).isEqualTo(storage);

            OutboundConnectionSettings remote = new OutboundConnectionSettings(otherDc).withConnectTo(otherDc).withDefaults(ConnectionCategory.MESSAGING);
            assertThat(remote.encryption).isNotNull();
            assertThat(remote.withLegacyPortIfNecessary(DSE_DIALECT).connectTo().getPort()).isEqualTo(ssl);
        }
        finally
        {
            DatabaseDescriptor.setEndpointSnitch(snitchBefore);
            DatabaseDescriptor.setInternodeMessagingEncyptionOptions(encryptionBefore);
            for (InetAddressAndPort peer : List.of(sameDc, otherDc))
                Nodes.peers().remove(peer, true, true);
        }
    }

    private static OutboundConnectionSettings settings(InetAddressAndPort peer, ServerEncryptionOptions encryption)
    {
        OutboundConnectionSettings settings = new OutboundConnectionSettings(peer).withConnectTo(peer);
        return encryption == null ? settings : settings.withEncryption(encryption);
    }

    private static int port(InetAddressAndPort peer, ServerEncryptionOptions encryption, int messagingVersion)
    {
        return settings(peer, encryption).withLegacyPortIfNecessary(messagingVersion).connectTo().getPort();
    }
}
