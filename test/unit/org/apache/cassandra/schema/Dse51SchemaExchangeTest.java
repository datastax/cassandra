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

package org.apache.cassandra.schema;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.net.DsePeers;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.DsePeersRecord;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The schema exchange of {@link DefaultSchemaUpdateHandler} with DSE 5.1 nodes in the DSE 5.1 compatibility mode. A DSE
 * 5.1.51 node stores for this node the messaging version it last learnt, the gossiped {@code NET_VERSION} or the 11 of
 * a handshake, so it may push its schema changes to this node and pull its schema at any time; this node stores 11 for
 * the DSE 5.1 node, from the handshake or from its gossiped {@code NET_VERSION}. Either way the push is ignored and
 * counted, with its WARN, and the pull answered with no schema, counted, with its INFO; a node of this software at this
 * node's version is served as before. Each test pushes a keyspace of its own name.
 */
public class Dse51SchemaExchangeTest extends CQLTester
{
    private InternodeCompatibilityMode modeBefore;
    private InetAddressAndPort handshakePeer;
    private InetAddressAndPort gossipPeer;
    private InetAddressAndPort upgradedPeer;
    private String pushedKeyspace;

    @Before
    public void saveMode() throws Exception
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        handshakePeer = InetAddressAndPort.getByName("127.0.0.230");
        gossipPeer = InetAddressAndPort.getByName("127.0.0.231");
        upgradedPeer = InetAddressAndPort.getByName("127.0.0.232");
    }

    /** As in {@code Dse6SchemaExchangeTest}: each call of the rate-limited logger sees twenty minutes pass. */
    @BeforeClass
    public static void advanceTheRateLimitClockAtEachCall()
    {
        AtomicLong now = new AtomicLong(Clock.Global.nanoTime());
        NoSpamLogger.unsafeSetClock(() -> now.addAndGet(TimeUnit.MINUTES.toNanos(20)));
    }

    @AfterClass
    public static void restoreTheRateLimitClock()
    {
        NoSpamLogger.unsafeSetClock(Clock.Global::nanoTime);
    }

    @After
    public void forgetPeers() throws Throwable
    {
        for (InetAddressAndPort peer : List.of(handshakePeer, gossipPeer, upgradedPeer))
        {
            MessagingService.instance().versions.reset(peer);
            DsePeers.forget(peer);
        }
        DsePeersRecord.awaitPersisted();
        assertThat(DsePeersRecord.peers()).isEmpty();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        if (pushedKeyspace != null)
            schemaChange("DROP KEYSPACE IF EXISTS " + pushedKeyspace);
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    @Test
    public void pushesOfDse51NodesIgnoredInDse51Mode() throws Throwable
    {
        setModeAndPeers();
        pushedKeyspace = "pushed_in_dse51_mode";
        long before = DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PUSH_IGNORED);
        for (InetAddressAndPort dse51 : List.of(handshakePeer, gossipPeer))
        {
            withLogAppender(DefaultSchemaUpdateHandler.class, appender -> {
                push(dse51, pushedKeyspace);
                assertThat(messages(appender)).contains("Ignoring the schema pushes of peers that do not speak this " +
                                                        "node's messaging version, DSE 5.1 nodes in the first " +
                                                        "place (for example " + dse51 + "): in " +
                                                        "internode_compatibility_mode DSE_5_1 the schema is frozen, " +
                                                        "and a schema change made on a DSE 5.1 node during the " +
                                                        "upgrade does not reach this node");
            });
            assertThat(Schema.instance.getKeyspaceMetadata(pushedKeyspace)).isNull();
        }
        assertThat(DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PUSH_IGNORED)).isEqualTo(before + 2);

        // another node of this software still pushes its schema changes here
        push(upgradedPeer, pushedKeyspace);
        assertThat(Schema.instance.getKeyspaceMetadata(pushedKeyspace)).isNotNull();
        assertThat(DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PUSH_IGNORED)).isEqualTo(before + 2);
    }

    @Test
    public void pullsOfDse51NodesAnsweredWithNoSchemaInDse51Mode() throws Throwable
    {
        setModeAndPeers();
        long before = DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PULL_WITHOUT_SCHEMA);
        for (InetAddressAndPort dse51 : List.of(handshakePeer, gossipPeer))
        {
            withLogAppender(DefaultSchemaUpdateHandler.class, appender -> {
                assertThat(handler().schemaMutationsForPull(dse51)).isEmpty();
                assertThat(messages(appender)).contains("Answering with no schema the schema pulls of peers that do " +
                                                        "not speak this node's messaging version, DSE 5.1 nodes in " +
                                                        "the first place (for example " + dse51 + "): in " +
                                                        "internode_compatibility_mode DSE_5_1 the schema is frozen " +
                                                        "and this node's schema must not reach the DSE 5.1 nodes");
            });
        }
        String reason = DefaultSchemaUpdateHandler.SCHEMA_PULL_WITHOUT_SCHEMA;
        assertThat(DseCompatRefusals.count(reason)).isEqualTo(before + 2);
        assertThat(handler().schemaMutationsForPull(upgradedPeer)).isNotEmpty();
        assertThat(DseCompatRefusals.count(reason)).isEqualTo(before + 2);
    }

    /** What the registered pull handler sends: an empty answer to a DSE 5.1 node, the schema to the upgraded peer. */
    @Test
    public void thePullHandlerSendsAnEmptyAnswerInDse51Mode() throws Exception
    {
        setModeAndPeers();
        Map<InetAddressAndPort, Message<?>> sent = new ConcurrentHashMap<>();
        BiPredicate<Message<?>, InetAddressAndPort> capture = (message, to) -> {
            sent.put(to, message);
            return false;
        };
        MessagingService.instance().outboundSink.add(capture);
        try
        {
            for (InetAddressAndPort peer : List.of(handshakePeer, gossipPeer, upgradedPeer))
                SchemaPullVerbHandler.instance.doVerb(Message.builder(Verb.SCHEMA_PULL_REQ, NoPayload.noPayload)
                                                             .from(peer).withId(1).build());
        }
        finally
        {
            MessagingService.instance().outboundSink.remove(capture);
        }
        for (InetAddressAndPort dse51 : List.of(handshakePeer, gossipPeer))
        {
            assertThat(sent.get(dse51).verb()).isEqualTo(Verb.SCHEMA_PULL_RSP);
            assertThat((Collection<?>) sent.get(dse51).payload).isEmpty();
        }
        assertThat(sent.get(upgradedPeer).verb()).isEqualTo(Verb.SCHEMA_PULL_RSP);
        assertThat((Collection<?>) sent.get(upgradedPeer).payload).isNotEmpty();
    }

    /** This node neither pushes its schema to a DSE 5.1 node nor exchanges it otherwise with one. */
    @Test
    public void thisNodeExchangesNoSchemaWithDse51Nodes() throws Exception
    {
        setModeAndPeers();
        MigrationCoordinator coordinator = handler().migrationCoordinator;
        assertThat(coordinator.exchangesSchemaWith(handshakePeer)).isFalse();
        assertThat(coordinator.exchangesSchemaWith(gossipPeer)).isFalse();
        assertThat(coordinator.exchangesSchemaWith(upgradedPeer)).isTrue();
    }

    /**
     * Sets the mode, then records the peers: a DSE 5.1 node at 11 from a handshake (the version map, as the inbound
     * handshake records it), another from its gossiped {@code NET_VERSION} 11, which also identifies it, and a node of
     * this software at this node's version. Both DSE 5.1 nodes enter the record of DSE 5.1 peers.
     */
    private void setModeAndPeers() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        MessagingService.instance().versions.set(handshakePeer, MessagingService.VERSION_3014);
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.NET_VERSION, VersionedValue.unsafeMakeVersionedValue("11", 1));
        EndpointState state = new EndpointState(new HeartBeatState(1, 1), states);
        state.markDead();
        StorageService.instance.onRestart(gossipPeer, state);
        MessagingService.instance().versions.set(upgradedPeer, MessagingService.current_version);
        assertThat(MessagingService.instance().versions.getRaw(gossipPeer)).isEqualTo(MessagingService.VERSION_3014);
        assertThat(DsePeers.isDse(gossipPeer)).isTrue();
        DsePeersRecord.awaitPersisted();
        assertThat(DsePeersRecord.peers()).containsExactlyInAnyOrder(handshakePeer, gossipPeer);
    }

    private static List<String> messages(ListAppender<ILoggingEvent> appender)
    {
        return appender.list.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList());
    }

    private static DefaultSchemaUpdateHandler handler()
    {
        return (DefaultSchemaUpdateHandler) Schema.instance.updateHandler;
    }

    /** Hands this node a schema push from the peer that creates the keyspace. */
    private static void push(InetAddressAndPort from, String keyspace)
    {
        KeyspaceMetadata metadata = KeyspaceMetadata.create(keyspace, KeyspaceParams.simple(1));
        Mutation create = SchemaKeyspace.makeCreateKeyspaceMutation(metadata, FBUtilities.timestampMicros()).build();
        Collection<Mutation> mutations = Collections.singletonList(create);
        SchemaPushVerbHandler.instance.doVerb(Message.builder(Verb.SCHEMA_PUSH_REQ, mutations).from(from).build());
    }
}
