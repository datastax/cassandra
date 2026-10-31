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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.net.DsePeers;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.DsePeersRecord;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The schema exchange of {@link DefaultSchemaUpdateHandler} with a peer speaking the DSE dialect (version 11) and with a
 * node of this software: in the DSE 6 compatibility mode the DSE peer's pulls are answered with no schema and its pushes
 * are ignored, each counted, and the pulls of any other peer this node does not exchange its schema with are answered
 * with a failure; without the mode both are served as before. Each test pushes a keyspace of its own name.
 */
public class Dse6SchemaExchangeTest extends CQLTester
{
    private InternodeCompatibilityMode modeBefore;
    private InetAddressAndPort dsePeer;
    private InetAddressAndPort upgradedPeer;
    private String pushedKeyspace;

    @Before
    public void saveMode() throws Exception
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        dsePeer = InetAddressAndPort.getByName("127.0.0.250");
        upgradedPeer = InetAddressAndPort.getByName("127.0.0.251");
    }

    /**
     * The lines this class asserts are logged at most once every ten minutes, and several tests log them: one clock for
     * the whole class, which only moves forward, lets each call see twenty minutes pass. The statements logged under it
     * stay silent once the real clock is back, for the rest of this JVM, which runs this test class only.
     */
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
        // in the mode of the test, so that the peer at 11 leaves the record of DSE 6 peers it entered in DSE_6 mode
        MessagingService.instance().versions.reset(dsePeer);
        MessagingService.instance().versions.reset(upgradedPeer);
        DsePeersRecord.awaitPersisted();
        assertThat(DsePeersRecord.peers()).isEmpty();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        if (pushedKeyspace != null)
            schemaChange("DROP KEYSPACE IF EXISTS " + pushedKeyspace);
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    @Test
    public void pullsAnsweredWithTheSchemaWithNone()
    {
        setModeAndPeers(InternodeCompatibilityMode.NONE);
        long before = DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PULL_WITHOUT_SCHEMA);
        assertThat(handler().schemaMutationsForPull(dsePeer)).isNotEmpty();
        assertThat(handler().schemaMutationsForPull(upgradedPeer)).isNotEmpty();
        assertThat(DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PULL_WITHOUT_SCHEMA)).isEqualTo(before);
    }

    @Test
    public void pullsOfTheDsePeerAnsweredWithNoSchemaInDse6Mode() throws Throwable
    {
        setModeAndPeers(InternodeCompatibilityMode.DSE_6);
        long before = DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PULL_WITHOUT_SCHEMA);
        withLogAppender(DefaultSchemaUpdateHandler.class, appender -> {
            assertThat(handler().schemaMutationsForPull(dsePeer)).isEmpty();
            assertThat(messages(appender)).contains("Answering with no schema the schema pulls of peers that do not speak this node's " +
                                                    "messaging version, DSE 6 nodes in the first place (for example " + dsePeer + "): in " +
                                                    "internode_compatibility_mode DSE_6 this node's schema must not reach the DSE 6 nodes");
        });
        assertThat(DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PULL_WITHOUT_SCHEMA)).isEqualTo(before + 1);
        assertThat(handler().schemaMutationsForPull(upgradedPeer)).isNotEmpty();
        assertThat(DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PULL_WITHOUT_SCHEMA)).isEqualTo(before + 1);
    }

    /**
     * A peer this node does not exchange its schema with but that is not known as a DSE 6 node (its messaging version
     * unknown) gets no answer to take as its schema: the pull is answered with a failure, so that it retries.
     */
    @Test
    public void pullsOfAPeerOfUnknownVersionFailInDse6Mode() throws Throwable
    {
        setModeAndPeers(InternodeCompatibilityMode.DSE_6);
        InetAddressAndPort unknownPeer = InetAddressAndPort.getByName("127.0.0.252");
        long before = DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PULL_WITHOUT_SCHEMA);
        withLogAppender(DefaultSchemaUpdateHandler.class, appender -> {
            assertThat(handler().schemaMutationsForPull(unknownPeer)).isNull();
            assertThat(messages(appender)).contains("Answering with a failure the schema pulls of peers whose messaging version is neither " +
                                                    "this node's nor DSE 6's (for example " + unknownPeer + "): in internode_compatibility_mode " +
                                                    "DSE_6 this node's schema is sent only to peers speaking its messaging version; the peer retries");
        });
        assertThat(DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PULL_WITHOUT_SCHEMA)).isEqualTo(before);
    }

    /**
     * The other cases of the rule in the mode: a peer known at a 4.0 or later version other than this node's gets the
     * failure too, a peer identified as a DSE 6 node gets no schema even while its version is not known.
     */
    @Test
    public void pullsOfAPeerAtAnotherVersionFailAndOfAnIdentifiedDseNodeGetNoSchemaInDse6Mode() throws Exception
    {
        setModeAndPeers(InternodeCompatibilityMode.DSE_6);
        InetAddressAndPort otherVersionPeer = InetAddressAndPort.getByName("127.0.0.253");
        InetAddressAndPort identifiedDsePeer = InetAddressAndPort.getByName("127.0.0.254");
        try
        {
            MessagingService.instance().versions.set(otherVersionPeer, MessagingService.VERSION_40);
            DsePeers.identify(identifiedDsePeer, "a test");
            long before = DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PULL_WITHOUT_SCHEMA);
            assertThat(handler().schemaMutationsForPull(otherVersionPeer)).isNull();
            assertThat(handler().schemaMutationsForPull(identifiedDsePeer)).isEmpty();
            assertThat(DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PULL_WITHOUT_SCHEMA)).isEqualTo(before + 1);
        }
        finally
        {
            MessagingService.instance().versions.reset(otherVersionPeer);
            DsePeers.forget(identifiedDsePeer);
        }
    }

    /**
     * What the registered pull handler sends in the mode: an empty answer to the DSE 6 peer, a failure to a peer whose
     * version is not known, the schema to the upgraded peer.
     */
    @Test
    public void thePullHandlerSendsAnEmptyAnswerOrAFailureInDse6Mode() throws Exception
    {
        setModeAndPeers(InternodeCompatibilityMode.DSE_6);
        InetAddressAndPort unknownPeer = InetAddressAndPort.getByName("127.0.0.252");
        Map<InetAddressAndPort, Message<?>> sent = new ConcurrentHashMap<>();
        BiPredicate<Message<?>, InetAddressAndPort> capture = (message, to) -> {
            sent.put(to, message);
            return false;
        };
        MessagingService.instance().outboundSink.add(capture);
        try
        {
            for (InetAddressAndPort peer : Arrays.asList(dsePeer, unknownPeer, upgradedPeer))
                SchemaPullVerbHandler.instance.doVerb(Message.builder(Verb.SCHEMA_PULL_REQ, NoPayload.noPayload).from(peer).withId(1).build());
        }
        finally
        {
            MessagingService.instance().outboundSink.remove(capture);
        }
        assertThat(sent.get(dsePeer).verb()).isEqualTo(Verb.SCHEMA_PULL_RSP);
        assertThat((Collection<?>) sent.get(dsePeer).payload).isEmpty();
        assertThat(sent.get(unknownPeer).verb()).isEqualTo(Verb.FAILURE_RSP);
        assertThat(sent.get(upgradedPeer).verb()).isEqualTo(Verb.SCHEMA_PULL_RSP);
        assertThat((Collection<?>) sent.get(upgradedPeer).payload).isNotEmpty();
    }

    @Test
    public void pushesOfTheDsePeerIgnoredInDse6Mode() throws Throwable
    {
        setModeAndPeers(InternodeCompatibilityMode.DSE_6);
        pushedKeyspace = "pushed_in_dse6_mode";
        long before = DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PUSH_IGNORED);
        withLogAppender(DefaultSchemaUpdateHandler.class, appender -> {
            push(dsePeer, pushedKeyspace);
            assertThat(messages(appender)).contains("Ignoring the schema pushes of peers that do not speak this node's messaging version, DSE 6 " +
                                                    "nodes in the first place (for example " + dsePeer + "): in internode_compatibility_mode DSE_6 " +
                                                    "a schema change made on a DSE 6 node during the upgrade does not reach this node");
        });
        assertThat(Schema.instance.getKeyspaceMetadata(pushedKeyspace)).isNull();
        assertThat(DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PUSH_IGNORED)).isEqualTo(before + 1);

        // another node of this software still pushes its schema changes here
        push(upgradedPeer, pushedKeyspace);
        assertThat(Schema.instance.getKeyspaceMetadata(pushedKeyspace)).isNotNull();
        assertThat(DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PUSH_IGNORED)).isEqualTo(before + 1);
    }

    @Test
    public void pushesAppliedWithNone()
    {
        setModeAndPeers(InternodeCompatibilityMode.NONE);
        pushedKeyspace = "pushed_with_none";
        long before = DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PUSH_IGNORED);
        push(dsePeer, pushedKeyspace);
        assertThat(Schema.instance.getKeyspaceMetadata(pushedKeyspace)).isNotNull();
        assertThat(DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PUSH_IGNORED)).isEqualTo(before);
    }

    /** Sets the mode, then records the two peers' messaging versions; in DSE_6 mode the peer at 11 enters the record of DSE 6 peers. */
    private void setModeAndPeers(InternodeCompatibilityMode mode)
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(mode);
        MessagingService.instance().versions.set(dsePeer, MessagingService.VERSION_3014);
        MessagingService.instance().versions.set(upgradedPeer, MessagingService.current_version);
        if (mode.isDse6())
            assertThat(DsePeersRecord.peers()).containsExactly(dsePeer);
        else
            assertThat(DsePeersRecord.peers()).isEmpty();
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
        Mutation create = SchemaKeyspace.makeCreateKeyspaceMutation(KeyspaceMetadata.create(keyspace, KeyspaceParams.simple(1)),
                                                                    FBUtilities.timestampMicros()).build();
        Collection<Mutation> mutations = Collections.singletonList(create);
        SchemaPushVerbHandler.instance.doVerb(Message.builder(Verb.SCHEMA_PUSH_REQ, mutations).from(from).build());
    }
}
