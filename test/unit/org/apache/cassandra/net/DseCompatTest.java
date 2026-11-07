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
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.Dse51Fixtures;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.dsecompat.DseFixtures.GossipState;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.JsonUtils;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The pure functions of {@link DseCompat}: DSE 6's release and messaging versions, the gossiped DSE version, and the
 * gate of the pre-4.0 layout; DSE 5.1's releases and the rule that tells a DSE 5.1 peer.
 */
public class DseCompatTest
{
    private InternodeCompatibilityMode modeBefore;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.clientInitialization();
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

    /** A DSE messaging version in a handshake is {@code relative << 8}, relative 1 to 255; nothing else is one. */
    @Test
    public void dseHandshakeVersionsAreTheRelativeVersionsShiftedBy8()
    {
        for (int version : new int[]{ 256, 1024, 1536, 65280 })
            assertThat(DseCompat.isDseHandshakeVersion(version)).as(Integer.toString(version)).isTrue();
        for (int version : new int[]{ 0, 11, 255, 257, 300, 1025, 65281, 65536, Integer.MAX_VALUE, -256, Integer.MIN_VALUE })
            assertThat(DseCompat.isDseHandshakeVersion(version)).as(Integer.toString(version)).isFalse();
    }

    @Test
    public void dse6ReleaseVersions()
    {
        for (DseVersion version : DseVersion.values())
            assertThat(DseCompat.isDse6ReleaseVersion(new CassandraVersion(version.releaseVersion))).as(version.name()).isTrue();
        // what DSE 6.0.12, 6.7.8 and 6.8.55 report
        for (String dse : new String[]{ "4.0.0.6012", "4.0.0.678", "4.0.0.6855" })
            assertThat(DseCompat.isDse6ReleaseVersion(new CassandraVersion(dse))).as(dse).isTrue();
        // Apache Cassandra 4.0.0, HCD 1.2, a 4.0 patch release, this software
        for (String other : new String[]{ "4.0.0", "4.0.11.0", "4.0.1", "4.1.0", "5.0.7.0", "5.0.7" })
            assertThat(DseCompat.isDse6ReleaseVersion(new CassandraVersion(other))).as(other).isFalse();
        assertThat(DseCompat.isDse6ReleaseVersion(null)).isFalse();
    }

    @Test
    public void dse6MinorAndPatchAreReadFromTheBuildNumber()
    {
        assertThat(DseCompat.dse6MinorAndPatch(new CassandraVersion(DseVersion.DSE_6_8_65.releaseVersion))).containsExactly(8, 65);
        assertThat(DseCompat.dse6MinorAndPatch(new CassandraVersion(DseVersion.DSE_6_9_26.releaseVersion))).containsExactly(9, 26);
        assertThat(DseCompat.dse6MinorAndPatch(new CassandraVersion("4.0.0.697"))).containsExactly(9, 7);
        assertThat(DseCompat.dse6MinorAndPatch(new CassandraVersion("4.0.0.6012"))).containsExactly(0, 12);
        // a build number of another length or not starting with 6, and releases that are not DSE 6's
        for (String other : new String[]{ "4.0.0.69", "4.0.0.69261", "4.0.0.5926", "4.0.11.0", "5.0.7.0" })
            assertThat(DseCompat.dse6MinorAndPatch(new CassandraVersion(other))).as(other).isNull();
        assertThat(DseCompat.dse6MinorAndPatch(null)).isNull();
    }

    @Test
    public void storedVersionsOfDseAreDecodedAtTheirWritesLayout()
    {
        // this software's versions are returned as they are
        assertThat(DseCompat.storedDecodingVersion(MessagingService.VERSION_40)).isEqualTo(MessagingService.VERSION_40);
        assertThat(DseCompat.storedDecodingVersion(MessagingService.VERSION_DSE_68)).isEqualTo(MessagingService.VERSION_DSE_68);
        assertThat(DseCompat.storedDecodingVersion(DseCompat.DSE_60_VERSION - 1)).isEqualTo(DseCompat.DSE_60_VERSION - 1);
        // DSE 6.0 and 6.0.3 write the 3.0 layout, DSE 6.8 and later DSE_68's
        assertThat(DseCompat.storedDecodingVersion(DseCompat.DSE_60_VERSION)).isEqualTo(MessagingService.VERSION_40);
        assertThat(DseCompat.storedDecodingVersion(DseCompat.DSE_603_VERSION)).isEqualTo(MessagingService.VERSION_40);
        for (int dse68OrLater : new int[]{ DseCompat.DSE_68_VERSION, DseCompat.DSE_681_VERSION, DseCompat.DSE_69_VERSION, DseCompat.DSE_697_VERSION })
        {
            assertThat(DseCompat.storedDecodingVersion(dse68OrLater)).as("%d", dse68OrLater).isEqualTo(MessagingService.VERSION_DSE_68);
            assertThat(DseCompat.isUnknownStoredDseVersion(dse68OrLater)).as("%d", dse68OrLater).isFalse();
        }
        // what DSE 6.8.65 and 6.9.26 store: their relative version, the byte of their first handshake message, shifted
        assertThat(DseCompat.DSE_681_VERSION).isEqualTo(DseVersion.DSE_6_8_65.relativeMessagingVersion << 8).isEqualTo(1024);
        assertThat(DseCompat.DSE_697_VERSION).isEqualTo(DseVersion.DSE_6_9_26.relativeMessagingVersion << 8).isEqualTo(1536);

        // a value in DSE's range that is none of DSE 6's versions is never decoded as this software's own layout
        for (int unknown : new int[]{ DseCompat.DSE_60_VERSION + 1, 7 << 8, Integer.MAX_VALUE })
        {
            assertThat(DseCompat.isUnknownStoredDseVersion(unknown)).as("%d", unknown).isTrue();
            assertThatThrownBy(() -> DseCompat.storedDecodingVersion(unknown)).isInstanceOf(IllegalArgumentException.class)
                                                                                .hasMessage("Not a DSE 6 messaging version: " + unknown);
        }
        assertThat(DseCompat.isUnknownStoredDseVersion(MessagingService.VERSION_40)).isFalse();
        assertThat(DseCompat.DSE_STORED_VERSIONS_TEXT).isEqualTo("256, 512, 768, 1024, 1280 or 1536");
    }

    @Test
    public void handshakeAndGossipValuesOfDse()
    {
        // DSE's first message to a peer it does not know: 0xFF, no minimum, its relative version as the maximum
        assertThat(DseCompat.isDseFlaggedInitiate(DseCompat.DSE_VERSION_BYTE, 0, DseVersion.DSE_6_9_26.relativeMessagingVersion)).isTrue();
        assertThat(DseCompat.isDseFlaggedInitiate(DseCompat.DSE_VERSION_BYTE, 0, 0)).isFalse();
        assertThat(DseCompat.isDseFlaggedInitiate(MessagingService.VERSION_3014, 0, DseVersion.DSE_6_9_26.relativeMessagingVersion)).isFalse();
        // a plain pre-4.0 first message has no accepted range, a 4.0-style one always has
        assertThat(DseCompat.isPre40Initiate(MessagingService.VERSION_3014, 0, 0)).isTrue();
        assertThat(DseCompat.isPre40Initiate(MessagingService.VERSION_40, 0, 0)).isTrue();
        assertThat(DseCompat.isPre40Initiate(MessagingService.VERSION_40, MessagingService.VERSION_40, MessagingService.current_version)).isFalse();
        // a DSE messaging version, from the handshake or from gossip, maps to the dialect; other values stay as they are
        for (DseVersion version : DseVersion.values())
        {
            int dseVersion = version.relativeMessagingVersion << 8;
            assertThat(DseCompat.dialectFor(dseVersion)).as(version.name()).isEqualTo(DseCompat.DSE_DIALECT);
            assertThat(DseCompat.messagingVersionFromGossip(dseVersion)).as(version.name()).isEqualTo(DseCompat.DSE_DIALECT);
        }
        assertThat(DseCompat.messagingVersionFromGossip(MessagingService.VERSION_40)).isEqualTo(MessagingService.VERSION_40);
        assertThat(DseCompat.dialectFor(MessagingService.VERSION_3014)).isEqualTo(MessagingService.VERSION_3014);
        assertThat(DseCompat.DSE_DIALECT).isEqualTo(MessagingService.VERSION_3014);
    }

    @Test
    public void thePre40LayoutOnlyInDse6Mode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        assertThat(DseCompat.enabled()).isFalse();
        assertThat(DseCompat.isPre40Layout(MessagingService.VERSION_3014)).isFalse();
        assertThatThrownBy(() -> DseCompat.checkPre40Layout(MessagingService.VERSION_3014)).isInstanceOf(AssertionError.class)
                                                                                         .hasMessage("pre-4.0 serialization version 11");

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertThat(DseCompat.enabled()).isTrue();
        assertThat(DseCompat.isPre40Layout(MessagingService.VERSION_3014)).isTrue();
        assertThat(DseCompat.isPre40Layout(MessagingService.VERSION_40)).isFalse();
        DseCompat.checkPre40Layout(MessagingService.VERSION_3014);
    }

    @Test
    public void gossipedDseVersion()
    {
        try (WithProperties unset = new WithProperties().clear(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION))
        {
            assertThat(DseCompat.gossipedDseVersion()).isEqualTo("6.8.1");
        }
        try (WithProperties v680 = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION, "6.8.0"))
        {
            assertThat(DseCompat.gossipedDseVersion()).isEqualTo("6.8.0");
        }
        try (WithProperties none = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION, DseCompat.NO_DSE_VERSION))
        {
            assertThat(DseCompat.gossipedDseVersion()).isNull();
        }
        for (String refused : new String[]{ "6.8.2", "6.9.26", "6.8", "6.8.1.0", "" })
        {
            try (WithProperties other = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION, refused))
            {
                assertThatThrownBy(DseCompat::gossipedDseVersion).as(refused)
                                                                 .isInstanceOf(ConfigurationException.class)
                                                                 .hasMessage(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION.getKey() + " is '" + refused + "': it must be 'none' (no DSE " +
                                                                             "version) or the DSE version 6.8.0 or 6.8.1, which this node gossips " +
                                                                             "to DSE nodes so that they do not send it the range reads of a " +
                                                                             "storage-attached index query with a verb only DSE nodes understand");
            }
        }
    }

    /**
     * In DSE_5_1 mode the node gossips 5.1.51, the version of the DSE 5.1 nodes, also with the property unset; any
     * other value, none and DSE 6's included, is refused.
     */
    @Test
    public void gossipedDseVersionInDse51Mode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        try (WithProperties unset = new WithProperties().clear(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION))
        {
            assertThat(DseCompat.gossipedDseVersion()).isEqualTo(DseCompat.DSE51_GOSSIPED_DSE_VERSION).isEqualTo("5.1.51");
        }
        try (WithProperties set = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION, "5.1.51"))
        {
            assertThat(DseCompat.gossipedDseVersion()).isEqualTo("5.1.51");
        }
        for (String refused : new String[]{ DseCompat.NO_DSE_VERSION, "6.8.1", "5.1.50", "5.1.51.0", "abc", "" })
        {
            try (WithProperties other = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION, refused))
            {
                assertThatThrownBy(DseCompat::gossipedDseVersion).as(refused)
                                                                 .isInstanceOf(ConfigurationException.class)
                                                                 .hasMessage(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION.getKey() + " is '" + refused +
                                                                             "': in internode_compatibility_mode DSE_5_1 it must be '5.1.51', its " +
                                                                             "default in that mode, the DSE version of the DSE 5.1 nodes, which this " +
                                                                             "node gossips to them so that their version gates stay as in a cluster " +
                                                                             "of DSE 5.1.51 nodes; 'none' (no DSE version) is not accepted in that " +
                                                                             "mode either: remove the property");
            }
        }
    }

    @Test
    public void dse51Version10Advice()
    {
        assertThat(DseCompat.dse51Version10Advice()).isEqualTo("a DSE 5.1 node speaks messaging version 10 only when started with " +
                                                               "-Dcassandra.force_3_0_protocol_version=true, which " +
                                                               "internode_compatibility_mode DSE_5_1 does not support: restart " +
                                                               "that node without the property");
    }

    @Test
    public void refusedVerbReason()
    {
        assertThat(DseCompat.refusedVerb(Verb.PING_REQ)).isEqualTo("verb PING_REQ to a DSE 6 peer");
    }

    /**
     * A DSE 5.1 release is {@code 3.11.3.<build>}; Apache Cassandra 3.11, DSE 6 and this software's releases are
     * not.
     */
    @Test
    public void dse51ReleaseVersions()
    {
        // DSE 5.1.51, 5.1.18 and 5.1.3
        for (String dse51 : new String[]{ "3.11.3.5151", "3.11.3.5118", "3.11.3.513" })
            assertThat(DseCompat.isDse51ReleaseVersion(new CassandraVersion(dse51))).as(dse51).isTrue();
        // a suffix, which DSE 5.1 never writes, does not change the answer, as for DSE 6
        assertThat(DseCompat.isDse51ReleaseVersion(new CassandraVersion("3.11.3.5151-SNAPSHOT"))).isTrue();
        // Apache Cassandra 3.11.3 and 3.11.11, other releases with four parts (another 3.11, DSE 5.0.15, DSE 6.9.26 and
        // 6.8.65, this software), and DSE versions read as releases
        for (String other : new String[]{ "3.11.3", "3.11.11", "3.11.4.5151", "3.0.15.5015", "4.0.0.6926",
                                          "4.0.0.6865", "5.0.7.0", "5.1.51", "6.8.1" })
            assertThat(DseCompat.isDse51ReleaseVersion(new CassandraVersion(other))).as(other).isFalse();
        assertThat(DseCompat.isDse51ReleaseVersion(null)).isFalse();
        // and the DSE 6 rule does not take a DSE 5.1 release
        assertThat(DseCompat.isDse6ReleaseVersion(new CassandraVersion("3.11.3.5151"))).isFalse();
    }

    /** A DSE 5.1 {@code dse_version} is {@code 5.1.<patch>}, nothing else. */
    @Test
    public void dse51DseVersions()
    {
        for (String dse51 : new String[]{ "5.1.51", "5.1.18", "5.1.0" })
            assertThat(DseCompat.isDse51DseVersion(dse51)).as(dse51).isTrue();
        for (String other : new String[]{ "6.8.1", "6.9.26", "5.0.15", "5.1", "5.1.51.0", "5.1.51-SNAPSHOT",
                                          "5.1.051", "5.1.05", "5.1.00", "5.1.100", "3.11.3.5151", "", "none" })
            assertThat(DseCompat.isDse51DseVersion(other)).as(other).isFalse();
        assertThat(DseCompat.isDse51DseVersion(null)).isFalse();
    }

    /**
     * The rule of the DSE 5.1 mode: a peer at messaging version 10 or 11, or, while its version is not known, a peer
     * with a DSE 5.1 release or {@code dse_version}, is a DSE 5.1 node.
     */
    @Test
    public void dse51PeerRule()
    {
        CassandraVersion dse5151 = new CassandraVersion("3.11.3.5151");
        CassandraVersion dse5118 = new CassandraVersion("3.11.3.5118");
        CassandraVersion apache = new CassandraVersion("3.11.11");
        CassandraVersion dse6926 = new CassandraVersion("4.0.0.6926");
        CassandraVersion hcd = new CassandraVersion("5.0.7.0");

        // the messaging version of gossip or of a handshake: 10 or 11, whatever else is known
        for (int version : new int[]{ MessagingService.VERSION_30, MessagingService.VERSION_3014 })
        {
            assertThat(DseCompat.isDse51Peer(version, null, null)).as("%d", version).isTrue();
            assertThat(DseCompat.isDse51Peer(version, apache, null)).as("%d", version).isTrue();
        }
        // a version of 4.0 or later, or DSE 6's own, is no DSE 5.1 node, whatever release or dse_version a row saved
        // before its upgrade still holds
        for (int version : new int[]{ MessagingService.VERSION_40, MessagingService.current_version,
                                      DseCompat.DSE_681_VERSION })
        {
            assertThat(DseCompat.isDse51Peer(version, hcd, null)).as("%d", version).isFalse();
            assertThat(DseCompat.isDse51Peer(version, dse6926, "6.8.1")).as("%d", version).isFalse();
            assertThat(DseCompat.isDse51Peer(version, dse5151, "5.1.51")).as("%d", version).isFalse();
        }
        // before any gossip or handshake: the release, or the dse_version
        assertThat(DseCompat.isDse51Peer(null, dse5151, null)).isTrue();
        assertThat(DseCompat.isDse51Peer(null, dse5118, null)).isTrue();
        assertThat(DseCompat.isDse51Peer(null, null, "5.1.51")).isTrue();
        assertThat(DseCompat.isDse51Peer(null, apache, null)).isFalse();
        assertThat(DseCompat.isDse51Peer(null, dse6926, "6.8.1")).isFalse();
        assertThat(DseCompat.isDse51Peer(null, hcd, null)).isFalse();
        assertThat(DseCompat.isDse51Peer(null, null, "6.8.1")).isFalse();
        assertThat(DseCompat.isDse51Peer(null, null, null)).isFalse();
    }

    /** The patch of a DSE 5.1 node: from its dse_version, else from the build number of its release. */
    @Test
    public void dse51PatchIsReadFromTheDseVersionOrTheBuildNumber()
    {
        assertThat(DseCompat.dse51Patch(new CassandraVersion("3.11.3.5151"), null)).isEqualTo(51);
        assertThat(DseCompat.dse51Patch(new CassandraVersion("3.11.3.5118"), null)).isEqualTo(18);
        assertThat(DseCompat.dse51Patch(new CassandraVersion("3.11.3.513"), null)).isEqualTo(3);
        assertThat(DseCompat.dse51Patch(null, "5.1.51")).isEqualTo(51);
        // the first release of the line, on both paths
        assertThat(DseCompat.dse51Patch(null, "5.1.0")).isEqualTo(0);
        assertThat(DseCompat.dse51Patch(new CassandraVersion("3.11.3.510"), null)).isEqualTo(0);
        // a dse_version that is no DSE 5.1 one: nothing, or the build number when there is one
        assertThat(DseCompat.dse51Patch(null, "5.1.100")).isNull();
        assertThat(DseCompat.dse51Patch(new CassandraVersion("3.11.3.5118"), "5.1.100")).isEqualTo(18);
        // the dse_version wins over the build number
        assertThat(DseCompat.dse51Patch(new CassandraVersion("3.11.3.5118"), "5.1.51")).isEqualTo(51);
        // a DSE 5.1 release whose build number names no 5.1 release, and other releases
        for (String other : new String[]{ "3.11.3.6926", "3.11.3.51", "3.11.3.515151", "3.11.3.5105", "3.11.11",
                                          "4.0.0.6926", "5.0.7.0" })
            assertThat(DseCompat.dse51Patch(new CassandraVersion(other), null)).as(other).isNull();
        assertThat(DseCompat.dse51Patch(null, "6.8.1")).isNull();
        assertThat(DseCompat.dse51Patch(new CassandraVersion("4.0.0.6926"), "6.8.1")).isNull();
        assertThat(DseCompat.dse51Patch(null, null)).isNull();
    }

    /**
     * What real DSE 5.1.51 nodes gossiped (the {@code nodetool gossipinfo} of the DSE 5.1.51 fixtures,
     * {@code gossip/dse-5.1.51*}: every node of the three-node cluster seen from each node, and the Search and Graph
     * node) and sent in their handshakes: each node is a DSE 5.1 peer by its {@code NET_VERSION}, by its
     * {@code RELEASE_VERSION} and by the {@code dse_version} of its DSE state, each alone, and its patch is 51; its
     * handshake is Apache Cassandra 3.11's at version 11, with no DSE flag. The DSE 6.9.26 node of the DSE 6 fixtures
     * is no DSE 5.1 peer by any of the three.
     */
    @Test
    public void theRuleRecognisesTheCapturedDse51NodesByTheirGossipAndHandshakes() throws IOException
    {
        Dse51Fixtures dse51 = DseFixtures.dse51();
        Map<Path, List<String>> endpoints = new LinkedHashMap<>();
        List<String> clusterEndpoints = new ArrayList<>();
        for (int node = 1; node <= Dse51Fixtures.CLUSTER_NODES; node++)
            clusterEndpoints.add("/127.0.51.1" + node);
        for (int node = 1; node <= Dse51Fixtures.CLUSTER_NODES; node++)
            endpoints.put(dse51.gossipInfo(node), clusterEndpoints);
        endpoints.put(dse51.searchGraphGossipInfo(), List.of("/127.0.51.21"));

        for (Map.Entry<Path, List<String>> gossipInfo : endpoints.entrySet())
        {
            // every endpoint of the file is checked
            assertThat(Files.readAllLines(gossipInfo.getKey()).stream().filter(line -> line.startsWith("/")))
            .as("%s", gossipInfo.getKey().getFileName())
            .containsExactlyElementsOf(gossipInfo.getValue());
            for (String endpoint : gossipInfo.getValue())
            {
                String where = endpoint + " in " + gossipInfo.getKey().getFileName();
                Map<String, String> states = new HashMap<>();
                for (GossipState state : dse51.gossipStates(gossipInfo.getKey(), endpoint))
                    states.put(state.name, state.value);
                // the DSE state, at ordinal 9, which DSE 5.1's nodetool prints under the Apache name of that ordinal
                assertThat(states).as(where).containsKeys("NET_VERSION", "RELEASE_VERSION", "X_11_PADDING");

                int messagingVersion = Integer.parseInt(states.get("NET_VERSION"));
                CassandraVersion release = new CassandraVersion(states.get("RELEASE_VERSION"));
                String dseVersion = (String) JsonUtils.<Object>fromJsonMap(states.get("X_11_PADDING"))
                                                      .get("dse_version");

                assertThat(messagingVersion).as(where).isEqualTo(Dse51Fixtures.MESSAGING_VERSION);
                assertThat(release.toString()).as(where).isEqualTo(Dse51Fixtures.RELEASE_VERSION);
                assertThat(dseVersion).as(where).isEqualTo(Dse51Fixtures.DSE_VERSION);
                // each of the three alone tells the peer
                assertThat(DseCompat.isDse51Peer(messagingVersion, null, null)).as(where).isTrue();
                assertThat(DseCompat.isDse51Peer(null, release, null)).as(where).isTrue();
                assertThat(DseCompat.isDse51Peer(null, null, dseVersion)).as(where).isTrue();
                assertThat(DseCompat.isDse6ReleaseVersion(release)).as(where).isFalse();
                assertThat(DseCompat.dse51Patch(release, null)).as(where).isEqualTo(51);
                assertThat(DseCompat.dse51Patch(null, dseVersion)).as(where).isEqualTo(51);
            }
        }

        // the handshakes: msg1 of the connecting node, the accepting node's answer (msg2), the connecting node's msg3
        List<Dse51Fixtures.Handshake> handshakes = dse51.handshakes();
        assertThat(handshakes).hasSize(2);
        for (Dse51Fixtures.Handshake handshake : handshakes)
        {
            String where = handshake.client + " -> " + handshake.server;
            assertThat(handshake.msg1).as(where).hasSize(8);
            assertThat(handshake.msg2).as(where).hasSize(4);
            assertThat(handshake.msg3).as(where).hasSize(9);
            // msg1: the magic, then the header: no flag in bits 0 to 7 (no compression, no streaming), the requested
            // version in bits 8 to 15, and bits 16 to 31, the accepted range of a 4.0 message, zero
            assertThat(handshake.msg1).as(where).isEqualTo(Hex.hexToBytes("ca552dfa00000b00"));
            int header = ByteBuffer.wrap(handshake.msg1, 4, 4).getInt();
            assertThat(header & 0xFF).as(where).isZero();
            int requested = (header >>> 8) & 0xFF;
            int min = (header >>> 16) & 0xFF;
            int max = (header >>> 24) & 0xFF;
            assertThat(DseCompat.isDseFlaggedInitiate(requested, min, max)).as(where).isFalse();
            assertThat(DseCompat.isPre40Initiate(requested, min, max)).as(where).isTrue();
            int answered = ByteBuffer.wrap(handshake.msg2).getInt();
            int connecting = ByteBuffer.wrap(handshake.msg3, 0, 4).getInt();
            for (int version : new int[]{ requested, answered, connecting })
            {
                assertThat(version).as(where).isEqualTo(Dse51Fixtures.MESSAGING_VERSION);
                assertThat(DseCompat.isDse51Peer(version, null, null)).as(where).isTrue();
            }
            // msg3 ends with the connecting node's address, of 4 bytes, the one the connection line names
            assertThat(handshake.msg3[4]).as(where).isEqualTo((byte) 4);
            assertThat(InetAddress.getByAddress(Arrays.copyOfRange(handshake.msg3, 5, 9)).getHostAddress())
            .as(where).isEqualTo(handshake.client);
        }

        // the DSE 6.9.26 node, the only DSE 6 node whose gossip is among the fixtures: its NET_VERSION, its release
        // and its dse_version are DSE 6's
        Map<String, String> dse6 = new HashMap<>();
        for (GossipState state : DseFixtures.gossipStates(DseVersion.DSE_6_9_26))
            dse6.put(state.name, state.value);
        assertThat(dse6).containsKeys("NET_VERSION", "RELEASE_VERSION", "DSE_GOSSIP_STATE");
        int dse6MessagingVersion = Integer.parseInt(dse6.get("NET_VERSION"));
        CassandraVersion dse6Release = new CassandraVersion(dse6.get("RELEASE_VERSION"));
        String dse6DseVersion = (String) JsonUtils.<Object>fromJsonMap(dse6.get("DSE_GOSSIP_STATE"))
                                                  .get("dse_version");
        assertThat(DseCompat.isDse6ReleaseVersion(dse6Release)).isTrue();
        assertThat(DseCompat.isDse51Peer(dse6MessagingVersion, null, null)).isFalse();
        assertThat(DseCompat.isDse51Peer(null, dse6Release, null)).isFalse();
        assertThat(DseCompat.isDse51Peer(null, null, dse6DseVersion)).isFalse();
    }

    /** {@link DseCompat#enabled()} stays DSE 6 only: a DSE_5_1 node takes no pre-4.0 code path. */
    @Test
    public void theModeGatesOfDse51()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        assertThat(DseCompat.isDse51()).isTrue();
        assertThat(DseCompat.isDse6()).isFalse();
        // the pre-4.0 dialect is open to DSE 5.1 peers
        assertThat(DseCompat.enabled()).isTrue();
        assertThat(DseCompat.isPre40Layout(MessagingService.VERSION_3014)).isTrue();
        assertThat(DseCompat.isPre40Layout(MessagingService.VERSION_40)).isFalse();
        DseCompat.checkPre40Layout(MessagingService.VERSION_3014);
        assertThat(DseCompat.source()).isEqualTo("DSE 5.1");
        assertThat(DseCompat.textMode()).isEqualTo(InternodeCompatibilityMode.DSE_5_1);
        assertThat(DseCompat.refusedVerb(Verb.PING_REQ)).isEqualTo("verb PING_REQ to a DSE 5.1 peer");

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertThat(DseCompat.isDse51()).isFalse();
        assertThat(DseCompat.isDse6()).isTrue();
        assertThat(DseCompat.enabled()).isTrue();
        assertThat(DseCompat.source()).isEqualTo("DSE 6");
        assertThat(DseCompat.textMode()).isEqualTo(InternodeCompatibilityMode.DSE_6);
        assertThat(DseCompat.refusedVerb(Verb.PING_REQ)).isEqualTo("verb PING_REQ to a DSE 6 peer");

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        assertThat(DseCompat.isDse51()).isFalse();
        assertThat(DseCompat.isDse6()).isFalse();
        assertThat(DseCompat.enabled()).isFalse();
        assertThatThrownBy(() -> DseCompat.checkPre40Layout(MessagingService.VERSION_3014))
        .isInstanceOf(AssertionError.class)
        .hasMessage("pre-4.0 serialization version 11");
        // the texts of a NONE node name DSE 6, as before the DSE 5.1 mode existed
        assertThat(DseCompat.source()).isEqualTo("DSE 6");
    }

    /**
     * The versions 10 and 11 at which DSE 5.1 stores its batches and Paxos rows are decoded at 4.0 with NONE and in
     * DSE_5_1, where they have no pre-4.0 path after the flag drop, and keep their pre-4.0 layout in DSE_6; the DSE 6
     * versions map the same way in every mode. The rows at 11 of the DSE 5.1.51 fixtures are read in
     * {@code DseBatchlogReplayTest} and {@code DsePaxosRowsTest}.
     */
    @Test
    public void storedVersionsOfDse51AreDecodedAt40OutsideDse6()
    {
        for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(mode);
            for (int pre40 : new int[]{ MessagingService.VERSION_30, MessagingService.VERSION_3014 })
            {
                int expected = mode.isDse6() ? pre40 : MessagingService.VERSION_40;
                assertThat(DseCompat.storedDecodingVersion(pre40)).as("%s in %s", pre40, mode).isEqualTo(expected);
                assertThat(DseCompat.isUnknownStoredDseVersion(pre40)).as("%s in %s", pre40, mode).isFalse();
            }
            // below 3.0 and this software's versions are returned as they are
            for (int other : new int[]{ 9, MessagingService.VERSION_40, MessagingService.VERSION_50,
                                        MessagingService.VERSION_DS_20 })
                assertThat(DseCompat.storedDecodingVersion(other)).as("%s in %s", other, mode).isEqualTo(other);
            assertThat(DseCompat.storedDecodingVersion(DseCompat.DSE_60_VERSION))
            .as("%s", mode).isEqualTo(MessagingService.VERSION_40);
            assertThat(DseCompat.storedDecodingVersion(DseCompat.DSE_697_VERSION))
            .as("%s", mode).isEqualTo(MessagingService.VERSION_DSE_68);
        }
    }
}
