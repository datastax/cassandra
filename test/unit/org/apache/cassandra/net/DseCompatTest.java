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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.CassandraVersion;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The pure functions of {@link DseCompat}: DSE 6's release and messaging versions, the gossiped DSE version, and the
 * gate of the pre-4.0 layout.
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
                                                                 .hasMessage(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION.getKey() + " is '" + refused + "': it must be 'none' or the " +
                                                                             "DSE version 6.8.0 or 6.8.1, so that DSE nodes do not send this node " +
                                                                             "the range reads of a storage-attached index query with a verb only " +
                                                                             "DSE nodes understand");
            }
        }
    }

    @Test
    public void refusedVerbReason()
    {
        assertThat(DseCompat.refusedVerb(Verb.PING_REQ)).isEqualTo("verb PING_REQ to a DSE 6 peer");
    }
}
