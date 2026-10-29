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

import java.util.regex.Pattern;
import javax.annotation.Nullable;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.CassandraVersion;

/**
 * The facts about DataStax Enterprise 6's internode protocol and releases that the DSE 6 compatibility mode
 * relies on, in one place. They come from DSE 6.8 and 6.9's code and from what DSE 6.8.65 and 6.9.26 nodes sent
 * and wrote (the captures and files under {@code test/data/dse-compat}).
 * <p>
 * DSE talks to a non-DSE peer in the pre-4.0 dialect of Cassandra 3.0.14, messaging version 11: the
 * 3-message handshake without framing or CRC, and 3.0-layout messages. Its own first handshake message
 * announces a DSE version instead: {@code 0xFF} in the byte where a 3.x peer puts its version and DSE's
 * "relative" version (4 = 6.8.1+, 5 = 6.9.0–6.9.6, 6 = 6.9.7+) in the top byte (captures
 * {@code test/data/dse-compat/handshake}). A 4-byte second message of 256 or more is such a relative
 * version shifted left by 8.
 */
public final class DseCompat
{
    /** The messaging version DSE uses with non-DSE peers, and the only one this software speaks with DSE. */
    public static final int DSE_DIALECT = MessagingService.VERSION_3014;

    /** The value DSE puts in the "requested version" byte of its first handshake message. */
    public static final int DSE_VERSION_BYTE = 255;

    /**
     * The lowest DSE relative messaging version accepted: 1 = 6.0, 2 = 6.0.3, 3 = 6.8.0, 4 = 6.8.1+, 5 = 6.9.0–6.9.6,
     * 6 = 6.9.7+. Any non-zero value of the 8-bit field is accepted, so future DSE releases are recognised too; there is
     * no upper bound to check.
     */
    public static final int DSE_RELATIVE_MIN = 1;

    /**
     * DSE 6.0, relative version 1. DSE 6's messaging versions are {@code relative << 8}; DSE also stores its current one
     * next to the update bytes of {@code system.paxos} ({@code proposal_version}, {@code most_recent_commit_version}; the
     * DSE 6.8.65 rows of {@code test/data/dse-compat/paxos}). {@code version} of {@code system.batches} holds the messaging
     * version a batch was stored at; that DSE stores the same values there, next to mutations serialized at its WRITES
     * version as in {@code system.paxos}, is inferred from those Paxos rows and from DSE's batchlog code: no row DSE wrote
     * in {@code system.batches} has been captured. The WRITES group version is OSS_30 for {@link #DSE_60_VERSION} and
     * {@link #DSE_603_VERSION}, DSE_68 from {@link #DSE_68_VERSION} on (only DSE_68 adds the maximum timestamp of the
     * update after the table id).
     */
    public static final int DSE_60_VERSION = 1 << 8;
    /** DSE 6.0.3, relative version 2 (see {@link #DSE_60_VERSION}). */
    public static final int DSE_603_VERSION = 2 << 8;
    /** DSE 6.8.0, relative version 3 (see {@link #DSE_60_VERSION}). */
    public static final int DSE_68_VERSION = 3 << 8;
    /**
     * DSE 6.8.1 and later 6.8 releases, relative version 4: what DSE 6.8.65 stored in system.paxos (see
     * {@link #DSE_60_VERSION}).
     */
    public static final int DSE_681_VERSION = 4 << 8;
    /** DSE 6.9.0 to 6.9.6, relative version 5 (see {@link #DSE_60_VERSION}). */
    public static final int DSE_69_VERSION = 5 << 8;
    /** DSE 6.9.7 and later, relative version 6 (see {@link #DSE_60_VERSION}). */
    public static final int DSE_697_VERSION = 6 << 8;

    /** The DSE 6 messaging versions {@link #storedDecodingVersion} maps, as error messages list them. */
    public static final String DSE_STORED_VERSIONS_TEXT = String.format("%d, %d, %d, %d, %d or %d", DSE_60_VERSION, DSE_603_VERSION,
                                                                        DSE_68_VERSION, DSE_681_VERSION, DSE_69_VERSION,
                                                                        DSE_697_VERSION);

    /**
     * A 4-byte second handshake message of at least this value is decoded as a DSE messaging version
     * ({@code relative << 8}) and mapped to the dialect ({@link #dialectFor}); {@link #isDseHandshakeVersion} tells a
     * well-formed one, which a handshake needs to identify a DSE node. A gossiped {@code NET_VERSION} of at least this
     * value is taken for a DSE version as it is, without that check: DSE gossips only its own version.
     */
    public static final int DSE_HANDSHAKE_MIN = DSE_60_VERSION;

    /** The lowest pre-4.0 version a peer may request; older versions are refused (Cassandra 3.0.0). */
    public static final int OLDEST_PRE40_VERSION = MessagingService.VERSION_30;

    /**
     * The gossip {@code NET_VERSION} value published in DSE_6 mode: DSE parses it as Cassandra 4.0. A DSE peer's own value
     * is read with {@link #messagingVersionFromGossip(int)}.
     */
    public static final int NET_VERSION_PUBLISHED = MessagingService.VERSION_40;

    /**
     * The value of {@link CassandraRelevantProperties#INTERNODE_COMPATIBILITY_MODE_DSE_VERSION} with which this node
     * gossips no {@code DSE_GOSSIP_STATE}.
     */
    public static final String NO_DSE_VERSION = "none";

    /**
     * The DSE versions a node may gossip: 6.8.0 and 6.8.1. DSE 6.8.2 and later group the range reads of an index query
     * by endpoint when every node gossips such a version, with a verb DSE cannot write to this software; below 6.8 other
     * DSE features change, which nothing here has looked at. Nothing else either: DSE fails on a version it cannot parse,
     * every second (observed with DSE 6.8.65), and only 6.8.1 has been tested against DSE nodes.
     */
    private static final Pattern GOSSIPED_DSE_VERSION = Pattern.compile("6\\.8\\.[01]");

    private DseCompat()
    {
    }

    /**
     * The DSE version this node gossips to DSE nodes in DSE 6 compatibility mode
     * ({@link CassandraRelevantProperties#INTERNODE_COMPATIBILITY_MODE_DSE_VERSION}), or null when it gossips none
     * ({@link #NO_DSE_VERSION}).
     *
     * @throws ConfigurationException if the property is neither {@link #NO_DSE_VERSION} nor such a version
     */
    @Nullable
    public static String gossipedDseVersion()
    {
        String configured = CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_VERSION.getString();
        if (NO_DSE_VERSION.equals(configured))
            return null;
        if (!GOSSIPED_DSE_VERSION.matcher(configured).matches())
            throw new ConfigurationException(String.format("%s is '%s': it must be '%s' (no DSE version) or the DSE version 6.8.0 or " +
                                                           "6.8.1, which this node gossips to DSE nodes so that they do not send it the " +
                                                           "range reads of a storage-attached index query with a verb only DSE nodes understand",
                                                           CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_VERSION.getKey(), configured,
                                                           NO_DSE_VERSION));
        return configured;
    }

    /** True while the node accepts DSE 6 peers: the guard of every pre-4.0 code path. */
    public static boolean enabled()
    {
        return DatabaseDescriptor.isDse6CompatibilityMode();
    }

    /** True for the first handshake message DSE sends to a peer whose version it does not know yet. */
    public static boolean isDseFlaggedInitiate(int requestedVersion, int minVersion, int maxVersion)
    {
        return requestedVersion == DSE_VERSION_BYTE && minVersion == 0 && maxVersion >= DSE_RELATIVE_MIN;
    }

    /**
     * True for a plain pre-4.0 first handshake message: a requested version up to 4.0 and no accepted range. DSE sends
     * one at 12 once it has stored {@code OSS_40} for us (from our gossip {@code NET_VERSION}, {@link #NET_VERSION_PUBLISHED});
     * a 4.0-style message always carries a maximum of 12 or more, so there is no ambiguity.
     */
    public static boolean isPre40Initiate(int requestedVersion, int minVersion, int maxVersion)
    {
        return requestedVersion <= MessagingService.VERSION_40 && minVersion == 0 && maxVersion == 0;
    }

    /** Maps the 4-byte second handshake message of a DSE peer to the dialect we will actually use with it. */
    public static int dialectFor(int peerHandshakeVersion)
    {
        return peerHandshakeVersion >= DSE_HANDSHAKE_MIN ? DSE_DIALECT : peerHandshakeVersion;
    }

    /**
     * True for a DSE messaging version as DSE writes it in a handshake: its relative version (1 to 255) shifted left by
     * 8 bits ({@link #DSE_60_VERSION}), so 256 to 65280 ({@code 255 << 8}) and a multiple of 256. Another 4-byte
     * answer of {@link #DSE_HANDSHAKE_MIN} or more is still mapped to the dialect, but identifies no DSE node and does
     * not complete a pre-4.0 handshake.
     */
    static boolean isDseHandshakeVersion(int version)
    {
        return version >= DSE_HANDSHAKE_MIN && version <= 255 << 8 && (version & 0xFF) == 0;
    }

    /**
     * Maps a gossip {@code NET_VERSION} value of a DSE peer (256 or more) to the dialect; other values are returned as they
     * are.
     */
    public static int messagingVersionFromGossip(int netVersion)
    {
        return dialectFor(netVersion);
    }

    /**
     * True when a payload serializer must use the pre-4.0 layout: a messaging version below 4.0 in DSE 6 compatibility
     * mode. With the mode off such a version keeps the layout it had before the mode existed, byte for byte: messaging
     * version 10 is still used then (the DSE to CC migration path of {@code DataLimits}), and the serializers that
     * asserted version 4.0 keep asserting it.
     */
    public static boolean isPre40Layout(int version)
    {
        return version < MessagingService.VERSION_40 && enabled();
    }

    /**
     * Refuses the pre-4.0 message codec when the mode is off: outside DSE 6 compatibility mode no peer speaks it. An
     * {@link AssertionError} whether or not assertions are enabled, as for the pre-4.0 address layout of the
     * handshake: without the mode the message header has no pre-4.0 layout to fall back to.
     *
     * @throws AssertionError when the mode is off
     */
    public static void checkPre40Layout(int version)
    {
        if (!enabled())
            throw new AssertionError("pre-4.0 serialization version " + version);
    }

    /**
     * The messaging version to decode bytes a node stored in a system table next to the messaging version they were
     * serialized at ({@code system.paxos}, {@code system.batches}) with. A version below {@link #DSE_60_VERSION} is this
     * software's and is returned as it is. A DSE 6 version, left in the table by DSE before the node was upgraded, is mapped
     * to the layout DSE wrote at that version, in every {@code internode_compatibility_mode}: the rows outlive the mode. DSE
     * serializes mutations and partition updates at the WRITES version of its messaging version (the DSE 6.8.65 rows of
     * {@code test/data/dse-compat/paxos} carry the maximum timestamp only that version writes; for the other DSE versions
     * this follows from DSE's version matrix, see {@link #DSE_60_VERSION}): DSE_68 from DSE 6.8 on, which is
     * {@link MessagingService#VERSION_DSE_68} here; OSS_30 for DSE 6.0 and 6.0.3, the 3.0 layout, decoded at
     * {@link MessagingService#VERSION_40} because this software decodes an update with the same layout at every version but
     * {@link MessagingService#VERSION_DSE_68}, and {@link MessagingService#VERSION_40} takes no mode-dependent pre-4.0 path.
     *
     * @throws IllegalArgumentException for a version {@link #isUnknownStoredDseVersion}, which is never decoded as this
     * software's own layout
     */
    public static int storedDecodingVersion(int storedVersion)
    {
        if (storedVersion < DSE_60_VERSION)
            return storedVersion;

        switch (storedVersion)
        {
            case DSE_60_VERSION:
            case DSE_603_VERSION:
                return MessagingService.VERSION_40;
            case DSE_68_VERSION:
            case DSE_681_VERSION:
            case DSE_69_VERSION:
            case DSE_697_VERSION:
                return MessagingService.VERSION_DSE_68;
            default:
                throw new IllegalArgumentException("Not a DSE 6 messaging version: " + storedVersion);
        }
    }

    /**
     * Whether a messaging version stored in a system table next to serialized bytes is in DSE's range (256 or more) but
     * none of DSE 6's versions: {@link #storedDecodingVersion} refuses it.
     */
    public static boolean isUnknownStoredDseVersion(int storedVersion)
    {
        switch (storedVersion)
        {
            case DSE_60_VERSION:
            case DSE_603_VERSION:
            case DSE_68_VERSION:
            case DSE_681_VERSION:
            case DSE_69_VERSION:
            case DSE_697_VERSION:
                return false;
            default:
                return storedVersion >= DSE_60_VERSION;
        }
    }

    /** The {@code compatstatus} refusal reason of a request whose verb does not exist in the pre-4.0 dialect. */
    public static String refusedVerb(Verb verb)
    {
        return "verb " + verb + " to a DSE 6 peer";
    }

    /**
     * True for the release a DSE 6 node reports: {@code 4.0.0.<build>}, the Cassandra-compatible version with DSE's build
     * number as a fourth component: {@code 4.0.0.6012} for 6.0.12, {@code 4.0.0.678} for 6.7.8, {@code 4.0.0.6855} for
     * 6.8.55, and {@code 4.0.0.6865} and {@code 4.0.0.6926}, what DSE 6.8.65 and 6.9.26 report ({@code system-local.json},
     * {@code nodes-local.json} and {@code nodetool-gossipinfo.txt} under {@code test/data/dse-compat}). Apache Cassandra
     * 4.0.0 reports three components, HCD 1.2 {@code 4.0.11.x}, this software 5.0 or later.
     */
    public static boolean isDse6ReleaseVersion(@Nullable CassandraVersion version)
    {
        return version != null && version.major == 4 && version.minor == 0 && version.patch == 0 && version.hotfix >= 0;
    }

    /**
     * The DSE version {@code 6.<minor>.<patch>} of a DSE 6 release ({@link #isDse6ReleaseVersion}), read from its build
     * number of 3 or 4 digits, {@code 6<minor><patch>}: {@code 4.0.0.697} is 6.9.7, {@code 4.0.0.6865} 6.8.65,
     * {@code 4.0.0.6926} 6.9.26. Returns {@code {minor, patch}}, or null for any other release or build number. The one
     * reading of DSE's build number, for the features that depend on the DSE release a cluster ran.
     */
    @Nullable
    public static int[] dse6MinorAndPatch(@Nullable CassandraVersion release)
    {
        if (!isDse6ReleaseVersion(release))
            return null;
        String build = Integer.toString(release.hotfix);
        if ((build.length() != 3 && build.length() != 4) || build.charAt(0) != '6')
            return null;
        return new int[]{ build.charAt(1) - '0', Integer.parseInt(build.substring(2)) };
    }
}
