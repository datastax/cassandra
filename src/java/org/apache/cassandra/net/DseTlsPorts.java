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

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.CassandraVersion;

/**
 * The internode TLS ports of the DSE 6 compatibility mode: where this node listens for DSE 6 nodes and which port it
 * dials them on, when internode encryption is on. Outside DSE 6 compatibility mode every public method answers what
 * upstream does: no extra socket, no pre-4.0 port choice.
 * <p>
 * DSE has two port models, read in DSE's code:
 * <ul>
 *   <li>DSE 6.9.7 and later (messaging version {@link DseCompat#DSE_697_VERSION}, consistent with the Apache 4.0
 *       development code of CASSANDRA-10404) always listen on {@code storage_port}: plain with
 *       {@code internode_encryption: none}, TLS required with {@code internode_encryption: all} and
 *       {@code optional: false}, optional with {@code optional: true} or with {@code dc} and {@code rack};
 *       {@code ssl_storage_port} is opened too, always TLS only, only when {@code internode_encryption} is not
 *       {@code none} and DSE's own {@code enable_legacy_ssl_storage_port} is true. They choose the port of a peer by
 *       the messaging version stored for it, whatever that setting: {@code storage_port} for a version of 6.9.7 or
 *       later (or none stored), {@code ssl_storage_port} for any lower version. Every version this software gives DSE,
 *       by the handshake (the dialect, 11) or by gossip, is lower, so a DSE 6.9.7+ node dials an upgraded node's
 *       {@code ssl_storage_port} for the connections it encrypts once it has stored one for the node; with none
 *       stored, or with the {@code DSE_697} entry of the node's DSE life (kept when the node is marked dead), it dials
 *       {@code storage_port}.</li>
 *   <li>DSE 6.8 has the port rule of Apache Cassandra 3.11 and no {@code enable_legacy_ssl_storage_port} (checked on
 *       the DSE 6.8.65 binary; the code of no other 6.8 release names the setting either): it listens with TLS on
 *       {@code ssl_storage_port} whenever {@code internode_encryption} is not {@code none} (and in plain on
 *       {@code storage_port} unless it is {@code all}), and connects with TLS to {@code ssl_storage_port} only. DSE
 *       6.9.0 to 6.9.6 have the same model: they dial {@code ssl_storage_port} on every encrypted connection, whatever
 *       the peer's version, have neither the setting nor {@code DSE_697}, and under {@code all} listen on
 *       {@code ssl_storage_port} only; 6.9.7 is the first release with the choice by version.</li>
 * </ul>
 * Hence the two rules of this class, both for DSE 6 compatibility mode only:
 * <ul>
 *   <li>{@link #listensOnSslStoragePortForDse}: with internode encryption on, this node also listens, TLS only, on
 *       {@code ssl_storage_port} when {@code legacy_ssl_storage_port_enabled} is false (with it true it does already,
 *       as upstream) and {@code ssl_storage_port} differs from {@code storage_port};</li>
 *   <li>{@link #dialsSslStoragePort}: a DSE 6 peer is dialled the way DSE 6.9.7+ dials its own peers, by its DSE
 *       version: {@code ssl_storage_port} below 6.9.7, {@code storage_port} from 6.9.7 on; a peer whose DSE version is
 *       not known follows {@code legacy_ssl_storage_port_enabled}.</li>
 * </ul>
 * So no setting of either side has to change for internode TLS: neither DSE's {@code enable_legacy_ssl_storage_port}
 * nor this node's {@code legacy_ssl_storage_port_enabled}; {@code ssl_storage_port} and {@code internode_encryption}
 * must be DSE's values. Observed with DSE 6.9.26 nodes under {@code internode_encryption: all}: they connect to an
 * upgraded node on {@code ssl_storage_port}, and it dials them on {@code storage_port}.
 */
public final class DseTlsPorts
{
    /** The DSE version of a peer is not known: neither gossip nor the stored release names one. */
    @VisibleForTesting
    static final int UNKNOWN = -1;

    private DseTlsPorts()
    {
    }

    /**
     * Whether this node also listens, TLS only, on {@code ssl_storage_port} for DSE 6 nodes: in DSE 6 compatibility mode,
     * with {@code internode_encryption} other than {@code none}, {@code legacy_ssl_storage_port_enabled} false (true
     * opens that socket already, as upstream) and {@code ssl_storage_port} different from {@code storage_port} (when
     * they are equal the socket on {@code storage_port} is the one DSE dials). False in every other case, so that
     * mode {@code NONE} listens exactly as upstream.
     */
    public static boolean listensOnSslStoragePortForDse(@Nullable ServerEncryptionOptions encryption)
    {
        return DseCompat.enabled()
               && encryption != null
               && !encryption.legacy_ssl_storage_port_enabled
               && encryption.internode_encryption != InternodeEncryption.none
               && DatabaseDescriptor.getSSLStoragePort() != DatabaseDescriptor.getStoragePort();
    }

    /**
     * Whether an encrypted connection to {@code peer} at a pre-4.0 version, in DSE 6 compatibility mode, goes to
     * {@code ssl_storage_port} rather than {@code storage_port}: by the DSE version of the peer
     * ({@link #dseMessagingVersion}) when it is known, else by {@code legacySslStoragePortEnabled}. Always false outside
     * the mode, where upstream has no pre-4.0 port choice.
     */
    public static boolean dialsSslStoragePort(InetAddressAndPort peer, boolean legacySslStoragePortEnabled)
    {
        return DseCompat.enabled() && dialsSslStoragePort(dseMessagingVersion(peer), legacySslStoragePortEnabled);
    }

    /**
     * As {@link #dialsSslStoragePort(InetAddressAndPort, boolean)}, for a DSE messaging version already looked up
     * ({@link #UNKNOWN} when there is none): DSE 6.9.7+ listens with TLS on {@code storage_port} whatever its settings,
     * and earlier DSE 6 releases only on {@code ssl_storage_port}.
     */
    @VisibleForTesting
    static boolean dialsSslStoragePort(int dseMessagingVersion, boolean legacySslStoragePortEnabled)
    {
        if (dseMessagingVersion == UNKNOWN)
            return legacySslStoragePortEnabled;
        return dseMessagingVersion < DseCompat.DSE_697_VERSION;
    }

    /**
     * DSE's own messaging version ({@code relative << 8}) for {@code peer}, or {@link #UNKNOWN}: the {@code NET_VERSION}
     * of its gossip state when it has one (any value below 256 is not a DSE node's, so {@link #UNKNOWN}); else the DSE
     * release stored for it ({@code system.peers}, imported from DSE's {@code nodes/peers} at the upgrade, refreshed by
     * gossip), so that the first connection after a restart, before any gossip, already has it.
     */
    @VisibleForTesting
    static int dseMessagingVersion(InetAddressAndPort peer)
    {
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(peer);
        if (state != null && state.getApplicationState(ApplicationState.NET_VERSION) != null)
        {
            int version = DsePeers.gossipedDseVersion(state);
            return version < 0 ? UNKNOWN : version;
        }
        return dseMessagingVersionOfRelease(SystemKeyspace.getReleaseVersion(peer));
    }

    /**
     * The DSE messaging version of a DSE 6 release ({@code 4.0.0.<build>}, read by
     * {@link DseCompat#dse6MinorAndPatch}), or {@link #UNKNOWN} for any other release: {@code 4.0.0.6865} is DSE
     * 6.8.65 and {@code 4.0.0.6926} DSE 6.9.26 (the releases of the captures of {@code test/data/dse-compat}),
     * {@code 4.0.0.697} DSE 6.9.7. Only the comparison with 6.9.7 matters to the caller, so 6.0 to 6.7 map to the
     * lowest version without telling them apart.
     */
    @VisibleForTesting
    static int dseMessagingVersionOfRelease(@Nullable CassandraVersion release)
    {
        int[] minorAndPatch = DseCompat.dse6MinorAndPatch(release);
        if (minorAndPatch == null)
            return UNKNOWN;
        int minor = minorAndPatch[0];
        int patch = minorAndPatch[1];
        if (minor == 9)
            return patch >= 7 ? DseCompat.DSE_697_VERSION : DseCompat.DSE_69_VERSION;
        if (minor == 8)
            return patch >= 1 ? DseCompat.DSE_681_VERSION : DseCompat.DSE_68_VERSION;
        return DseCompat.DSE_60_VERSION;
    }
}
