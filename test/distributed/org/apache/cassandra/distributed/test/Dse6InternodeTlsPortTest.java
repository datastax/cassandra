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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.net.DseCompat;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_CURRENT_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.apache.cassandra.distributed.test.AbstractEncryptionOptionsImpl.ConnectResult.NEGOTIATED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Internode TLS in the DSE 6 compatibility mode: DSE 6 nodes dial an upgraded node's {@code ssl_storage_port} for the
 * connections they encrypt (DSE 6.8 and 6.9 before 6.9.7 always, DSE 6.9.7+ once it has stored a messaging version
 * below DSE 6.9.7's for the node), so a node in {@code DSE_6} mode with internode encryption on listens there, TLS
 * only, although {@code legacy_ssl_storage_port_enabled} is false, and logs why at INFO; the same node with
 * {@code NONE} does not (upstream). The extra socket refuses a plain connection. The two nodes, one in each mode, keep
 * talking over TLS on {@code storage_port}.
 * <p>
 * Only the inbound side is tested here. No DSE node takes part, and no in-JVM node chooses its port the way DSE does:
 * the two nodes of this software talk at a 4.0 or later version, so the port this node dials a DSE node on
 * ({@code DseTlsPorts.dialsSslStoragePort}) is covered by {@code DseTlsPortsTest}. The node enters the mode on a
 * restart, as an upgraded DSE node does, after a first start without it on the storage settings DSE reads.
 */
public class Dse6InternodeTlsPortTest extends AbstractEncryptionOptionsImpl
{
    private static final int SSL_STORAGE_PORT = 7013;
    private static final String EXTRA_SOCKET_LOG = "DSE 6 compatibility mode: also listening for TLS-only internode connections on .*:" +
                                                   SSL_STORAGE_PORT + " \\(ssl_storage_port\\) although legacy_ssl_storage_port_enabled is false, " +
                                                   "because DSE 6 nodes connect to this node on ssl_storage_port for the connections " +
                                                   "internode_encryption \\(all\\) encrypts: DSE 6\\.8 and 6\\.9 before 6\\.9\\.7 always, " +
                                                   "DSE 6\\.9\\.7\\+ once it has stored a messaging version below DSE 6\\.9\\.7's for this node";

    @Test
    public void aDse6NodeListensWithTlsOnSslStoragePort() throws Throwable
    {
        // the mode sets the trie version as a system property the in-JVM instances share, and needs the SAI version pinned
        // (these nodes store no DSE release, so the pin is taken as set): both restored at the end
        try (WithProperties properties = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION).set(SAI_CURRENT_VERSION, "aa");
             Cluster cluster = builder().withNodes(2).withConfig(c -> {
                 c.with(Feature.NETWORK, Feature.GOSSIP)
                  .set("storage_compatibility_mode", "HCD_1")
                  .set("uuid_sstable_identifiers_enabled", false)
                  .set("storage_port", 7012) // must match in-jvm dtest assigned ports
                  .set("ssl_storage_port", SSL_STORAGE_PORT)
                  .set("server_encryption_options",
                       ImmutableMap.builder().putAll(validKeystore)
                                   .put("internode_encryption", "all")
                                   .put("optional", false)
                                   .put("legacy_ssl_storage_port_enabled", "false")
                                   .build());
             }).createWithoutStarting())
        {
            try (WithProperties bb = new WithProperties().set(TRIE_INDEX_FORMAT_VERSION, InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION))
            {
                cluster.startup();
                init(cluster);
            }
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (id int PRIMARY KEY, v int)"));
            // NONE: nothing listens on ssl_storage_port without the legacy setting
            for (int node = 1; node <= 2; node++)
                assertNotListening(cluster.get(node));

            IInvokableInstance upgraded = cluster.get(1);
            upgraded.shutdown().get();
            upgraded.config().set("internode_compatibility_mode", "DSE_6");
            upgraded.startup();

            assertThat(new TlsConnection(host(upgraded), SSL_STORAGE_PORT).connect()).isEqualTo(NEGOTIATED);
            // DSE's pre-4.0 first messages (at 11, at 12 and flagged with 255), sent here without TLS: no reply
            for (byte version : new byte[]{ 11, 12, (byte) DseCompat.DSE_VERSION_BYTE })
                assertPlainConnectionRefused(host(upgraded), new byte[]{ (byte) 0xCA, 0x55, 0x2D, (byte) 0xFA, 0, 0, version, 0 });
            assertThat(upgraded.logs().grep(EXTRA_SOCKET_LOG).getResult()).hasSize(1);
            assertNotListening(cluster.get(2));
            assertThat(cluster.get(2).logs().grep(EXTRA_SOCKET_LOG).getResult()).isEmpty();

            // the nodes of this software still reach each other with TLS on storage_port, in both directions
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1 + i % 2).execute(withKeyspace("INSERT INTO %s.t (id, v) VALUES (?, ?)"), ConsistencyLevel.ALL, i, i);
            for (int node = 1; node <= 2; node++)
                assertThat(cluster.coordinator(node).execute(withKeyspace("SELECT count(*) FROM %s.t"), ConsistencyLevel.ALL)[0][0]).isEqualTo(10L);
        }
    }

    private void assertNotListening(IInvokableInstance node)
    {
        assertThatThrownBy(() -> new TlsConnection(host(node), SSL_STORAGE_PORT).connect()).isInstanceOf(ConnectException.class);
    }

    /**
     * A plain first handshake message sent to ssl_storage_port gets nothing back but the end of the stream, a reset or
     * a TLS alert (content type 21): never a handshake reply.
     */
    private static void assertPlainConnectionRefused(String host, byte[] firstMessage) throws IOException
    {
        try (Socket socket = new Socket())
        {
            socket.connect(new InetSocketAddress(host, SSL_STORAGE_PORT), 10_000);
            socket.setSoTimeout(30_000);
            OutputStream out = socket.getOutputStream();
            out.write(firstMessage);
            out.flush();
            InputStream in = socket.getInputStream();
            int first;
            try
            {
                first = in.read();
            }
            catch (SocketException e)
            {
                return; // reset by the node
            }
            assertThat(first).as("first byte of the answer to a plain first message").isIn(-1, 21);
        }
    }

    private static String host(IInvokableInstance node)
    {
        return node.config().broadcastAddress().getAddress().getHostAddress();
    }
}
