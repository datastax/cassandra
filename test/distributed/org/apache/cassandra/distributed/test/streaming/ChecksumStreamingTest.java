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
package org.apache.cassandra.distributed.test.streaming;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.config.CassandraRelevantProperties.SSTABLE_CHECKSUM_TYPE;
import static org.apache.cassandra.config.CassandraRelevantProperties.SSTABLE_FORMAT_STREAM_NEW_CHECKSUMS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Distributed tests verifying which digest components are streamed between cluster nodes
 * depending on the checksum type and the {@code cassandra.sstable.format.stream_new_checksums} flag.
 * <p>
 * The {@code cassandra.sstable.format.stream_new_checksums} flag shall be enabled only when the target cluster
 * is running a version of Cassandra that supports the new digest components.
 *
 * <p>The streamability of each digest component type is fixed at class-load time:
 * <ul>
 *   <li>{@code Digest.crc32}    — {@code streamable=true} always</li>
 *   <li>{@code Digest.crc32c}   — {@code streamable=SSTABLE_FORMAT_STREAM_NEW_CHECKSUMS}</li>
 *   <li>{@code Digest.crc64nvme}— {@code streamable=SSTABLE_FORMAT_STREAM_NEW_CHECKSUMS}</li>
 * </ul>
 *
 * <p>Both properties must be set <em>before</em> the cluster starts because the in-process instance
 * classloader reads {@code SSTABLE_FORMAT_STREAM_NEW_CHECKSUMS} once when it first initialises
 * {@code SSTableFormat.Components.Types}.
 */
public class ChecksumStreamingTest extends TestBaseImpl
{
    // -------------------------------------------------------------------------
    // CRC32 — always streamed regardless of stream_new_checksums
    // -------------------------------------------------------------------------

    @Test
    public void testCRC32DigestIsStreamedWhenStreamNewChecksumsDisabled() throws IOException
    {
        testDigestStreaming("CRC32", false, true);
    }

    @Test
    public void testCRC32DigestIsStreamedWhenStreamNewChecksumsEnabled() throws IOException
    {
        testDigestStreaming("CRC32", true, true);
    }

    // -------------------------------------------------------------------------
    // CRC32C — only streamed when stream_new_checksums=true
    // -------------------------------------------------------------------------

    @Test
    public void testCRC32CDigestIsNotStreamedByDefault() throws IOException
    {
        testDigestStreaming("CRC32C", false, false);
    }

    @Test
    public void testCRC32CDigestIsStreamedWhenEnabled() throws IOException
    {
        testDigestStreaming("CRC32C", true, true);
    }

    // -------------------------------------------------------------------------
    // CRC64NVME — only streamed when stream_new_checksums=true
    // -------------------------------------------------------------------------

    @Test
    public void testCRC64NVMEDigestIsNotStreamedByDefault() throws IOException
    {
        testDigestStreaming("CRC64NVME", false, false);
    }

    @Test
    public void testCRC64NVMEDigestIsStreamedWhenEnabled() throws IOException
    {
        testDigestStreaming("CRC64NVME", true, true);
    }

    // -------------------------------------------------------------------------
    // Shared helper
    // -------------------------------------------------------------------------

    /**
     * Populates an SSTable on node 1 with the given checksum type, streams it to node 2 via
     * {@code nodetool rebuild}, and asserts whether the digest file is present on node 2.
     *
     * @param checksumType       value for {@code cassandra.sstable.checksums.type}
     * @param streamNewChecksums value for {@code cassandra.sstable.format.stream_new_checksums}
     * @param expectStreamed     whether the digest file should be present on the receiving node
     */
    private void testDigestStreaming(String checksumType, boolean streamNewChecksums, boolean expectStreamed)
    throws IOException
    {
        try (WithProperties properties = new WithProperties())
        {
            // Both properties must be set before the cluster starts so that the isolated
            // instance classloader picks them up when it first loads SSTableFormat.Components.Types.
            properties.set(SSTABLE_CHECKSUM_TYPE, checksumType);
            properties.set(SSTABLE_FORMAT_STREAM_NEW_CHECKSUMS, streamNewChecksums);

            try (Cluster cluster = init(Cluster.build(2)
                                               .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                                 .set("stream_entire_sstables", true))
                                               .start()))
            {
                cluster.stream().forEach(i -> i.nodetoolResult("disableautocompaction", KEYSPACE).asserts().success());
                cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY) " +
                                                  "WITH compression = { 'enabled': false }"));

                IInvokableInstance node1 = cluster.get(1);
                IInvokableInstance node2 = cluster.get(2);

                // Write and flush data only on node 1 so that a rebuild of node 2 triggers streaming.
                for (int i = 0; i < 10; i++)
                    node1.executeInternal(withKeyspace("INSERT INTO %s.tbl (pk) VALUES (?)"), i);
                node1.flush(KEYSPACE);

                // Verify that node 1 has written the expected digest file before streaming.
                node1.runOnInstance(() -> {
                    SSTableReader sstable = new ArrayList<>(Keyspace.open(KEYSPACE)
                                                                    .getColumnFamilyStore("tbl")
                                                                    .getLiveSSTables()).get(0);
                    Path digestPath = digestPath(sstable, checksumType);
                    assertThat(digestPath)
                        .as("Digest file for %s should exist on node1 before streaming", checksumType)
                        .exists();
                });

                // Trigger entire-SSTable streaming from node 1 → node 2.
                node2.nodetoolResult("rebuild", "--keyspace", KEYSPACE).asserts().success();

                // Verify whether the digest file was (or was not) received by node 2.
                node2.runOnInstance(() -> {
                    SSTableReader sstable = new ArrayList<>(Keyspace.open(KEYSPACE)
                                                                    .getColumnFamilyStore("tbl")
                                                                    .getLiveSSTables()).get(0);
                    Path digestPath = digestPath(sstable, checksumType);
                    if (expectStreamed)
                        assertThat(digestPath)
                            .as("Digest file for %s should have been streamed to node2 " +
                                "(stream_new_checksums=%s)", checksumType, streamNewChecksums)
                            .exists();
                    else
                        assertThat(digestPath)
                            .as("Digest file for %s should NOT have been streamed to node2 " +
                                "(stream_new_checksums=%s)", checksumType, streamNewChecksums)
                            .doesNotExist();
                });
            }
        }
    }

    /** Returns the path of the digest file on disk for the given checksum type. */
    private static Path digestPath(SSTableReader sstable, String checksumType)
    {
        switch (checksumType)
        {
            case "CRC32":    return sstable.descriptor.pathFor(Components.DIGEST);
            case "CRC32C":   return sstable.descriptor.pathFor(Components.DIGEST_CRC32C);
            case "CRC64NVME": return sstable.descriptor.pathFor(Components.DIGEST_CRC64NVME);
            default: throw new IllegalArgumentException("Unknown checksum type: " + checksumType);
        }
    }
}
