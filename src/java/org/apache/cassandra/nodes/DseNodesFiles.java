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

package org.apache.cassandra.nodes;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Reads DataStax Enterprise's {@code nodes/local} and {@code nodes/peers} files of a given {@code nodes} directory, for
 * the DSE 6 preflight inspection, which runs on a DSE node, DSE running or stopped, before any upgrade. The files are
 * only read: an interrupted DSE write ({@code local.old}, {@code peers.old}) is not recovered, unlike the import of
 * {@link CC4NodesFileReader#tryReadLocalInfo()}, and nothing depends on this process's configured metadata directory.
 * The format is the one {@link CC4NodesFileReader} decodes (msgpack, addresses as raw bytes, UUIDs as two longs).
 */
public final class DseNodesFiles
{
    private DseNodesFiles()
    {
    }

    /**
     * {@code nodes/local} of {@code nodesDirectory}.
     *
     * @return the record, or null when the file does not exist
     * @throws RuntimeException when the file exists but cannot be read or decoded, or when it cannot be told whether it
     * exists
     */
    @Nullable
    public static DseNodeRecord readLocal(Path nodesDirectory)
    {
        Path local = nodesDirectory.resolve("local");
        if (absent(local))
            return null;
        try (InputStream in = new BufferedInputStream(Files.newInputStream(local, StandardOpenOption.READ)))
        {
            RecordFields fields = CC4NodesFileReader.createCC4ObjectMapper().readerFor(RecordFields.class).readValue(in);
            return fields.toRecord(false);
        }
        catch (Exception e)
        {
            throw new RuntimeException("cannot decode " + local + ": " + e.getMessage(), e);
        }
    }

    /**
     * Every entry of {@code nodes/peers} of {@code nodesDirectory}.
     *
     * @return the records, empty when the file does not exist or holds no peer (DSE 6.9 writes an empty map then)
     * @throws RuntimeException when the file exists but cannot be read or decoded, or when it cannot be told whether it
     * exists
     */
    public static List<DseNodeRecord> readPeers(Path nodesDirectory)
    {
        Path peers = nodesDirectory.resolve("peers");
        if (absent(peers))
            return Collections.emptyList();
        try (InputStream in = new BufferedInputStream(Files.newInputStream(peers, StandardOpenOption.READ)))
        {
            if (CC4NodesFileReader.holdsNoPeer(in))
                return Collections.emptyList();
            Collection<RecordFields> entries = CC4NodesFileReader.createCC4ObjectMapper().readerFor(new TypeReference<Collection<RecordFields>>() {}).readValue(in);
            List<DseNodeRecord> records = new ArrayList<>(entries.size());
            for (RecordFields entry : entries)
                records.add(entry.toRecord(true));
            return records;
        }
        catch (Exception e)
        {
            throw new RuntimeException("cannot decode " + peers + ": " + e.getMessage(), e);
        }
    }

    /**
     * Whether a nodes file is known not to exist; a file whose existence cannot be told (a directory the process may not
     * search) is an error, never taken for an absent file.
     *
     * @throws RuntimeException when it cannot be told whether the file exists
     */
    private static boolean absent(Path file)
    {
        if (Files.exists(file))
            return false;
        if (Files.notExists(file))
            return true;
        throw new RuntimeException("cannot tell whether " + file + " exists (may this process search " + file.getParent() + "?)");
    }

    /** The keys of an entry of {@code nodes/local} or {@code nodes/peers} that {@link DseNodeRecord} holds. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class RecordFields
    {
        @JsonProperty("peer")
        InetAddressAndPort peer;
        @JsonProperty("broadcast_address")
        InetAddress broadcastAddress;
        @JsonProperty("listen_address")
        InetAddress listenAddress;
        @JsonProperty("host_id")
        UUID hostId;
        @JsonProperty("cluster_name")
        String clusterName;
        @JsonProperty("data_center")
        String dataCenter;
        @JsonProperty("rack")
        String rack;
        @JsonProperty("release_version")
        String releaseVersion;
        @JsonProperty("dse_version")
        String dseVersion;
        @JsonProperty("schema_version")
        UUID schemaVersion;
        /** the tokens, only counted: decoding them needs the partitioner */
        @JsonProperty("tokens")
        JsonNode tokens;
        @JsonProperty("workloads")
        List<String> workloads;
        @JsonProperty("graph")
        Boolean graph;
        @JsonProperty("workload")
        String workload;

        DseNodeRecord toRecord(boolean isPeer)
        {
            InetAddress address = peer == null ? null : peer.getAddress();
            return new DseNodeRecord(isPeer ? address : null,
                                     isPeer ? null : broadcastAddress,
                                     isPeer ? null : listenAddress,
                                     hostId, clusterName, dataCenter, rack, releaseVersion, dseVersion, schemaVersion,
                                     tokens != null && tokens.isArray() ? tokens.size() : null,
                                     new DseNodeWorkloads(address, workloads, graph, workload));
        }
    }
}
