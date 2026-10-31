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

package org.apache.cassandra.tools.dsepreflight;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The report of the preflight inspection of one DSE node, written as JSON with {@code --json}. Plain fields; unknown
 * properties are ignored on reading, and {@link #FORMAT_VERSION} changes only when a field changes meaning.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class NodeReport
{
    public static final String TOOL = "dsepreflight";
    public static final int FORMAT_VERSION = 1;
    public static final String READY = "READY";
    public static final String BLOCKED = "BLOCKED";
    /** The {@link #kind} of a node report. */
    public static final String NODE = "node";

    @JsonProperty("tool")
    public String tool = TOOL;
    @JsonProperty("format_version")
    public int formatVersion = FORMAT_VERSION;
    /** {@link #NODE}: what this report describes */
    @JsonProperty("kind")
    public String kind = NODE;
    /** the release of the software the tool came with */
    @JsonProperty("software_version")
    public String softwareVersion;
    @JsonProperty("generated_at")
    public String generatedAt;
    /** the host name the tool ran on */
    @JsonProperty("hostname")
    public String hostname;
    /** {@link #READY} or {@link #BLOCKED} */
    @JsonProperty("result")
    public String result;
    @JsonProperty("node")
    public Node node = new Node();
    @JsonProperty("peers")
    public List<Peer> peers = new ArrayList<>();
    @JsonProperty("findings")
    public List<Finding> findings = new ArrayList<>();
    @JsonProperty("upgrade_path")
    public UpgradePath upgradePath = new UpgradePath();
    /** what the access control checks read */
    @JsonProperty("access_controls")
    public AccessControls accessControls = new AccessControls();
    /** the files and directories read, by role, and what the inspection measured in them (sizes, free space, segment counts) */
    @JsonProperty("inputs")
    public Map<String, Object> inputs = new LinkedHashMap<>();
    /** the output of each {@code nodetool} command run with {@code --nodetool}, by command */
    @JsonProperty("live")
    public Map<String, LiveCommand> live = new LinkedHashMap<>();

    /** This node, from DSE's {@code nodes/local} and the {@code ds_version.txt} of its installation. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class Node
    {
        @JsonProperty("host_id")
        public String hostId;
        @JsonProperty("broadcast_address")
        public String broadcastAddress;
        @JsonProperty("listen_address")
        public String listenAddress;
        @JsonProperty("cluster_name")
        public String clusterName;
        @JsonProperty("data_center")
        public String dataCenter;
        @JsonProperty("rack")
        public String rack;
        /** {@code release_version} of {@code nodes/local}, such as {@code 4.0.0.6865} */
        @JsonProperty("release_version")
        public String releaseVersion;
        /** {@code dse_version} of {@code nodes/local}, such as {@code 6.8.65} */
        @JsonProperty("dse_version")
        public String dseVersion;
        /** the version in {@code ds_version.txt} of the DSE installation, such as {@code 6.8.65-4a7b9eb} */
        @JsonProperty("installed_dse_version")
        public String installedDseVersion;
        @JsonProperty("schema_version")
        public String schemaVersion;
    }

    /** A peer as DSE recorded it in {@code nodes/peers}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class Peer
    {
        @JsonProperty("address")
        public String address;
        @JsonProperty("host_id")
        public String hostId;
        @JsonProperty("data_center")
        public String dataCenter;
        @JsonProperty("rack")
        public String rack;
        @JsonProperty("release_version")
        public String releaseVersion;
        @JsonProperty("dse_version")
        public String dseVersion;
        @JsonProperty("schema_version")
        public String schemaVersion;
        /** whether the recorded release has the major version of this build: the peer already runs this software */
        @JsonProperty("upgraded")
        public boolean upgraded;
    }

    /** How this node is upgraded. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class UpgradePath
    {
        /** the DSE release, such as {@code DSE 6.8.65 (release 4.0.0.6865, installation 6.8.65-4a7b9eb)} */
        @JsonProperty("dse_release")
        public String dseRelease;
        /** the SAI on-disk version to pin, {@code aa} or {@code ca}; null when the release is not 6.8 or 6.9 */
        @JsonProperty("sai_version")
        public String saiVersion;
        /** the settings to carry into the configuration of HCD 2.0, by key */
        @JsonProperty("settings")
        public Map<String, String> settings = new LinkedHashMap<>();
        /** the upgrade steps that apply to this node, with what this node needs */
        @JsonProperty("upgrade_steps")
        public List<String> upgradeSteps = new ArrayList<>();
    }

    /** What the access control checks of a node read. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class AccessControls
    {
        /** whether DSE checks permissions: {@code DseAuthorizer} with {@code authorization_options.enabled}, or any authorizer but {@code AllowAllAuthorizer} */
        @JsonProperty("authorization_enabled")
        public Boolean authorizationEnabled;
        /** the rows of {@code system_auth.role_permissions} read from this node's replica; 0 when no data directory holds the table, null when it could not be read */
        @JsonProperty("role_permissions_rows")
        public Integer rolePermissionsRows;
    }

    /** One {@code nodetool} command. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class LiveCommand
    {
        @JsonProperty("exit_code")
        public Integer exitCode;
        @JsonProperty("output")
        public String output;
        /** why the command could not be run or did not finish */
        @JsonProperty("error")
        public String error;
    }

    @JsonIgnore
    public long count(Finding.Severity severity)
    {
        return findings.stream().filter(f -> f.severity == severity).count();
    }

    @JsonIgnore
    public boolean blocked()
    {
        return count(Finding.Severity.BLOCKING) > 0;
    }
}
