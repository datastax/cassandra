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
 * The report of the preflight inspection of one DSE node, written as JSON with {@code --json} and read back by the
 * aggregate mode, which takes only complete reports of {@link #FORMAT_VERSION} written by the release of the tool it
 * runs. Plain fields; unknown properties of the report and of its sections are ignored on reading (not those of a
 * {@link Finding}, whose fields are fixed), and {@link #FORMAT_VERSION} changes only when a field changes meaning. The
 * report of a DSE 5.1 node has the same fields, read from DSE 5.1's {@code system.local} and {@code system.peers} where
 * those of DSE 6 come from {@code nodes/local} and {@code nodes/peers}, and a section of its own ({@link #dse51}),
 * absent from the report of a DSE 6 node.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class NodeReport
{
    public static final String TOOL = "dsepreflight";
    public static final int FORMAT_VERSION = 1;
    public static final String READY = "READY";
    public static final String BLOCKED = "BLOCKED";
    /** The {@link #kind} of a node report; an aggregate report has {@code aggregate}. */
    public static final String NODE = "node";
    public static final String AGGREGATE = "aggregate";

    @JsonProperty("tool")
    public String tool = TOOL;
    @JsonProperty("format_version")
    public int formatVersion = FORMAT_VERSION;
    /** {@link #NODE}: what the aggregate mode reads; it skips the reports of kind {@link #AGGREGATE} */
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
    /** what the access control checks read, for the aggregate mode */
    @JsonProperty("access_controls")
    public AccessControls accessControls = new AccessControls();
    /** the files and directories read, by role, and what the inspection measured in them (sizes, free space, segment counts) */
    @JsonProperty("inputs")
    public Map<String, Object> inputs = new LinkedHashMap<>();
    /** the output of each {@code nodetool} command run with {@code --nodetool}, by command */
    @JsonProperty("live")
    public Map<String, LiveCommand> live = new LinkedHashMap<>();
    /** what the inspection of a DSE 5.1 node read beyond the fields of a DSE 6 node's report; null for a DSE 6 node */
    @JsonProperty("dse51")
    public Dse51 dse51;

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

    /** The settings of a DSE 5.1 node the upgrade depends on, as the inspection read them. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class Dse51
    {
        /** the DSE release the report is of: {@code DSE 5.1} */
        @JsonProperty("source")
        public String source;
        /** {@code start_rpc} of DSE's {@code cassandra.yaml} as written; null when it does not set it */
        @JsonProperty("start_rpc")
        public String startRpc;
        /** the value of {@code -Dcassandra.start_rpc} in DSE's {@code jvm.options}; null when absent */
        @JsonProperty("start_rpc_jvm_option")
        public String startRpcJvmOption;
        /** whether Thrift starts with DSE, from the two above ({@code true} when neither sets it) */
        @JsonProperty("thrift_on")
        public Boolean thriftOn;
        /** the output of {@code nodetool statusthrift}, with {@code --nodetool}; null otherwise */
        @JsonProperty("thrift_live")
        public String thriftLive;
        /** {@code native_transport_port} of DSE's {@code cassandra.yaml}, or its default */
        @JsonProperty("native_transport_port")
        public String nativeTransportPort;
        /** {@code native_transport_port_ssl} of DSE's {@code cassandra.yaml}; null when it does not set it */
        @JsonProperty("native_transport_port_ssl")
        public String nativeTransportPortSsl;
        /**
         * whether DSE enforces row-level access control: {@code DseAuthorizer}, {@code authorization_options.enabled}
         * and {@code allow_row_level_security}; null when it is not known
         */
        @JsonProperty("row_level_access_control_enforced")
        public Boolean rowLevelAccessControlEnforced;
        /** the Java feature release the tool runs on, the one HCD 2.0 is to run on */
        @JsonProperty("tool_java_version")
        public String toolJavaVersion;
        /** the TLS settings of {@code server_encryption_options} and {@code client_encryption_options}, by key */
        @JsonProperty("tls")
        public Map<String, String> tls = new LinkedHashMap<>();
        /**
         * the materialized views restricted to rows in this node's schema, as {@code ks.view (tenant column c)}: DSE
         * 5.1 keeps a {@code RESTRICT ROWS} on a view on the node that ran it only, so the aggregate names the nodes;
         * null when the schema or its views could not be read, or its inspection failed
         */
        @JsonProperty("restricted_views")
        public List<String> restrictedViews;
        /**
         * the rows of {@code system_schema.tables} named after a materialized view, or with its id, in this node's
         * schema, which a failed {@code UNRESTRICT ROWS} on the view left on the node that ran it; null when the schema
         * or its views could not be read, or its inspection failed
         */
        @JsonProperty("view_table_rows")
        public List<String> viewTableRows;
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
