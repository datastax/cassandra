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
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The report of the Paxos rollback mode of {@code dsepreflight} ({@link PaxosRollback}), written as JSON with
 * {@code --json}: the partitions with an open Paxos round in the {@code system.paxos} sstables of a node about to be
 * rolled back to DSE, and the serial read each needs. Its {@link #kind} is {@link #KIND}, which the aggregate mode
 * skips.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class PaxosRollbackReport
{
    public static final int FORMAT_VERSION = 1;
    public static final String KIND = "paxos_rollback";
    /** {@link #result}: no partition has an open round */
    public static final String NOTHING_TO_READ = "NOTHING_TO_READ";
    /** {@link #result}: every open round is covered by the reads listed (or needs none) */
    public static final String READS_LISTED = "READS_LISTED";
    /** {@link #result}: open rounds, none of which needs a read (replication factor 2, or LocalStrategy) */
    public static final String NO_READ_NEEDED = "NO_READ_NEEDED";
    /**
     * {@link #result}: some open rounds need a decision of the operator (they cannot be protected, their serial consistency
     * is not known, or the reads listed may not be enough), or the report lists {@link #problems}
     */
    public static final String NEEDS_ATTENTION = "NEEDS_ATTENTION";

    /** What to do for a partition at one serial consistency. */
    public enum Action
    {
        /** the read is listed */
        READ,
        /** no read is needed: every quorum at that consistency includes all the other replicas */
        SKIPPED,
        /** the node is the whole quorum at that consistency: no read protects the round */
        CANNOT_PROTECT,
        /** the read is listed, but the replication rules could not be applied: check by hand whether it is enough */
        CHECK,
        /** SERIAL and LOCAL_SERIAL may be different quorums and the serial consistency of the table is not given: no read is listed */
        CHOOSE,
        /** no read is listed: the partition cannot be located (unknown table, undecodable key), or the replication contradicts this node */
        MANUAL
    }

    @JsonProperty("tool")
    public String tool = NodeReport.TOOL;
    @JsonProperty("format_version")
    public int formatVersion = FORMAT_VERSION;
    @JsonProperty("kind")
    public String kind = KIND;
    @JsonProperty("software_version")
    public String softwareVersion;
    @JsonProperty("generated_at")
    public String generatedAt;
    @JsonProperty("hostname")
    public String hostname;
    /** {@link #NOTHING_TO_READ}, {@link #READS_LISTED}, {@link #NO_READ_NEEDED} or {@link #NEEDS_ATTENTION} */
    @JsonProperty("result")
    public String result;
    /** this node's data center; null when unknown */
    @JsonProperty("data_center")
    public String dataCenter;
    /** where {@link #dataCenter} comes from */
    @JsonProperty("data_center_source")
    public String dataCenterSource;
    /** whether nodes/local and nodes/peers name a single data center; null when they cannot tell */
    @JsonProperty("single_data_center")
    public Boolean singleDataCenter;
    @JsonProperty("data_directories")
    public List<String> dataDirectories = new ArrayList<>();
    /** the directories taken from DSE's defaults because DSE's {@code cassandra.yaml} does not set them */
    @JsonProperty("defaults_assumed")
    public List<String> defaultsAssumed = new ArrayList<>();
    /** the {@code pre-hcd-<timestamp>} snapshot read; null when none was */
    @JsonProperty("snapshot")
    public String snapshot;
    /**
     * until when the reads listed cover the Paxos state read: a commit that expires sooner and before the proposal of its
     * row is not counted on; the tool is run again if DSE does not start by then
     */
    @JsonProperty("valid_until")
    public String validUntil;
    /** whether the operator stated with {@code --not-restored} that system has not been restored yet */
    @JsonProperty("not_restored")
    public boolean notRestored;
    /** the schemas read: {@code data directories}, {@code snapshot <tag>} */
    @JsonProperty("schemas")
    public List<String> schemas = new ArrayList<>();
    /**
     * every directory read for system.paxos sstables (data directories, snapshot, {@code --paxos-dir}), those that hold none
     * included; a {@code --paxos-dir} already read is not repeated, and one whose normalized path is not, or cannot be shown
     * to be, the directory it names (a ".." after a symbolic link) is not listed
     */
    @JsonProperty("directories")
    public List<Directory> directories = new ArrayList<>();
    @JsonProperty("sstables")
    public List<SSTable> sstables = new ArrayList<>();
    /** the rows of {@code system.paxos} read, all sstables together */
    @JsonProperty("rows_read")
    public long rowsRead;
    @JsonProperty("partitions")
    public List<Partition> partitions = new ArrayList<>();
    /**
     * the problems that make the result {@link #NEEDS_ATTENTION} besides the partitions: unreadable sstables, schemas or nodes
     * files, a missing system.paxos in the data directories or the snapshot, a directory of {@code --paxos-dir} that holds no sstable
     * of system.paxos, that is already read or whose normalized path is not, or cannot be shown to be, the directory it names, a
     * system that looks already restored or partly restored, a data center that contradicts the nodes files or that nothing names,
     * a keyspace of {@code --serial-consistency} in no schema
     */
    @JsonProperty("problems")
    public List<String> problems = new ArrayList<>();
    /** the cqlsh file of the SERIAL reads written with {@code --cql}; null when none was */
    @JsonProperty("cql_file")
    public String cqlFile;
    /** the cqlsh file of the LOCAL_SERIAL reads written with {@code --cql}, next to {@link #cqlFile}; null when none was */
    @JsonProperty("cql_local_serial_file")
    public String cqlLocalSerialFile;

    /** One directory of {@code system.paxos} read. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class Directory
    {
        /** {@code data directory}, {@code snapshot <tag>} or {@code --paxos-dir <dir>} */
        @JsonProperty("source")
        public String source;
        @JsonProperty("path")
        public String path;
        /** the sstables found in it, those of another table and those that cannot be read included; null when {@link #error} is set */
        @JsonProperty("sstables")
        public Integer sstables;
        /** of {@link #sstables}, those of another table than {@code system.paxos}, which are not read; null when {@link #error} is set */
        @JsonProperty("other_table_sstables")
        public Integer otherTableSSTables;
        /** the rows of {@code system.paxos} read from its sstables; null when {@link #error} is set */
        @JsonProperty("rows")
        public Long rows;
        /** why its sstables could not be listed; null when they were */
        @JsonProperty("error")
        public String error;
    }

    /** One sstable of {@code system.paxos} read. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class SSTable
    {
        /** {@code data directory}, {@code snapshot <tag>} or {@code --paxos-dir <dir>} */
        @JsonProperty("source")
        public String source;
        /** the sstable, without its component suffix */
        @JsonProperty("path")
        public String path;
        /** the sstable format and version, such as {@code bti bb} */
        @JsonProperty("format")
        public String format;
        @JsonProperty("rows")
        public long rows;
        /** the rows with an open round */
        @JsonProperty("open_rows")
        public long openRows;
        /** why it could not be read; null when it was */
        @JsonProperty("error")
        public String error;
    }

    /** A partition with an open round in at least one sstable. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class Partition
    {
        @JsonProperty("cf_id")
        public String cfId;
        /** the serialized partition key, in hex without {@code 0x} */
        @JsonProperty("row_key")
        public String rowKey;
        /** null when the table is in no schema read */
        @JsonProperty("keyspace")
        public String keyspace;
        @JsonProperty("table")
        public String table;
        /** the partition key as a CQL condition, such as {@code id = 10}; null when it cannot be rendered */
        @JsonProperty("where")
        public String where;
        /** the serial read, without the consistency; null when there is none */
        @JsonProperty("select")
        public String select;
        /** what to do at each serial consistency considered */
        @JsonProperty("reads")
        public List<Read> reads = new ArrayList<>();
        /** the notes on the partition: a key component rendered as a blob, a reason for manual handling */
        @JsonProperty("notes")
        public List<String> notes = new ArrayList<>();
        /** the open rows, one per sstable */
        @JsonProperty("open_rows")
        public List<OpenRow> openRows = new ArrayList<>();

        /** Whether the operator has to decide something for this partition. */
        @JsonIgnore
        boolean needsAttention()
        {
            return reads.stream().anyMatch(r -> r.action != Action.READ && r.action != Action.SKIPPED);
        }
    }

    /** What to do for a partition at one serial consistency. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class Read
    {
        /** {@code SERIAL} or {@code LOCAL_SERIAL}; null for {@link Action#CHOOSE} and for a partition that cannot be located */
        @JsonProperty("consistency")
        public String consistency;
        @JsonProperty("action")
        public Action action;
        @JsonProperty("reason")
        public String reason;

        Read()
        {
        }

        Read(String consistency, Action action, String reason)
        {
            this.consistency = consistency;
            this.action = action;
            this.reason = reason;
        }
    }

    /** A row of {@code system.paxos} with an open round, in one sstable. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class OpenRow
    {
        @JsonProperty("source")
        public String source;
        @JsonProperty("sstable")
        public String sstable;
        /**
         * by {@code proposal_version}: {@code DSE} (256 or more), {@code HCD 2.0} (from the 4.0 version, below 256) or
         * {@code unknown} (below the 4.0 version, or no {@code proposal_version})
         */
        @JsonProperty("written_by")
        public String writtenBy;
        /** {@code proposal_version}; null when the row has none */
        @JsonProperty("version")
        public Integer version;
        @JsonProperty("proposal_ballot")
        public String proposalBallot;
        /** null when the row has no commit */
        @JsonProperty("most_recent_commit_at")
        public String mostRecentCommitAt;
        /** whether the proposal changes nothing (a serial read or an LWT that did not apply); null when it cannot be told */
        @JsonProperty("empty_proposal")
        public Boolean emptyProposal;
        @JsonProperty("note")
        public String note;
    }
}
