<!--
Copyright IBM Corp.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Preflight: the checks of `dsepreflight`

This page describes every check of the preflight tool, `dsepreflight`, for engineers and operators new to the project.
The procedure that uses the tool is [RUNBOOK.md](RUNBOOK.md) (section 1 item 0, section 3a P0, section 4 steps 5
and 6, appendix A.11 and appendix B). The security rules behind some checks are in [SECURITY.md](SECURITY.md). The
code is the reference: `src/java/org/apache/cassandra/tools/dsepreflight/`. If this page and the tool's output ever
disagree, the output wins. Terms such as "mixed phase", SAI and `bb` are explained in the glossary of
[OVERVIEW.md](OVERVIEW.md); P0 to P9 are the checks of runbook section 3a, made before each node. The sections
below, but the last, describe the tool on a DSE 6.8 or 6.9 node; on a DSE 5.1.51 node it takes another inspection and
has one more mode, the rollback check, both in section 7 below, whose procedure is [RUNBOOK-DSE51.md](RUNBOOK-DSE51.md).

## 1. What the tool is for

Before a DSE 6.8 or 6.9 node (or a DSE 5.1.51 node, section 7 below) is upgraded to HCD 2.0, the tool inspects it and
says whether it can be upgraded, and how. Each thing it reports is a **finding**: a stable code, a severity and a
message. Blocking findings and warnings also carry a "What to do" line (the remedy); notes do not. A node with no
blocking finding is `READY`, unless the live state asked for with `--nodetool` could not be read completely
(`LIVE_UNAVAILABLE`): it is then `INCOMPLETE`, never `READY`.

**Where it runs.** On the DSE node itself, from the unpacked HCD 2.0 tarball (`<hcd>/tools/bin/dsepreflight`), with
JDK 17 (`JAVA_HOME=<jdk17>`; the tarball does not need to be installed). DSE can be running or stopped. Run it as
root or as the user DSE runs as, so that it can read DSE's files.

**What it reads.** Offline, from DSE's files:

| Input | Where the tool finds it (each can be overridden by an option) |
|---|---|
| DSE's `cassandra.yaml` | `/etc/dse/cassandra/` (package install), else `<dse home>/resources/cassandra/conf/` |
| DSE's `dse.yaml` | `/etc/dse/dse.yaml`, else `<dse home>/resources/dse/conf/dse.yaml` |
| `cassandra-rackdc.properties` | next to `cassandra.yaml` |
| The DSE installation (`ds_version.txt`) | `--dse-home`, else `DSE_HOME`, else `/opt/dse`, else `/usr/share/dse` |
| The data, metadata, commit log and hints directories | the directories of `cassandra.yaml`, else DSE's defaults under `/var/lib/cassandra` |
| `nodes/local` and `nodes/peers` | DSE's metadata directory: this node's release, identity, workloads, and what it knows of its peers |
| The schema and the grants | the sstables of `system_schema` and `system_auth.role_permissions` in the data directories, in DSE 6's trie format only (a `big` sstable is not opened) |
| The sstables, hints and commit log segments | their file names (and, for sstables, the transaction logs of each table directory; for DSE hints files, the header of each) |
| The live state (optional) | the output of read-only `nodetool` commands, with `--nodetool` |

The YAML files are read as plain YAML maps, never through HCD 2.0's configuration loader, so DSE-only keys do not
stop the tool. The tool runs as a client: it does not read or need the configuration of the HCD 2.0 tarball it runs
from.

**What it writes.** Only its own output: the screen report, and the files named with `--json` (and `--cql` in the
rollback mode), each written to a new temporary file next to it and then renamed. It never writes to DSE's data,
configuration or metadata, and opens no sstable that it could change (section 3, `UNREADABLE_SCHEMA`; section 5). No
secret reaches the output: the settings to carry name the TLS keys, never their passwords, and `system_auth.roles` is
not read. The `nodetool` commands it runs are read-only: `version`, `status`, `describecluster`, `nodesyncservice
status`, `compactionstats`, `netstats`, `listpendinghints`, `gossipinfo` and `tpstats` (the last two are only recorded
in the report).

**Flush first.** The schema and the grants are read from sstables, so a change a running DSE node has not flushed yet (a
`GRANT`, a `RESTRICT`, a schema change) is not seen. Run `nodetool flush system_schema` and `nodetool flush system_auth`
right before the tool, one keyspace per command, as nodetool reads a second argument as a table (finding `SSTABLES_ONLY`
reminds of it in every report, naming the two commands).

## 2. Modes, command lines and exit codes

The usage, as `--help` prints it:

```
usage: dsepreflight [options]                                 inspect this DSE 6 or DSE 5.1 node
       dsepreflight --aggregate <directory> [--json <file>]   check the reports of a cluster
       dsepreflight --paxos-rollback [options]                list the open Paxos rounds of a node to roll back to DSE
       dsepreflight --rollback-check [options]                check a node to roll back to DSE 5.1
```

The rollback check (`--rollback-check`) and the option `--jvm-options` apply to DSE 5.1 only: section 7 below.

| Mode | Typical command | When |
|---|---|---|
| Node | `JAVA_HOME=<jdk17> <hcd>/tools/bin/dsepreflight --nodetool <dse home>/bin/nodetool --json /tmp/preflight-$(hostname).json` | On every DSE node before the first upgrade (runbook section 1 item 0), and on the next node before each upgrade (section 3a, P0) |
| Aggregate | `dsepreflight --aggregate <directory> [--json <file>]` | Once every node's JSON report is in one directory, with the same release of the tool, within 24 hours of the node reports (section 1 item 0). It reads only the reports, so it runs anywhere |
| Rollback | `JAVA_HOME=<jdk17> <hcd>/tools/bin/dsepreflight --paxos-rollback --cql /tmp/paxos-reads.cql --json /tmp/paxos-$(hostname).json` | On a node being rolled back to DSE, with HCD 2.0 stopped after its drain and DSE not started (section 4, steps 5 and 6) |

**Options.**

| Option | Mode | Meaning |
|---|---|---|
| `--dse-home <dir>`, `--conf-dir <dir>`, `--cassandra-yaml <file>`, `--dse-yaml <file>`, `--rackdc <file>` | node, rollback | Where DSE's installation and configuration files are, when the defaults above do not find them |
| `--data-dir <dir>` (repeatable), `--metadata-dir <dir>` | node, rollback | Instead of `data_file_directories` and `metadata_directory` of `cassandra.yaml` |
| `--commitlog-dir <dir>`, `--hints-dir <dir>` | node | Instead of `commitlog_directory` and `hints_directory` of `cassandra.yaml` |
| `--nodetool <path>` | node | DSE's `nodetool`, to read the live state of the running node |
| `--nodetool-arg=<arg>` (repeatable) | node | An argument put before every `nodetool` command; write it `--nodetool-arg=<arg>` when it starts with a dash. For example `--nodetool-arg=-u --nodetool-arg=<user> --nodetool-arg=-pwf --nodetool-arg=<password file>`: give the password in a file with `-pwf`, never with `-pw`, which the process list shows |
| `--nodetool-java-home <dir>` | node | The Java installation DSE's `nodetool` runs on (its `JAVA_HOME`, first in its `PATH`); default the Java of the tool. The `nodetool` of DSE 6.8.65 and 6.9.26 runs on the tool's JDK 17; DSE 5.1's needs a Java 8. With `--nodetool`, a directory without a runnable `bin/java` is a usage error (exit 2); without it, the option is ignored |
| `--nodetool-timeout <seconds>` | node | The time limit of each `nodetool` command; default 120 |
| `--json <file>` | all | Also write the report as JSON. In the aggregate mode it refuses to replace an existing file that is not an aggregate report of the tool (exit 2), so that a node report named by mistake is not lost |
| `-h`, `--help` | all | Print the help |
| `--snapshot <tag>`, `--paxos-dir <dir>` (repeatable), `--not-restored`, `--dc <name>`, `--serial-consistency <keyspace>=<level>` (repeatable), `--cql <file>` | rollback | See section 5 below (`--snapshot` also with `--rollback-check`) |
| `--jvm-options <file>`, `--rollback-check`, `--saved-caches-dir <dir>` | DSE 5.1 only | See section 7 below |

The rollback-only options outside the rollback mode, and `--aggregate`, the `--nodetool` options, `--commitlog-dir`
and `--hints-dir` in it, are usage errors (exit 2). With `--aggregate`, every other option but `--json` and `--help` is
a usage error too.

**Exit codes.**

| Mode | 0 | 1 | 2 | 3 |
|---|---|---|---|---|
| Node | `READY`: no blocking finding (warnings allowed) | `BLOCKED`: at least one blocking finding | Wrong usage, or an error that stopped the inspection | `INCOMPLETE`: no blocking finding, but the live state asked for with `--nodetool` not read completely (`LIVE_UNAVAILABLE`) |
| Aggregate | `READY` | The cluster is not ready (any blocking finding of the aggregate, `NODE_BLOCKED` and `NODE_INCOMPLETE` included) | Wrong usage, the directory is not a directory, or an error | – |
| Rollback | `NOTHING_TO_READ`, `READS_LISTED` or `NO_READ_NEEDED` | `NEEDS_ATTENTION` | Wrong usage (the error says which; a `--snapshot` that is a path rather than a tag), no `pre-hcd-<timestamp>` snapshot (or several without `--snapshot`, or none of the tag given; the error adds the problems found before it), no readable schema, or an error | – |

**The JSON report.** Each report is one JSON object with `tool` (`dsepreflight`), `format_version` (1), `kind`
(`node`, `aggregate` or `paxos_rollback`), `software_version` (the HCD 2.0 release of the tool), `generated_at` and
`result` (for a node report `READY`, `BLOCKED` or `INCOMPLETE`).

- A node report adds `hostname`; `node` (host id, addresses, cluster name, data center, rack, releases and schema
  version from `nodes/local`, and `installed_dse_version` from `ds_version.txt`); `peers` (from `nodes/peers`);
  `findings` (each with `severity`, `code`, `message`, and `remedy` except on notes); `upgrade_path`; `access_controls`
  (`authorization_enabled`, whether DSE checks permissions, and `role_permissions_rows`, the rows of
  `system_auth.role_permissions` read, for the aggregate's `NO_GRANTS_READ`); `inputs` (the files and directories read,
  the schema summary, the sstable bytes, the free space, the commit log segments by version); and `live` (the `nodetool`
  outputs).
- An aggregate report adds `reports_read`, `findings`, `nodes`, `data_centers` and `warnings_by_code` (section 4 below).
- A rollback report adds the data center and its source, the snapshot, `valid_until` (until when its reads hold), the
  directories and sstables read, the `partitions` with their reads, the `problems`, and the cqlsh files written
  (section 5 below).

The screen report of a node prints a header (node, DSE release, number of peers:
`none (no nodes/peers file, see NO_PEERS_FILE)` when `NO_PEERS_FILE` is reported, `not used (see UNREADABLE_INPUT)` when
`nodes/peers` cannot be read or trusted), the result with its counts (`Result: READY (0 blocking, 2 warnings, 5 notes)`,
for example, or `Result: INCOMPLETE (0 blocking, 3 warnings, 3 notes; the live state was not checked completely, see
LIVE_UNAVAILABLE)`), the findings by severity (`[<code>] <message>`, then `What to do:` and the remedy), the upgrade
path with its "Upgrade steps for this node", and the exit code of each `nodetool` command. The peers, the inputs, the
access controls and the `nodetool` outputs are in the JSON only.

**The upgrade path.** Every node report also gives, under `upgrade_path`:

- `dse_release`: the DSE release of the node (and of its installation).
- `sai_version`: the SAI on-disk version to pin (`aa` for DSE 6.8, `ca` for DSE 6.9; runbook section 2).
- `settings`: the settings to carry into HCD 2.0's configuration. The fixed ones (`internode_compatibility_mode:
  DSE_6`, `storage_compatibility_mode: HCD_1`, `paxos_variant: v1`, `sstable.selected_format: bti`,
  `uuid_sstable_identifiers_enabled: false`, the SAI pin line for `jvm-server.options`), and those taken from DSE's
  files: cluster name, tokens, partitioner, snitch and `cassandra-rackdc.properties`, seeds, addresses, ports,
  directories, internode compression and encryption, client encryption, and the security classes.
- `upgrade_steps`: the steps of the upgrade that apply to this node, each with what the tool found for it, labelled
  `Preflight`, `EC2 names` (with an EC2 snitch), `Internode TLS on DSE 6.9` (with internode TLS on DSE 6.9.7 and later:
  DSE's legacy TLS port), `Security`, `DSE core only`, `Materialized views`, `NodeSync`, `Replication of system_auth`,
  `Replication of the other system keyspaces`, `SAI version pin`, `During the mixed phase`, `Sstable formats`,
  `Drivers`, `Hints`, `Snapshot`, `Install` and `Internode TLS` (with internode TLS). Most steps whose inputs could not
  all be read say `not checked completely (not read: …)` and name them (for example `dse.yaml`, the schema or the schema
  tables not read, `system_auth.role_permissions`, `nodes/local`, every data directory); the `Sstable formats` step says
  `not checked (the data directories could not be inspected completely)`, and the `Replication of system_auth` and
  `Replication of the other system keyspaces` steps `not checked (the schema could not be read, or its inspection
  failed)` or `not checked (…)` with its reason; the `Replication of the other system keyspaces` step also says
  `with one data center they stay as they are` (the data center of every node known) or
  `incomplete, see INSPECTION_ERROR`, and names the keyspaces whose replication cannot be read (`… not checked (its
  replication cannot be read, see UNLOADABLE_KEYSPACE)`, `their` for several); with `NO_PEERS_FILE`, the `Replication of
  system_auth` step, when it finds enough replicas, and the `with one data center they stay as they are` form of the
  other keyspaces' step add `(no nodes/peers: this node counted alone, see NO_PEERS_FILE)`, and `see
  KEYSPACE_REPLICATION (<keyspaces>; no nodes/peers: …)` carries it inside the parentheses; the `Hints` step says `no
  DSE hints file pending`, `<n> target(s) with DSE hints files pending; see PENDING_HINTS`, `not checked (the hints
  directory cannot be listed); see PENDING_HINTS` when that directory cannot be listed, `<n> host id(s) of no known node
  with DSE hints files; see PENDING_HINTS` for the host ids of the warning, and `<n> host id(s) with DSE hints files,
  not checked against nodes/local and nodes/peers (not both read and trusted); see PENDING_HINTS` when it cannot tell
  whose files they are. The steps cite no runbook section; the findings below give the sections that cover them.

## 3. Node mode: every finding

**Blocking** findings make the node `BLOCKED` (exit 1): resolve them (most on DSE), then run the tool again.
Without one, a node whose live state could not be read completely (`LIVE_UNAVAILABLE`) is `INCOMPLETE` (exit 3): make
`nodetool` reach DSE and run the tool again.
**Warnings** allow the upgrade but ask for an action or a decision: read each one. **Info** findings are notes. A code
can have more than one severity; the table says when. Sections cited are those of [RUNBOOK.md](RUNBOOK.md) (the same
numbers in [RUNBOOK-REFERENCE.md](RUNBOOK-REFERENCE.md), which keeps the full text).

### Release and build

| Code | Severity | What it checks | What the operator does |
|---|---|---|---|
| `DSE_RELEASE` | blocking; info on DSE 6.8 and 6.9 | The release `nodes/local` records. Blocking when it is not a DSE 6 release (`4.0.0.<build>`), or is a DSE 6 release other than 6.8 or 6.9. Info when it is DSE 6.8 (accepted, 6.9 preferred) or DSE 6.9: both notes say that the tool does not check the patch release | Run the tool on DSE 6.8 or 6.9 nodes only (a node already on HCD 2.0 needs none); upgrade every node to the latest DSE 6.9 (or 6.8) patch release first, with DataStax's procedure (section 1, "Required DSE release") |
| `INSTALLED_RELEASE_MISMATCH` | blocking | The release of the DSE installation (`DSVersion` in `ds_version.txt`) against the DSE release this node last ran (`nodes/local`) | Restart DSE so that it runs the installed release, or reinstall the release it runs (section 3a, P3) |
| `INSTALLED_RELEASE_UNKNOWN` | warning | `ds_version.txt` cannot be read, is missing, or no DSE installation is found, so the two releases are not compared | Pass `--dse-home`, or check with `dse -v` that every node runs the same release |
| `MIXED_RELEASES` | blocking | A DSE 6 peer in `nodes/peers` with another release than this node | Bring every DSE node to the same release, 6.9 preferred (section 3a, P3) |
| `PEER_RELEASE` | blocking | A peer in `nodes/peers` whose release is neither DSE 6 nor HCD 2.0 | Upgrade those nodes to the cluster's DSE release; a stale entry (a node that left) is removed with `nodetool removenode` on DSE |
| `PEER_RELEASE_UNKNOWN` | warning | A peer whose release `nodes/peers` does not record | Check its release with `nodetool version`, or with that node's report |
| `UPGRADED_PEERS` | info | Peers already running HCD 2.0: the cluster is in the mixed phase (expected at P0 then) | Nothing |
| `MIXED_PHASE_MARKER` | info | The upgrade marker `dse-upgrade-in-progress` in the metadata directory: HCD 2.0 wrote it at its first `DSE_6` start (or someone created it by hand), and a rollback leaves it, in the mixed phase and after the last rollback alike | Keep it while any node runs HCD 2.0 and, after the last rollback, while the node may be upgraded again (section 4); delete it at the flag drop (section 5, F2), or when the upgrade is abandoned for good (section 1 item 6), as the runbook says. While it exists, HCD 2.0 refuses to start on the node with `internode_compatibility_mode: NONE` (appendix A.2); an empty marker on a node where HCD 2.0 has not started since its drain: section 3b, way back 4; DSE does not read it |
| `NODE_RAN_HCD` | info | Files that an earlier run of HCD 2.0 left in the metadata directory: the node was rolled back to DSE | Nothing |

### DSE features outside DSE core

HCD 2.0 supports DSE core only (appendix B). An upgraded node refuses to start while the workloads, the keyspaces,
the search indexes or `solr.data` remain; the `dse.yaml` settings below are caught by the preflight only. The runbook
covers most of them in section 1 item 1b.

| Code | Severity | What it checks | What the operator does |
|---|---|---|---|
| `NON_CORE_WORKLOAD` | blocking | Workloads other than Cassandra recorded for this node (`nodes/local`) or for a peer (`nodes/peers`): Search, Analytics, Graph | Restart that node as a plain Cassandra node (`dse cassandra` without `-s`, `-k` or `-g`; `SOLR_ENABLED`, `SPARK_ENABLED`, `GRAPH_ENABLED` set to 0 in `/etc/default/dse`) while every other node is up. A stale peer entry is refreshed by a restart of DSE on this node |
| `NON_CORE_KEYSPACE` | blocking | The keyspaces `dse_analytics`, `HiveMetaStore`, `dsefs` (or the `dsefs_options.keyspace_name` of `dse.yaml`), `cfs`, `cfs_archive`, `dse_advrep`, even empty | Migrate what they hold if needed, then drop them on DSE |
| `GRAPH_KEYSPACE` | blocking | Keyspaces with a `graph_engine` in `system_schema.keyspaces`, or with rows in `system_schema.vertices` or `edges` | Migrate the graph data elsewhere and drop the keyspaces on DSE |
| `SEARCH_INDEX` | blocking | Custom indexes (`system_schema.indexes`) of a DSE Search class | `DROP SEARCH INDEX ON <keyspace>.<table>` for each on DSE, then delete the `solr.data` directories |
| `SOLR_DATA` | blocking | A `solr.data` directory in a data directory, or the `solr_data_dir` of `dse.yaml`, exists | Drop every search index on DSE, then delete the directory |
| `DSEFS` | blocking | `dsefs_options.enabled: true` in `dse.yaml` | Disable DSEFS; migrate what its keyspace holds if needed and drop it |
| `ADVANCED_REPLICATION` | blocking | `advanced_replication_options.enabled: true` in `dse.yaml`, or a non-empty `advanced_replication_directory` | Stop Advanced Replication, set `enabled: false`, migrate and drop `dse_advrep`, delete the directory |
| `TIERED_STORAGE` | blocking | `tiered_storage_options` in `dse.yaml` not empty, or a table or view with the compaction class `TieredCompactionStrategy` | `ALTER TABLE` (or `ALTER MATERIALIZED VIEW`) `… WITH compaction = {'class': 'SizeTieredCompactionStrategy'}` (or `LeveledCompactionStrategy`) on DSE; remove `tiered_storage_options` from `dse.yaml` |

### Configuration

| Code | Severity | What it checks | What the operator does |
|---|---|---|---|
| `NODESYNC` | warning | DSE's `cassandra.yaml` does not set `enabled: false` under `nodesync:` (while NodeSync runs, DSE closes its connections to upgraded nodes) | Add `enabled: false` under `nodesync:` and run `nodetool nodesyncservice disable` on every DSE node (section 1 item 3) |
| `TDE_COMMITLOG` | blocking | `transparent_data_encryption_options.enabled: true` in DSE's `cassandra.yaml`: DSE encrypts its commit log. The code of DSE 6.8.65 and 6.9.26 encrypts its hints files with `system_info_encryption` in `dse.yaml`, not with this setting (`TDE_TABLE`, `HINTS_COMPRESSION`) | Set `enabled: false` and restart DSE on every node, with the keystore that section of `cassandra.yaml` names still in place, so that DSE replays the commit log segments written before, before the upgrade (section 0, appendix B) |
| `CONFIG_ENCRYPTION` | warning | `config_encryption_active: true` in `dse.yaml`: the tool sees encrypted values | Carry the decrypted values into HCD 2.0's configuration |
| `AUDIT_LOGGING` | warning | `audit_logging_options.enabled: true` in `dse.yaml`, which HCD 2.0 does not read | Configure `audit_logging_options` in HCD 2.0's `cassandra.yaml` before the node starts |
| `UNLOADABLE_CLASS` | blocking | Here: the `partitioner` of DSE's `cassandra.yaml` is not in HCD 2.0 (the same code is used for tables and indexes, below) | None: the partitioner of a cluster cannot change, so the cluster cannot be upgraded in place |

### Security

The background is [SECURITY.md](SECURITY.md). The runbook covers `RESTRICT` and `ROW_LEVEL_ACCESS` in section 1
item 1, and all of them in "Security first". The node report's `Security` step says whether access controls were
found, and which of its inputs (`dse.yaml`, the schema, `system_auth.role_permissions`) it could not read.

| Code | Severity | What it checks | What the operator does |
|---|---|---|---|
| `RESTRICT` | blocking | Rows of `system_auth.role_permissions` with a non-empty `restricted` set: DSE `RESTRICT` entries, which HCD 2.0 ignores (ignored, they would grant access) | `UNRESTRICT` each of them on DSE |
| `ROW_LEVEL_ACCESS` | blocking | DSE row-level access control, from any of: `authorization_options.allow_row_level_security: true` in `dse.yaml`; `rows/…` resources granted permissions in `system_auth.role_permissions` (a row without permissions grants nothing); tables with the `DSE_RLACA` schema extension | `UNRESTRICT ROWS ON <keyspace>.<table>` on every restricted table, `REVOKE` the row-level grants, then `allow_row_level_security: false` |
| `SCHEME_PERMISSIONS` | blocking | `DseAuthenticator` with `authentication_options.enabled` and `scheme_permissions: true` in `dse.yaml`: HCD 2.0 ignores the `EXECUTE` grants on `authentication_schemes/…`, so a role DSE keeps out could log in | On DSE, give the roles that must log in `EXECUTE` on their scheme, make the others unable to log in (`ALTER ROLE … WITH LOGIN = false`), then `scheme_permissions: false` |
| `AUTH_SCHEME` | blocking | DSE authentication or role management HCD 2.0 does not have: `DseAuthenticator` enabled with `ldap` or `kerberos` in `default_scheme` or `other_schemes`; `DseRoleManager` with `role_management_options.mode: ldap`; any other `com.datastax.bdp.` authenticator, role manager or authorizer | Move every role to internal authentication on DSE (`default_scheme: internal` without `other_schemes`, `mode: internal`) |
| `AUTH_CONFIG_CHANGE` | warning | `DseAuthenticator`, `DseRoleManager` or `DseAuthorizer` in their internal modes: no class of that name in HCD 2.0; the roles and grants of `system_auth` are kept. The message gives the mapping (`PasswordAuthenticator` or `AllowAllAuthenticator`, `CassandraRoleManager`, `CassandraAuthorizer` or `AllowAllAuthorizer`). Reported only when `AUTH_SCHEME` is not | Set these classes in HCD 2.0's `cassandra.yaml` (they are in the settings to carry) |
| `UNSUPPORTED_AUTH_CLASS` | warning | A non-DSE `authenticator` class that is not in HCD 2.0 | Put its jar, built against HCD 2.0, in HCD 2.0's `lib` directory, or use `PasswordAuthenticator` |
| `TRANSITIONAL_MODE` | warning | `authentication_options.transitional_mode` other than `disabled` (with `DseAuthenticator` enabled): DSE lets some clients in without a valid login | Make every client log in with valid credentials, then `transitional_mode: disabled` |
| `PROXY_GRANT` | warning | `PROXY.LOGIN` or `PROXY.EXECUTE` grants. DSE gives them to the creator of every role, so most clusters have them | Make every application that uses proxy login or execution log in as the role it needs; the grants can stay |
| `DELEGATION` | warning | `AUTHORIZE FOR` delegations (the `grantables` column), which HCD 2.0 drops | Grant `AUTHORIZE` where roles must keep granting, after the upgrade |
| `LONE_WRITE_GRANT` | warning | `UPDATE` or `TRUNCATE` granted alone on a data resource; HCD 2.0 has only `MODIFY` (both), so it drops them rather than widen them | `GRANT MODIFY` where the role should also have the other permission, or accept the loss |
| `DSE_ONLY_PERMISSION` | warning | Other permissions HCD 2.0 does not have, which it ignores | Check that no application relies on them; grant the closest HCD 2.0 permission |
| `DSE_ONLY_RESOURCE` | warning | Grants on DSE-only resources (such as `search/…`, `rpc/…`), which HCD 2.0 ignores | Check that no application relies on them |
| `LEGACY_AUTH_TABLES` | blocking | The tables `system_auth.users`, `credentials` or `permissions` exist; HCD 2.0 refuses to start with them | Check that their content is in `roles`, `role_members` and `role_permissions`, then drop them on DSE |
| `NO_ROLE_PERMISSIONS` | warning; info | Warning: no data directory holds `system_auth.role_permissions`. Info: the table has no sstable on this node (no replica here, or nothing flushed) | Warning: point the tool at every data directory. Info: nothing; the other nodes' reports cover the grants |
| `AUTH_REPLICA` | info | `role_permissions` was read from this node's replica only (a node holds only the partitions it replicates) | Check the reports of every node (the aggregate reads them all; see `NO_GRANTS_READ`) |
| `TABLE_EXTENSIONS` | info | Table schema extensions other than `DSE_RLACA`, kept and not interpreted by HCD 2.0 | Nothing |

### Schema and tables

The tool rebuilds each keyspace, type, table and view with HCD 2.0's own schema parsers, so that what an upgraded
node would fail to load shows here first.

| Code | Severity | What it checks | What the operator does |
|---|---|---|---|
| `UNREADABLE_SCHEMA` | blocking | `system_schema.keyspaces`, `tables` or `columns` is in no data directory, or a schema table cannot be read, or `keyspaces` has no row; views exist but DSE's `system_schema.hidden_columns` is missing; `system_auth.role_permissions` cannot be read. An sstable of these tables in the `big` format cannot be read: the tool refuses it before opening it, since opening it offline could delete its `Summary.db` or rewrite its `TOC.txt` (`SSTABLE_FORMAT` reports it too) | Point the tool at the node's data directories (`--data-dir`), run it as root or the DSE user; on a running node, run it again (an sstable may have been compacted away while it was read); for a `big` sstable, `nodetool upgradesstables` on DSE first; a damaged sstable is repaired, or the schema restored from a snapshot, on DSE |
| `UNLOADABLE_KEYSPACE` | blocking | A keyspace whose replication HCD 2.0 cannot load | `ALTER KEYSPACE` on DSE to `NetworkTopologyStrategy`, `SimpleStrategy` or `EverywhereStrategy` |
| `SYSTEM_AUTH_REPLICATION` | blocking; warning | An upgraded node reads DSE's access controls from `system_auth` at `LOCAL_QUORUM` when it starts and refuses to start when it cannot. Blocking, where it cannot start even with every node up: a data center that `nodes/local` and `nodes/peers` record is not listed by `NetworkTopologyStrategy`, or has 0 replicas; `NetworkTopologyStrategy` lists no data center with a replica, or a value that is no number; `SimpleStrategy` while the nodes files record several data centers (it does not place replicas in each data center), or with a replication factor that is missing, 0 or no number (a negative value, in either class, is no number); a class other than the two and DSE's `EverywhereStrategy`, or none; `EverywhereStrategy` over several data centers, always (`LOCAL_QUORUM` counts the replicas of the whole ring, which only a data center holding more than half of the nodes meets, and at most one can); a data center with more replicas than its nodes can make a `LOCAL_QUORUM` of (two replicas or more on one node, four or more on two, six or more on three); or the schema read has no `system_auth` keyspace. With one data center, `SimpleStrategy` is the replication of that data center: DSE's default, one replica, passes on a single node, and so does `NetworkTopologyStrategy` with one replica in a data center of one node, as does `EverywhereStrategy` in one data center. Warning, where an upgraded node cannot start while another node of its data center is down and three replicas (or as many as the nodes) would let it: one or two replicas in a data center of three nodes or more, or so many replicas above its nodes that `LOCAL_QUORUM` needs every node (four or five on three nodes); never in a data center of one or two nodes, where no replication lets a node be down. Warning too when the data center of some or all nodes is not known from `nodes/local` and `nodes/peers`, or `nodes/local` is missing, or one of the two files could not be read, or `nodes/peers` was being rewritten by DSE (a missing `nodes/peers` is read as no peer: DSE 6.9.26 did not write it on a node that never had a peer; DSE can also write an empty one): the nodes counted are a lower bound, the replication is checked against them (for its class only when no data center is known, and for `EverywhereStrategy` over at most one known data center), with no upper bound, and it blocks only on what blocks whatever the other nodes are; the warning names any shortfall among the nodes counted. The upgrade step "Replication of system_auth" gives the result. With several data centers, the other replicated system keyspaces of runbook section 1 item 7 are checked by `KEYSPACE_REPLICATION` | Blocking: before the first node of the cluster is upgraded, with the NodeSync flag off on the tables of `system_auth`, run through one DSE node the `ALTER KEYSPACE system_auth WITH replication = {'class': 'NetworkTopologyStrategy', '<dc1>': 3, …};` the remedy gives, with every data center the node knows and the recommended replicas: three, or as many as its nodes if fewer (in a data center of three nodes or more they let the upgrade go on while a node is down), a higher replication factor kept up to the nodes of the data center; a data center listed with no node in `nodes/local` and `nodes/peers`, or listed with no number and no node, left out and named; the replication factor of `SimpleStrategy` carried over only with one data center known (raised to three, a higher one kept, when the nodes counted are a lower bound); the replicas of `EverywhereStrategy`, those of the whole ring, not carried over, which the remedy says. `DESCRIBE KEYSPACE system_auth` then shows the new map and `nodetool describecluster` one schema version; right after, in the same quiet period, run `nodetool repair -full system_auth` on every node, one at a time, each exiting 0 with no `Skipping anti-entropy repair` line, a failed one run again; then `nodetool flush system_schema` and a new run (section 1 item 7). In a data center of two nodes, an upgraded node still cannot start while the other one is down. When the nodes counted are a lower bound, the remedy asks for that check by hand too, keeps every data center listed with replicas, raises each data center it names to three replicas, keeps a higher number as listed, and asks to lower the three to the nodes of a data center that has fewer; a data center listed with no number and with no node counted is left out, to be set by hand if it has nodes. When `nodes/peers` cannot be read or trusted, so that the tool cannot tell whether a peer already runs HCD 2.0, the blocking remedy and the check by hand add that they apply only if none does, and, if one does, to upgrade no further node and report to the upgrade owner. When `nodes/peers` is missing (`NO_PEERS_FILE`), the remedies that change the replication, of this finding and of `KEYSPACE_REPLICATION`, add that they apply only if `nodetool status` lists no other node, otherwise to report to the upgrade owner. Once a peer runs HCD 2.0 (the mixed phase) the replication can no longer be changed (no schema change and no repair then, and the upgraded nodes do not receive DSE's schema changes): upgrade no further node and report to the upgrade owner. With no `system_auth` keyspace: `nodetool flush system_schema` and a new run, else the right data directories. Warning on the replicas: the same `ALTER KEYSPACE` and repair, optional; in the mixed phase no change, only the risk: keep every node of that data center up while an upgraded node there starts. Warning on nodes not all known: check the replication by hand as section 1 item 7 says; in the mixed phase, if a data center has no replica or too few nodes for its `LOCAL_QUORUM`, upgrade no further node and report to the upgrade owner, and, where it names a shortfall among the nodes counted, keep every node of that data center up while an upgraded node there starts *(blocking: not validated, unit tests only; the warning on too few replicas validated on DSE 6.9.26 on one data center of three nodes at one replica; the warning on nodes not all known, and the caveat when `nodes/peers` cannot be trusted: unit tests only)* |
| `KEYSPACE_REPLICATION` | warning | Checked in full only when `nodes/local` and `nodes/peers` record several data centers (with one, the data center of every node known, these keyspaces stay as they are, but for a replication that gives no node a replica; with one known at most and the data centers of the nodes not all known, because of nodes of no data center, `nodes/local` missing, or a nodes file not read or not trusted, they are not checked, but for a replication that gives no node a replica): the other replicated system keyspaces of runbook section 1 item 7 on `SimpleStrategy` or `NetworkTopologyStrategy`, that is `system_distributed`, `system_traces`, DSE's keyspaces replicated per data center (`dse_security`, `dse_leases`, `dse_perf`), any other `dse_*` or `system_*` keyspace and `solr_admin`, one finding per keyspace, chosen by name (a user keyspace named so is checked too). They follow the rule of `system_auth`: replicas in every data center, three recommended, or as many as the nodes if fewer. Never blocking, since an upgraded node needs none of them to start or to work: HCD 2.0 writes the build status of materialized views to `system_distributed` at `ONE` and reads it at `ONE`, writes the traces of queries to `system_traces` at `ANY`, does not use DSE's keyspaces, and refuses repairs during the upgrade (afterwards it writes the repair history at `ANY` and the repair traces, read back at `ONE`); the message says so (for DSE core's own keyspaces, `dse_security`, `dse_leases`, `dse_perf`, `dse_system`, `dse_insights`, `dse_audit`, `solr_admin` and `system_backups`, that only DSE uses them; for another keyspace chosen by its name, that HCD 2.0 itself does not use it, its clients may). A warning where a data center that the nodes files record is not listed by `NetworkTopologyStrategy`, has 0 replicas or a value that is no number; where `NetworkTopologyStrategy` lists no data center with a replica; where `SimpleStrategy` spans the data centers (unless its replication factor is at least the nodes, all counted: a replica on every node), or has a replication factor that is missing, 0 or no number (no node then holds a replica); where a data center of no node is listed with a value that is no number; or where a data center of three nodes or more has fewer than the three recommended (which costs HCD 2.0 nothing, as it reads and writes these keyspaces at `ANY` or `ONE`, served by any data center: the warning follows the recommendation). No such warning in a data center of one or two nodes, and nothing for more replicas than nodes. When the data centers of some nodes are not known, the data centers known are checked against the nodes counted, as for `SYSTEM_AUTH_REPLICATION`. A keyspace absent from the schema read (a DSE feature never installed), on `EverywhereStrategy`, `LocalStrategy` or another class (left to `UNLOADABLE_KEYSPACE` when HCD 2.0 cannot load it), the keyspaces of `NON_CORE_KEYSPACE` (a DSEFS keyspace named in `dse.yaml` among them), a DSE Graph keyspace, or with a replication that cannot be read (`UNLOADABLE_KEYSPACE`) gives nothing. If the check fails, a warning `INSPECTION_ERROR` asks to report the error and to check these keyspaces by hand. The upgrade step "Replication of the other system keyspaces" gives the result | Before the first node of the cluster is upgraded (where the remedy says `optional`, fewer replicas than the three recommended only, change the keyspace or write down that it stays; otherwise change it, though the tool does not block on it): with the NodeSync flag off on the tables of that keyspace, run through one DSE node the `ALTER KEYSPACE <keyspace> WITH replication = {'class': 'NetworkTopologyStrategy', '<dc1>': 3, …};` the remedy gives, as the one of `SYSTEM_AUTH_REPLICATION` (the recommended replicas in every data center the node knows, a higher replication factor kept up to the nodes of the data center; when the nodes counted are a lower bound, kept as listed, with the data center of each node and its replicas to check by hand); `DESCRIBE KEYSPACE <keyspace>` then shows the new map and `nodetool describecluster` one schema version; right after, in the same quiet period, run `nodetool repair -full <keyspace>` on every node, one at a time, each exiting 0 with no `Skipping anti-entropy repair` line, a failed one run again; then `nodetool flush system_schema` and a new run (section 1 item 7). When `nodes/peers` cannot be read or trusted, the remedy adds that it applies only if no peer runs HCD 2.0 yet, and, if one does, to make no change, upgrade no further node and report to the upgrade owner; when `nodes/peers` is missing, that it applies only if `nodetool status` lists no other node, otherwise to report to the upgrade owner (see `NO_PEERS_FILE`). In the mixed phase, no change: the replication stays as it is (no schema change and no repair then) *(this finding: unit tests only)* |
| `UNLOADABLE_TYPE` | blocking | A user type HCD 2.0 cannot load (a DSE-only field type, for example) | Change or drop it on DSE, with the tables that use it |
| `UNLOADABLE_TABLE` | blocking | A table HCD 2.0 cannot load (its flags, options or columns); a view HCD 2.0 cannot load (its options, its columns, or a compaction or compression class HCD 2.0 does not have), or whose base table cannot be loaded; a dropped column whose type HCD 2.0 cannot parse | Tables: change or drop them on DSE. Views: drop them on DSE and create them again once no node runs DSE. Dropped columns: recreate the table without that history (copy the data to a new table), or contact support |
| `UNLOADABLE_CLASS` | blocking | A table with a compaction or compression class HCD 2.0 cannot load; a custom index whose class HCD 2.0 does not have (other than DSE Search), whose class is not an index class, or that has no `class_name` option | `ALTER` the table on DSE to classes both products have (`SizeTieredCompactionStrategy`, `LeveledCompactionStrategy`, `LZ4Compressor`); `DROP INDEX` on DSE, and recreate it as SAI after the upgrade if needed |
| `COMPACT_STORAGE` | blocking | Tables with `COMPACT STORAGE` (their `flags`) | `ALTER TABLE … DROP COMPACT STORAGE` on DSE |
| `VECTOR_COLUMN` | blocking; warning | ANN workloads and tables with `VECTOR` columns are not supported by the in-place upgrade from DSE 6.9 to HCD 2.0. One finding per table outside the system keyspaces and DSE's own keyspaces. Blocking for a table with a column of the vector type at any depth of its declared type (`vector<float, 3>`, or inside a collection, a map key or value, a tuple or a field of a user type the table uses), of any kind of column (partition key, clustering, static, regular): the message names the table, each such column with its declared type as `system_schema.columns` holds it, the indexes on those columns (with the `similarity_function` of a vector SAI index), the materialized views that select them, the table's dropped vector columns, and its indexes whose target cannot be parsed (which may be on a vector column). A warning for a table whose only vector columns are dropped ones (`system_schema.dropped_columns`): the sstables written before the drop may still hold their data. A table HCD 2.0 cannot load is `UNLOADABLE_TABLE`'s. DSE 6.8 and DSE 5.1 have no vector type, so the finding never appears there | Blocking: remove these columns or tables, or move that data by other means, before the upgrade; a vector in the primary key goes only with its table; drop the indexes and views on a column before the column, then rewrite the table's sstables (`nodetool upgradesstables -a <keyspace> <table>` on every DSE node) so that none keeps its data. Warning: the same rewrite, unless it was done since the drop. In the mixed phase no schema change and no `upgradesstables`: for the blocking finding, upgrade no further node and report to the upgrade owner; for the warning, report it to the upgrade owner *(unit tests only)* |
| `TDE_TABLE` | blocking | A table or view whose compression class contains "encrypt", other than the four tables below; or `system_info_encryption.enabled: true` in `dse.yaml`, with which DSE 6.8.65 and 6.9.26 (seen in a test run of both, the saved caches by DSE's start line only; later patch releases taken to do the same) encrypt the tables `system.batches`, `system.paxos`, `system_backups.backup_configurations` and `system_backups.backup_stores` (their compression becomes `EncryptingLZ4Compressor`), their commit log, their saved caches and their hints files; or, with the setting off (or `dse.yaml` not read), those four tables still encrypted in the schema: DSE sets their compression back to `LZ4Compressor` itself at its first start with the setting off, so a node has not restarted since (for the `system_backups` tables, another node started with the setting on since then), or `system_schema` was not flushed since; if every node already runs with the setting off, the remedy asks for `nodetool flush system_schema` and a new run *(that finding: unit tests only)*. Encrypted tables are out of scope of this upgrade (section 0) | Decrypt on DSE: `ALTER TABLE` (or `ALTER MATERIALIZED VIEW`) `… WITH compression = {'class': 'LZ4Compressor'}`, then `nodetool upgradesstables -a` on every node, never for the four system tables; for `system_info_encryption`, set `enabled: false` under it in `dse.yaml`; restart DSE on every node, with the whole directory that `system_key_directory` names in `dse.yaml` (by default `/etc/dse/conf`) still in place, its `system/system_table_keytab` included, so that DSE replays the commit log segments written before and writes its commit log, saved caches and hints files unencrypted from then on (DSE applies the setting only at its start; HCD 2.0 skips a saved cache it cannot read); then run `nodetool upgradesstables -a system batches paxos` and `nodetool upgradesstables -a system_backups` on every node (a rewrite before the restart would write those tables encrypted again, from DSE's code: not tried); keep that directory in place until the rewrite is done and, with every node up, DSE has delivered the hints files written before (`HINTS_COMPRESSION` names those left); all of it before the first node of the cluster is upgraded. Without its original `system/system_table_keytab`, DSE makes a new one at its start, with no warning, and cannot read what it encrypted before: DSE 6.8.65 failed on its encrypted hints files (`Failed to dispatch hints file <file>: file is corrupted`) and, with `disk_failure_policy: stop`, stopped serving (encrypted sstables and segments were not read without the keytab). The finding of the setting names the four tables and the finding of the other encrypted tables leaves them out; with the setting off, their finding of their own gives the restart, then the rewrite. The tool's remedies give these steps in this order; the tool checks neither that the rewrite is done (once the setting is off, DSE restarted and `system_schema` flushed, it no longer blocks, although sstables of the four tables may still be encrypted) nor that no encrypted commit log segment is left. *(The order confirmed by a test run of DSE 6.8.65 and 6.9.26, but for the replay of encrypted commit log segments at the restart and the rewrite once every node runs with the setting off, which did not run; the key's absence tried on DSE 6.8.65 only; that run had the tool's earlier texts, without the key directory and the four tables' finding of their own: those texts, unit tests only)* |
| `VIEW_HIDDEN_COLUMNS` | blocking | Materialized views with DSE hidden columns (`system_schema.hidden_columns`) that HCD 2.0 does not reproduce: it handles only the hidden column of the one regular base column in the view's primary key | Drop the view on DSE before the upgrade, and create it again once no node runs DSE (section 1 item 2) |
| `VIEW_OLD_FORMAT` | info | Views for which DSE lists no hidden column although the one regular base column of the view's primary key would need one (DSE's old view format); their data is exchanged as it is | Nothing |
| `VIEWS_HANDLED` | info | The views whose data HCD 2.0 exchanges with DSE nodes: those with the one hidden column HCD 2.0 reproduces, and those that need none | Nothing |
| `JAVASCRIPT_UDF` | blocking | User-defined functions in JavaScript | Rewrite them in Java, or drop them (and the aggregates using them), on DSE |
| `UDF_LANGUAGE` | blocking | User-defined functions in any other language than Java and JavaScript | The same |
| `TRIGGER` | warning | Triggers (`system_schema.triggers`) | Build each trigger class against HCD 2.0 and put its jar in HCD 2.0's `triggers` directory before the node starts |

### Topology, snitch and EC2

| Code | Severity | What it checks | What the operator does |
|---|---|---|---|
| `SNITCH` | warning | The snitch DSE runs with (`endpoint_snitch` of `cassandra.yaml`, or `delegated_snitch` of `dse.yaml` behind `DseDelegateSnitch`) is not in HCD 2.0: no `endpoint_snitch` (DSE then runs `DseSimpleSnitch`), a DSE snitch class, `DseDelegateSnitch` (with a delegated snitch known or not), or a third-party snitch | Set the `endpoint_snitch` the remedy names in HCD 2.0's `cassandra.yaml`: the same snitch when HCD 2.0 has it, else `GossipingPropertyFileSnitch` with the data center and rack of `nodes/local` in its `cassandra-rackdc.properties`; for a third-party class, its jar built against HCD 2.0 |
| `EC2_NAMING` | warning | DSE runs `Ec2Snitch` or `Ec2MultiRegionSnitch` (of `org.apache.cassandra.locator`), which in DSE always use the legacy EC2 names; HCD 2.0 defaults to the standard names and refuses to start on a ring with legacy ones | Set `ec2_naming_scheme=legacy` in HCD 2.0's `cassandra-rackdc.properties` on every node, and carry DSE's `dc_suffix` (the remedy gives its value; copy the line as it is) (section 1 item 0) |
| `IDENTITY_MISMATCH` | warning | DSE's files against `nodes/local`: with `GossipingPropertyFileSnitch`, the `dc` or `rack` of `cassandra-rackdc.properties`; the `cluster_name` of `cassandra.yaml`; `num_tokens` against the number of tokens the node owns | Carry what the node runs with (`nodes/local`, `nodetool status`) into HCD 2.0: another data center or rack moves its data, and a node does not start with another number of tokens |

### Ports and TLS

| Code | Severity | What it checks | What the operator does |
|---|---|---|---|
| `INTERNODE_TLS` | blocking or warning, by DSE release and DSE's `enable_legacy_ssl_storage_port` (table below) | DSE encrypts internode traffic (`server_encryption_options.internode_encryption` other than `none`). The message also says what is validated (the runbook's header and section 0 give the details) | By case, in the table below; in every case, the HCD 2.0 settings listed after it |

The cases of `INTERNODE_TLS`. The release is the one `nodes/local` records. DSE reads the key as true when it is
`true`, `yes` or `on`, in any case, quoted or not; a quoted value with spaces is not true.

| DSE release | DSE's `enable_legacy_ssl_storage_port` | Severity | What the operator does |
|---|---|---|---|
| 6.9.7 and later | not true: absent, empty, `false` or anything else | blocking | Set `enable_legacy_ssl_storage_port: true` on every DSE node: change the key's line under `server_encryption_options`, or add it. Restart each DSE node whose file did not set it, one at a time, as "Stopping a node" says, with P1 between nodes; check with `ss -tln` that DSE listens on `ssl_storage_port`. Before the first upgrade, do it at the restarts of step 2b of the order of work, or in a rolling restart of its own. Once the upgrade has started (`UPGRADED_PEERS`), report to the upgrade owner first (section 3b step 3). Then run the tool again (section 1, "Internode TLS on DSE 6.9") |
| 6.9.7 and later | true | warning | Keep it on every DSE node, and check with `ss -tln` that DSE was restarted since it was set |
| 6.8 | (DSE 6.8 has no such key) | warning | No TLS change on DSE: DSE 6.8 always listens with TLS on `ssl_storage_port`. Remove the key's line if DSE's file has it: DSE refuses a key it does not have |
| 6.9.0 to 6.9.6 | (these releases have no such key) | warning | After DSE's own upgrade to the required release, set the key to `true` on every DSE node before the first upgrade. Until then, remove the key's line if DSE's file has it |
| unknown (no `nodes/local`, not 6.8 or 6.9, or a build number the tool cannot read) | any | warning | The remedy gives both rules: the key set to `true` (or kept) if the node runs DSE 6.9.7 or later, and, when DSE's file has the key, its line removed if the node runs DSE 6.8 or 6.9.0 to 6.9.6 |

Why the key matters: without it, a node rolled back to DSE listens with TLS on `storage_port` only, while the other
DSE nodes go on dialing its `ssl_storage_port` and can see it `DN`.

In every case, in HCD 2.0's `cassandra.yaml` (section 3b step 3):

- Carry the keys of `server_encryption_options` under the names HCD 2.0 accepts.
- Leave out the keys it does not have: DSE's `keystore_type` and `truststore_type` become one `store_type`, and the
  remedy says which.
- Leave out the keys DSE's file sets to no value, where HCD 2.0 refuses that.
- Give `keystore` and `truststore` absolute paths.
- Carry `ssl_storage_port`, and check by hand that DSE's JVM options set no other `-Dcassandra.ssl_storage_port`
  (appendix A.11).
- Keep `legacy_ssl_storage_port_enabled: false`, and do not carry `enable_legacy_ssl_storage_port`, its old name in
  HCD 2.0.

The tool reads `enable_legacy_ssl_storage_port` in DSE's file, not what the running DSE uses, and only on the node it
inspects. On DSE 6.9.7 and later with internode TLS, the node report's `upgrade_steps` adds a step
`Internode TLS on DSE 6.9: …`, and the aggregate is `READY` only if every node passes (`NODE_BLOCKED` names the blocked
ones). Keep the runbook's check by hand as a cross-check (section 1, "Internode TLS on DSE 6.9"; section 3a, P3).

Whether the nodes agree on `storage_port` and on their TLS settings is checked by the aggregate (section 4 below).

### Data on disk

| Code | Severity | What it checks | What the operator does |
|---|---|---|---|
| `SSTABLE_FORMAT` | blocking | The `-Data.db` files of committed transactions, by name: `big` sstables (HCD 2.0 does not take them from DSE 6); `big` sstables newer than `me`, trie sstables newer than `bb`, non-numeric identifiers, or unknown names (none written by DSE 6) | For `big`: `nodetool upgradesstables` on DSE first (section 3a, P5). For the others: find out what wrote them (DSE cannot read them either); move them out once their data is safe |
| `OLDER_TRIE` | info | Trie sstables older than `bb` (DSE 6.0 to 6.7), which HCD 2.0 reads as they are | Nothing |
| `SSTABLE_WITHOUT_TOC` | warning | sstables without a `TOC.txt`, named, up to 20 per table; HCD 2.0 writes the missing one when it opens them; neither DSE 6.8 nor DSE 6.9 writes it at its start (DSE 6.9 from DSE's code, not run; their `nodetool verify`, offline sstable tools and backup service do) | Find out why before the upgrade. With a `ba` or `bb` sstable among them (the versions whose format supports DSE's zero-copy streaming), a likely cause right after a repair (section 1 item 6, step 8 of the order of work; also item 7) or another streaming operation is DSE's zero-copy streaming, which writes an sstable without `TOC.txt` (DSE 6.8 and 6.9 alike; a partially streamed one also without `Filter.db` and `Digest.crc32`): if DSE's `system.log` or its archives have the line `ZeroCopySSTableWriter … Writing partial component DATA to <the sstable's Data.db file>` (or, for an sstable streamed whole, `… Writing entire component DATA to <the sstable's Data.db file>`) for every such sstable of the table (the finding names the first 20; section 1 item 0 lists them all), the warning can be accepted (HCD 2.0 reads such an sstable); otherwise look for an incomplete copy or restore, or a damaged disk |
| `TXN_LOG_UNREADABLE` | warning | Transaction logs that cannot be read; every sstable of those directories was checked instead. HCD 2.0 refuses to start while such a log remains | Run the tool again once DSE's operation is over; a log left after DSE's drain and stop must go (restart DSE) before the upgrade |
| `LOW_DISK_SPACE` | warning | Per file system: less usable space than the bytes of the sstables it holds (the upgrade snapshot keeps every sstable after compaction rewrites it); or the free space cannot be read | Free space, or plan to clear the snapshot (`nodetool clearsnapshot -t pre-hcd-<timestamp>`) as soon as a rollback is no longer needed; check with `df` |
| `HINTS_COMPRESSION` | blocking | DSE's `cassandra.yaml` sets `hints_compression`, so DSE writes compressed hints files; or DSE hints files still on the node are written compressed or encrypted, as the parameters their header records say (the message names each file with the names of its parameters, never their values). DSE encrypts its hints files while `dse.yaml` sets `enabled: true` under `system_info_encryption`, through an encrypting compressor that the header records as the file's compression. HCD 2.0 cannot convert such a file at its start: it keeps the file and does not deliver its hints. *(Validated for encrypted files only: DSE 6.8.65 and 6.9.26, run with `system_info_encryption`, wrote hints files whose header records `EncryptingLZ4Compressor` as their compression, and the tool named them (that run had the earlier remedy, without the key directory: the present text unit tests only); on `hints_compression`, not validated: unit tests only, DSE was not run with it)* | Remove `hints_compression` from DSE's `cassandra.yaml` and set `enabled: false` under `system_info_encryption` in `dse.yaml`, each one that is set, and restart DSE on the node (section 1, "Required DSE release": with the restarts of step 2b of the order of work), so that the hints files it writes from then on are neither compressed nor encrypted; the files written before keep their compression or encryption: with every node up, and for encrypted files the whole directory that `system_key_directory` names in `dse.yaml` still in place, its `system/system_table_keytab` included, wait until DSE has delivered them and they are gone, before the first node of the cluster is upgraded, then run the tool again, which names the compressed or encrypted files left. If `system_info_encryption` was enabled, rewrite the tables it encrypted as for `TDE_TABLE`, once DSE runs with it off on every node, before the first node of the cluster is upgraded. The remedy ends with what happens without that keytab (`TDE_TABLE`) |
| `HINTS_UNREADABLE` | warning | DSE hints files whose header cannot be read (a checksum that does not match, a header cut short), so whether they are compressed or encrypted is not known | Run the tool again (DSE may have been writing them); if a file stays unreadable, find out why before the first node of the cluster is upgraded: HCD 2.0 cannot convert a damaged hints file either |
| `PENDING_HINTS` | warning; info | DSE hints files not delivered yet, by target; files for host ids in neither `nodes/local` nor `nodes/peers` (dropped at the conversion), as info when `nodes/local` and `nodes/peers` could not both be read and trusted, since those host ids may be of peers or of this node; or the hints directory cannot be listed | Before the node is upgraded, wait until the files for DSE nodes are gone (HCD 2.0 converts and delivers the others); find out which nodes unknown host ids were for (section 3b, steps 0 and 3b). For the info: fix what keeps `nodes/local` or `nodes/peers` from being read and trusted, then run the tool again |
| `NON_DSE_HINTS` | warning | Hints files DSE did not write (left by an earlier run of HCD 2.0) | Remove them as the rollback procedure says (section 4) |
| `HINTS` | info | No hints directory | Nothing |
| `COMMITLOG_VERSION` | warning | Commit log segments of another version than DSE's (680), or the directory cannot be listed | Find out what wrote them; drain DSE before the upgrade (section 3b, step 2) |
| `COMMITLOG` | info | No commit log directory, or segments already replayed by an earlier run of HCD 2.0 | Nothing |

### Live state (with `--nodetool`)

| Code | Severity | What it checks | What the operator does |
|---|---|---|---|
| `LIVE_UNAVAILABLE` | warning | A `nodetool` command failed, timed out or could not run (any of them, `gossipinfo` and `tpstats` included, and a command failing on an argument `nodetool` rejects): the node is `INCOMPLETE` (exit 3) unless a finding blocks. Only `listpendinghints`, which DSE 6.8 and 6.9 lack, is skipped when `nodetool` rejects it as an unknown command (`LIVE_COMMAND_UNKNOWN`) | Check that DSE runs and that `nodetool` reaches it (host, JMX port, user and password file: `--nodetool-arg`, with `-pwf <file>` rather than `-pw <password>`, which the process list shows). DSE's `nodetool` runs without the `CASSANDRA_HOME`, `CASSANDRA_CONF`, `CASSANDRA_INCLUDE`, `JAVA_OPTS` and other variables of the tool's launch, taking its configuration from its own installation, and on the tool's Java unless `--nodetool-java-home <dir>` names another (DSE 5.1's needs Java 8). Then run the tool again |
| `LIVE_COMMAND_UNKNOWN` | info | A command this DSE release does not have (the `nodetool` of DSE 6.8.65 and of DSE 6.9.26 has no `listpendinghints`); the hints directory is still checked on disk | Nothing |
| `LIVE_STATUS` | warning | Nodes not `UN` in `nodetool status` | Every node must be `UN` before a node is upgraded (section 3a, P1) |
| `LIVE_SCHEMA` | warning | More than one schema version in `nodetool describecluster` | One version before the first upgrade; exactly two in the mixed phase, which is expected (section 3a, P2) |
| `LIVE_NODESYNC` | warning | `nodetool nodesyncservice status` says the service is running | `nodetool nodesyncservice disable` on every DSE node (section 1 item 3) |
| `LIVE_COMPACTIONS` | warning | Pending compactions in `nodetool compactionstats` | Let them finish before the node is upgraded |
| `LIVE_REPAIR` | warning | `Validation` or `Anticompaction` in `nodetool compactionstats`: a repair runs | Let it finish; no repair during the upgrade (section 3a, P6) |
| `LIVE_STREAMS` | warning | `nodetool netstats` does not say `Not sending any streams` | Let the streams finish (section 3a, P6) |
| `LIVE_HINTS` | warning | `nodetool listpendinghints` lists pending hints | Wait until the hints for DSE nodes are delivered (section 3b, step 0) |

### Inputs and the run itself

| Code | Severity | What it checks | What the operator does |
|---|---|---|---|
| `UNREADABLE_INPUT` | blocking; warning or info for `cassandra-rackdc.properties` | An input cannot be read: `cassandra.yaml`, `dse.yaml`, `nodes/local`, `nodes/peers`, a data directory or a table directory (blocking: the checks that depend on it cannot be made). `cassandra-rackdc.properties`: a warning when the snitch needs it (`GossipingPropertyFileSnitch`, or an EC2 snitch for its `dc_suffix`), else info. `nodes/peers` (blocking: the releases and workloads of the peers, and the mixed phase, cannot be checked): also when it changed while it was read (DSE rewrote it), when a `nodes/peers.old` is left, which DSE at its next start, and HCD 2.0's import of DSE's node metadata, put back in place of `nodes/peers` (DSE is rewriting it, or was interrupted while it did), or when it is missing with a `nodes/peers.txn` left (DSE is writing it for the first time, or was interrupted while it did, which DSE does not recover from `peers.txn`: it writes the file anew once it knows its peers), or when its attributes cannot be read *(these cases: unit tests only)*. A missing `nodes/peers` alone is read as no peer: DSE 6.9.26 did not write it on a node that never had a peer; DSE can also write an empty one; but run with `--nodetool`, when `nodetool status` lists more than one node, a missing `nodes/peers` is lost, and one that records no peer stale: both block, unless `nodes/local` is missing or unreadable (with peers recorded, the count of `nodetool status` is not compared: nodes joining or leaving make it differ for a while) *(unit tests only)* | Run the tool as root or as the DSE user, or point it at the file or directory (`--cassandra-yaml`, `--dse-yaml`, `--conf-dir`, `--dse-home`, `--data-dir`, `--rackdc`). For `nodes/peers` changed or left half written: run the tool again; if it persists with DSE stopped, start DSE once (with `peers.old`, it puts the file back; with `peers.txn`, wait until `nodetool status` shows its peers, DSE then writes the file anew), then run the tool again; with `peers.txn` on a node that has no peer, delete `nodes/peers.txn`; for `nodes/peers` missing or recording no peer while `nodetool status` lists other nodes: restart DSE on this node and wait until `nodetool status` shows its peers (DSE then writes the file anew), then run the tool again; if it is still missing or records no peer, report to the upgrade owner; a damaged `nodes/peers` is repaired on DSE first |
| `NO_PEERS_FILE` | info | No `nodes/peers` in the metadata directory (only a missing file: DSE's empty map is read as no peer without it), taken for a node that never had a peer, as on a single DSE 6.9.26 node, so the replication checks count this node alone, and no other node's release, workloads or upgrade state is checked (not said with `nodes/local` missing or unreadable, or recording no data center, when this node is not counted; run with `--nodetool`, when `nodetool status` lists more than one node, a blocking `UNREADABLE_INPUT` in its place); a `PENDING_HINTS` warning on host ids of no known node then says that the file was probably lost *(unit tests only)* | If `nodetool status` lists other nodes, the file is lost: apply no remedy of `SYSTEM_AUTH_REPLICATION` or `KEYSPACE_REPLICATION` (whose remedies say so then) and report to the upgrade owner |
| `NOT_A_DSE_NODE` | blocking | No `nodes/local` in the metadata directory (with a `nodes/local.old` left: DSE was interrupted while it rewrote the file) | Point the tool at DSE's metadata directory (`--metadata-dir`); with `local.old`, start DSE once (it recovers the file), then run the tool again |
| `INSPECTION_ERROR` | blocking; warning | One part of the inspection failed unexpectedly; the others still ran. Blocking: the node is not declared ready. A warning, which does not stop `READY`, when the part that failed is the check of `KEYSPACE_REPLICATION`, which never blocks | Report the error with the tool's output; for the warning, check by hand that each of the other keyspaces of section 1 item 7 has replicas in every data center |
| `DEFAULTS_ASSUMED` | info | Directories DSE's `cassandra.yaml` does not set, for which DSE's defaults were assumed | Check that they are the right ones |
| `SSTABLES_ONLY` | info | Always: the schema and the grants come from sstables, so unflushed changes are not seen | Run `nodetool flush system_schema` and `nodetool flush system_auth` right before the tool |

## 4. Aggregate mode

The aggregate reads the regular files named `*.json` of one directory, not its subdirectories, and follows no link, in
the order of their names, which every list of files in its findings follows, whatever the file system; it checks the
cluster as a whole, whatever the number of data centers. It never reads a node's files and needs no running node. Its
result is `READY` (exit 0) when it has no blocking finding, else `BLOCKED` (exit 1). It reads each report strictly and
fails closed: every key the node inspection always writes must be there, with its type, once.

`READY` needs all of these: every report is a complete node report of this tool, of its report format and of the release
of the tool that aggregates, written at most 24 hours before the aggregate; no node report is blocked or incomplete;
every node runs the same DSE release, with one schema version and one cluster name; no node is reported twice; every DSE
peer a report lists has a report of its own (a peer already on HCD 2.0 needs none); no ready node records no peer while
another node has a report; every DSE node has the same `storage_port`.

| Code | Severity | What it checks | What the operator does |
|---|---|---|---|
| `NODE_BLOCKED` | blocking | Node reports not ready: each listed with its blocking codes, or as `BLOCKED` without a blocking finding | Resolve them, then run the tool on those nodes again |
| `NODE_INCOMPLETE` | blocking | Node reports whose live state was not checked completely: `INCOMPLETE`, or `READY` with a `LIVE_UNAVAILABLE` finding (a blocked one is left to `NODE_BLOCKED`) | On each node, make `nodetool` reach DSE (see `LIVE_UNAVAILABLE` in its report), run the tool again with `--nodetool`, then aggregate again |
| `MISSING_REPORT` | blocking | A DSE node listed in some report's `peers` has no report of its own (a peer already on HCD 2.0 needs none) | Run the tool on it and add its report; a stale entry of a node that left is refreshed by a restart of DSE on the nodes listing it |
| `NO_PEERS_RECORDED` | blocking | A ready node report records no peer (its `nodes/peers` missing, said by `NO_PEERS_FILE` when that node was counted, or empty) while the reports are of more than one node: its `nodes/peers` is lost or stale, and its report counted the node alone, or could not count it *(unit tests only)* | On each node named, restart DSE and wait until `nodetool status` shows its peers (DSE then writes `nodes/peers` anew), run the tool on it again and aggregate again; apply no remedy of `SYSTEM_AUTH_REPLICATION` or `KEYSPACE_REPLICATION` from the reports it replaces; if `nodes/peers` still records no peer, report to the upgrade owner |
| `DUPLICATE_REPORT` | blocking | More than one report for one node: the same host id, or the same broadcast address | Keep the latest report of each node |
| `MIXED_RELEASES` | blocking | The nodes run different DSE releases | One release on every node, 6.9 preferred (section 3a, P3) |
| `SCHEMA_DISAGREEMENT` | blocking | The nodes record different schema versions in `nodes/local` | Wait for schema agreement (`nodetool describecluster`), then run the tool on every node again |
| `CLUSTER_NAMES` | blocking | The reports come from more than one cluster | Aggregate one cluster at a time |
| `MIXED_TOOL_BUILDS` | blocking | The reports come from different releases of the tool, or from another release than the one aggregating | Run the tool of the HCD 2.0 release to be installed on every node, and aggregate with that release |
| `MIXED_STORAGE_PORTS` | blocking | The DSE nodes differ in `storage_port`. A report without one counts as 7000, the default of both products; `07000` reads as octal and `0x1B62` as hexadecimal, as a YAML loader reads them. Why it matters: DSE's pre-4.0 messages give addresses without a port, and HCD 2.0 completes them with its own `storage_port` | Make every DSE node's `cassandra.yaml` set the `storage_port` DSE runs with, the same everywhere. Check by hand DSE's JVM options for `-Dcassandra.storage_port` (and, with internode encryption, `-Dcassandra.ssl_storage_port`): each must be absent or give the file's port (appendix A.11). Run the tool again. Carry that port into HCD 2.0 unchanged (section 3b step 3) |
| `UNREADABLE_REPORT` | blocking | A file that is not JSON or not a JSON object, is over 16 MiB, writes a key twice, or is not a node report of this tool and of its report format (another tool, another `kind`, no or another `format_version`); a node report without a key the node inspection always writes (`software_version`, `generated_at`, `result`, `node`, `peers`, `findings`, `upgrade_path` with its `settings`, `access_controls`), with a value of the wrong type, a `result` other than `READY`, `BLOCKED` or `INCOMPLETE`, an `INCOMPLETE` report without a `LIVE_UNAVAILABLE` finding, a `generated_at` that is no time, an empty entry in its peers, findings or settings, or a finding without a code or severity; a `READY` or `INCOMPLETE` report whose node section lacks the host id, the addresses, the cluster name, the release or the schema version | Run the tool again on those nodes with `--json`, with the same release, and remove from the directory any file that is not a node report of this tool |
| `STALE_REPORT` | blocking | Reports written more than 24 hours before the aggregate, or more than 10 minutes after its time (the message gives the time of each and of the aggregate). | Run the tool again on those nodes, then aggregate the new reports at once; check the clocks of the nodes if a report was written after the aggregate |
| `NO_REPORT` | blocking | No readable node report in the directory | Collect the `--json` report of every DSE node into one directory |
| `INTERNODE_TLS` | warning | The DSE nodes differ in `internode_encryption`, or, with encryption, in `ssl_storage_port` | Give every DSE node the same settings before the upgrade; the remedy explains which port each product dials, and asks for the JVM option check by hand (appendix A.11) |
| `TOPOLOGY_MISMATCH` | warning | A node recorded with more than one data center or rack: in its own `nodes/local` and in the `nodes/peers` of the others (also for a node without a report) | Check that node's snitch settings and `nodetool status` on the nodes that record it otherwise, then run the tool again on them |
| `NO_GRANTS_READ` | warning | Some node checks permissions, but no report read a row of `system_auth.role_permissions`, so the access-control checks had nothing to check | Inspect every node with all its data directories, after `nodetool flush system_auth` |
| `UPGRADED_PEERS` | info | Nodes already on HCD 2.0, listed as peers (no report needed) | Nothing |
| `AGGREGATE_REPORT_SKIPPED` | info | Aggregate reports found in the directory, skipped | Nothing |
| `NOT_A_FILE_SKIPPED` | info | Entries of the directory named `*.json` that are not regular files (directories, links), skipped | Nothing |
| `PAXOS_ROLLBACK_REPORT_SKIPPED` | info | Rollback-mode reports found in the directory, skipped | Nothing |

A report whose `cassandra.yaml` could not be read (it is already in `NODE_BLOCKED`) is left out of the
`MIXED_STORAGE_PORTS` and `INTERNODE_TLS` comparisons; a report without `upgrade_path` is an `UNREADABLE_REPORT`.

**Output.** The aggregate prints `Reports read: <n>, in <m> data centers` (and how many reports have no data center
recorded), then the nodes grouped by data center, each with its file, rack, result and counts, as each node's
`nodes/local` records them (`(unknown)` when it records none). The JSON has `nodes` (one line per report) and
`data_centers` (nodes by data center, then by rack). Then come the result, the findings, and the warnings of the node
reports grouped by code, with the nodes that have each (`warnings_by_code`). Every data center and rack must be the
one `nodetool status` shows (section 1 item 0).

## 5. Rollback mode (`--paxos-rollback`)

Before a node rolled back to DSE starts, its open Paxos rounds must be completed through another node, because the
restore replaces `system.paxos` with the pre-upgrade snapshot (section 4, steps 5 and 6). The tool runs with HCD 2.0
stopped after its drain and DSE not started. It does not check that precondition.

**Its options.** `--snapshot <tag>` names the snapshot the rollback restores, by its tag, not a path; it is needed when
the node has several `pre-hcd-*` snapshots, which is always the case after an earlier rollback and re-upgrade.
`--paxos-dir <dir>` adds a directory of `system.paxos` sstables, needed when the restore of `system` already ran: pass
each quarantined `<quarantine>/pre-hcd-<timestamp>/<n>/system/paxos-<id>` table directory that holds HCD 2.0's sstables
(section 4 step 5). `--not-restored` states that `system` has not been restored yet; it is refused with `--paxos-dir`.
`--dc <name>` gives this node's data center; by default it comes from `nodes/local`, else `cassandra-rackdc.properties`.
`--serial-consistency` and `--cql` are described below.

**What it lists.** It reads, each sstable on its own, the `system.paxos` sstables of the data directories, of the
`pre-hcd-<timestamp>` snapshot (the only one, or the one named with `--snapshot`) and of each `--paxos-dir`. It opens
sstables offline, without validation, and only in the trie format: a `big` sstable is refused before it is opened, since
opening it could delete its `Summary.db` or rewrite its `TOC.txt`, and is a problem of the report (check it by hand with
`sstabledump` on a copy of its files). A row is open when `proposal_ballot` is set and `most_recent_commit_at` is absent
or older (by time-UUID timestamp); empty proposals (from a serial read or an LWT that did not apply) count. A proposal
no later than its commit is listed too when the commit expires before the proposal within 24 hours of the run, since DSE
would then read the proposal alone. It finds each table by `cf_id` in the schema of the data directories and of the
snapshot, renders the partition key as CQL, and writes one `SELECT … LIMIT 1` serial read per partition.

**What each partition gets**, by the keyspace's replication and the serial consistency:

| Action | Meaning |
|---|---|
| `READ` | The read is listed |
| `SKIPPED` | No read is needed: a replication factor of 2 at that consistency (every quorum includes the other replica), or a `LocalStrategy` keyspace |
| `CANNOT_PROTECT` | Replication factor 1: this node is the whole quorum; the round's Paxos state is lost at the rollback, and an open proposal of the snapshot can replay over newer data |
| `CHECK` | The read is listed, but the replication rules could not be applied: check by hand that it is needed and enough |
| `CHOOSE` | The keyspace has, or may have, replicas outside this node's data center (or the tool cannot tell), so `SERIAL` and `LOCAL_SERIAL` may be different quorums: no read until `--serial-consistency <keyspace>=<level>` names the level the application uses (`*` for every keyspace not named) |
| `MANUAL` | The partition cannot be located (a table in no schema, a key that cannot be decoded) or the replication contradicts this node: handle it by hand |

**Results.**

| Result | Exit | Meaning |
|---|---|---|
| `NOTHING_TO_READ` | 0 | No partition has an open round |
| `READS_LISTED` | 0 | Every open round is covered by the reads listed, or needs none: run the reads (step 6) |
| `NO_READ_NEEDED` | 0 | Open rounds, none of which needs a read |
| `NEEDS_ATTENTION` | 1 | Some partitions need a decision (`CHOOSE`, `MANUAL`, `CANNOT_PROTECT`, `CHECK`), or the report lists problems: unreadable sstables (a `big` one included), schemas or nodes files; a `nodes/peers` that DSE is rewriting or left half written (a `peers.old`, or a `peers.txn` without the file), that changed while read, is damaged or whose attributes cannot be read, whose peers are then not used, so the reads of the keyspaces whose replicas may span data centers are left to choose (name their consistency with `--serial-consistency <keyspace>=<level>`, and do not start DSE before every read has succeeded; for a file that cannot be read for its permissions, run the tool as the user DSE runs as, or as root) *(unit tests only)*; a data directory without `system.paxos` (`system/paxos-<id>`), whose Paxos state is then not read (check `--data-dir`), or `system.paxos` missing from the snapshot; an unreadable `cassandra.yaml`, for which DSE's default directories were assumed (pass `--conf-dir` or `--cassandra-yaml`, or `--data-dir` and `--metadata-dir`); a `--paxos-dir` with no `system.paxos` sstable, read twice, or not the directory it names; a `system` that looks restored or partly restored (pass `--paxos-dir` or `--not-restored`); an unknown data center, or one that contradicts the nodes files (pass or check `--dc`), or no data center in the nodes files with an unreadable `cassandra-rackdc.properties` (pass `--dc`, or `--rackdc <file>`); a directory whose sstables cannot be listed; a `--serial-consistency` keyspace in no schema |

On exit 1, read the report, run the tool again with the options it names, and repeat until only entries that need a
decision are left (section 4 step 5 lists them).

**How long the reads hold.** The tool reads the Paxos state as of its run. The screen output, both cqlsh files and the
JSON (`valid_until`) give a time 24 hours after the run: if DSE does not start before it, run the tool again, and run
the reads of the new files.

**The cqlsh files.** With `--cql <file>`, the tool writes the `SERIAL` reads to `<file>` and the `LOCAL_SERIAL` reads to
`<file>-local-serial.cql` (`<file>` without its `.cql`), deleting both files of an earlier run first. Without `--cql` it
prints both. Run the `SERIAL` file through another node, and the `LOCAL_SERIAL` file through another node of this node's
data center, with `cqlsh <node> -f <file>`: through a DSE node while one exists, as a role with `SELECT` on these
tables, with the other replicas of these partitions up. Run each file again until no statement fails; do not start DSE
before every read has succeeded (step 6). When the result is `NEEDS_ATTENTION`, each file starts with `-- INCOMPLETE:`
(what it leaves out; a mark of the rollback mode's files, not the node result `INCOMPLETE` of section 3) or `-- NOTE:`
(it leaves nothing out; the other consistency needs attention). A read to check is preceded by `-- CHECK by hand`.

## 6. What the tool does not check

The tool sees DSE's files, not the running process, and only one node at a time. Check these by hand:

| Not checked | Where the runbook covers it |
|---|---|
| DSE's JVM options: a `-Dcassandra.storage_port` or `-Dcassandra.ssl_storage_port` that overrides `cassandra.yaml`; and the port DSE actually listens on (`Starting Messaging Service on` in `system.log`) | Appendix A.11, checks (1) and (2) |
| With internode TLS on DSE 6.9.7 and later: that the running DSE uses `enable_legacy_ssl_storage_port` (it was restarted since it was set, and `ss -tln` lists both ports); in the mixed phase, the key on the DSE nodes other than the next one | Section 1, "Internode TLS on DSE 6.9"; section 3a, P3 |
| That every node runs the required DSE patch release (the tool checks 6.8 or 6.9, and one release across nodes) | Section 1, "Required DSE release"; section 3a, P3 |
| `hint_window_persistent_enabled` unset or `true` in DSE's `cassandra.yaml` | Section 1, "Required DSE release" |
| The seed lists, with several data centers | Section 1, order of work step 4b; section 3, "Seeds" |
| JDK 17 and the HCD 2.0 tarball staged on each node | Section 3, "Staging, before 3a" |
| The per-table NodeSync flag, and the short `gc_grace_seconds` | Section 1 item 6 |
| The replication of the replicated system keyspaces other than `system_auth` when `nodes/local` and `nodes/peers` record one data center at most and the data centers of some nodes are not known, so that there may be several (the tool checks them with several data centers known: `KEYSPACE_REPLICATION`), or when their check failed (`INSPECTION_ERROR`); with several known and the data centers of some nodes not known, the data centers not known (the tool asks for this check by hand); `SimpleStrategy` whose replication factor is at least the nodes, and more replicas than the nodes of a data center, which the rule of section 1 item 7 still covers | Section 1 item 7 |
| Driver settings: native protocol V4, no continuous paging (no trace on disk) | Section 3a, P8; appendix C |
| Batches left in `system.batches` | Section 3b, "Batches, before step 0" |
| In the mixed phase, the DSE nodes other than the next one (P0 inspects one node) | Section 3a, P1 to P4 and P9 |
| The upgraded nodes (`nodetool compatstatus`), and HCD 2.0's configuration once written | Section 3a, P7; section 3b step 3 |
| Changes not flushed to sstables (`SSTABLES_ONLY`) | Section 1 item 0: flush first |
| That the data center and rack of each node are the ones `nodetool status` shows | Section 1 item 0 |
| In the rollback mode: that HCD 2.0 was drained and stopped, and DSE not started | Section 4, step 5 |

## 7. DSE 5.1 nodes

The procedure of the upgrade from DSE 5.1.51 is [RUNBOOK-DSE51.md](RUNBOOK-DSE51.md); in this section "runbook" means
that file. The tool runs as on a DSE 6 node (from the unpacked HCD 2.0 tarball, with JDK 17, as root or as DSE's user;
a JDK its launcher refuses, the Java 8 of a DSE 5.1 node among them, ends the run with exit 1 before any report). What
differs:

**Which inspection.** The tool takes a node for a DSE 5.1 node when its metadata directory has no DSE 6 `nodes/local`
(or `nodes/local.old`) and the `DSVersion=` line of the installation's `ds_version.txt` names a release before 6, or,
without a readable `ds_version.txt`, when `system.local` is in big `m*` sstables. Every other node takes the DSE 6
inspection. A release before 6 other than 5.1.51 is then refused by the DSE 5.1 inspection (`DSE_RELEASE`).

**What it reads.** `system.local`, `system.peers`, `system_schema` and `system_auth.role_permissions` from their big
sstables, each opened from a scratch copy, so the node's files are never changed; DSE's `cassandra.yaml`, `dse.yaml`,
`cassandra-rackdc.properties` and `jvm.options` (`--jvm-options <file>`, by default next to `cassandra.yaml`; only its
lines that start with `-`); the data, commit log and hints directories; and the metadata directory HCD 2.0 uses on the
node (DSE 5.1 has none: by default `/var/lib/cassandra/metadata`, or `metadata_directory` of DSE's `cassandra.yaml`, or
`--metadata-dir`; `METADATA_DIRECTORY_ASSUMED` names it). With `--nodetool`, the read-only commands `version`, `status`,
`describecluster`, `statusthrift`, `compactionstats`, `netstats`, `listendpointspendinghints`, `gossipinfo` and
`tpstats`, run on Java 8: DSE 5.1's `nodetool` runs on Java 8 only, so pass `--nodetool-java-home <DSE's Java 8>`
(otherwise they run on the tool's JDK 17, which DSE 5.1's `nodetool` does not take: `LIVE_UNAVAILABLE`, the node
`INCOMPLETE` unless a finding blocks). **Flush first:** `nodetool flush system`, `nodetool flush system_schema` and
`nodetool flush system_auth`, one keyspace per command (`SSTABLES_ONLY` names the three).

**The report.** The header names the source (`DSE 5.1 preflight inspection for the upgrade to HCD 2.0`), the release
(`DSE release: DSE 5.1.51 (release 3.11.3.5151, …)`) and the peers of `system.peers`; there is no SAI version to pin.
The JSON report adds a `dse51` section (`source`, `start_rpc` and its JVM option, whether Thrift is on, the native
ports, whether row-level access control is enforced, the tool's Java, the TLS settings, the views restricted to rows and
the leftover view rows), which the aggregate reads. The settings to carry start with
`internode_compatibility_mode: DSE_5_1`, `storage_compatibility_mode: DSE_5_1`, `paxos_variant: v1`,
`sstable.selected_format: big` and `uuid_sstable_identifiers_enabled: false`, then the cluster name, `num_tokens` (from
`system.local` when `cassandra.yaml` sets none) and the token settings, the partitioner, the snitch
(`GossipingPropertyFileSnitch` in place of a DSE snitch, with the data center and rack of `system.local`), the seeds,
the addresses and storage ports, `native_transport_port` (required: the port of every DSE 5.1 node), the directories,
`metadata_directory` (the one the tool read), `saved_caches_directory`, `cdc_raw_directory`, `internode_compression`,
the encryption switches and the security classes (`PasswordAuthenticator`, `CassandraAuthorizer`,
`CassandraRoleManager`, or the allow-all ones). The upgrade steps are labelled `Preflight`, `Release`, `Thrift`, `JVM
options`, `DSE core only`, `Schema`, `Replication of system_auth`, `Replication of the other system keyspaces`,
`Materialized views`, `Security`, `Native port`, `Sstable formats`, `During the mixed phase`, `Hints`, `Snapshot` and
`Install`.

**In the mixed phase** (`UPGRADED_PEERS`), the remedies that are schema changes say instead that no schema change is
made then (`… no schema change is made in the mixed phase, whose DSE schema changes the upgraded nodes do not take:
upgrade no further node and report to the upgrade owner`), and those that drop or create a view say the same of views.
Some texts of the node's settings (`ROW_LEVEL_ACCESS` of `dse.yaml`, the traces of features outside DSE core) keep their
words "on DSE 5.1, before the first node is upgraded" in the mixed phase too: no schema change is made then either.

### 7.1 Node mode on a DSE 5.1 node

Sections cited are those of the runbook ("section N above" is one of this page's). The codes below are those of the DSE
5.1 inspection; the checks shared with DSE 6 (the authentication classes `AUTH_SCHEME`, `AUTH_CONFIG_CHANGE`,
`SCHEME_PERMISSIONS`, `TRANSITIONAL_MODE`, `UNSUPPORTED_AUTH_CLASS`; `UNLOADABLE_CLASS` of the partitioner;
`CONFIG_ENCRYPTION`, `AUDIT_LOGGING`, `DSEFS`, `SOLR_DATA` and `TIERED_STORAGE` of `dse.yaml`; `LOW_DISK_SPACE`,
`TXN_LOG_UNREADABLE`, the `LIVE_*` checks; and the grant warnings `PROXY_GRANT`, `DSE_ONLY_PERMISSION`,
`DSE_ONLY_RESOURCE`) work as section 3 above says. `DELEGATION` and `LONE_WRITE_GRANT` run but cannot fire: DSE 5.1 has
no `AUTHORIZE FOR` and no `UPDATE` or `TRUNCATE` permission. DSE 5.1 has no `RESTRICT`, no NodeSync and no SAI: those
checks are not made.

| Code | Severity | What it checks | What the operator does |
|---|---|---|---|
| `DSE_RELEASE` | blocking; info on 5.1.51 | `release_version` (`3.11.3.5151`) and `dse_version` (`5.1.51`) of `system.local`: another DSE 5.1 release, or no DSE 5.1 release at all, blocks | Upgrade every node to DSE 5.1.51 first, with DataStax's procedure (section 1, "Required DSE release") |
| `INSTALLED_RELEASE_MISMATCH`, `INSTALLED_RELEASE_UNKNOWN` | blocking; warning | As in section 3 above, against `system.local` | As in section 3 above |
| `NOT_A_DSE_NODE` | blocking | No row of `system.local` in the data directories | `nodetool flush system` on DSE, or point the tool at DSE's data directories (`--data-dir`) |
| `MIXED_RELEASES`, `PEER_RELEASE` | blocking | A peer of `system.peers` on another DSE 5.1 release; a peer neither DSE 5.1 nor HCD 2.0 | Upgrade them to DSE 5.1.51; a stale entry is removed with `nodetool removenode` on DSE |
| `PEER_RELEASE_UNKNOWN`, `NO_PEERS`, `UPGRADED_PEERS` | warning; info; info | A peer whose release `system.peers` does not record; no peer recorded; peers already on HCD 2.0 (the mixed phase) | Check those peers' releases; with `NO_PEERS` while `nodetool status` lists other nodes, `nodetool flush system` and run again |
| `SSTABLE_FORMAT` | blocking | Every sstable big `me` with a numeric generation: older big versions (`upgradesstables` on DSE 5.1 first), and files DSE 5.1 did not write (newer big versions, the trie format, non-numeric identifiers, unknown names) | `nodetool upgradesstables` on every node (section 1 item 2); move the other files out once their data is safe |
| `SSTABLE_WITHOUT_TOC` | warning | sstables without `TOC.txt` | Find out why before the upgrade |
| `THRIFT` | blocking; warning; info | Thrift on at DSE's start (`-Dcassandra.start_rpc` in `jvm.options`, else `start_rpc` of `cassandra.yaml`, true by default and as installed); with `--nodetool`, Thrift running although off at start is a warning. It names the JMX gauge `connectedThriftClients` without reading it | Move every Thrift client to CQL, `start_rpc: false`, no `-Dcassandra.start_rpc`, `nodetool disablethrift` or a restart (section 1 item 3) |
| `JVM_OPTIONS` | blocking | `-Dcassandra.force_3_0_protocol_version=true` or `-Denable-old-dse-state=true` in `jvm.options` | Remove them and restart DSE on those nodes, one at a time |
| `JVM_OPTIONS_READ` | info | Only `jvm.options` was read: options set in `cassandra-env.sh`, `JVM_OPTS`, `JVM_EXTRA_OPTS` or `/etc/default/dse` are not seen | Check the command line of the running DSE process by hand (section 1 item 0) |
| `JAVA` | info; warning when not 17 | The JDK the tool, and so HCD 2.0, runs on; DSE's Java 8 is not seen | Run the tool and HCD 2.0 on JDK 17; check DSE's Java 8 by hand (section 1, "Required DSE release") |
| `SNITCH`, `EC2_NAMING`, `IDENTITY_MISMATCH` | warning | The snitch (no `endpoint_snitch`, `DseSimpleSnitch`, `DseDelegateSnitch` or another DSE class, a class HCD 2.0 lacks); the EC2 legacy names; `cassandra-rackdc.properties`, `cluster_name` or `num_tokens` against `system.local` | Carry the snitch, data center, rack, cluster name and token count the node runs with (the settings to carry) |
| `NATIVE_PORTS` | blocking; info | Two native ports (`native_transport_port_ssl` set and different); otherwise the port to keep | One native port on DSE 5.1 first (section 1 item 4) |
| `TLS` | info | The internode and client encryption, `protocol`, `algorithm` and `cipher_suites`; never blocking | Carry them; check that DSE 5.1 on Java 8 and HCD 2.0 on JDK 17 negotiate before the first node (not validated) |
| `ROW_LEVEL_ACCESS` (settings) | warning; info | DSE 5.1 enforces row-level access control (`DseAuthorizer`, `authorization_options.enabled` and `allow_row_level_security`); info when allowed but not enforced | Section 1 item 1 |
| `TDE_TABLE` (setting), `TDE_COMMITLOG` | blocking | `system_info_encryption.enabled` in `dse.yaml`; `transparent_data_encryption_options.enabled` in `cassandra.yaml` | Appendix B: turn them off, with the restarts and rewrites the remedy gives |
| `HINTS_COMPRESSION` | blocking | `hints_compression` set, or DSE 5.1 hints files whose header records a compression, an encryption, a compressor HCD 2.0 lacks or another parameter | Remove the setting, restart DSE, wait until the files written before are delivered |
| `PENDING_HINTS` | warning (info when the tables cannot be read) | DSE 5.1 hints files (`<host id>-<timestamp>-1.hints`) not delivered yet, by target; files for host ids in neither `system.local` nor `system.peers`; a hints directory that cannot be listed | Before this node's upgrade, wait for their delivery, but for those of upgraded nodes (section 3b step 0) |
| `HINTS_UNREADABLE`, `HINTS` | warning; info | A DSE 5.1 hints file whose header cannot be read; no hints directory | Run the tool again |
| `NON_DSE_HINTS` | blocking | Hints files of another version than 1 (left by an earlier run of HCD 2.0) | Remove them, as the rollback does |
| `COMMITLOG_VERSION`, `COMMITLOG` | blocking (a warning when the directory cannot be listed); info | Segments of another version than DSE 5.1's 6, or whose version cannot be read; replayed segments left by HCD 2.0, or no directory | Find out what wrote them, move them out once their data is safe |
| `NON_CORE_WORKLOAD`, `SEARCH_INDEX`, `GRAPH_KEYSPACE`, `NON_CORE_KEYSPACE`, `ADVANCED_REPLICATION`, `TIERED_STORAGE`, `REMOVED_COMPACTION`, `SOLR_DATA` | blocking | The traces an upgraded node refuses at start: workloads of `system.local` and `system.peers`, search indexes, graph, Analytics, DSEFS and CFS keyspaces, `dse_advrep`, tiered storage and in-memory compaction, `DateTieredCompactionStrategy`, `solr.data` (also from `-Ddse.solr.data.dir`) | Section 1 item 1b |
| `COMPACT_STORAGE`, `SASI_INDEX` | blocking | `COMPACT STORAGE` tables; SASI indexes | `DROP COMPACT STORAGE` after `upgradesstables`; `DROP INDEX` (section 1 item 2) |
| `UNLOADABLE_KEYSPACE`, `UNLOADABLE_TYPE`, `UNLOADABLE_TABLE`, `UNLOADABLE_CLASS`, `TDE_TABLE` (schema), `LEGACY_AUTH_TABLES`, `JAVASCRIPT_UDF`, `UDF_LANGUAGE`, `TRIGGER` | blocking (`TRIGGER` a warning) | As in section 3 above, on DSE 5.1's schema | As in section 3 above, on DSE 5.1 |
| `COLUMN_TYPE` | blocking | A column of a table or a view of a type HCD 2.0 loads but does not write for DSE 5.1: a type outside those of Cassandra 3.11 and DSE's geometric and date range types, such as a custom type (a type HCD 2.0 cannot load, or a view whose base table it cannot load, is `UNLOADABLE_TABLE`); an upgraded node refuses to start on it | Drop the column on DSE 5.1, once its secondary indexes and the views that select it are dropped (the views created again without it), or its table for a column of the primary key (section 1 item 2) |
| `ROW_LEVEL_ACCESS` (schema and grants) | blocking | Tables restricted to rows (`DSE_RLACA`), and `rows/` grants, whether or not a table is still restricted | `UNRESTRICT ROWS` on each table, then `REVOKE` each grant (section 1 item 1) |
| `RESTRICTED_VIEW` | blocking | Views restricted to rows in this node's schema (another node's reports may not show them: the aggregate names every node) | `REVOKE` the view's `rows/` grants, drop it and create it again; never `UNRESTRICT ROWS` on a view (section 1 item 1) |
| `VIEW_TABLE_ROW` | blocking | A row of `system_schema.tables` named after a view, or with its id: the row a failed `UNRESTRICT ROWS` on a view leaves | Repair it on the node while DSE 5.1 runs there (section 6); a node already down: section 6, "A node already down" |
| `SYSTEM_AUTH_REPLICATION`, `KEYSPACE_REPLICATION` | blocking or warning; warning | As in section 3 above, with the data centers of `system.local` and `system.peers`; DSE 5.1's default is `SimpleStrategy` with one replica | Section 1 item 7 |
| `UNREADABLE_SCHEMA`, `UNREADABLE_INPUT`, `INSPECTION_ERROR` | blocking (`UNREADABLE_INPUT` a warning or a note for an input no blocking check needs) | `system_schema`, `system_auth.role_permissions`, `system.local`, `system.peers`, `jvm.options`, `cassandra.yaml`, `dse.yaml` or a directory that cannot be read; a part of the inspection that failed | Run as DSE's user or root, point the tool at the file, run it again |
| `DSE6_UPGRADE_FILES` | blocking | DSE 6's marker `dse-upgrade-in-progress`, or a record `dse-peers-seen` that lists a peer or cannot be read, in the metadata directory: HCD 2.0 refuses to start in `DSE_5_1` while they are there | Find out why; delete them once no DSE 6 node is in the cluster |
| `DSE51_SYSTEM_COLUMNS` | blocking with the marker; info without | The record `dse51-system-columns` (its `.tmp`, its `.unreadable-*` copies) beside the marker `dse51-upgrade-in-progress`: a rollback that kept both | Delete the record and its `.tmp` file, keep the marker |
| `MIXED_PHASE_MARKER`, `NODE_RAN_HCD`, `METADATA_DIRECTORY_ASSUMED` | info | The marker `dse51-upgrade-in-progress`; `dse51-peers-seen` (an earlier run of HCD 2.0, rolled back); the metadata directory read | Keep the marker while any node runs HCD 2.0 and while the node may be upgraded again; delete it at the flag drop (section 5, F2) |
| `SSTABLES_ONLY`, `DEFAULTS_ASSUMED` | info | The tables are read from sstables; the directories DSE's defaults gave | Flush first; check the directories |
| `LIVE_HINTS` | warning | With `--nodetool`: `listendpointspendinghints` lists pending hints | Wait for their delivery |

### 7.2 Aggregate of DSE 5.1 reports

The aggregate works as section 4 above says, with these differences. Its header names the release of the nodes:
`DSE 5.1 preflight inspection, cluster aggregate`, or `DSE 5.1 and DSE 6 …` beside `MIXED_SOURCES`. A report with a
`dse51` section is a DSE 5.1 report; a ready one without its lists of restricted views and leftover rows is
`UNREADABLE_REPORT`.

| Code | Severity | What it checks | What the operator does |
|---|---|---|---|
| `MIXED_SOURCES` | blocking | Reports of DSE 5.1 and of DSE 6 nodes in one aggregate | Aggregate one cluster at a time; one release supported for the upgrade first |
| `DSE_RELEASE`, `MIXED_RELEASES` | blocking | A DSE 5.1 node not on 5.1.51; nodes on different releases | Every node on DSE 5.1.51 |
| `RESTRICTED_VIEW`, `VIEW_TABLE_ROW` | blocking | The views restricted to rows and the leftover view rows, each with the nodes whose schema holds it | Runbook section 1 item 1 and section 6, on the nodes named |
| `VIEWS_NOT_KNOWN` | info | DSE 5.1 nodes whose views and rows are not known (their reports are blocked without them) | Inspect them again |
| `NO_PEERS_RECORDED` | blocking | A node whose `system.peers` records no other node while other nodes have reports | `nodetool flush system` on it and run the tool again |
| `NO_GRANTS_READ` | warning | DSE checks permissions, but no report read a row of `system_auth.role_permissions` | Inspect every node with all its data directories, after `nodetool flush system_auth` |
| `INTERNODE_TLS` | warning | The DSE 5.1 nodes differ in `internode_encryption` or `ssl_storage_port` | The same settings on every node before the upgrade |
| `MISSING_REPORT`, `SCHEMA_DISAGREEMENT`, `STALE_REPORT`, `NODE_BLOCKED`, `MIXED_STORAGE_PORTS`, `TOPOLOGY_MISMATCH` and the others of section 4 above | as in section 4 above | With `system.local` and `system.peers` in place of the nodes files | As in section 4 above |
| `ROLLBACK_CHECK_REPORT_SKIPPED` | info | Reports of the rollback check in the directory, skipped | Nothing |

### 7.3 Paxos rollback of a node going back to DSE 5.1

`--paxos-rollback` works as section 5 above says, its options, results and exit codes the same; on a node going back to
DSE 5.1 (told as the node inspection tells a DSE 5.1 node) the report is titled `DSE 5.1 rollback: open Paxos rounds of
this node`, its JSON has `rollback_to: DSE 5.1`, and:
- the data centers come from `system.local` and `system.peers` of the `pre-hcd-<timestamp>` snapshot (DSE 5.1 has no
  nodes files), else `--dc`, else `cassandra-rackdc.properties`; when those tables cannot be read, the data centers are
  unknown and the reads of the keyspaces whose replicas may span data centers are left to choose
  (`--serial-consistency`);
- a proposal stored at messaging version 10 or 11 is DSE 5.1's, one at `VERSION_40` HCD 2.0's; one at a DSE 6 version or
  below 3.0 is listed with a note, not decoded;
- on a node where HCD 2.0 wrote no `system.paxos` sstable (no lightweight transaction touched its replicas and no
  compaction of `system.paxos` ran), the first run exits 1 with `system looks already restored`, as for DSE 6: run it
  again with `--not-restored`.

### 7.4 Rollback check (`--rollback-check`)

On a node of HCD 2.0 in `DSE_5_1` mode about to go back to DSE 5.1.51, drained and stopped, `system` and `system_schema`
not restored yet, DSE 5.1 not started (runbook section 4 step 4; the tool does not check this precondition). It reads
file names, the descriptors and entries of the hints files, the entries of the commit log segments (counted, never
decoded) and `system.local` and `system.peers`, from scratch copies, and writes nothing to the node. Its directories
come from DSE's `cassandra.yaml`: pass `--commitlog-dir`, `--hints-dir`, `--metadata-dir` and `--saved-caches-dir` where
HCD 2.0's `cassandra.yaml` sets others, and `--snapshot <tag>` for the snapshot the restore takes when the node has
several complete `pre-hcd-*` ones. It takes no other option but `--dse-home`, `--conf-dir`, `--cassandra-yaml`,
`--data-dir`, `--json` and `--help`, and refuses a node inspected as DSE 6 (exit 2). Exit 0: the node can go back to DSE
5.1 (warnings allowed); 1: a blocking finding; 2: a usage or inspection error. The report lists the files the rollback
removes (the commit log segments, replayed ones included; the saved caches; `dse51-system-columns` and its `.tmp`),
those it may remove (`SAI+` component files) and those it keeps (the snapshot; the version-1 hints files with their
`.crc32` and `-Statistics.hints` companions; `dse51-upgrade-in-progress`, `dse51-peers-seen` and the `.unreadable-*`
copies of the record); its JSON report has `kind` `dse51_rollback_check`, with `hints_by_target`, `to_remove`,
`may_remove` and `kept`.

| Code | Severity | What it checks | What the operator does |
|---|---|---|---|
| `SSTABLE_FORMAT`, `SSTABLE_WITHOUT_TOC` | blocking; warning | An sstable that is not big `me` with a numeric generation; one without `TOC.txt` | Find out what wrote it; move it out once its data is safe, then check again |
| `TRANSACTION_LOG` | blocking | A `*_txn_*.log` left: an operation HCD 2.0 did not finish | Start HCD 2.0 again in `DSE_5_1` mode and repeat the rollback from step 1 (runbook section 4) |
| `COMMITLOG_UNFLUSHED`, `COMMITLOG_UNREADABLE` | blocking | A segment that holds mutations (a drain leaves none); a segment or directory that cannot be read | The same: start again and repeat the rollback from step 1 |
| `COMMITLOG_DIRECTORY_MISSING`, `HINTS_DIRECTORY_MISSING`, `SAVED_CACHES_DIRECTORY_MISSING`, `SAVED_CACHES_UNREADABLE` | warning | The directory is not there (or not HCD 2.0's), or cannot be listed | Pass the directory option; if it is HCD 2.0's directory, go on |
| `HINTS_VERSION` | blocking | A hints file of another version than 1, which DSE 5.1.51 never delivers | Start HCD 2.0 again and let it deliver them, or remove them with their companions and repair their tables later |
| `HINTS_DESCRIPTOR_UNREADABLE`, `HINTS_DAMAGED`, `HINTS_UNREADABLE` | blocking | An empty or damaged descriptor; a version-1 file DSE 5.1.51 would set aside as corrupt (its disk failure policy then stops the node's transports) or never deliver; a hints directory that cannot be listed | Move such files, with their companions, out of the hints directory before DSE 5.1 starts, and repair the tables of their targets once the cluster runs one release |
| `HINTS_UNVERIFIED` | blocking | A version-1 file the check could not read to its end here (its compressor is DSE 5.1.51's encrypting one, cannot be built here, an I/O error, a block too large) | Run again where it can be read, or move it out as above |
| `HINTS_TRUNCATED` | warning | A version-1 file of which DSE 5.1.51 would drop some hints (a partial entry at its end, hints whose checksum does not match) and deliver the others | Go on; repair the tables of its targets once the cluster runs one release |
| `HINTS_FOR_HCD` | warning | Version-1 files for targets that `system.peers` does not show running DSE 5.1 (HCD 2.0, or no release recorded): DSE 5.1.51 delivers hints only to a peer that gossips its own schema version | Before the drain, let HCD 2.0 deliver them (runbook section 4 step 2); otherwise they wait for the target's rollback or this node's next upgrade, or are removed and their tables repaired |
| `HINTS_FOR_UNKNOWN_TARGET`, `HINTS_TARGETS_UNKNOWN` | warning | Version-1 files for host ids in neither `system.local` nor `system.peers`, which DSE 5.1.51 neither delivers nor removes; the two tables cannot be read | Find out which node each host id was; remove the files of a node gone for good and repair their tables |
| `HINTS` | info | The version-1 files kept, by target | Nothing |
| `SNAPSHOT_MISSING`, `SNAPSHOT_AMBIGUOUS` | blocking | No complete `pre-hcd-*` snapshot (`system.local`, `system.peers` and DSE 5.1's ten `system_schema` tables, with sstables of `system.local` and `system_schema.keyspaces`), or none of the tag given; several without `--snapshot` | Find the snapshot taken before the upgrade, or pass `--snapshot <tag>` |
| `SNAPSHOT` | info | The snapshot the restore takes | Nothing |
| `MARKER_NOT_FOUND`, `METADATA_UNREADABLE` | blocking | The metadata directory read holds no `dse51-upgrade-in-progress`, is missing, or cannot be listed: not HCD 2.0's metadata directory, or HCD 2.0 never got past its startup checks in the mode, a post-action of which writes the marker; the message adds when the files show such a start (a segment of DSE 5.1 it replayed, the record `dse51-system-columns`, which every rollback removes), for a missing directory when the commit log shows that HCD 2.0 started (a segment it wrote or replayed), which makes that directory not HCD 2.0's, and when the commit log was not read | Pass `--metadata-dir` with HCD 2.0's `metadata_directory` (the directory a line `Wrote the DSE 5.1 upgrade marker <path>` of its logs names). For that directory: when the files show such a start, or HCD 2.0's logs say it could not write the marker or that a post-action of its startup checks failed, create the marker by hand and check again; otherwise it may remain alone only on the conditions of runbook section 4 step 4 (HCD 2.0's logs since the drain show its start refused, with none of those lines before it); else stop and report to the upgrade owner |
| `MIXED_PHASE_MARKER`, `DSE51_SYSTEM_COLUMNS`, `DSE51_SYSTEM_COLUMNS_COPIES` | info | The marker, kept; the record, which the rollback deletes; its unreadable copies, kept | As the report lists |
| `SAI_FILES` | warning | `SAI+` component files left (DSE 5.1.51 ignores them and removes them with their sstable) | Remove them or leave them; the rollback goes on |
| `UNREADABLE_INPUT`, `INSPECTION_ERROR`, `DEFAULTS_ASSUMED` | blocking; blocking; info | DSE's `cassandra.yaml`, a data or table directory that cannot be read; a part of the check that failed; the defaults taken | Run as HCD 2.0's user or root, pass the files or directories; report an `INSPECTION_ERROR` |
| `ROLLBACK_REMOVES`, `ROLLBACK_KEEPS` | info | The summary of what the rollback removes and keeps | Follow it (runbook section 4) |

### 7.5 What the tool does not check on a DSE 5.1 node

| Not checked | Where the runbook covers it |
|---|---|
| DSE's Java 8 build (the fix of JDK-8177809): the tool sees only the JDK it runs on | Section 1, "Required DSE release" |
| The compression of the tables and views beyond `TDE_TABLE` and `UNLOADABLE_CLASS` (`UNLOADABLE_TABLE` for a view): a `min_compress_ratio`, or a compressor HCD 2.0 loads other than LZ4, Snappy or Deflate (an upgraded node refuses to start on them) | Section 1 item 2 |
| JVM options set outside `jvm.options` (`cassandra-env.sh`, `JVM_OPTS`, `JVM_EXTRA_OPTS`, `/etc/default/dse`) | Section 1 item 0 |
| The Thrift clients still connected (`connectedThriftClients`, a JMX gauge) | Section 1 item 3 |
| That DSE 5.1 on Java 8 and HCD 2.0 on JDK 17 negotiate internode and client TLS | Section 1 item 0 (`TLS`) |
| Schema agreement among the DSE 5.1 nodes before each node, and DDL or repair run on a DSE 5.1 node during the mixed phase | Section 1 item 5; section 3a, P2 |
| Drivers and `cqlsh` on native protocol V4 | Appendix C |
| In the rollback check and the Paxos rollback: that HCD 2.0 was drained and stopped, and DSE 5.1 not started | Section 4 step 4 |
