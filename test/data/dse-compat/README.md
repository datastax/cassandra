# Artefacts written by real DataStax Enterprise nodes

These files are the test fixtures of the DSE 6 internode compatibility mode
(`internode_compatibility_mode: DSE_6`, see `docs/dse-to-cc5/`). They were produced by running DSE,
never by hand (the one exception, `conf/`, is the configuration the lab's Ansible rendered for a DSE
node), and are read through `org.apache.cassandra.dsecompat.DseFixtures`. Two DSE versions:

| Directory suffix | DSE build | `RELEASE_VERSION` | relative messaging version | origin |
|---|---|---|---|---|
| `dse-6.8.65` | 6.8.65, the public release (no build hash recorded) | `4.0.0.6865` | 4 | three-node lab cluster `dse-lab` (192.168.57.11–13, DC `dc1`) |
| `dse-6.9.26` | 6.9.26-0c77b67 (tag `6.9.26`, commit `0c77b67`) | `4.0.0.6926` | 6 | four different clusters, see below |

The 6.9.26 fixtures come from four clusters: `nodes/dse-6.9.26/` from a two-node cluster on the loopback
interface (127.0.0.1 and 127.0.0.2, DC `Cassandra`, cluster `Test Cluster`; collection answer A28),
`schema/` from a single-node cluster on the loopback interface (127.0.0.1, DC `Cassandra`;
collection answer A27), while `handshake/` (A31), `sstables/` (A29), `commitlog/` and `hints/`
(A32) were taken on the three-node lab cluster `dse-lab` at 192.168.57.11–13 running 6.9.26, and
`nodes/dse-6.9.26-2dc/` and `nodes/dse-6.9.26-2dc-mixed/` (A49) on the six-node lab cluster
`dse-lab` of two data centers at 192.168.57.11–16. The lab itself (Vagrant and Ansible) is a private
submodule, `docs/dse-to-cc5/lab`, pinned at `6b2e50b8e2` when the first fixtures were taken (the
sections whose fixtures were taken at a later lab commit name that commit); everything needed to
reproduce the fixtures is spelled out below, so the submodule is not needed to read them.

## `nodes/<version>/`

DSE keeps node identity in msgpack files under `<metadata_directory>/nodes/`.

* `local`: a map with 29 keys (host id, tokens, DC, rack, release and DSE versions, addresses and
  ports, cluster name, bootstrap state, truncation records, …). 6.8.65: `dc1`/`rack1`, cluster
  `dse-lab`, 8 tokens. 6.9.26: `Cassandra`/`rack1`, cluster `Test Cluster`, 8 tokens.
* `peers`: an array of 20-key maps, one per peer (6.8.65: two peers; 6.9.26: one peer, from the
  two-node run of A28). Host ids are msgpack arrays `[int64 msb, int64 lsb]`.
* `dse-6.9.26/peers-empty`: what a single node writes before it has any peer: an empty map (`80`).
  It does not depend on the version; the tests use it for 6.8.65 as well.
* `dse-6.9.26/local-after-truncate`: `local` after `TRUNCATE test_ks.c`; the `truncated_at` map holds
  one record keyed by the table id string `467ee541-b828-11f1-9f9f-754297a7ef99`, value
  `[timestamp ms, commit log segment id, position]` = `[1790262194516, 1790256233261, 253237]`
  (the plain `local` has an empty `truncated_at` map).

Copied from `docs/dse-to-cc5/collect/answers/captures/dse-6.8.65-lab/` and
`…/captures/dse-6.9.26-0c77b67/{node1-nodes-*.msgpack, nodes-peers.msgpack}`.

`dse-6.8.65` are the files of the first lab node (192.168.57.11, host id
`0c646ff5-7983-460a-9aad-b0cdd24df296`), taken while all three nodes ran DSE.

## `nodes/dse-6.8.65-mixed/`

`local` and `peers` of the second lab node (192.168.57.12, `dc1`/`rack2`, host id
`9833dda0-6508-4036-aec2-2813215da12f`), taken on 2026-10-02 at the same time as `schema/dse-6.8.65/sstables/`
and `conf/dse-6.8.65/`, while nodes 1 and 2 ran DSE 6.8.65 and node 3 already ran this software: the
entry of 192.168.57.11 is a DSE 6.8.65 peer, the entry of 192.168.57.13 records the release
`5.0.7.0-SNAPSHOT` and the DSE version `6.8.1` that upgraded nodes gossip to DSE nodes. Copied with
`sudo cat` from `/var/lib/cassandra/metadata/nodes/` on the running node.

## `nodes/dse-6.9.26-2dc/node<N>/` and `nodes/dse-6.9.26-2dc-mixed/node<N>/`

`local` and `peers` of the nodes of a DSE 6.9.26 cluster of two data centers (collection answer
A49, 2026-10-09; DSE 6.9.26 `0c77b67`, the lab at commit `264ac25`, this software built from commit
`48749214c4` of `dse-to-cc5-integration`): cluster `dse-lab`, six nodes at 192.168.57.11–16, eight
tokens per node, in each data center a rack of two nodes and a rack of one:

| node | address | data center | rack | host id |
|---|---|---|---|---|
| 1 | 192.168.57.11 | `dc1` | `rack1` | `519bda59-070b-4e10-b6f5-c5ea3f53208a` |
| 2 | 192.168.57.12 | `dc1` | `rack1` | `cf32b1f0-a357-4dbd-98b3-2ac5f1130827` |
| 3 | 192.168.57.13 | `dc1` | `rack2` | `2e6cba03-2570-4c06-983e-cac8637531d6` |
| 4 | 192.168.57.14 | `dc2` | `rack1` | `4edea6c5-e95e-49ea-8d05-ddf09fddc10f` |
| 5 | 192.168.57.15 | `dc2` | `rack1` | `dc59d445-10df-4270-acf0-00b534b86a24` |
| 6 | 192.168.57.16 | `dc2` | `rack2` | `80f593ae-296a-4114-8aab-61487b6e03a3` |

The files were read from `/var/lib/cassandra/metadata/nodes/` of each node by the read-only captures
of the questionnaire (Q49 step 1.7 and section 2, step 6) and copied unchanged.

* `dse-6.9.26-2dc/node1` to `node6`: the files of every node, taken while all six ran DSE 6.9.26
  (step 1.7, before any upgrade; `local` 728 bytes, `peers` 2381 bytes, an array of five 20-key
  entries, all with the release `4.0.0.6926` and the DSE version `6.9.26`). Copied from
  `docs/dse-to-cc5/collect/answers/captures/q49-6926/1-fresh/07-nodes-{local,peers}-n<N>.msgpack`.
* `dse-6.9.26-2dc-mixed/node1` and `node4`: the files of nodes 1 and 4 in the mixed phase, once the
  nodes of `dc2` were upgraded to this software in the order 5, 6, 4 (section 2, step 6). Node 1 ran
  DSE: its `peers` (2396 bytes, written by DSE at 10:29 by the node's clock) lists nodes 4 to 6 with
  the release `5.0.7.0-SNAPSHOT` and the DSE version `6.8.1` that upgraded nodes gossip to DSE nodes.
  Node 4 ran this software: its `peers` (2391 bytes) is the one DSE wrote at 10:25, before node 4 was
  drained (10:28) and switched, and lists nodes 5 and 6 already upgraded; the stage 3 import read it
  at node 4's first start (its marker `.imported-by-cc5-<host id>`, not copied, was next to it).
  Both `local` files are byte for byte those of the first set. Copied from
  `docs/dse-to-cc5/collect/answers/captures/q49-6926/2-dc2/06-nodes-{local,peers}-n{1,4}.msgpack`.

In every file, each node's data center, rack, host id and tokens are those of `system.local` and
`system.peers` on the same node (A49, checked when the answer was reviewed).

## `sstables/<version>-t/`

One flush of the collection table, written by the first node of each cluster. Schema and data, as
run with `cqlsh` (keyspace `test_ks`, `SimpleStrategy` with replication factor = number of nodes):

```
CREATE KEYSPACE IF NOT EXISTS test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
CREATE TYPE IF NOT EXISTS test_ks.addr (street text, zip int);
CREATE TABLE IF NOT EXISTS test_ks.t (id int, ck text, fu frozen<addr>, l list<int>, m map<text, int>, n bigint,
    s int static, tp frozen<tuple<int, text>>, u addr, v text, PRIMARY KEY (id, ck)) WITH CLUSTERING ORDER BY (ck ASC);
CREATE CUSTOM INDEX IF NOT EXISTS t_v_sai ON test_ks.t (v) USING 'StorageAttachedIndex';
-- for i in 1..100, for (j, ck) in (1, 'ck1'), (2, 'ck2'), with k = 2 * (i - 1) + j:
INSERT INTO test_ks.t (id, ck, fu, l, m, n, s, tp, u, v) VALUES (<i>, '<ck>', {street: 'Street<i>', zip: <100000 + i>},
    [<i>, <i + 1>, <i + 2>], {'m': <i>}, <100 * k>, <10 * i>, (<i>, 'tuple<i>'), {street: 'Avenue<i>', zip: <200000 + i>}, 'val<k>');
DELETE FROM test_ks.t WHERE id = 1;
DELETE v FROM test_ks.t WHERE id = 2 AND ck = 'ck1';
DELETE FROM test_ks.t WHERE id = 3 AND ck >= 'ck1' AND ck <= 'ck2';
```

followed by `nodetool flush test_ks`. The 200 inserted rows lose the two of partition 1 (partition
deletion) and the two of partition 3 (range deletion, inclusive at both ends); the cell deletion
removes no row. That leaves 196 live regular rows (`DseFixtures.COLLECTION_TABLE_LIVE_REGULAR_ROWS`).
The static row of partition 3 (`s = 30`) survives the range deletion, so `SELECT count(*) FROM t`
answers 197 (`COLLECTION_TABLE_COUNT_STAR`: one extra row for the one static-only partition,
`COLLECTION_TABLE_STATIC_ONLY_PARTITIONS`). `WHERE v = 'val50'` returns (25, ck2). Trie format `bb`,
numeric generation 1, with the SAI components in the `aa` naming (6.8.65, `SAI_…`) and the `ca`
naming (6.9.26, `SAI+ca+…`). Produced by the lab playbook `dse-write-t-sstable.yml` (6.8.65) and by
the collector (A29), both with the statements above. Both sets were read by this tree in the lab,
cold, with the index served in place (plan §6.17).

## `sstables/dse-6.8.65-mv/`

The sstables of a DSE 6.8.65 materialized view (`wide_by_v/`) and of its base table (`wide/`); the
first stopped the start of this software on an upgraded lab node (milestone M1, second attempt) with
`Unknown column .v during deserialization`. Schema and data come from the lab's data set (Ansible role
`roles/dataset`, template `dataset.cql.j2` with the role defaults, at the submodule commit above;
keyspace `lab`, `NetworkTopologyStrategy` with `dc1: 3`), loaded with `cqlsh` in this order (the
other tables of the data set left out):

```
CREATE TABLE IF NOT EXISTS lab.wide (pk int, ck int, v text, PRIMARY KEY (pk, ck));
CREATE MATERIALIZED VIEW IF NOT EXISTS lab.wide_by_v AS
  SELECT pk, ck, v FROM lab.wide WHERE v IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL
  PRIMARY KEY (v, pk, ck);
-- for p in 0..9, for r in 0..499:
INSERT INTO lab.wide (pk, ck, v) VALUES (<p>, <r>, 'v<r % 7>');
```

Both sstables were flushed by DSE on `192.168.57.13` (the node drained before the upgrade) and copied
unchanged from `/var/lib/cassandra/data/lab/wide_by_v-aa20e6b0b80b11f1acb39d12a1a4d70a/` and
`…/lab/wide-a6472ef0b80b11f1acb39d12a1a4d70a/`; trie format `bb`, generation 1, LZ4 compression. The
base sstable holds the 5000 rows in 10 partitions. The view sstable holds the 5000 view rows in 7
partitions (`v0` to `v2` with 720 rows, `v3` to `v6` with 710). DSE's schema lists `v` (partition key),
`pk` and `ck` (clustering) in `system_schema.columns` and one more column in
`system_schema.hidden_columns`: `.v`, kind `regular`, type `text`, `required_for_liveness = true`
(`system-schema-wide_by_v.txt`: cqlsh output, on the same node, of `SELECT *` from
`system_schema.views`, `system_schema.hidden_columns`, the view's rows of `system_schema.columns`
and `system_schema.dropped_columns`). The
serialization header of the view sstable lists `.v` as the only regular column; DSE's `sstabledump`
(`sstabledump-wide_by_v.json.gz`, gzipped) shows every row with the primary key liveness of its base
row and one `.v` cell with an empty value and the same timestamp, no TTL and no tombstone, and DSE's
`sstablemetadata` (`sstablemetadata-wide_by_v.txt`) counts `totalColumnsSet: 5000` and
`RegularColumns: .v:org.apache.cassandra.db.marshal.UTF8Type`; both ran on the DSE node against the
sstable as copied. DSE keeps such a hidden column for
a regular base column in the view primary key and decides the liveness of a view row by its cell alone
(`org.apache.cassandra.db.view.DseViewHiddenColumns` describes the rules).

`select-star.txt` is what DSE answered, unchanged cqlsh output, for

```
CONSISTENCY ALL;
PAGING OFF;
SELECT * FROM lab.wide_by_v;
```

run on the same node while the whole cluster ran DSE 6.8.65: 5000 rows, in the order of the sstable.

Every row of this view sstable was written by an `INSERT`, so its hidden cell has the timestamp of its
primary key liveness; rows whose hidden cell differs (base rows written by `UPDATE`, a changed, deleted
or expiring `v`) are not in it. The tests cover those with sstables written through a table that has a
regular column named `.v`; a DSE-written set needs writes on a DSE cluster that this fixture's
collection did not make.

## `handshake/<version>.txt`

The first bytes of storage-port connections between two DSE nodes of the same version (three-node
lab, 192.168.57.11–13), taken with `tcpdump` while the third node restarted (lab playbook
`capture-handshake.yml`; A31 for 6.9.26). On every node the capture ran as

```
tcpdump -Z root -i <interface of the private IP> -s 256 'tcp port 7000 or tcp port 7001' -w <tag>-<node>.pcap
```

then the node `192.168.57.13` was drained and restarted, the capture stopped once it was `UN`
everywhere, and each file was trimmed to its first 6000 storage-port packets with
`tcpdump -r <in> -w <out> -c 6000 'tcp port 7000 or tcp port 7001'`. The pcaps are
`docs/dse-to-cc5/collect/answers/captures/hs-6865-lab/` and `…/hs-6926/`; the fixtures were extracted
from the `dse-node-3` files with `docs/dse-to-cc5/tools/extract_handshake.py --fixture <pcap>`;
the `#` comment lines at the top of each file were added by hand.

Per connection: `msg1` (8 bytes, `CA 55 2D FA <relative> 00 FF 00`), `msg2` (the 4-byte answer,
`relative << 8`), `msg3` (the 4-byte version, the 1-byte-length IPv4 address of the connecting
node, then the DSE parameter block `02 000e "BASE_TIMESTAMP" 10 <8-byte timestamp in ms>`, 35
bytes in all) and `frame1`, the first frame after the handshake in DSE's own framing (4-byte
big-endian length, then that many bytes; a gossip SYN). `frame1` is not a whole TCP segment: the
segment after `msg3` can carry more than one frame and, with the 256-byte snaplen, was captured only
up to 190 bytes of payload (66 bytes of Ethernet, IP and TCP headers + 190 = 256). Provenance of
`frame1`: TCP segment payload, snaplen 256, cut to the first frame (4 + its declared length). The
tool warns on stderr about packets cut by the snaplen and counts a retransmitted segment once. Two connections per
file: a peer connecting to the restarted node, and the restarted node connecting to a peer.

## `commitlog/<version>/`

One `CommitLog-680-<id>.log.gz`: the active commit log segment (DSE version 680) of the first node,
gzipped, taken right after it inserted the collection table into keyspace `test_ks_fixture`
(`SimpleStrategy`, RF 3; type `addr`; table `t`, the statements above with `test_ks_fixture` for
`test_ks`, run at `CONSISTENCY ONE`) while the last node was down. The segment also holds earlier
mutations of the lab's other keyspaces; a replay test creates only `test_ks_fixture.t` with the id
of the table on the DSE node and expects the 196 live regular rows (`count(*)` 197) and the three
deletions. Lab playbook `dse-fetch-commitlog-hints.yml`.

* 6.8.65: table id `269b3740-b8d4-11f1-acb3-9d12a1a4d70a`; the segment holds the 200 inserts and the
  three deletions once.
* 6.9.26: table id `bd1016f0-bb1d-11f1-afdb-fb0517c9d423` (collection answer A32, with the type
  `addr (street text, zip int)`; `docs/dse-to-cc5/collect/answers/A32-commitlog-hints-fixtures.md`
  on the documentation branch `dse-to-cc5`, where both files are also kept under
  `collect/answers/captures/dse-6.9.26-0c77b67/commitlog-hints/`). The playbook ran twice on that cluster while the segment was
  active (the keyspace, type and table created by the first run, `IF NOT EXISTS`), so the segment
  holds the 203 mutations of the collection table twice, the second time with later timestamps;
  replayed, they leave the same data set.

## `hints/<version>/`

The hints file the same node wrote for the node that was down during those inserts
(`<host id>-<timestamp>-v1.hints`, DSE's hints format). Same playbook.

* 6.8.65: 141 KB, fetched before the peer came back and the hints were delivered; the host id
  `ed39c7b2-7f97-4a0a-91fc-71398afe1d12` is the one of peer 192.168.57.13 in `nodes/dse-6.8.65/peers`.
* 6.9.26: host id `ac3923c3-6b54-4752-af03-ff2831b62388`, the lab's third node (not in
  `nodes/dse-6.9.26/peers`, which comes from another cluster). The cluster was mixed when the
  playbook ran, twice: nodes 1 and 2 on DSE 6.9.26, node 3 already upgraded to this software
  (milestone M1) and drained and stopped by the playbook; node 1, which wrote both fixtures, kept
  the hints of the first run for node 3. The file was 55 037 bytes after the first run and 109 526
  bytes when fetched after the second: it holds the hints of both runs (twice the 203 hints of the
  fixture table; A32 describes the growth as a re-delivery after node 3 came back, which the content
  does not support), and the tests do not pin how many there are.

The header starts with the int `0xFFFFFFFF` (not a Cassandra hints descriptor version), then the
timestamp (8 bytes) and the host id (16 bytes) of the file name, then the parameters as
`len = 2`, a crc, `{}`, a crc. The two crcs are those of Cassandra's `HintsDescriptor`: a CRC32
over the version, the timestamp, the host id and the length, each long fed as its low then its high
32 bits (not over the bytes as they lie in the file), then continued over the parameters. The
records that follow are framed as Cassandra frames its own: the size of the hint, the CRC32 of the
size, the hint (creation time, gc grace seconds, then the mutation), the CRC32 of the size and the
hint. Both files hold hints of `test_ks_fixture.t` and of `system_auth.role_permissions` (the
permissions DSE grants on the new keyspace and table); the 6.8.65 file also holds hints of DSE's
`system_distributed.nodesync_status`. The hints catalog never opens a DSE hints file, in either mode
(`DseHintsFileTest`); in `DSE_6` the start of hints dispatch converts it into hints files of this
software and delivers them (`DseHintsConversionTest`, `Dse6HintsConversionTest`). `DseHintsContentTest`
shows what one holds: every hint of a table this software knows decodes with the mutation layout of
the DSE 6.8 messaging version (and not with its own), and those of the fixture table, applied, give
back the data set.

## `gossip/<version>/gossipinfo.txt`

`nodetool gossipinfo` of the first 6.9.26 node (A27): the application states DSE publishes, with the
values a peer of this software receives at messaging version 11 (`DSE_GOSSIP_STATE` JSON,
`NET_VERSION` 1536, `RELEASE_VERSION` 4.0.0.6926, the DSE-only port states). The names map to DSE's
`ApplicationState` ordinals as listed in collection answer A14 Q14.01; `TOKENS` is printed hidden.

## `paxos/dse-6.8.65/system-paxos.tsv`

Paxos state DSE 6.8.65 left in `system.paxos`: the 118 rows tagged with DSE's messaging version 1024
in `proposal_version` or `most_recent_commit_version` on the first node of the lab cluster (milestone
M4), read on 2026-10-03 with `cqlsh` after the node was upgraded to this software and the mode
dropped, before any fix touched them. One line per row, tab separated, blobs in hex without `0x`,
`null` for a column DSE left null; `in_progress_read_ballot`, which DSE does not have, is null in every
row and left out. The second node holds the same rows, except that the 9 empty commits carry their
commit time as maximum timestamp where the first node has `Long.MIN_VALUE`; the third node holds only
the 100 rows of `lab.lwt`.

* 100 rows of `lab.lwt` (`id int PRIMARY KEY, v int`, id `a77c0571-b80b-11f1-acb3-9d12a1a4d70a`): the
  data set loaded each id on 2026-09-24 with `INSERT … VALUES (<id>, 1) IF NOT EXISTS` then
  `UPDATE … SET v = 2 WHERE id = <id> IF v = 1` (lab role `dataset`); each row holds the commit of the
  second, `v = 2`, and no proposal (DSE nulls the proposal at commit but leaves `proposal_version`).
* 18 rows of `bbtest.t` (the collection table above, id `9f201230-b811-11f1-acb3-9d12a1a4d70a`)
  written by the lightweight transactions of milestone M3 on 2026-10-02, when the third node already
  ran this software (`m3-checks.txt` next to the capture), and stored by the two DSE nodes with DSE's
  version: 9 committed conditional batches (`UPDATE … IF` of row `u1` and `INSERT` of row `u3`, both
  with `v = 'cas-<coordinator>'`), 6 coordinated by the DSE nodes, whose ballots end in 0 (in tenths of a
  microsecond), 3 by the third node, whose ballots end in 2, this software's digit for a round at
  `SERIAL`; and 9 rounds that changed nothing: an empty commit of a DSE coordinator, then the empty
  proposal a serial read of the third node leaves (CASSANDRA-12126).

DSE wrote every column with the TTL of the table's `gc_grace_seconds` (864000; when the rows were
read, 88719 to 88720 seconds were left on the commits of `lab.lwt`, 779601 to 781322 on those of
`bbtest.t`) and with the timestamp of its ballot; the file keeps neither, the tests write the rows
back that way.

The update bytes carry the maximum timestamp of the update after the table id: the layout of DSE's
WRITES version DSE_68, which this software reads as `MessagingService.VERSION_DSE_68`, as in DSE's
commit log. That DSE uses the WRITES version of its current messaging version for every DSE 6 release
(OSS_30, without the timestamp, for 6.0 and 6.0.3; DSE_68 from 6.8.0 on, so also for the 1280 and
1536 of 6.9) follows from DSE's version matrix (collection answer A13 Q13.02); the 6.8.65 rows here
show it for 1024, the sstable of `paxos/dse-6.9.26/` for 1536.

Extracted from `docs/dse-to-cc5/collect/answers/captures/m4-6865-paxos/system-paxos-blobs-all-nodes.txt`,
the output of the three nodes as the lab's ad-hoc Ansible command printed it (no playbook): `cqlsh`
on each node, `PAGING OFF; SELECT row_key, cf_id, in_progress_ballot,
in_progress_read_ballot, proposal_ballot, proposal, proposal_version, most_recent_commit_at,
most_recent_commit, most_recent_commit_version, writetime(most_recent_commit_at),
ttl(most_recent_commit_at) FROM system.paxos;`, the first node's rows tagged 1024 kept.

## `paxos/dse-6.9.26/`

A `system.paxos` sstable as DSE 6.9.26 left it on disk (`bb-4-bti-*`, the eight components of one trie
`bb` sstable, generation 4, copied unchanged) on the first node of the three-node lab cluster `dse-lab`
at 192.168.57.11–13 running 6.9.26 (collection answer A39 Q39.08): `Statistics.db` names the
originating host id `1e7863b1-6b66-4b70-949a-dd3dd4c8ebba`, 192.168.57.11 (`dse-node-1.lab`). It was
copied after `nodetool flush system paxos`, later than the last commit it holds (2026-10-03 11:32:03 UTC),
while every node ran DSE. Its metadata covers commit log segments of 2026-09-28 07:31 and 2026-10-03
11:26 UTC (replay positions), so it may be a compaction of earlier flushes, which would explain why it
holds no tombstone for the proposals DSE nulled at commit (`system` tables have a `gc_grace_seconds` of
0). 103 rows, all of `lab.lwt` (`id int PRIMARY KEY, v int`, id `1fab9c81-bb0f-11f1-91d0-b3b5fc586fca`
on that cluster, not the 6.8.65 lab's):

* ids 0 to 99: the commits of the lab data set's `UPDATE lab.lwt SET v = 2 WHERE id = <id> IF v = 1`
  (lab role `dataset`), 2026-09-28 07:35:22–23 UTC;
* ids 9001, 9002 and 9003: the commits of the capture's `CONSISTENCY QUORUM; INSERT INTO lab.lwt (id, v)
  VALUES (...) IF NOT EXISTS` through the same node, 2026-10-03 11:32:03 UTC, with `v` = 1, 2 and 3 (read
  from the commits; the answer does not list the values). The keys were deleted afterwards with
  `DELETE ... IF EXISTS`, after this flush.

Every row has `proposal_version` and `most_recent_commit_version` 1536, the messaging version of DSE
6.9.7 and later; no `proposal` and no `proposal_ballot` (DSE nulls the proposal at commit and leaves its
version); `in_progress_ballot` equal to `most_recent_commit_at`, a ballot whose last digit in tenths of a
microsecond is 0, as for every DSE coordinator. The commits are in the layout of DSE's WRITES version
DSE_68 (the maximum timestamp after the table id), as the 6.8.65 rows above. Every cell has the
timestamp of its ballot and a TTL of 864000 seconds counted from DSE's clock at the write (the ballot's
second or the next), so the cells expire between 2026-10-08 07:35:22 UTC and 2026-10-13 11:32:03 UTC
(`DseFixtures.PAXOS_DSE_6_9_26_FIRST_EXPIRY` and `LAST_EXPIRY`, the local deletion times in
`Statistics.db`): the decoding tests read the sstable at fixed times, before and just after that; the
rollback tool reads it at the real clock, and its test expects every row before the first expiry and
none after the last. The
`Statistics.db` component holds DSE's incremental NodeSync metadata, which this software skips with
the WARN `Ignoring incremental node sync metadata`; `Rows.db` is empty (no partition is large enough
for a row index); `Digest.crc32` (1808493471) is the CRC32 of `Data.db` (`DseFixturesTest`).

Copied from `docs/dse-to-cc5/collect/answers/captures/dse-6.9.26-0c77b67/paxos/` of the project's
documentation branch `dse-to-cc5` (commit `78cc52d1c9`).

## `paxos/dse-6.8.65-m5/node3-paxos-sstables-during-hold.txt`

The `system.paxos` sstables of the third node of the DSE 6.8.65 lab cluster during the rollback of milestone M5
(2026-10-03), as `tools/bin/sstabledump -d` of this software printed them, rows of `m5.px` only (`id int PRIMARY
KEY, v int, note text`, id `0f59f853-bf41-11f1-9b7d-b57cc343566d`, keyspace `m5` with `SimpleStrategy` and replication
factor 3): HCD 2.0 was stopped after its drain, `system` was restored from the `pre-cc5-20261003T155007Z` snapshot and
DSE was not started yet. `bb-7` (this software's compaction of DSE's `bb-3` with its own flushes) and `bb-8` (its flush
at the drain) were in the data directory before the restore moved them to a quarantine; `bb-3` is DSE's own sstable,
restored from the snapshot. Each cell with its write time, and its TTL and local deletion time when it has them (the
cells written by hand for the experiment have none); a commit of this software deletes the proposal (`<tombstone>`).
The rows: open proposals of DSE (version 1024, ids 10 and 11, in `bb-3` and again in `bb-7`), open proposals of this
software (version 12, ids 20 to 22), the empty proposals the serial reads of this software left (ids 100 to 109), a
committed row of DSE with `proposal_version` and no proposal (id 12), and in `bb-8` the commits of ids 10, 11 and 20.
The `-- rows:` line after each sstable counts all its rows, of every table (122, 3 and 103); only the rows of `m5.px` are
printed (16, 3 and 3). The tests write the rows back cell by cell into sstables (`PaxosRollbackTest`). Copied from
`docs/dse-to-cc5/collect/answers/captures/m5-6865-paxos/node3-paxos-sstables-during-hold.txt` of the project's
documentation branch `dse-to-cc5`.

## `schema/<version>/describe-schema.txt`

The whole schema of the single-node 6.9.26 cluster of collection answer A27 (Q27.05) as cqlsh
describes it, copied unchanged from
`docs/dse-to-cc5/collect/answers/captures/dse-6.9.26-0c77b67/schema-full.txt`: the system keyspaces,
every keyspace DSE creates (`dse_system_local` and `dse_insights_local` with `LocalStrategy`,
`dse_system`, `dse_insights`, `solr_admin` and `system_backups` with `EverywhereStrategy`, `dse_leases`,
`dse_perf`, `dse_security`, `system_auth`, `system_distributed` and `system_traces` with
`SimpleStrategy`), their types and tables with DSE's table options (`nodesync`, `read_repair = 'BLOCKING'`,
`speculative_retry = '99PERCENTILE'`), the user keyspace `test_ks`, then the virtual keyspaces as
comments. `DseSchemaLoadTest` turns it back into the `system_schema` rows DSE stores and loads them as
a node does at startup. The row format it follows is the one of the `system-schema-*.json` files of the
same capture directory; they come from the separate two-node run of A28 and cover only its `test_ks.c`
and `test_ks.t`. Only 6.9.26 so far: the replication classes of DSE's keyspaces come from DSE 6.9.25's
code (A21.01), and the DSE 6.8.65 lab cluster of milestone M1 was reported with the same keyspaces and
classes (`dse_system_local` and `dse_insights_local` with `LocalStrategy`); the 6.8.65 schema is kept
as DSE's own sstables instead, see the next section.

## `schema/dse-6.8.65/sstables/`

A data directory of the second lab node (192.168.57.12) holding only DSE's `system_schema` (every
table directory, including DSE's own `hidden_columns`, `vertices` and `edges`) and
`system_auth/role_permissions-3afbe79f219431a7add7f5ab90d8ec9c`, as DSE 6.8.65 left them: trie `bb`
sstables with numeric generations (DSE compacts `system_schema` often, hence generations above 20000),
no snapshot or backup. Taken on 2026-10-02 from the running node, read-only, with
`sudo tar -C /var/lib/cassandra/data -cf - --exclude=snapshots --exclude=backups system_schema
system_auth/role_permissions-3afbe79f219431a7add7f5ab90d8ec9c`, at the same time as
`nodes/dse-6.8.65-mixed/`. The schema is the lab data set's (keyspaces `lab`, `bbtest`, `hintlab`,
`rfcheck`, `test_ks`, `test_ks_fixture`, the materialized view `lab.wide_by_v` with its hidden column
`.v`, SAI indexes) plus DSE core's own keyspaces (`dse_system`, `dse_system_local`, `dse_security`,
`dse_perf`, `dse_leases`, `dse_insights`, `dse_insights_local`, `solr_admin`, `system_backups`); the
grants are those DSE and the lab made, among them the `PROXY.LOGIN` and `PROXY.EXECUTE` DSE grants the
creator of a role (`cassandra` on `roles/dseadmin`). The preflight inspection
(`org.apache.cassandra.tools.dsepreflight`) reads it as it reads a node.

## `conf/dse-6.8.65/`

The configuration of the same node: `cassandra.yaml`, `dse.yaml` and `cassandra-rackdc.properties`
as the lab's Ansible rendered them under `/opt/dse/resources/{cassandra,dse}/conf/` (no comment
line; DSE core with internal authentication through `DseAuthenticator`, `DseAuthorizer` and
`DseRoleManager`, `GossipingPropertyFileSnitch`, NodeSync off in the YAML, `internode_compression: all`),
and the `ds_version.txt` of its DSE installation (`/opt/dse`), `DSVersion=6.8.65-4a7b9eb`. The
directories in `cassandra.yaml` are the node's (`/var/lib/cassandra/...`); the tests move them.
