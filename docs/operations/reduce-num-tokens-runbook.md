# Runbook: reducing `num_tokens` (e.g. 256 → 16) on a live cluster

Applies to: this fork, branch `main-5.0` (HCD 2.0 line), gossip-based token
metadata (no TCM).

Reproduced by the in-JVM dtest
`test/distributed/org/apache/cassandra/distributed/test/ring/ChangeNumTokensTest.java`.

## 1. Summary

**A node's token count cannot be changed in place.** The number of vnodes is
fixed when a node first joins the ring and is persisted in `system.local`.
The only supported, online way to move a cluster from 256 to fewer vnodes is
to **build a new logical datacenter with the new `num_tokens`, rebuild it from
the old datacenter, switch the clients, and decommission the old
datacenter**. It needs temporary extra hardware (one extra node per node of
the datacenter being converted), and the clients must be able to pin a local
datacenter and use `LOCAL_*` consistency levels.

Everything else we checked either fails or leaves the cluster badly
unbalanced:

| Attempt | Result | Where in the code |
|---|---|---|
| Change `num_tokens` in `cassandra.yaml` and restart | Node refuses to start: `Cannot change the number of tokens from 256 to 16` | `StorageService.joinTokenRing` |
| `nodetool move` | Rejected for vnodes: `This node has more than one token and cannot be moved thusly.` | `StorageService.move` |
| Replace a node (`replace_address_first_boot`) with a node configured with `num_tokens: 16` | Refused at startup: `Cannot replace <node>, which owns 256 tokens, with a node configured with num_tokens: 16`. A replacement always takes over **all** the tokens of the dead node (older builds accepted the replacement, took the 256 tokens and then failed on the next restart) | `StorageService.replaceNodeAndOwnTokens` takes the tokens from the replaced node's gossip state and checks them against `num_tokens` |
| Decommission a node, wipe it, bootstrap it back with `num_tokens: 16` in the same DC (rolling, one node at a time) | Works mechanically, but each converted node owns a share of the data proportional to its token count: in a 4-node RF=3 ring (3×256 + 1×16) the new node replicates 6.1% of the data instead of 75%, the old nodes keep ~98%. As the rollout progresses the remaining 256-token nodes absorb almost all data. Not viable. | `ReplicationAwareTokenAllocator.optimalTokenOwnership` = `replicas / (totalTokens + tokensToAdd)`: the allocator targets equal ownership **per token**, not per node. Random allocation has the same property on average. |
| New DC with `num_tokens: 16` + rebuild + decommission old DC | **Works**, verified end-to-end by the dtest | see §3 |

## 2. Background (what the code does)

* On first boot the node picks its tokens in
  `BootStrapper.getBootstrapTokens`: `initial_token` if set, otherwise
  `allocate_tokens_for_keyspace` / `allocate_tokens_for_local_replication_factor`
  (algorithmic allocation), otherwise `num_tokens` random tokens.
* The tokens are saved in `system.local` when the node joins. On every later
  start `joinTokenRing` loads the saved tokens and aborts if their count differs
  from `num_tokens`. There is no system property to bypass this and none
  should be invented: editing `system.local` by hand would not move any data.
* On replacement the new node's tokens are *copied from the replaced node*
  (`replaceNodeAndOwnTokens`), and the replacement is refused when
  `num_tokens` differs from their count, so the only way to change the token
  count of a "slot" is to remove it and add a new node.
* The token allocator (`TokenAllocation` / `ReplicationAwareTokenAllocator`)
  works per datacenter (and per rack when racks == RF). A new datacenter is
  therefore allocated independently of the old one, which is what makes the
  new-DC approach produce a perfectly balanced 16-token ring.
* Mixing token counts **across** datacenters is fine: replica placement of
  `NetworkTopologyStrategy` is computed per datacenter.
* Mixing token counts **inside** one datacenter is allowed but ownership
  follows the token count (see the table above).

## 3. Procedure: migrate through a new datacenter

Notation: `DC_OLD` is the existing datacenter (256 vnodes, N nodes),
`DC_NEW` the new one, `RF` the replication factor you want in `DC_NEW`
(normally the same as in `DC_OLD`). Repeat the whole procedure for every
datacenter of the cluster, one datacenter at a time.

### 3.0 Prerequisites and checks

1. Hardware for N new nodes (same sizing as `DC_OLD`). The new datacenter can
   live in the same physical location; it is only a logical DC name.
2. All nodes on the same version, cluster healthy: `nodetool status` shows
   every node `UN`; `nodetool describecluster` shows a single schema version.
   No bootstrap/decommission/repair/rebuild in progress.
3. **Every keyspace that must survive uses `NetworkTopologyStrategy`**,
   including `system_auth`, `system_distributed`, `system_traces` (and any
   product-specific keyspace using `SimpleStrategy`). `SimpleStrategy`
   keyspaces would be spread across both DCs at the moment the new DC
   joins. Check:
   ```
   SELECT keyspace_name, replication FROM system_schema.keyspaces;
   ```
   Convert every `SimpleStrategy` keyspace (except `LocalStrategy`/`system*`
   local keyspaces), e.g.
   ```
   ALTER KEYSPACE system_auth WITH replication =
     {'class': 'NetworkTopologyStrategy', 'DC_OLD': 3};
   ```
4. Clients:
   * driver load balancing policy is DC-aware with `local_dc = DC_OLD`;
   * reads/writes use `LOCAL_ONE` / `LOCAL_QUORUM`, and LWTs use
     `LOCAL_SERIAL`. `QUORUM`, `EACH_QUORUM`, `ALL`, `SERIAL` would involve
     the new DC as soon as it is added to the replication settings, which
     both adds cross-DC latency and can return incomplete data before the
     rebuild is finished.
   * `allow remote DC for local consistency levels` (or equivalent) is
     disabled.
5. Snitch is `GossipingPropertyFileSnitch` (or another snitch that lets you
   assign a new DC name to the new nodes).
6. Take a snapshot of the schema (`DESCRIBE SCHEMA > schema.cql`).
7. Optional but recommended: run a full repair of `DC_OLD` (`nodetool repair
   -full` or your usual repair tooling) so that the rebuild copies consistent
   data.

### 3.1 Configure the new nodes

On every new node, before the first start:

* `cassandra-rackdc.properties`: `dc=DC_NEW`, `rack=...`. Use **either one
  rack or at least RF racks**: the token allocator rejects
  `1 < racks < RF` ("number of racks ... is lower than its replication
  factor"). With racks == RF, start the first RF nodes in different racks.
* `cassandra.yaml`:
  ```yaml
  cluster_name: <same as the existing cluster>
  num_tokens: 16
  allocate_tokens_for_local_replication_factor: 3   # = RF of DC_NEW
  auto_bootstrap: false                             # data comes from rebuild
  seed_provider: ... existing DC_OLD seeds (+ 1-2 DC_NEW nodes once they are up)
  endpoint_snitch: GossipingPropertyFileSnitch
  ```
  Every other setting identical to `DC_OLD` (encryption, authenticator,
  partitioner, TDE keys, `commitlog`/disk layout, JVM options...).
  Do not set `initial_token`.
* Empty data, commitlog, hints and saved_caches directories.

### 3.2 Start the new nodes, **one at a time**

For each new node:

1. Start Cassandra.
2. Wait until it is `UN` in `nodetool status` from an old node and from the
   new node itself, and check it has 16 tokens:
   `nodetool info -T | grep -c Token` → 16 (or `nodetool ring`).
3. Check the log for `Algorithmic token allocation` / allocator statistics and
   no `Generated random tokens` line (random tokens would mean the allocation
   settings were not picked up).
4. Only then start the next node — the allocator uses the tokens of the nodes
   already in the ring; starting several nodes at once can produce poorly
   allocated tokens.

`DC_NEW` is now part of the ring but owns nothing: no keyspace replicates to
it yet. No client should be connected to it (keep `DC_NEW` nodes out of the
client contact points).

### 3.3 Replicate the keyspaces to the new DC

Only possible once at least one `DC_NEW` node is in the ring (unknown DC
names are rejected by `ALTER KEYSPACE`). For every NTS keyspace:

```
ALTER KEYSPACE <ks> WITH replication =
  {'class': 'NetworkTopologyStrategy', 'DC_OLD': 3, 'DC_NEW': 3};
```

Include `system_auth`, `system_distributed`, `system_traces`. Wait for schema
agreement (`nodetool describecluster`).

From this point **new writes are replicated to both DCs** (writes at
`LOCAL_QUORUM` in `DC_OLD` do not wait for `DC_NEW`; missed mutations go to
hints as usual). Existing data is not there yet.

### 3.4 Rebuild the new DC

On each `DC_NEW` node:

```
nodetool rebuild -- DC_OLD
```

* You may run it on several new nodes in parallel if `DC_OLD` can take the
  streaming load; throttle with `nodetool setstreamthroughput` /
  `setinterdcstreamthroughput` if needed.
* Monitor with `nodetool netstats`. If a rebuild fails, just run it again on
  that node (it re-streams the missing ranges).
* SAI / secondary indexes are streamed or rebuilt as part of the streaming;
  wait for index builds to finish (`nodetool compactionstats`;
  `SELECT * FROM system_views.indexes` must show `is_queryable = true` and
  `is_building = false` on every `DC_NEW` node) before sending reads that
  use them.
* Materialized views: rebuild streams base and view tables as sstables (no
  view rebuild), so views stay consistent with the base only if they were
  consistent on the source replicas.

When all rebuilds completed:

1. Recommended: run a full repair of the keyspaces **restricted to
   `DC_NEW`** (`nodetool repair -full -dc DC_NEW <ks>` on each node, or with
   your repair tool), or a cluster-wide full repair. `rebuild` copies each
   range from a single `DC_OLD` replica.
2. Validate: `nodetool status <ks>` shows `DC_NEW` owning 100% × RF / N per
   node, evenly; row counts / application checks from a `DC_NEW` coordinator
   at `LOCAL_QUORUM`.

### 3.5 Switch the clients to the new DC

Rolling restart / reconfiguration of the applications:

* `local_dc = DC_NEW`, contact points in `DC_NEW`, still `LOCAL_*`
  consistency levels.
* Watch `DC_OLD` client request metrics go to zero
  (`nodetool clientstats`, `ClientRequest` metrics). Also check
  cross-datacenter jobs (Spark, CDC, backups, monitoring) that may be pinned
  to `DC_OLD` nodes.

Rollback up to here is trivial: point clients back to `DC_OLD`.

### 3.6 Stop replicating to the old DC

For every keyspace **except `system_auth`**:

```
ALTER KEYSPACE <ks> WITH replication =
  {'class': 'NetworkTopologyStrategy', 'DC_NEW': 3};
```

`system_auth` cannot drop a datacenter that still has nodes
(`NetworkTopologyStrategy.validateExpectedOptions`: "Following datacenters
have active nodes and must be present in replication options for keyspace
system_auth"); keep `DC_OLD` there until 3.7 is done.

This is the point of no return for writes: from now on `DC_OLD` no longer
receives writes for these keyspaces.

### 3.7 Decommission the old DC

On each `DC_OLD` node, one at a time:

```
nodetool decommission --force
```

* `--force` is needed because `system_auth` still has RF=3 in `DC_OLD` and
  the DC has ≤ RF nodes left (`StorageService.decommission` refuses
  otherwise). For the other keyspaces `DC_OLD` has RF 0 so nothing is
  streamed; the decommission is quick.
* Wait for `nodetool status` to stop listing the node before moving to the
  next one; then stop Cassandra and remove it from the seed lists.
* Alternatively, once no keyspace except `system_auth` replicates to `DC_OLD`,
  stopping the node and `nodetool removenode <host-id>` from a `DC_NEW` node
  is equivalent (no data needs to move).

### 3.8 Clean up

1. From a `DC_NEW` node (the coordinator's own DC is always valid):
   ```
   ALTER KEYSPACE system_auth WITH replication =
     {'class': 'NetworkTopologyStrategy', 'DC_NEW': 3};
   ```
2. Update the seed list of every node to `DC_NEW` nodes only; rolling restart
   is not required for seeds but do it at the next maintenance.
3. Remove `auto_bootstrap: false` from the `DC_NEW` configuration (it is
   ignored once the node has joined, but a leftover `false` is dangerous on a
   future node built from the same template).
4. Verify: `nodetool status` only `DC_NEW`, all `UN`, 16 tokens each, even
   ownership; restart one node to prove the configuration is stable.
5. Optionally rename nothing: the DC name `DC_NEW` stays. Changing a DC name
   in place is not supported; if the old name must be kept, run the
   procedure twice (256 → `DC_TMP` → original name) or plan the name change
   into the migration.

## 4. Failure handling / rollback

| Step | Rollback |
|---|---|
| 3.1–3.2 | Stop the new node, `nodetool removenode` it, wipe it. |
| 3.3–3.4 | `ALTER KEYSPACE` back to `DC_OLD` only, then decommission / removenode the `DC_NEW` nodes. |
| 3.5 | Point clients back to `DC_OLD`. |
| 3.6 onward | `DC_OLD` misses writes: to roll back you must re-add `DC_OLD` to the replication and `nodetool rebuild -- DC_NEW` / repair it, i.e. run the procedure in the other direction. |

## 5. Why not in place (details)

With `ReplicationAwareTokenAllocator` a node's ideal ownership is
`numTokens × replicas / totalTokens`. Replacing one 256-token node of an
N-node DC by a 16-token node gives that node `16 / (256·(N−1) + 16)` of the
token space (×RF with replication). For the last step of a rolling
conversion of a 12-node DC, the one remaining 256-token node would own
`256 / (256 + 16·11) ≈ 59%` of the primary ranges and, with RF=3, a replica
of nearly all the data. The dtest
`testBootstrapWithFewerTokensInSameDatacenterIsUnbalanced` measures this on a
small ring (3×256 + 1×16, RF=3).

Hand-picking `initial_token` does not help: every new token can only split
one of the existing (tiny) 256-vnode ranges, so a node with 16 tokens can
never own more than ~16 of those slots while any 256-token node remains in
the same DC.

## 6. Choosing the new `num_tokens`

* 16 with `allocate_tokens_for_local_replication_factor: <RF>` is the
  default of `conf/cassandra.yaml` in this branch and what the procedure was
  tested with. `StartupChecks` warns for values above 16.
* Lower values (8, 4) are possible with the allocator; they make repair,
  streaming and range scans cheaper but make each future node addition move
  bigger chunks of data. Do not go to 1 unless you manage tokens by hand.
* Keep the same value on every node of the new datacenter.

## 7. Evidence (dtest)

`ChangeNumTokensTest` (in-JVM dtest, JDK 17,
`ant test-jvm-dtest-some -Dtest.name=org.apache.cassandra.distributed.test.ring.ChangeNumTokensTest`):

| Test | What it proves |
|---|---|
| `testRestartWithDifferentNumTokensFails` | restart with 256 saved tokens and `num_tokens: 16` is rejected; setting 256 back fixes it |
| `testReplacementWithDifferentNumTokensIsRefused` | a replacement configured with 16 tokens for a 256-token node is refused before streaming; with `num_tokens: 256` the same replacement succeeds |
| `testBootstrapWithFewerTokensInSameDatacenterIsUnbalanced` | 3 nodes × 256 random tokens + 1 node × 16 allocated tokens, RF=3: the new node replicates **6.1%** of the ring (60/1000 rows) while the old ones replicate **~98%** each (balanced would be 75%) |
| `testMigrateToFewerTokensThroughNewDatacenter` | the full §3 procedure: dc2 with 16 tokens joins with `auto_bootstrap: false`, keyspaces (incl. `system_auth`, `system_distributed`, `system_traces`) extended to dc2, `nodetool rebuild dc1`, dc1 dropped from replication, `nodetool decommission --force` on every dc1 node, `system_auth` cleaned up; all data readable at `ALL`, 3 × 16 tokens left in the ring, nodes restart fine, writes at `LOCAL_QUORUM` succeed |
