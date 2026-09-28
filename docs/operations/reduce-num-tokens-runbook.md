# Runbook: reducing `num_tokens` (e.g. 256 → 16) on a live cluster

Applies to: this fork, branch `main-5.0` (HCD 2.0 line), gossip-based token
metadata (no TCM).

Reproduced by the in-JVM dtest
`test/distributed/org/apache/cassandra/distributed/test/ring/ChangeNumTokensTest.java`.

## 1. Summary

Changing `num_tokens` in `cassandra.yaml` has no effect on a node that already
joined the ring: its tokens are fixed at join and persisted in `system.local`.
There are two online procedures:

| | A. New datacenter (§3) | B. In place, `nodetool settokens` (§8) |
|---|---|---|
| Available in | any version | versions with `nodetool settokens` (every node must advertise it) |
| Extra hardware | one new node per node of the DC | none |
| Data streamed | one copy of the DC data set | about 1.3–2.1 copies, spread over several rounds |
| Disk headroom | none on the old nodes | the planner's peak ownership (about 1.5–1.7× the fair share on one node per round) plus the data kept until cleanup |
| Client changes | move clients to the new DC (`LOCAL_*` consistency levels, DC-aware policy) | none |
| Duration | one rebuild and a decommission | one shrink per node per round (e.g. 4 rounds for 256 → 16) |
| Rollback | easy until the old DC is dropped from the replication | every step is atomic; stop between steps at any time, the ring is always valid |

Procedure A is the only option on versions without `nodetool settokens`. The rest of this summary
explains why nothing simpler works.

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

## 5. Why a plain in-place change doesn't work (details)

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

## 8. Procedure B: in-place reduction with `nodetool settokens`

Every node keeps a subset of its tokens, in rounds (e.g. 256 → 128 → 64 → 32 → 16), one node at a
time. Each step streams the ranges the node gives up to their new replicas, like a partial
decommission. See `num-tokens-reduction-design.md` for how it works and the guarantees.

### 8.0 Prerequisites

1. **Versions and cluster state.**
   * Every node runs a version with `nodetool settokens`: a shrink is refused while any node that
     didn't leave doesn't advertise the capability, or is down.
   * Every node is `UN`, with no bootstrap, decommission, move, replace or removenode in progress.
   * Gossip is enabled on every node.
2. **No transient replication.** A keyspace that uses it makes every shrink refuse.
3. **Repairs.** A full repair of every keyspace completed recently: a shrinking node streams its own
   copy of the ranges it gives up, as decommission does. No repair may run during a step (the step
   is refused if one involves the shrinking node).
4. **Hints.** No hints pending for the node about to shrink (the script checks
   `nodetool listpendinghints` on every node).
5. **Disk capacity.** Size it from the plan (§8.1): the peak ownership of any node, plus the data a
   shrinking node keeps until its cleanup.
6. **Paxos.** With Paxos v2 each step runs the topology-change Paxos repair. Paxos v1 state
   (`system.paxos`) is not streamed, as with move and decommission.

### 8.1 Plan

```
nodetool ring -pp > ring.txt                  # on any node, with ports
tokenreductionplanner --ring ring.txt --replication dc1:3,dc2:3 --target 16 --output plan-$(date +%F)
```

* `--replication`: the RF of the keyspaces that hold most of the data, for every datacenter. A DC
  with RF 0 is balanced as RF 1.
* The default is to halve the token count each round. Use `--factor 1.5` or
  `--rounds 192,128,...` for a lower peak with more rounds.
* Read `plan.txt`:
  * **peak ownership**: with the loads from `nodetool ring`, the report gives it in bytes; compare
    it with the free disk of every node;
  * **streamed data**: gives the expected duration;
  * **final balance**.
* Always write a new plan directory; a plan is only valid for the ring it was made from.
  Re-plan if the ring changes (nodes added or removed).

### 8.2 Run

The script runs the steps of the plan in order, one node at a time:

```
tokenreduction-run --plan plan-2026-10-01 --host <any node> [--port 7199 -u <jmx user> -pw <jmx password>]
```

For each step it:
1. skips the step if the node already has the tokens to keep (or fewer, from a later round of the
   plan), and stops if they aren't a subset of its tokens;
2. checks that every node is up, none is joining, leaving or moving, and no node has hints for it;
3. runs `nodetool settokens --keep-file <step file>` on the node, which blocks until the shrink is
   done (RING_DELAY before streaming, the streaming itself, RING_DELAY after the new tokens are
   announced);
4. waits until every node sees the new tokens;
5. runs `nodetool flush` and `nodetool cleanup` on the node. Cleanup only rewrites sstables; the
   writes the node received for the ranges it gave up are in memtables until flushed.

The script stops at the first problem, with the reason. Fix the cause and run the same command
again: the steps already done are skipped.

Useful options:
* `--dry-run` checks every step without changing anything;
* `--round N` and `--datacenter DC` run part of the plan, e.g. one round, then a pause;
* `--no-cleanup` defers cleanups, for example to run them at night. The disk usage then grows
  until they run.

Manual equivalent, per step (in the order of the plan's step files):

```
nodetool listpendinghints                                              # on every node: no hints for the node
nodetool -h <node> settokens --keep-file plan/round-1-128/dc1/0001-10.0.0.1_7000.tokens
nodetool -h <each node> ring -pp | grep 10.0.0.1                      # every node sees the new tokens
nodetool -h <node> flush && nodetool -h <node> cleanup
```

While a node shrinks, `nodetool status`/`ring` show it as `Moving`, and its operation mode
(`nodetool info`, `netstats`) is `SHRINKING`.

### 8.3 After each round, and at the end

* `nodetool status <keyspace>`: ownership close to the plan's figures for the round.
* **`num_tokens`.** Set it in `cassandra.yaml` to the new count, on every node of the round. Until
  then a node restarts with its new token count thanks to `<metadata_directory>/token_count_override`
  (logging a warning), but it refuses any other `num_tokens` value. The file is removed once
  `num_tokens` matches.
* **At the end.** Set `num_tokens` (and `allocate_tokens_for_local_replication_factor`) for future
  nodes in your configuration management. Run a repair, as after any topology change.

### 8.4 Failure handling

| What | What happens | What to do |
|---|---|---|
| A shrink fails (streaming error, a node went down, a precondition changed during `RING_DELAY`) | The node rolls back: it keeps its tokens and every node drops the shrinking state | Fix the cause, run the script again |
| The shrinking node is restarted or crashes | This aborts the shrink: the node comes back with its previous tokens | Run the script again |
| The shrinking node dies for good | Its shrink state stays until the node is removed | `nodetool removenode` (allowed for the shrinking node itself) or replace it |
| You need to stop | Stop the script between steps (Ctrl-C while it waits); a running `settokens` finishes, or restart the node to abort it | Resume later with the same plan, as long as the ring didn't change |
| Out of disk during a round | The data of the ranges given up is still on the nodes that shrank | Flush and cleanup the nodes that already shrank; plan again with a smaller `--factor` |

Data streamed to the would-be replicas of an aborted step is harmless; their next cleanup removes it.

