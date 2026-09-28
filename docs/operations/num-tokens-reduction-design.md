# Design: in-place reduction of `num_tokens`

Status: agreed design (decisions in §7). Phase 0 is implemented; Phases 1–3 are to be implemented in this order,
one pull request each, against `main-5.0` of the eolivelli fork.

Companion documents: `reduce-num-tokens-runbook.md` (today's procedure, via a new datacenter)
and the dtest `ChangeNumTokensTest`.

## 1. Goal and constraint

Goal: take a live datacenter from 256 (or any N) vnodes per node to fewer (e.g. 16) without extra
hardware, keeping the cluster available at `LOCAL_QUORUM` throughout.

Constraint (measured by `testBootstrapWithFewerTokensInSameDatacenterIsUnbalanced`): a node
owns the sum of the gaps in front of its tokens. While other nodes of the datacenter keep 256
dense tokens, a node with 16 tokens owns almost nothing (6.1% instead of 75% with 4 nodes,
RF=3). Therefore:

* every node of the datacenter must shrink, **in rounds** (e.g. 256 → 128 → 64 → 32 → 16);
* within a round, nodes shrink one at a time. The node that is last in a round carries up to
  `old/new` times its fair share (2× with halving), so the step factor is a trade-off between
  peak disk usage and the number of rounds (i.e. total data streamed);
* which tokens each node keeps decides the final balance, so it must be planned (Phase 1).

A node only ever **keeps a subset of its current tokens** (decision recorded 2026-09-28).
Consequence, from the replica walk of `SimpleStrategy`/`NetworkTopologyStrategy` (with or
without racks, also with fewer racks than RF): removing one of X's tokens `t` merges X's
primary range `(prev(t), t]` into the next range, and can remove X from the replica sets of
up to RF−1 ranges before `t`. It can never add X to a range: if X is not a replica of a
range, every token of X met on that range's walk was rejected by
`NetworkTopologyStrategy.addEndpointAndCheckIfDone` without changing the walk state, so
removing it leaves the walk identical. For the same reason, ranges X doesn't replicate are
not affected at all. So a shrinking node **only streams data out**, like a partial
decommission. It never fetches. The property is proved for these two strategies only; the
implementation checks it at runtime (§4.4) instead of assuming it for other strategies.

The data X streams is only as fresh as X's own replicas (as with decommission), so the
runbook requires a repair of the node's ranges before it shrinks (§5).

## 2. Phase 0 — fail fast (implemented)

* A replacement whose `num_tokens` differs from the replaced node's token count is refused in
  `StorageService.replaceNodeAndOwnTokens`, before any streaming. Previously the replacement
  succeeded with the old token count and then refused to restart.
* The restart error (`Cannot change the number of tokens from X to Y`) explains that the token
  count is fixed at join and what to do instead.

## 3. Phase 1 — planner / simulator (offline, no cluster changes)

New tool `tools/bin/tokenreductionplanner` (next to `generatetokens`, reusing
`OfflineTokenAllocator` infrastructure):

* **Input:** the current ring (output of `nodetool ring` or a CSV of
  endpoint, dc, rack, token), replication settings per datacenter (`dc:RF` list), the target token count, and
  either a step factor (default 2, i.e. halving) or an explicit list of rounds, e.g.
  `128,64,32,16`.
* **Selection algorithm:** a variant of `ReplicationAwareTokenAllocator` run in reverse. For each
  node in turn, choose which tokens to *drop*, greedily, always dropping the token whose
  removal most improves the variance of replicated ownership relative to the target (the same
  objective the allocator uses, including rack groups when racks == RF). Candidates are
  restricted to the node's current tokens.
* **Output:**
  * per round, per node: the tokens to keep (a file per node that Phase 2's command accepts);
  * after every step: max/min/stddev of replicated ownership per node, and the maximum
    ownership any node reaches during the round (which sizes the disk headroom);
  * bytes to stream per step, estimated as ownership change × per-node load (load from
    `nodetool status`, optional input);
  * a comparison with the new-datacenter procedure (RF copies of the data set once).
* **Tests:** unit tests on synthetic random rings (sizes 3–100 nodes, RF 1/3/5, 1 and 3 racks)
  asserting that the final ring balance is within a tolerance of what the allocator achieves
  for a fresh datacenter of the same size, and that the per-step peak matches the prediction.

This phase also answers whether subset-only selection balances well enough. If it doesn't,
we revisit the decision to allow new tokens before starting Phase 2.

## 4. Phase 2 — core: shrink a node's token set online

### 4.1 Operator interface

* `nodetool settokens --keep-file <file>` (the planner's output), or
  `--keep <t1,t2,...>`; plus a JMX operation `StorageServiceMBean.shrinkTokens(List<String>)`.
* Preconditions (refused otherwise):
  * the new set is a strict, non-empty subset of the node's current tokens;
  * the node is `NORMAL`, and the local token metadata shows no bootstrapping, leaving,
    moving or shrinking node (the check done before bootstrap in `prepareForBootstrap`:
    "Other bootstrapping/leaving/moving nodes detected"; `move` itself does not check this).
    This only reflects the local gossip view, so two operators starting on two nodes at the
    same time are not detected: the Phase 3 script serialises the steps. Before bootstrap the
    check runs only with `useStrictConsistency`; the shrink always runs it;
  * no endpoint is in HIBERNATE or BOOT_REPLACE state. A replacement at the same address gossips
    HIBERNATE, gets no bootstrap tokens and is skipped by the version gate (§4.2), yet it may run
    older code or turn NORMAL while X is SHRINKING and then ignore the status;
  * every node in the cluster runs a version that understands the new gossip state
    (§4.2);
  * the node has no pending ranges;
  * no keyspace uses transient replication. The pending-range calculation compares endpoint
    sets only (`Sets.difference` in the moving-endpoint loop), so another replica switching
    from transient to full when X drops out would get no pending full range.
* `--dry-run` prints the ranges and estimated bytes each target node will receive.

### 4.2 Gossip

* New status `SHRINKING,<t1>,<t2>,...` listing the tokens kept
  (`VersionedValueFactory.shrinking(Collection<Token>)`), published in both
  `STATUS_WITH_PORT` and the legacy `STATUS`, as `move` does.
* Size: `MAX_NUM_TOKENS` is 1536 and a `RandomPartitioner` token can be 39 digits, so the
  worst case (~61 KB) comes close to the 65535-byte `writeUTF` limit of `VersionedValue`
  serialization. The command refuses a kept set whose encoded status exceeds 32 KB. With
  Murmur3 (≤ 20 characters per token) that still allows about 1500 tokens, so real rings
  (≤ 256 tokens) are far from the limit.
* Nodes running older code **silently ignore unknown statuses** (`StorageService.onChange`
  switch), so they would not send writes to the pending replicas. The operation is therefore
  gated on a minimum release version (decision 2026-09-28): the first release that contains
  this code. `Gossiper.getMinVersion()` is not used as is. It is cached for 60 s, it returns
  `NULL_VERSION` while gossip stabilises, and it skips endpoints in LEFT, REMOVED or HIBERNATE
  state. The gate reads `RELEASE_VERSION` of every endpoint in gossip except those in LEFT or
  REMOVED state (live or down, HIBERNATE included, which §4.1 refuses anyway), and refuses if
  any version is missing, unparsable or older.
* On completion the node publishes `TOKENS` = kept set and `NORMAL`, exactly like `move`. Peers
  already handle a `NORMAL` endpoint whose token set shrank: `TokenMetadata.updateNormalTokens`
  replaces the endpoint's tokens.

### 4.3 Token metadata and pending ranges

* `TokenMetadata`: add `shrinkingEndpoints: Map<InetAddressAndPort, Set<Token>>` (kept
  tokens) with `addShrinkingEndpoint` / `removeFromShrinking` (bumping the ring version),
  cleared by `updateNormalTokens`/`removeEndpoint`/`clearUnsafe`, applied by
  `cloneAfterAllSettled`, and copied into the pending-range snapshot. It must be added to
  every place that lists range movements, in particular:
  * the early return of `calculatePendingRanges` / `unsafeCalculatePendingRanges`
    (`bootstrapTokens.isEmpty() && leavingEndpoints.isEmpty() && movingEndpoints.isEmpty()`).
    If it is missing there, no pending range is computed and writes are lost;
  * the "other bootstrapping/leaving/moving nodes" checks, `nodetool status`/`ring` state
    reporting and `getMovingEndpoints`-style accessors.
* `calculatePendingRanges`: process shrinking endpoints like the existing moving-endpoint loop.
  Compute the replicas before and after `allLeftMetadata.updateNormalTokens(kept, endpoint)`;
  every endpoint that becomes a replica of an affected range gets a pending range. With the
  subset-only rule, the shrinking node itself never gets one.
* Writes: coordinators already add pending replicas to the write set, so no change.
  Reads: served by the current natural replicas until the switch to `NORMAL`, as for
  move/decommission.

### 4.4 Local operation (`StorageService.shrinkTokens`)

Mirrors `move(Token)`:

1. Validate the preconditions, set mode `SHRINKING`, gossip the `SHRINKING` status, and sleep
   `RING_DELAY` so that every coordinator sees the pending ranges.
2. `repairPaxosForTopologyChange("shrink")` (Paxos v2), like bootstrap/decommission/move.
3. Stream out: `RangeRelocator` generalised to a token set. Today `calculateToFromStreams`
   loops over the target tokens and asserts `currentTokens.size() == 1`. For a shrink it must
   call `getPendingAddressRanges(metadata, keptSet, endpoint)` **once** with the whole kept
   set. If the resulting fetch side isn't empty (the subset property doesn't hold for this
   strategy), abort before streaming.
   Hints: a hint for X delivered after X has streamed a range is applied on X only, so the new
   replica misses it. The Phase 3 script checks with `nodetool listpendinghints` on every node
   that no hints for X are pending before starting. Writes during the operation reach the new
   replicas through the pending ranges. The batchlog is unaffected.
4. `setTokens(kept)`: `SystemKeyspace.updateTokens`, `TokenMetadata` update, gossip `TOKENS` and
   `NORMAL`, bump the ring version. Disk boundaries and UCS replica-aware shards are recomputed
   on ring-version change; verify that for `DiskBoundaryManager`, `ShardManagerReplicaAware`
   and the CNDB token tracker hook in `TokenAllocation`.
5. Log and `nodetool` output: remind to run `nodetool cleanup` on this node and to update
   `num_tokens` in `cassandra.yaml`.

Failure handling:

* Streaming fails, node still up: explicit rollback. Gossip `NORMAL` with the unchanged
  tokens, so peers drop the shrinking state and the pending ranges, and set the mode back
  to `NORMAL`. `move` lacks this and stays `MOVING` forever; the shrink must not copy that.
* Node restarts mid-operation, before step 4: `system.local` still has the old tokens. On
  restart the node announces `NORMAL` with them and peers drop the shrinking state
  (`handleStateNormal`).
* Crash after step 4 has written `system.local`: the tokens and the recorded token count (§4.5)
  are written in the same `system.local` mutation, so the restart accepts the new count.
* In every case, data already streamed to the would-be replicas is harmless and removed by
  their next `cleanup`, and the operation can be retried.

### 4.5 Restart after a successful shrink

The `system.local` token count now differs from `num_tokens` in the yaml, and `joinTokenRing`
would refuse to start. Decision (2026-09-28): **record the shrink and accept it**. Step 4
writes the new token count to `system.local` (a new nullable column, e.g.
`token_count_override`, in the same mutation as the tokens). At startup `joinTokenRing`
accepts saved tokens whose count differs from `num_tokens` only if it equals the recorded
count, and logs a warning to update `num_tokens` in `cassandra.yaml`. When the yaml matches
again, the override is cleared.

In this fork `system.local` is written through `Nodes`/`LocalInfo`, one INSERT per save
(`NodesPersistence.saveLocal`, `INSERT_LOCAL_STMT`). "Same mutation" therefore means: a new
`LocalInfo` field, the column in `INSERT_LOCAL_STMT` and in the `SystemKeyspace` table
definition, and handling in `CC4UpgradeNodesPersistence` / `CC4NodesFileReader`. Downgrade
risk: an older binary reading `system.local` sstables that contain the unknown column. The
Phase 2 PR must test the downgrade, or document that downgrading requires the override to be
cleared first.

The Phase 0 restart message ("the number of tokens of a node is fixed when it joins the
ring") becomes inaccurate once this ships; the Phase 2 PR updates it.

### 4.6 Tests

* Unit: `TokenMetadata` pending ranges for shrinking endpoints (SimpleStrategy, NTS with 1
  rack and with racks == RF), clone-after-settled, interaction with a concurrent leave (must be
  refused).
* dtests, extending `ChangeNumTokensTest`:
  * shrink one node under continuous `QUORUM` writes, then verify with `ALL` reads and
    `nodetool repair --validate`-style checksums that no write was lost;
  * restart the shrinking node in the middle of streaming and retry;
  * refusal while another range movement is in progress, and when the new set isn't a subset;
  * full rounds 256 → 64 → 16 on 4 nodes following a planner output, with ownership asserted
    against the planner's prediction;
  * mixed-version refusal (a node that reports an older `RELEASE_VERSION`).

## 5. Phase 3 — orchestration and runbook

* Runbook section "in-place reduction": capacity check (disk ≥ the planner's peak), repair of
  the node's ranges and no pending hints for it before each step, planner run, per-step
  `settokens` → wait `UN` → `cleanup`, verification after each round, abort/rollback (stop
  between steps: the ring is always valid).
* A small script (`tools/bin/tokenreduction-run`, optional) that drives the steps through
  `nodetool`, one node at a time, stopping on the first failure.

## 6. Risks

* Total data streamed is several times what a new datacenter copies; the planner quantifies
  it. The two procedures trade hardware for streaming time.
* New gossip status: must never be emitted to a cluster that still has older nodes (§4.2).
* Peak ownership during a round: halving gives up to 2× on one node; the planner must report
  it and the runbook must require the headroom.
* Interaction with running repairs (incremental repair sessions spanning a topology change):
  refuse the operation while repairs are running on the node. `move` doesn't check this
  either; the shrink should.

## 7. Decisions (2026-09-28)

1. Only subsets of the current tokens (§1).
2. Replacement with a different `num_tokens` is refused at startup (§2).
3. Restart after a shrink: record the new count in `system.local`, accept it, warn (§4.5).
4. Version gate: minimum release version, read directly from gossip (§4.2).
5. Planner default: halve the token count each round; the factor or explicit rounds can be
   passed (§3).
