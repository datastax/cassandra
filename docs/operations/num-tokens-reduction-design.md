# Design: in-place reduction of `num_tokens`

Status: proposal. Phase 0 is implemented; Phases 1–3 are to be implemented in this order,
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
Consequence, from the replica walk of `SimpleStrategy`/`NetworkTopologyStrategy`: removing one
of X's tokens can only remove X from the replica set of the ranges before that token, never
add X to a new range. So a shrinking node **only streams data out**, from a single
consistent source (itself), like a partial decommission. It never fetches.

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
  either a step factor or an explicit list of rounds, e.g. `128,64,32,16`.
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
  * the node is `NORMAL`, and no other node is bootstrapping/leaving/moving/shrinking (same
    check as `move`);
  * every node in the cluster runs a version that understands the new gossip state
    (§4.2);
  * the node has no pending ranges.
* `--dry-run` prints the ranges and estimated bytes each target node will receive.

### 4.2 Gossip

* New status `SHRINKING,<t1>,<t2>,...` in `STATUS_WITH_PORT`, listing the tokens kept
  (`VersionedValueFactory.shrinking(Collection<Token>)`). At most 255 tokens × ~20 chars,
  well under the `writeUTF` limit.
* Nodes running older code **silently ignore unknown statuses** (`StorageService.onChange`
  switch), so they would not send writes to the pending replicas. The operation is therefore
  gated on `Gossiper.instance.getMinVersion()` ≥ the first release that contains this code,
  which also covers nodes that are down: their last known version counts.
* On completion the node publishes `TOKENS` = kept set and `NORMAL`, exactly like `move`. Peers
  already handle a `NORMAL` endpoint whose token set shrank: `TokenMetadata.updateNormalTokens`
  replaces the endpoint's tokens.

### 4.3 Token metadata and pending ranges

* `TokenMetadata`: add `shrinkingEndpoints: Map<InetAddressAndPort, Set<Token>>` (kept
  tokens) with `addShrinkingEndpoint` / `removeFromShrinking`, cloned by `cloneAfterAllSettled`
  (which applies the kept set), cleared by `updateNormalTokens`/`removeEndpoint`, and included
  in the "any range movement in progress" checks.
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
3. Stream out: `RangeRelocator` generalised to a token set. Drop the `currentTokens.size() > 1`
   assertion and use the existing
   `getPendingAddressRanges(metadata, Collection<Token>, endpoint)`. With the subset rule the
   fetch side is empty and the stream side is the ranges whose replica set changes. Hints and
   batchlog are unaffected.
4. `setTokens(kept)`: `SystemKeyspace.updateTokens`, `TokenMetadata` update, gossip `TOKENS` and
   `NORMAL`, bump the ring version. Disk boundaries and UCS replica-aware shards are recomputed
   on ring-version change; verify that for `DiskBoundaryManager`, `ShardManagerReplicaAware`
   and the CNDB token tracker hook in `TokenAllocation`.
5. Log and `nodetool` output: remind to run `nodetool cleanup` on this node and to update
   `num_tokens` in `cassandra.yaml`.

Failure handling: if streaming fails or the node restarts mid-operation, the tokens in
`system.local` are still the old ones. On restart the node announces `NORMAL` with its old tokens and
peers drop the shrinking state (`handleStateNormal`). Data already streamed to the would-be
replicas is harmless and removed by their next `cleanup`. The operation can simply be retried.

### 4.5 Restart after a successful shrink

The `system.local` token count now differs from `num_tokens` in the yaml, and `joinTokenRing`
refuses to start. Options (decision pending, see §7):

* (a) keep the check: the runbook requires updating `num_tokens` before the next restart; or
* (b) record the shrink in `system.local` (e.g. the kept token count and the timestamp) and
  accept a saved count that matches the recorded one, logging a warning to update the yaml.

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

* Runbook section "in-place reduction": capacity check (disk ≥ the planner's peak), planner
  run, per-step `settokens` → wait `UN` → `cleanup`, verification after each round,
  abort/rollback (stop between steps: the ring is always valid).
* A small script (`tools/bin/tokenreduction-run`, optional) that drives the steps through
  `nodetool`, one node at a time, stopping on the first failure.

## 6. Risks

* Total data streamed is several times what a new datacenter copies; the planner quantifies
  it. The two procedures trade hardware for streaming time.
* New gossip status: must never be emitted to a cluster that still has older nodes (§4.2).
* Peak ownership during a round: halving gives up to 2× on one node; the planner must report
  it and the runbook must require the headroom.
* Interaction with running repairs (incremental repair sessions spanning a topology change):
  refuse the operation while repairs are running on the node, as `move` implicitly relies on.

## 7. Open decisions

1. Restart after shrink: §4.5 (a) require a yaml update, or (b) record the shrink and accept.
2. Version gate: a minimum release version (simple; needs the version number at release time)
   versus a capability advertised in gossip (version-independent, but consumes one of the
   `X1..X10` padding application states shared with upstream).
3. Default step factor for the planner: halving (4 rounds from 256 to 16, 2× peak) or a smaller
   factor (more rounds, lower peak).
