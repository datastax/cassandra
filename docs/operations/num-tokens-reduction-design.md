# Design: in-place reduction of `num_tokens`

Status: agreed design (decisions in §7). Phases 0 and 1 are implemented; Phases 2–3 are to be implemented in this order,
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
  objective the allocator uses; the target of each node is proportional to the number of tokens
  it keeps, which with racks == RF also balances inside each rack). Candidates are
  restricted to the node's current tokens.
* **Output:**
  * per round, per node: the tokens to keep (a file per node that Phase 2's command accepts);
  * after every step: max/min/stddev of replicated ownership per node, and the maximum
    ownership any node reaches during the round (which sizes the disk headroom);
  * bytes to stream per step, estimated as ownership change × per-node load (load from
    `nodetool status`, optional input);
  * a comparison with the new-datacenter procedure (RF copies of the data set once).
* **Tests:** unit tests on synthetic random rings (6–100 nodes, RF 1/3/5, 1, 3 and 5 racks)
  asserting that the final ring is within 8% of the fair share, and a replay of every step of a
  plan against the real NetworkTopologyStrategy (ownership after each step, streamed data, peak).

This phase also answers whether subset-only selection balances well enough. If it doesn't,
we revisit the decision to allow new tokens before starting Phase 2.

### 3.1 Implementation and results (Phase 1 done)

* `DatacenterRing` (`org.apache.cassandra.dht.tokenallocator`): the tokens of one datacenter with
  the NetworkTopologyStrategy replica walk, supporting incremental removal and "what if removed"
  evaluation. Each token records the ranges whose walk accepted its node at it, so a removal only
  recomputes those ranges and the merged range. `TokenReductionPlannerTest` checks it against
  the real `NetworkTopologyStrategy` + `TokenMetadata` on 60 random clusters (1–2 DCs, 1–4 racks,
  RF 1–5, heterogeneous token counts), before and after every removal.
* `TokenReductionPlanner`: per round, drops tokens round-robin (the node furthest above its
  target first), each time the token that most reduces the sum of squared deviations from the
  target ownership. It then replays the steps in execution order (most loaded node first) to
  report ownership after every step, the peak, and the data streamed.
* `tools/bin/tokenreductionplanner` (`TokenReductionPlannerTool`): reads `nodetool ring` output
  (with loads, which size the report in bytes) or CSV `endpoint,datacenter,rack,token`. Every row
  of a `nodetool ring` datacenter section must parse, and nodes that aren't `Normal` are refused,
  so a ring is never planned with a node silently missing. It writes `plan.txt` and
  `round-<n>-<tokens>/<datacenter>/<step>-<endpoint>.tokens`, the kept tokens for Phase 2's
  `--keep-file`, into a new or empty directory only. A datacenter with RF 0 is balanced as RF 1.

Measured by `TokenReductionPlannerTest.testPlanBalanceComparedWithNewDatacenter` on random
256-token rings, 256 → 16 by halving. Ownership is relative to the fair share. "Streamed" counts
copies of the datacenter data set; a new datacenter copies 1.00.

| Nodes | Racks | RF | Initial max | Final max / min | Peak during rounds | Streamed | Planning time |
|---|---|---|---|---|---|---|---|
| 6 | 1 | 3 | 1.047 | 1.024 / 0.984 | 1.47 | 1.41 | 0.1 s |
| 12 | 1 | 3 | 1.024 | 1.030 / 0.961 | 1.60 | 1.87 | 0.1 s |
| 12 | 3 | 3 | 1.139 | 1.027 / 0.980 | 1.46 | 1.32 | 0.2 s |
| 24 | 3 | 3 | 1.093 | 1.035 / 0.962 | 1.53 | 1.47 | 0.4 s |
| 48 | 1 | 3 | 1.135 | 1.051 / 0.958 | 1.66 | 2.11 | 0.6 s |
| 100 | 3 | 3 | 1.140 | 1.048 / 0.937 | 1.54 | 1.56 | 1.9 s |
| 12 | 1 | 1 | 1.061 | 1.036 / 0.974 | 1.52 | 1.50 | < 0.1 s |
| 20 | 5 | 5 | 1.068 | 1.022 / 0.964 | 1.52 | 1.35 | 0.7 s |
| 20 | 3 | 5 | 1.093 | 1.027 / 0.957 | 1.62 | 1.93 | 0.5 s |

Keeping a subset ends close to the fair share (within about 6%) and close to a fresh allocation
(a new datacenter allocated with 16 tokens reaches max ~1.02 with one rack; in this test the
offline allocator gives max 1.3–1.5 with racks == RF, so rack cases have no useful fresh
baseline). The price
is 1.3–2.1× the streaming of the new-datacenter procedure and a 1.5–1.7× peak ownership on one node
per round, without extra hardware. The peak is ownership, not disk: a shrinking node keeps the data
of the ranges it gives up until `nodetool cleanup`, so the runbook sizes disks for the peak plus
that, and runs cleanup after every step. The tool plans a 200-node ring in about 8 seconds with its
default 256 MB heap. Conclusion:
subset-only is good enough, and Phase 2 proceeds as designed.

## 4. Phase 2 — core: shrink a node's token set online (implemented)

### 4.1 Operator interface

* `nodetool settokens --keep-file <file>` (the planner's output) or `--keep <t1,t2,...>`; JMX
  `StorageServiceMBean.shrinkTokens(List<String>)`. It blocks until the operation is done.
  **Restarting the node aborts a shrink**: the node comes back with its previous tokens.
* Preconditions (refused otherwise), checked before anything is announced:
  * the new set is a strict, non-empty subset of the node's current tokens, with more than one token
    (shrinking a vnode node to a single token is refused: `num_tokens > 1` would still drive
    `DiskBoundaryManager` and `RangeCommands`), and its encoded status fits in 32 KB;
  * the node is `NORMAL`, gossip is enabled, and no other shrink runs on the node (a dedicated
    guard, not the `StorageService` monitor: `drain` and shutdown are never blocked);
  * the local token metadata shows no bootstrapping, leaving, moving or shrinking node;
  * every endpoint in gossip that didn't leave and wasn't removed:
    * advertises `ApplicationState.SHRINK_TOKENS_SUPPORTED` (§4.2);
    * is alive;
    * is not bootstrapping, replacing, hibernating, leaving, moving, shrinking or being removed;
  * no keyspace uses transient replication (the pending-range calculation compares endpoint sets
    only), and the node has no pending ranges;
  * no repair session involving the node is running;
  * the node would not have to fetch any range (`RangeRelocator.checkShrinkOnlyStreamsOut`, computed
    on a copy of the ring: impossible with SimpleStrategy and NetworkTopologyStrategy, checked for
    the others).
* The other range movements refuse to start while a node is shrinking: `decommission`, `move`,
  `removenode` (except removing the shrinking node itself) and node replacement, as well as strict
  bootstrap. This uses the local view only, like the existing checks, so the Phase 3 script
  serialises the steps.

### 4.2 Gossip

* **Capability** (decision 2026-09-28, replacing a version gate). Every node running this code
  publishes `ApplicationState.SHRINK_TOKENS_SUPPORTED`. The state is added right above the padding
  states, so older nodes see its ordinal as `X1` and ignore it, and it is not sent to pre-4.0
  peers. A version gate was rejected because `base.version` has been 5.0.7.0 for months: builds
  without this code already report it.
* **Status.** A new status `SHRINKING,<t1>,<t2>,...` lists the kept tokens
  (`VersionedValueFactory.shrinking`). It is published in `STATUS_WITH_PORT` and in the legacy
  `STATUS`, as `move` does. Tokens containing the delimiter `,` are refused.
* **How it is displayed.** `nodetool ring`/`status` show a shrinking node as `Moving`
  (`getMovingNodes` includes shrinking nodes), which fits the 8-character state column that the
  planner parses. The node's own operation mode is `SHRINKING`.
* **Receiving the status.** A peer registers a shrink only if the kept tokens are a non-empty strict
  subset of the node's tokens; otherwise it logs an error and ignores the state. The end of the
  shrink (`NORMAL` with the new tokens) and a rollback (`NORMAL` with the same tokens) both clear the
  state in `handleStateNormal`. A peer that missed the `SHRINKING` state gets the new tokens there
  too, and fires the "moved" notification when a member's token set changed.

### 4.3 Token metadata and pending ranges

* `TokenMetadata.shrinkingEndpoints` maps each shrinking endpoint to the tokens it keeps
  (`addShrinkingEndpoint` / `removeFromShrinking`, both bumping the ring version). It is cleared
  by `updateNormalTokens`, `removeEndpoint` and `clearUnsafe`, applied by `cloneAfterAllSettled`,
  included in the early return of `unsafeCalculatePendingRanges`, and checked when a node's
  address changes.
* **Pending ranges.** `calculatePendingRanges` builds the ring with every shrink done, then walks
  the current ring token by token. For each range, the new replicas that aren't current replicas
  become pending for that range.
  * This is linear in the ring size: two natural-replica walks per token, per keyspace. A 24×256
    ring takes well under a second.
  * It gives disjoint ranges per endpoint, even with several shrinking nodes (a peer can briefly
    see two shrinks when one ends and the next starts). An endpoint already pending for the range
    because of another movement is skipped, so the write path never sees duplicate endpoints.
* **Reads and writes.** Coordinators add the pending replicas to writes. Reads are served by the
  current natural replicas until the switch to `NORMAL`, as for move and decommission.

### 4.4 Local operation (`StorageService.shrinkTokens`)

1. Check the preconditions (§4.1), gossip `SHRINKING`, set mode `SHRINKING`, and sleep
   `RING_DELAY`.
2. Re-check that no other range movement started and that every node can still take part, and that
   the local token metadata has this shrink.
3. `repairPaxosForTopologyChange("shrink")` (Paxos v2): it covers the local and pending ranges, i.e.
   every range the node gives up. Paxos v1 state (`system.paxos`) is not streamed, as with move and
   decommission.
4. Stream out with `RangeRelocator.forShrink(kept)`.
   * The ranges the node gives up go to their new replicas, computed against the current ring
     with only this shrink applied.
   * Hints: a hint for the node delivered after it has streamed a range is applied on the node
     only, so the Phase 3 script checks that no hints for it are pending (`nodetool
     listpendinghints` on every node) before starting.
5. Commit point:
   * `TokenCountOverride.record(previous, kept, num_tokens)`, synced to disk;
   * then `SystemKeyspace.updateTokens(kept)`.
6. Announce `TOKENS` and `NORMAL` with the new tokens, update the local token metadata, set mode
   `NORMAL`, then sleep `RING_DELAY` so that every coordinator knows the new tokens before cleanup
   or the next step.
7. The log and the nodetool output remind the operator:
   * run `nodetool flush` and then `nodetool cleanup` on the node. Cleanup only rewrites sstables,
     and the writes the node received for the ranges it gave up, while it was still their
     replica, are in memtables until flushed;
   * update `num_tokens`.

Failure handling:

* **Any failure before the commit point, node still up.** This covers streaming, the re-checks,
  and writing the record or `system.local`. The node rolls back: it gossips `NORMAL` with the
  unchanged tokens, so every peer drops the shrinking state and the pending ranges; it clears its
  own state, rolls the record back, and sets the mode back to `NORMAL`.
* **The node restarts mid-operation**, including a graceful stop, which is never blocked by the
  shrink. `system.local` still has the old tokens, the node announces `NORMAL` with them, and
  peers drop the shrinking state. A dead shrinking node can also be removed (`removenode` /
  `assassinate`), which clears the state.
* **In every case**, the data already streamed to the would-be replicas is harmless and removed
  by their next cleanup, and the operation can be retried.

UCS and disk boundaries: the local ranges, disk boundaries and the replica-aware shard manager are
recomputed when the ring version changes, which the shrink bumps. The CNDB token tracker hook in
`TokenAllocation` is used by CNDB outside this repository.

### 4.5 Restart after a shrink (`TokenCountOverride`)

The count of the saved tokens differs from `num_tokens` until the configuration is updated.
Decision (2026-09-28): record the shrink and accept it, in a file rather than in a `system.local`
column (no schema change, no downgrade risk).

* `<metadata_directory>/token_count_override` is written atomically and synced (file and
  directory). It holds:
  * the token sets the node may have saved: the new set, and the previous one, because the record
    is written before `system.local`;
  * the accepted `num_tokens` values: every value configured when a shrink ran, and every token
    count the node had. So a yaml updated between two rounds is accepted, but not an unrelated
    value.
* At startup, `joinTokenRing` accepts saved tokens whose count differs from `num_tokens` only if
  both the token set and `num_tokens` are in the record, and logs a warning to update the yaml.
  When `num_tokens` matches the saved tokens again, the record is deleted.
* A read error is logged and treated as "no record", so the node refuses to start with the usual
  message.

### 4.6 Tests

* Unit, `ShrinkingEndpointPendingRangesTest`, against the strategies' natural replicas on random
  clusters (SimpleStrategy and NTS, 1–2 DCs, random racks):
  * pending endpoints are exactly the new replicas, including with several shrinking nodes, with
    no duplicates;
  * the shrinking node never gains a range, and no other node loses one;
  * bookkeeping, and the time of a 24×256 ring.
* Unit, `ShrinkTokensChecksTest`: the capability, liveness and state gate; the override across
  rounds, crash windows and rollbacks.
* dtests, `ShrinkTokensTest` (4 nodes, 32 random tokens, RF 3):
  * a shrink under continuous `QUORUM` writes, with no write failures, every row on all its
    replicas of the new ring, and exact placement after flush and cleanup;
  * the refusals, and the restart rules;
  * rollback on a streaming failure, on a failure at the commit point, and with gossip disabled;
  * a restart during streaming: the graceful stop doesn't hang, other movements are refused
    meanwhile, and the node comes back with its tokens and can retry;
  * a planner-driven 32 → 8 reduction, where the ownership matches the planner's prediction.

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
3. Restart after a shrink: record the new count and accept it, in a marker file in the metadata
   directory rather than a `system.local` column (§4.5).
4. Cluster support: a gossip capability (`SHRINK_TOKENS_SUPPORTED`), not a minimum version, which
   builds without the code already report (§4.2).
5. Nodes that are down or unreachable block a shrink (§4.1).
6. Planner default: halve the token count each round; the factor or explicit rounds can be
   passed (§3).
