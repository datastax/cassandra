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
package org.apache.cassandra.service;

/**
 * Work that must run on a node that is being decommissioned, before that node finishes leaving.
 *
 * Register with {@link StorageService#registerDecommissionHook(DecommissionHook)}; several hooks
 * may be registered, and they run one after another in registration order. A hook only ever runs
 * on the node being decommissioned, and only for {@code nodetool decommission} -- not on a
 * graceful shutdown, not on {@code nodetool removenode} (where this node is already gone), and
 * not on the nodes that merely observe a peer leaving.
 *
 * <h2>Where this runs</h2>
 *
 * {@link StorageService#decommission(boolean)} invokes hooks after {@code unbootstrap()} has
 * returned and before it tears any subsystem down. At that point:
 *
 * <ul>
 *   <li>the batch log has completed its final replay, and hints have been transferred or dropped;</li>
 *   <li>all ranges have been streamed to their new owners;</li>
 *   <li>this node has left the ring -- {@code leaveRing()} removed it from {@code TokenMetadata},
 *       announced LEFT and waited for the peers to notice, so coordinators no longer route
 *       mutations here. Note this needs LEFT, not LEAVING: {@code addLeavingEndpoint} only records
 *       the endpoint in a set that write routing never consults, so a LEAVING node stays a natural
 *       write replica;</li>
 *   <li>decommission has not yet stopped messaging, the native transport or the stages, so a hook
 *       <b>may run CQL queries</b> and coordinate against the rest of the cluster.</li>
 * </ul>
 *
 * <h2>Caveats a hook has to live with</h2>
 *
 * <ul>
 *   <li>A late mutation can still <i>arrive</i>: a peer that has not yet processed LEFT may send
 *       one, and this node applies it, because {@code reject_out_of_token_range_requests} defaults
 *       to false. "No mutations" means none are routed here, not that none can land. Set that
 *       option to true if a hook needs the stronger guarantee.</li>
 *   <li>Hints do not help a hook's writes. {@code unbootstrap()} has already dealt with the hints
 *       this node held: by default ({@code transfer_hints_on_decommission}) it streamed them to a
 *       peer, otherwise it disabled hinted handoff and deleted them. Either way that happened
 *       <i>before</i> the hooks, so a hint a hook's write creates now is never transferred, and is
 *       lost when the process stops. A hook's write should meet its consistency level against live
 *       replicas rather than rely on being completed later.</li>
 *   <li>The native transport is only guaranteed not to have been shut down <i>by the decommission</i>;
 *       it may still be off for unrelated reasons (never started, {@code nodetool disablebinary}).
 *       A hook coordinating in-process via {@code QueryProcessor} does not depend on it.</li>
 * </ul>
 *
 * <h2>Contract</h2>
 *
 * A hook may block for as long as it needs -- minutes or hours. Decommission does not proceed
 * until every hook has returned, and {@code nodetool decommission} blocks for that whole time.
 * Hooks run on the thread serving the decommission request, so they must not assume any particular
 * executor.
 *
 * Within one process a hook runs at most once: hooks are only reached once {@code unbootstrap()}
 * has succeeded, and from there the decommission always runs to completion, so a repeated
 * {@code nodetool decommission} returns early rather than making a second pass. A decommission that
 * fails <i>before</i> unbootstrap() completes never reaches the hooks at all, so a later retry runs
 * them for the first time. That is not an across-restarts guarantee: {@code leaveRing()} persists
 * NEEDS_BOOTSTRAP before the hooks run, so if the process is killed while a hook is still working,
 * the node bootstraps back into the ring on restart and a later decommission runs every hook again.
 * A hook that cannot tolerate that must make itself idempotent.
 *
 * If a hook fails, the remaining hooks still run and the decommission still completes -- by this
 * point the data has streamed away and the node has left the ring, so there is nothing to roll back
 * to, and refusing to finish would only strand the node (a retry is rejected because the node is no
 * longer a ring member, and a restart would re-bootstrap it). The failure is logged, and
 * {@code decommission} then throws naming the offending hooks, so a hook failure is never silent.
 * Note what that means for tooling: a failure reported by {@code nodetool decommission} does not
 * imply the decommission was not performed -- check the node's operation mode to tell the two apart.
 *
 * Anything a hook throws is reported this way, including an {@link Error}: escalating it to the JVM
 * failure policy from here would strand the node in the window this design exists to avoid.
 */
public interface DecommissionHook
{
    /**
     * Identifies this hook in the decommission log lines. Should be short and stable.
     */
    String name();

    /**
     * Runs the work. May block indefinitely; may run CQL queries. Throwing is reported and fails
     * {@code nodetool decommission}, but does not stop the node from finishing its decommission.
     */
    void onDecommission() throws Exception;
}
