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

package org.apache.cassandra.dht.tokenallocator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.cassandra.dht.Token;

/**
 * The tokens of one datacenter, with the replica placement of {@link org.apache.cassandra.locator.NetworkTopologyStrategy}
 * for that datacenter, supporting the removal of tokens and the evaluation of a removal without applying it.
 * <p>
 * With NetworkTopologyStrategy the replicas of a datacenter only depend on the tokens of that datacenter: all the
 * global ranges between two consecutive tokens of the datacenter have the same replicas in it. So the replicated
 * ownership of the nodes of a datacenter can be computed on the ring of its tokens only. The replica walk reproduces
 * {@code NetworkTopologyStrategy.DatacenterEndpoints#addEndpointAndCheckIfDone}, assuming the snitch accepts
 * several replicas in the same rack when there are fewer racks than replicas (the default of every snitch).
 * <p>
 * Removing a token {@code t} of node X changes only:
 * <ul>
 *     <li>the range ending at {@code t}, which merges into the range ending at the next token;</li>
 *     <li>the ranges whose replica walk accepted X at {@code t}: a token rejected by the walk doesn't change the walk
 *     state, so removing it doesn't change the walk.</li>
 * </ul>
 * Each token keeps the list of ranges whose walk accepted its node at it, so both sets are found without scanning.
 * <p>
 * Removing a token of X never removes another node from a replica set: after the removed position, the new walk has
 * one replica less and at least as many acceptable rack repeats, so it accepts every node the old walk accepted
 * except X, plus exactly one node. Hence the ownership X loses is exactly the ownership the other nodes gain, i.e.
 * the data X streams, and X never gains a range (the Phase 2 operation checks this at runtime too).
 * Not thread safe.
 */
public class DatacenterRing
{
    /** A token of the ring, and the range that ends at it. */
    private static final class Vnode
    {
        final Token token;
        final int node;
        Vnode prev;
        Vnode next;
        /** Replicas of the range ending at this token, in walk order. */
        int[] replicas;
        /** For each replica, the token at which the walk accepted it. */
        Vnode[] acceptedAt;
        /** Ranges whose walk accepted {@link #node} at this token. */
        final Set<Vnode> acceptedIn = new LinkedHashSet<>(); // deterministic iteration, hence summation, order

        Vnode(Token token, int node)
        {
            this.token = token;
            this.node = node;
        }

        double rangeSize()
        {
            return prev == this ? 1.0 : prev.token.size(token);
        }
    }

    private final int replicationFactor;
    private final int[] rackOf;
    private final int rackCount;
    private final List<List<Vnode>> tokensOf;
    private final double[] ownership;
    private final Map<Token, Vnode> byToken = new HashMap<>();
    private int size;

    // scratch state of the evaluations and of the walks, reused to avoid allocations
    private final double[] scratchDelta;
    private final boolean[] scratchTouched;
    private final int[] touched;
    private int touchedCount;
    private final int[] seenNodeEpoch;
    private final int[] seenRackEpoch;
    private int walkEpoch;

    /**
     * @param replicationFactor replication factor of the datacenter
     * @param racks rack of each node, nodes are identified by their index
     * @param tokens tokens of each node, same indexing as {@code racks}
     */
    public DatacenterRing(int replicationFactor, int[] racks, List<? extends Collection<Token>> tokens)
    {
        Preconditions.checkArgument(replicationFactor > 0, "replication factor must be positive");
        Preconditions.checkArgument(racks.length == tokens.size(), "racks and tokens must have one entry per node");
        Preconditions.checkArgument(racks.length > 0, "the datacenter has no nodes");
        Preconditions.checkArgument(Arrays.stream(racks).allMatch(rack -> rack >= 0), "racks must be non-negative identifiers");
        this.replicationFactor = replicationFactor;
        this.rackOf = racks.clone();
        this.rackCount = (int) Arrays.stream(racks).distinct().count();
        this.ownership = new double[racks.length];
        this.tokensOf = new ArrayList<>(racks.length);
        this.scratchDelta = new double[racks.length];
        this.scratchTouched = new boolean[racks.length];
        this.touched = new int[racks.length];
        this.seenNodeEpoch = new int[racks.length];
        this.seenRackEpoch = new int[Arrays.stream(racks).max().getAsInt() + 1];

        List<Vnode> all = new ArrayList<>();
        for (int node = 0; node < racks.length; node++)
        {
            List<Vnode> nodeTokens = new ArrayList<>();
            for (Token token : tokens.get(node))
            {
                Vnode vnode = new Vnode(token, node);
                Preconditions.checkArgument(byToken.put(token, vnode) == null, "token %s is owned by more than one node", token);
                nodeTokens.add(vnode);
                all.add(vnode);
            }
            Preconditions.checkArgument(!nodeTokens.isEmpty(), "node %s has no tokens", node);
            tokensOf.add(nodeTokens);
        }
        all.sort((a, b) -> a.token.compareTo(b.token));
        for (int i = 0; i < all.size(); i++)
        {
            Vnode vnode = all.get(i);
            vnode.next = all.get((i + 1) % all.size());
            vnode.prev = all.get((i + all.size() - 1) % all.size());
        }
        size = all.size();

        for (Vnode range : all)
            setReplicas(range, walk(range, null));
    }

    public int nodeCount()
    {
        return rackOf.length;
    }

    public int tokenCount()
    {
        return size;
    }

    public int tokenCount(int node)
    {
        return tokensOf.get(node).size();
    }

    /**
     * @return the i-th token of the node, in no particular order, without copying the token list
     */
    public Token token(int node, int i)
    {
        return tokensOf.get(node).get(i).token;
    }

    public List<Token> tokens(int node)
    {
        List<Token> tokens = new ArrayList<>(tokensOf.get(node).size());
        for (Vnode vnode : tokensOf.get(node))
            tokens.add(vnode.token);
        return tokens;
    }

    /**
     * @return the fraction of the ring replicated by the node, i.e. {@code nodetool status} "Owns (effective)"
     */
    public double ownership(int node)
    {
        return ownership[node];
    }

    public double[] ownership()
    {
        return ownership.clone();
    }

    /**
     * @return the change of replicated ownership of each node if the token was removed, without removing it
     */
    public double[] ownershipChangeIfRemoved(Token token)
    {
        accumulateRemoval(vnode(token));
        double[] delta = new double[ownership.length];
        for (int i = 0; i < touchedCount; i++)
            delta[touched[i]] = scratchDelta[touched[i]];
        resetScratch();
        return delta;
    }

    /**
     * Change of the sum of squared differences between ownership and target if the token was removed, without
     * removing it: negative values improve the balance.
     */
    public double balanceChangeIfRemoved(Token token, double[] target)
    {
        accumulateRemoval(vnode(token));
        double change = 0;
        for (int i = 0; i < touchedCount; i++)
        {
            int node = touched[i];
            double delta = scratchDelta[node];
            change += delta * (2 * (ownership[node] - target[node]) + delta);
        }
        resetScratch();
        return change;
    }

    /**
     * Accumulates in {@link #scratchDelta} the ownership change of every node if the token was removed; the nodes
     * with a change are listed in {@link #touched}.
     */
    private void accumulateRemoval(Vnode vnode)
    {
        // the range ending at the token merges into the next range; the replicas of the next range only change if its
        // walk wraps around the whole ring up to the removed token
        Vnode next = vnode.next;
        double merged = vnode.rangeSize();
        for (int replica : vnode.replicas)
            addScratch(replica, -merged);
        int[] nextReplicas = vnode.acceptedIn.contains(next) ? walk(next, vnode).replicas : next.replicas;
        for (int replica : nextReplicas)
            addScratch(replica, merged);

        for (Vnode range : vnode.acceptedIn)
        {
            if (range == vnode)
                continue;
            int[] replicas = range == next ? nextReplicas : walk(range, vnode).replicas;
            double size = range.rangeSize();
            for (int replica : range.replicas)
                addScratch(replica, -size);
            for (int replica : replicas)
                addScratch(replica, size);
        }
    }

    private void addScratch(int node, double delta)
    {
        if (!scratchTouched[node])
        {
            scratchTouched[node] = true;
            touched[touchedCount++] = node;
        }
        scratchDelta[node] += delta;
    }

    private void resetScratch()
    {
        for (int i = 0; i < touchedCount; i++)
        {
            scratchDelta[touched[i]] = 0;
            scratchTouched[touched[i]] = false;
        }
        touchedCount = 0;
    }

    /**
     * Removes the token from the ring and updates the replicas and the ownership.
     */
    public void remove(Token token)
    {
        Vnode vnode = vnode(token);
        Preconditions.checkState(size > 1, "cannot remove the last token of the ring");
        Preconditions.checkState(tokensOf.get(vnode.node).size() > 1, "cannot remove the last token of node %s", vnode.node);

        Vnode next = vnode.next;
        List<Vnode> affected = new ArrayList<>(vnode.acceptedIn);
        affected.remove(vnode);
        affected.remove(next);

        // unaccount the ranges that change while their sizes are still the current ones: the range ending at the token
        // disappears and its span goes to the next range
        clearReplicas(vnode);
        clearReplicas(next);
        for (Vnode range : affected)
            clearReplicas(range);

        vnode.prev.next = next;
        next.prev = vnode.prev;
        byToken.remove(token);
        tokensOf.get(vnode.node).remove(vnode);
        size--;

        setReplicas(next, walk(next, null));
        for (Vnode range : affected)
            setReplicas(range, walk(range, null));
    }

    private Vnode vnode(Token token)
    {
        Vnode vnode = byToken.get(token);
        Preconditions.checkArgument(vnode != null, "token %s is not in the ring", token);
        return vnode;
    }

    private static final class Walk
    {
        final int[] replicas;
        final Vnode[] acceptedAt;

        Walk(int[] replicas, Vnode[] acceptedAt)
        {
            this.replicas = replicas;
            this.acceptedAt = acceptedAt;
        }
    }

    /**
     * Replica walk of NetworkTopologyStrategy for one datacenter, starting at the token the range ends at.
     *
     * @param skip token to ignore, to evaluate its removal; null for none
     */
    private Walk walk(Vnode start, Vnode skip)
    {
        int rfLeft = Math.min(replicationFactor, rackOf.length);
        int acceptableRackRepeats = replicationFactor - rackCount;
        int[] replicas = new int[rfLeft];
        Vnode[] acceptedAt = new Vnode[rfLeft];
        int found = 0;
        // a node or rack is seen in this walk if its stamp is the current epoch; on overflow the stamps are cleared so
        // that no stale stamp can match
        if (walkEpoch == Integer.MAX_VALUE)
        {
            Arrays.fill(seenNodeEpoch, 0);
            Arrays.fill(seenRackEpoch, 0);
            walkEpoch = 0;
        }
        int epoch = ++walkEpoch;

        Vnode current = start == skip ? start.next : start;
        Vnode first = current;
        do
        {
            if (current != skip && seenNodeEpoch[current.node] != epoch)
            {
                int rack = rackOf[current.node];
                boolean newRack = seenRackEpoch[rack] != epoch;
                if (newRack || acceptableRackRepeats > 0)
                {
                    if (newRack)
                        seenRackEpoch[rack] = epoch;
                    else
                        acceptableRackRepeats--;
                    seenNodeEpoch[current.node] = epoch;
                    replicas[found] = current.node;
                    acceptedAt[found] = current;
                    found++;
                    rfLeft--;
                }
            }
            current = current.next;
        }
        while (rfLeft > 0 && current != first);

        if (found < replicas.length)
        {
            replicas = Arrays.copyOf(replicas, found);
            acceptedAt = Arrays.copyOf(acceptedAt, found);
        }
        return new Walk(replicas, acceptedAt);
    }

    private void setReplicas(Vnode range, Walk walk)
    {
        range.replicas = walk.replicas;
        range.acceptedAt = walk.acceptedAt;
        double size = range.rangeSize();
        for (int i = 0; i < walk.replicas.length; i++)
        {
            ownership[walk.replicas[i]] += size;
            walk.acceptedAt[i].acceptedIn.add(range);
        }
    }

    private void clearReplicas(Vnode range)
    {
        double size = range.rangeSize();
        for (int i = 0; i < range.replicas.length; i++)
        {
            ownership[range.replicas[i]] -= size;
            range.acceptedAt[i].acceptedIn.remove(range);
        }
        range.replicas = new int[0];
        range.acceptedAt = new Vnode[0];
    }
}
