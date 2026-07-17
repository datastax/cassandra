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
package org.apache.cassandra.batchlog;

import java.util.List;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_BATCHLOG_MANAGER_INTERCEPTOR_CLASS;

/**
 * A pluggable hook that observes the batches replayed by batchlog recovery
 * ({@link BatchlogManager#replayFailedBatches}).
 * <br/>
 * The callback is invoked on the node performing the recovery (the batchlog replica replaying its
 * local batchlog), on the batchlog replay thread, once per replayed batch, with the complete list
 * of the batch's mutations — whether they were applied locally, delivered to remote replicas, or
 * hinted — so implementations can process a batch's mutations together.
 * <br/>
 * The interceptor runs as part of the recovery of a batch: it is invoked <b>after</b> the batch's
 * mutations have completed (applied locally and acknowledged by the remote replicas, or hinted)
 * and <b>before</b> the batch is removed from the batchlog. If the interceptor throws, the batch
 * is not removed and the callback — again with the batch's complete mutation list — is retried on
 * the next replay cycle; the batch's mutations, having already completed, are not re-sent and no
 * additional hints are written for them. Should the node restart before the callback succeeds,
 * the whole batch is replayed from scratch, mutations included. Semantics are therefore
 * at-least-once: the same batch (and thus every mutation in it) may be observed (and, across
 * restarts, re-applied) multiple times, and implementations must be idempotent. An implementation
 * should only throw for failures that a later retry can resolve, as a batch whose interceptor
 * keeps failing is retried indefinitely (and reported by a periodic warning in the log).
 * <br/>
 * At-least-once has the same carve-outs as batchlog replay itself: no callback is delivered for a
 * batch dropped because all its tables were truncated or dropped, or because the batch outlived
 * its smallest gc_grace_seconds — replay treats those as "delivered, then truncated/expired" and
 * discards them, before or between callback retries — and mutations whose individual table was
 * truncated or dropped are excluded from the list. A callback still pending when the batch is
 * removed from the batchlog by another path (e.g. its coordinator completing the batch after this
 * node already replayed it) is dropped, with a warning in the log. Callbacks still pending when
 * the node is decommissioned are lost with the node's batchlog. And since retries happen on later
 * replay cycles, callbacks are delivered at-least-once, but not necessarily in batch order.
 * <br/>
 * The callback cannot alter or veto the mutations themselves. Implementations should be fast and
 * non-blocking, as they run inline on the replay path.
 * <br/>
 * A custom implementation can be provided with the
 * {@code cassandra.custom_batchlog_manager_interceptor_class} system property; it must have a
 * public no-argument constructor. By default a no-op implementation is used.
 */
public interface BatchlogManagerInterceptor
{
    BatchlogManagerInterceptor instance = getCustomOrDefault();

    static BatchlogManagerInterceptor getCustomOrDefault()
    {
        if (CUSTOM_BATCHLOG_MANAGER_INTERCEPTOR_CLASS.isPresent())
        {
            return FBUtilities.construct(CUSTOM_BATCHLOG_MANAGER_INTERCEPTOR_CLASS.getString(),
                                         "custom batchlog manager interceptor (set with " + CUSTOM_BATCHLOG_MANAGER_INTERCEPTOR_CLASS.getKey() + ')');
        }
        return new Noop();
    }

    /**
     * Invoked once for a batch whose replay has completed, before the batch is removed from the
     * batchlog.
     *
     * @param batchId   id of the batchlog entry being replayed
     * @param writtenAt original write time of the batch, in milliseconds since the epoch
     * @param mutations all the mutations of the replayed batch; the list is unmodifiable and only
     *                  valid for the duration of the callback — copy it if it must be retained
     */
    void onReplayedBatch(TimeUUID batchId, long writtenAt, List<Mutation> mutations);

    class Noop implements BatchlogManagerInterceptor
    {
        @Override
        public void onReplayedBatch(TimeUUID batchId, long writtenAt, List<Mutation> mutations)
        {
        }
    }
}
