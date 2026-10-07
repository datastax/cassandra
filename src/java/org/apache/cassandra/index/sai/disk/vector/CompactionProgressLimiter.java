/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.disk.vector;

import com.google.common.util.concurrent.RateLimiter;

import io.github.jbellis.jvector.util.work.ProgressLimiter;
import io.github.jbellis.jvector.util.work.ProgressTracker;
import io.github.jbellis.jvector.util.work.WorkStage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.metrics.VectorCompactionMetrics;

/**
 * Cassandra's single {@link ProgressLimiter} for an on-disk vector graph merge. It melds the two
 * facets jvector's compactor calls:
 *
 * <ul>
 *   <li><b>Progress (up):</b> {@link #onProgress} forwards jvector's per-phase counters to the
 *       merge's {@link VectorMergeOperation}, so {@code nodetool compactionstats} and
 *       {@code system_views.sstable_tasks} advance <em>while</em> {@code compact()} runs (rather than
 *       jumping from 0% to 100%).</li>
 *   <li><b>Throttle (down):</b> {@link #acquire} admits jvector's write bandwidth against the
 *       <em>shared</em> compaction rate limiter — the same budget {@code compaction_throughput_mb_per_sec}
 *       and {@code nodetool setcompactionthroughput} control — so the merge's (often dominant)
 *       internal write participates in that budget, not just the SAI-side copy.</li>
 * </ul>
 *
 * <p>Progress is reported through a {@link ProgressTracker.PhaseScope}: jvector opens a scope per
 * phase and every counter for that phase arrives on it, so the stage identity is captured once
 * rather than passed on each report.
 *
 * <p><b>Threading.</b> jvector opens and closes phases on the orchestrating (compaction) thread —
 * the caller of {@code compact()} — but delivers {@link ProgressTracker.PhaseScope#onProgress} and
 * {@link #acquire} from the <em>workers</em> that produced the batch, serialized per scope. So
 * {@link #acquire} blocks a build-pool worker on the shared rate limiter rather than the
 * compaction thread; the admitted bandwidth is charged to the same budget either way, which is
 * the point.
 *
 * <p><b>Cancellation.</b> Both {@code onProgress} and {@code acquire} call
 * {@link org.apache.cassandra.db.compaction.TableOperation#throwIfStopRequested()}, so a DROP or
 * compaction interrupt stops the merge at the next batch boundary. jvector documents these as
 * fire-and-forget hooks that should not throw; Cassandra deliberately throws from them, because
 * they are the only points inside {@code compact()} that a host can interrupt at. That is
 * well-defined rather than merely tolerated: a body failure is rethrown to the caller of
 * {@code compact()}, and {@code CompactionInterruptedException} is a {@code RuntimeException}, so
 * it arrives unwrapped and the compaction framework handles it without error noise.
 *
 * <p>Throwing from a worker is safe here specifically because the merge runs on
 * {@link BoundedParallelExecutor}, which never unwinds beneath a running body: the failure is
 * recorded, remaining chunks are skipped, every started chunk is waited out, and only then does
 * {@code compact()} return. That is what lets {@code SegmentBuilder} release the source
 * {@code SSTableIndex} pins once {@code merge()} returns without racing a still-running read
 * against an unmapped file.
 */
public class CompactionProgressLimiter implements ProgressLimiter
{
    private final VectorMergeOperation operation;
    private final IndexContext indexContext;

    /**
     * @param operation the merge's registered {@link VectorMergeOperation} to feed progress into
     */
    public CompactionProgressLimiter(VectorMergeOperation operation)
    {
        this(operation, null);
    }

    public CompactionProgressLimiter(VectorMergeOperation operation, IndexContext indexContext)
    {
        this.operation = operation;
        this.indexContext = indexContext;
    }

    @Override
    public ProgressTracker.PhaseScope startPhase(WorkStage stage)
    {
        ProgressTracker.PhaseScope timer = VectorCompactionMetrics.INSTANCE.start("jvector", stage, indexContext);
        // The merger's own epilogue stage reports bytes (footer CRC re-read, codes tail); jvector's
        // stages report ordinals. Decided once per phase now that the scope carries the stage,
        // instead of re-testing it on every counter.
        boolean inBytes = stage instanceof CompactionGraphMerger.EpilogueStage;
        return new ProgressTracker.PhaseScope()
        {
            @Override
            public void onProgress(long completed, long total)
            {
                // Cancellation checkpoint: a stopped merge (DROP/interrupt) throws here, and the
                // failure is rethrown to the caller of compact(). See the class javadoc on why
                // throwing from a hook jvector documents as fire-and-forget is deliberate.
                operation.throwIfStopRequested();
                if (inBytes)
                    operation.reportBytes(completed, total);
                else
                    operation.report(completed, total);
            }

            @Override
            public void close()
            {
                timer.close();
            }
        };
    }

    @Override
    public Grant acquire(long bytes)
    {
        // Cancellation checkpoint: acquire is called frequently (per write batch), so a stopped merge
        // unwinds promptly here rather than only at phase boundaries.
        operation.throwIfStopRequested();
        // Disabled throttling (compaction_throughput_mb_per_sec == 0) sets the shared limiter's rate
        // to Double.MAX_VALUE; skip acquiring to avoid needless work, mirroring Cassandra's own
        // compactionRateLimiterAcquire guard. getRateLimiter() itself never returns null.
        if (bytes <= 0 || DatabaseDescriptor.getCompactionThroughputMebibytesPerSec() <= 0)
            return Grant.NOOP;

        RateLimiter rateLimiter = CompactionManager.instance.getRateLimiter();
        try (ProgressTracker.PhaseScope ignored = VectorCompactionMetrics.INSTANCE.start(
                VectorCompactionMetrics.Phase.THROTTLE_WAIT, indexContext))
        {
            long remaining = bytes;
            while (remaining > 0)
            {
                int chunk = (int) Math.min(remaining, Integer.MAX_VALUE);
                rateLimiter.acquire(chunk);
                remaining -= chunk;
            }
        }
        // Rate-limiter model: the cost is paid here, nothing to release.
        return Grant.NOOP;
    }
}
