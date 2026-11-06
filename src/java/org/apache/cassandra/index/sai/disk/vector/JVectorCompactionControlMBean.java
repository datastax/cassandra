/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.index.sai.disk.vector;

/**
 * JMX control surface for the runtime-tunable jvector vector-index compaction/insert parameters. Every
 * getter/setter delegates to the same {@link JVectorVersionUtil} state that the writable
 * {@code system_views.jvector} virtual table drives, so JMX and CQL share one source of truth. Changes
 * are in-memory (not persisted).
 */
public interface JVectorCompactionControlMBean
{
    boolean getGraphCompactionMergeEnabled();

    void setGraphCompactionMergeEnabled(boolean enabled);

    int getMergeBytesPerOrdinal();

    void setMergeBytesPerOrdinal(int bytes);

    /** Desired build-pool worker-thread count (takes effect on the next compaction via lazy pool rebuild). */
    int getCompactionBuildThreads();

    void setCompactionBuildThreads(int threads);

    /** In-flight insert budget in MiB; 0 disables the bound (unbounded fan-out). */
    int getInsertInflightMb();

    void setInsertInflightMb(int mb);

    /** Whether memtable flushes encode PQ incrementally during ingest (effective for the next new memtable). */
    boolean getAmortizePqEncoding();

    void setAmortizePqEncoding(boolean enabled);

    /** Whether residual flush-time PQ work is serialized node-wide (effective next flush). */
    boolean getSerializeFlushPq();

    void setSerializeFlushPq(boolean enabled);
}
