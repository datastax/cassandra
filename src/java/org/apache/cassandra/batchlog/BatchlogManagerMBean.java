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
package org.apache.cassandra.batchlog;

public interface BatchlogManagerMBean
{
    /**
     * Counts all batches currently in the batchlog.
     *
     * @return total batch count
     */
    public int countAllBatches();

    /**
     * @return total count of batches replayed since node start
     */
    public long getTotalBatchesReplayed();

    /**
     * @return number of batches replayed since node start for which at least one hint was written, because a
     *         replica was down, failed, or did not acknowledge the replayed mutations in time. A batch is counted
     *         once its mutations complete, even while it stays in the batchlog awaiting its interceptor callback,
     *         so this can transiently exceed {@link #getTotalBatchesReplayed()}; like that counter, it may count a
     *         batch again if a replay cycle aborts and the batch is later replayed from scratch. Same value as the
     *         {@code org.apache.cassandra.metrics:type=Storage,name=HintedBatchlogReplays} meter.
     */
    public long getHintedBatchesReplayed();

    /**
     * Forces batchlog replay. Blocks until completion.
     */
    public void forceBatchlogReplay() throws Exception;
}
