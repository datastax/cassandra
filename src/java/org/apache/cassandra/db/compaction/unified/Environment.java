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

package org.apache.cassandra.db.compaction.unified;

import org.apache.cassandra.db.compaction.CompactionSSTable;
import org.apache.cassandra.utils.MovingAverage;

/**
 * This class supplies to the cost calculator the required parameters for the calculations.
 * There are two implementations, one used in real life and one for the simulation.
 */
public interface Environment
{
    /**
     * @return an exponential moving average. New values have greater representation in the average, and older samples'
     * effect exponentially decays with new data.
     */
    MovingAverage makeExpMovAverage();

    /**
     * @return the cache miss ratio in the last 5 minutes
     */
    double cacheMissRatio();

    /**
     * @return the bloom filter false positive ratio for all sstables
     */
    double bloomFilterFpRatio();

    /**
     * @return the size of the chunk that read from disk.
     */
    int chunkSize();

    /**
     * @return the total bytes inserted into the memtables so far
     */
    long bytesInserted();

    /**
     * @return the total number of partitions read so far
     */
    long partitionsRead();

    /**
     * @return the mean read latency in nano seconds to read a partition from an sstable
     */
    double sstablePartitionReadLatencyNanos();

    /**
     * @return the mean compaction time per 1 Kb of input, in nano seconds
     */
    double compactionTimePerKbInNanos();

    /**
     * @return the mean flush latency per 1 Kb of input, in nano seconds
     */
    double flushTimePerKbInNanos();

    /**
     * @return the write amplification (bytes flushed + bytes compacted / bytes flushed).
     */
    double WA();

    /**
     * @return the average size of sstables when they are flushed, averaged over the last 5 minutes.
     */
    double flushSize();

    /**
     * @return the maximum number of concurrent compactions that can be running at any one time
     */
    int maxConcurrentCompactions();

    /**
     * @return the maximum compaction throughput
     */
    double maxThroughput();

    /**
     * This method returns the expected temporary space overhead of performing
     * a compaction. This overhead is due to the fact that whilst compactions
     * are in progress, both input and output sstables need to be present, since
     * the input sstables can only be deleted after compaction has completed.
     * <p/>
     * The default implementation looks at the size of the input data files of the
     * compaction, assuming that the output compaction will be just as large.
     * This does not take into account indexes, and thus may underestimate the
     * total required space. This method is used to evaluate the actual space
     * that may be required.
     *
     * @param sstables set of sstables to be compacted
     * @param totalDataSize precalculated data size, to use when total space
     *                      adjustment is not required
     * @return the expecte overhead size in bytes for compacting the given sstables
     */
    default long getOverheadSizeInBytes(Iterable<? extends CompactionSSTable> sstables, long totalDataSize)
    {
        return totalDataSize;
    }
}
