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

import io.github.jbellis.jvector.pq.BinaryQuantization;
import io.github.jbellis.jvector.pq.CompressedVectors;

import static java.lang.Math.max;
import static java.lang.Math.pow;

/**
 * Utility methods for dealing with overquerying.
 */
public class OverqueryUtils
{
    /**
     * @return the topK >= `limit` results to ask the index to search for, forcing
     * the greedy search deeper into the graph.  This serves two purposes:
     * 1. Smoothes out the relevance difference between small LIMIT and large
     * 2. Compensates for using lossily-compressed vectors during the search
     */
    public static int topKFor(int limit, CompressedVectors cv)
    {
        // if the vectors are uncompressed, bump up the limit a bit to start with but decay it rapidly
        if (cv == null)
        {
            var n = max(1.0, 0.979 + 4.021 * pow(limit, -0.761)); // f(1) =  5.0, f(100) = 1.1, f(1000) = 1.0
            return (int) (n * limit);
        }

        // Most compressed vectors should be queried at ~2x as much as uncompressed vectors.  (Our compression
        // is tuned so that this should give us approximately the same recall as using uncompressed.)
        // Again, we do want this to decay as we go to very large limits.
        var n = max(1.0, 0.509 + 9.491 * pow(limit, -0.402)); // f(1) = 10.0, f(100) = 2.0, f(1000) = 1.1

        // we compress extra-large vectors more aggressively, so we need to bump up the limit
        // for those.  3x if using PQ and 4x for BQ.
        if (cv instanceof BinaryQuantization)
            n *= 2; // total of ~4x compared to uncompressed
        else if ((double) cv.getOriginalSize() / cv.getCompressedSize() > 16.0)
            n *= 1.5; // total of ~3x compared to uncompressed

        return (int) (n * limit);
    }
}
