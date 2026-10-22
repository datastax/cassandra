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

package org.apache.cassandra.index.sai.utils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.utils.Pair;

/**
 * Term frequencies across all documents.  Each document is only counted once.
 */
public class DocBm25Stats
{
    public Map<ByteBuffer, Long> getFrequencies()
    {
        return frequencies;
    }

    private final Map<ByteBuffer, Long> frequencies = new HashMap<>();
    private long docCount;
    private long totalTermCount;
    private double avgDocLength;
    private boolean hasOldFormatIndex;

    public long getDocCount()
    {
        return docCount;
    }

    public double getAvgDocLength()
    {
        return avgDocLength;
    }

    /**
     * @return  {@link true} if any on-disk index in the query view was written before version ED,
     * meaning it lacks a serialized {@code totalTermCount} and cannot contribute to a reliable global
     * {@code avgDocLength}. When this is true, each segment must compute its own local average from
     * the {@code DOC_LENGTHS} data rather than using the global value.
     */
    public boolean hasOldFormatIndex()
    {
        return hasOldFormatIndex;
    }

    public void setHasOldFormatIndex()
    {
        this.hasOldFormatIndex = true;
    }

    public void add(long docCount, long totalTermCount, List<Pair<ByteBuffer, Expression>> termAndExpressions, DocumentFrequencyEstimator estimator)
    {
        this.docCount += docCount;
        this.totalTermCount += totalTermCount;
        if (this.docCount > 0 && this.totalTermCount > 0)
            this.avgDocLength = (double) this.totalTermCount / this.docCount;
        for (Pair<ByteBuffer, Expression> pair : termAndExpressions)
            frequencies.merge(pair.left,
                              Math.min(estimator.estimate(pair.right), docCount),
                              Long::sum);
    }

    @FunctionalInterface
    public interface DocumentFrequencyEstimator
    {
        long estimate(Expression predicate);
    }
}
