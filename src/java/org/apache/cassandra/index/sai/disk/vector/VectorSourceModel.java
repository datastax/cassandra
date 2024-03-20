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

import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import io.github.jbellis.jvector.pq.BinaryQuantization;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import static io.github.jbellis.jvector.vector.VectorSimilarityFunction.COSINE;
import static io.github.jbellis.jvector.vector.VectorSimilarityFunction.DOT_PRODUCT;
import static java.lang.Math.max;
import static java.lang.Math.pow;
import static org.apache.cassandra.index.sai.disk.vector.VectorCompression.CompressionType.BINARY_QUANTIZATION;
import static org.apache.cassandra.index.sai.disk.vector.VectorCompression.CompressionType.PRODUCT_QUANTIZATION;

public enum VectorSourceModel
{
    ADA002((dimension) -> new VectorCompression(BINARY_QUANTIZATION, dimension / 8), 2.0),
    OPENAI_V3_SMALL((dimension) -> new VectorCompression(PRODUCT_QUANTIZATION, dimension / 16), 1.5),
    OPENAI_V3_LARGE((dimension) -> new VectorCompression(PRODUCT_QUANTIZATION, dimension / 16), 1.5),
    BERT((dimension) -> new VectorCompression(PRODUCT_QUANTIZATION, dimension / 4), 1.0),
    GECKO((dimension) -> new VectorCompression(PRODUCT_QUANTIZATION, dimension / 8), 1.5),
    NV_QA_4((dimension) -> new VectorCompression(PRODUCT_QUANTIZATION, dimension / 8), 1.5),

    OTHER(COSINE, VectorSourceModel::genericCompression, VectorSourceModel::genericOverquery);

    /**
     * Default similarity function for this model.
     */
    public final VectorSimilarityFunction defaultSimilarityFunction;
    /**
     * Compression provider optimized for this model.
     */
    public final Function<Integer, VectorCompression> compressionProvider;
    /**
     * Factor by which to multiply the top K requested by to search deeper in the graph.
     * This is IN ADDITION to the tapered 2x applied by OverqueryUtils.
     */
    public final Function<CompressedVectors, Double> overqueryProvider;

    VectorSourceModel(Function<Integer, VectorCompression> compressionProvider, double overqueryFactor)
    {
        this(DOT_PRODUCT, compressionProvider, __ -> overqueryFactor);
    }

    VectorSourceModel(VectorSimilarityFunction defaultSimilarityFunction,
                      Function<Integer, VectorCompression> compressionProvider,
                      Function<CompressedVectors, Double> overqueryProvider)
    {
        this.defaultSimilarityFunction = defaultSimilarityFunction;
        this.compressionProvider = compressionProvider;
        this.overqueryProvider = overqueryProvider;
    }

    public static VectorSourceModel fromString(String value)
    {
        return valueOf(value.toUpperCase());
    }

    private static VectorCompression genericCompression(int originalDimension)
    {
        // Model is unspecified / unknown, so we guess.
        // Guessing heuristic is simple: 1536 dimensions is probably ada002, so use BQ.  Otherwise, use PQ.
        if (originalDimension == 1536)
            return new VectorCompression(BINARY_QUANTIZATION, originalDimension / 8);
        return new VectorCompression(PRODUCT_QUANTIZATION, defaultPQBytesFor(originalDimension));
    }

    private static int defaultPQBytesFor(int originalDimension)
    {
        // the idea here is that higher dimensions compress well, but not so well that we should use fewer bits
        // than a lower-dimension vector, which is what you could get with cutoff points to switch between (e.g.)
        // D*0.5 and D*0.25.  Thus, the following ensures that bytes per vector is strictly increasing with D.
        int compressedBytes;
        if (originalDimension <= 32) {
            // We are compressing from 4-byte floats to single-byte codebook indexes,
            // so this represents compression of 4x
            // * GloVe-25 needs 25 BPV to achieve good recall
            compressedBytes = originalDimension;
        }
        else if (originalDimension <= 64) {
            // * GloVe-50 performs fine at 25
            compressedBytes = 32;
        }
        else if (originalDimension <= 200) {
            // * GloVe-100 and -200 perform well at 50 and 100 BPV, respectively
            compressedBytes = (int) (originalDimension * 0.5);
        }
        else if (originalDimension <= 400) {
            // * NYTimes-256 actually performs fine at 64 BPV but we'll be conservative
            //   since we don't want BPV to decrease
            compressedBytes = 100;
        }
        else if (originalDimension <= 768) {
            // allow BPV to increase linearly up to 192
            compressedBytes = (int) (originalDimension * 0.25);
        }
        else if (originalDimension <= 1536) {
            // * ada002 vectors have good recall even at 192 BPV = compression of 32x
            compressedBytes = 192;
        }
        else {
            // We have not tested recall with larger vectors than this, let's let it increase linearly
            compressedBytes = (int) (originalDimension * 0.125);
        }
        return compressedBytes;
    }

    private static double genericOverquery(CompressedVectors cv)
    {
        assert cv != null;
        // we compress extra-large vectors more aggressively, so we need to bump up the limit for those.
        if (cv instanceof BinaryQuantization)
            return 2.0;
        else if ((double) cv.getOriginalSize() / cv.getCompressedSize() > 16.0)
            return 1.5;
        else
            return 1.0;
    }

    /**
     * @param limit the number of results the user asked for
     * @param cv the compressed vectors being queried.  Null if uncompressed.
     * @return the topK >= `limit` results to ask the index to search for, forcing
     * the greedy search deeper into the graph.  This serves two purposes:
     * 1. Smoothes out the relevance difference between small LIMIT and large
     * 2. Compensates for using lossily-compressed vectors during the search
     */
    public int topKFor(int limit, CompressedVectors cv)
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
        var n = tapered2x(limit);

        // per-model adjustment on top of the ~2x factor
        int originalDimension = cv.getOriginalSize() / 4;
        if (compressionProvider.apply(originalDimension).matches(cv))
        {
            n *= overqueryProvider.apply(cv);
        }
        else
        {
            // we're using an older CV that wasn't created with the currently preferred parameters,
            // so use the generic defaults instead
            n *= OTHER.overqueryProvider.apply(cv);
        }

        return (int) (n * limit);
    }

    @VisibleForTesting
    static double tapered2x(int limit)
    {
        return max(1.0, 0.509 + 9.491 * pow(limit, -0.402)); // f(1) = 10.0, f(100) = 2.0, f(1000) = 1.1
    }
}
