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

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import static org.apache.cassandra.index.sai.disk.vector.VectorCompression.CompressionType.BINARY_QUANTIZATION;
import static org.apache.cassandra.index.sai.disk.vector.VectorCompression.CompressionType.PRODUCT_QUANTIZATION;

public enum VectorSourceModel
{
    ADA002,
    OPENAI_V3_SMALL,
    OPENAI_V3_LARGE,
    BERT,
    GECKO,

    OTHER;

    public static VectorSourceModel fromString(String value)
    {
        return valueOf(value.toUpperCase());
    }

    public VectorSimilarityFunction defaultSimilarityFunction()
    {
        if (this == OTHER)
            return VectorSimilarityFunction.COSINE;
        return VectorSimilarityFunction.DOT_PRODUCT;
    }

    public VectorCompression preferredCompression(int originalDimension)
    {
        // if model is specified, use specifically tuned compression parameters
        switch (this)
        {
            case ADA002:
                return new VectorCompression(BINARY_QUANTIZATION, originalDimension / 8);
            case OPENAI_V3_SMALL:
                return new VectorCompression(PRODUCT_QUANTIZATION, originalDimension / 16);
            case OPENAI_V3_LARGE:
                return new VectorCompression(PRODUCT_QUANTIZATION, originalDimension / 16);
            case BERT:
                // VSTODO test if we can be more aggressive here
                return new VectorCompression(PRODUCT_QUANTIZATION, originalDimension / 4);
            case GECKO:
                return new VectorCompression(PRODUCT_QUANTIZATION, originalDimension / 4);
        }

        // Model is unspecified / unknown, so we guess.
        // Guessing heuristic is simple: 1536 dimensions is probably ada002, so use BQ.  Otherwise, use PQ.
        assert this == OTHER : this;
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
}
