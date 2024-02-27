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

import io.github.jbellis.jvector.pq.BQVectors;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.pq.PQVectors;

public class VectorCompression
{
    public final CompressionType type;
    public final int compressToBytes;

    public VectorCompression(CompressionType type, int compressToBytes)
    {
        this.type = type;
        this.compressToBytes = compressToBytes;
    }

    /**
     * @return true if the given CompressedVectors implements a matching compression type and size
     */
    public boolean matches(CompressedVectors cv)
    {
        if (type == CompressionType.NONE)
            return cv == null;
        if (cv == null)
            return false;

        if (type == CompressionType.PRODUCT_QUANTIZATION)
            return cv instanceof PQVectors && cv.getCompressedSize() == compressToBytes;

        assert type == CompressionType.BINARY_QUANTIZATION;
        // BQ algorithm is the same no matter what the size is
        return cv instanceof BQVectors;
    }

    public String toString()
    {
        return String.format("VectorCompression(%s, %d)", type, compressToBytes);
    }

    public enum CompressionType
    {
        NONE,
        PRODUCT_QUANTIZATION,
        BINARY_QUANTIZATION
    }
}
