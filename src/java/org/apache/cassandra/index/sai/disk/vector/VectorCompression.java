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

import java.util.Objects;

public class VectorCompression
{
    public static final VectorCompression NO_COMPRESSION = new VectorCompression(CompressionType.NONE, -1, -1);

    public final CompressionType type;
    private final int originalSize; // in bytes
    private final int compressedSize; // in bytes

    public VectorCompression(CompressionType type, int dimension, double ratio)
    {
        this.type = type;
        this.originalSize = dimension * Float.BYTES;
        this.compressedSize = (int) (originalSize * ratio);
    }

    public VectorCompression(CompressionType type, int originalSize, int compressedSize)
    {
        this.type = type;
        this.originalSize = originalSize;
        this.compressedSize = compressedSize;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VectorCompression that = (VectorCompression) o;
        if (type == CompressionType.NONE)
            return that.type == CompressionType.NONE;
        return originalSize == that.originalSize && compressedSize == that.compressedSize && type == that.type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, getOriginalSize(), getCompressedSize());
    }

    public String toString()
    {
        return String.format("VectorCompression(%s, %d->%d)", type, originalSize, compressedSize);
    }

    public int getOriginalSize()
    {
        if (type == CompressionType.NONE)
            throw new UnsupportedOperationException();
        return originalSize;
    }

    public int getCompressedSize()
    {
        if (type == CompressionType.NONE)
            throw new UnsupportedOperationException();
        return compressedSize;
    }

    public enum CompressionType
    {
        NONE,
        PRODUCT_QUANTIZATION,
        BINARY_QUANTIZATION
    }
}
