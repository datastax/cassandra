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

package org.apache.cassandra.io.util;

import java.util.Objects;
import java.util.StringJoiner;

public class SliceDescriptor
{
    public static final SliceDescriptor NONE = new SliceDescriptor(0, 0, 0);

    /**
     * The position of the beginning of the data in the original file (inclusive).
     */
    public final long dataStart;

    /**
     * The position of the end of the data in the original file (exclusive).
     */
    public final long dataEnd;

    /**
     * The size of the chunk to which the slice is aligned.
     */
    public final int chunkSize;

    /**
     * The position of beginning of this slice in the original file (inclusive). It is the {@link #dataStart}
     * aligned to the chunk size.
     */
    public final long sliceStart;

    /**
     * The position of the end of this slice in the original file (exclusive). It is the {@link #dataEnd}
     * aligned to the chunk size. {@code sliceEnd - sliceStart} is equals to the actual size of the partial file.
     */
    public final long sliceEnd;

    public SliceDescriptor(long dataStart, long dataEnd, int chunkSize)
    {
        this.dataStart = dataStart;
        this.dataEnd = dataEnd;
        this.chunkSize = chunkSize;

        this.sliceStart = chunkSize == 0 ? dataStart : dataStart & -chunkSize;
        this.sliceEnd = chunkSize == 0 ? dataEnd : (chunkSize + dataEnd - 1) & -chunkSize;
    }

    public boolean exists()
    {
        return dataStart > 0 || dataEnd > 0;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SliceDescriptor that = (SliceDescriptor) o;
        return dataStart == that.dataStart && dataEnd == that.dataEnd && chunkSize == that.chunkSize;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(dataStart, dataEnd, chunkSize);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", SliceDescriptor.class.getSimpleName() + "[", "]")
               .add("dataStart=" + dataStart)
               .add("dataEnd=" + dataEnd)
               .add("chunkSize=" + chunkSize)
               .add("sliceStart=" + sliceStart)
               .add("sliceEnd=" + sliceEnd)
               .toString();
    }

    public long dataEndOr(long dataEndIfNotExists)
    {
        return exists() ? dataEnd : dataEndIfNotExists;
    }
}
