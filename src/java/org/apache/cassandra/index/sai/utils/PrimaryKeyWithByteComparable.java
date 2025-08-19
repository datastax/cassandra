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

package org.apache.cassandra.index.sai.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;

import io.github.jbellis.jvector.util.RamUsageEstimator;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * A {@link PrimaryKey} that includes a {@link ByteComparable} value from a source index.
 * Note: this class has a natural ordering that is inconsistent with equals.
 */
public class PrimaryKeyWithByteComparable extends PrimaryKeyWithSortKey
{
    private final ByteComparable byteComparable;

    public PrimaryKeyWithByteComparable(IndexContext context, Memtable sourceTable, PrimaryKey primaryKey, ByteComparable byteComparable)
    {
        this(context, (Object) sourceTable, primaryKey, byteComparable);
    }

    public PrimaryKeyWithByteComparable(IndexContext context, SSTableId<?> sourceTable, PrimaryKey primaryKey, ByteComparable byteComparable)
    {
        this(context, (Object) sourceTable, primaryKey, byteComparable);
    }

    private PrimaryKeyWithByteComparable(IndexContext context, Object sourceTable, PrimaryKey primaryKey, ByteComparable byteComparable)
    {
        super(context, sourceTable, primaryKey);
        this.byteComparable = byteComparable;
    }

    @Override
    public PrimaryKeyWithByteComparable forStaticRow()
    {
        return new PrimaryKeyWithByteComparable(context, sourceTable, primaryKey.forStaticRow(), byteComparable);
    }

    @Override
    protected boolean isIndexDataEqualToLiveData(ByteBuffer value)
    {
        if (context.isLiteral())
        {
            ByteSource byteSource = byteComparable.asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION);
            byte[] indexedValue = ByteSourceInverse.readBytes(byteSource);
            byte[] liveValue = ByteBufferUtil.getArray(value);
            return Arrays.compare(indexedValue, liveValue) == 0;
        }
        else
        {
            var peekableBytes = ByteSource.peekable(byteComparable.asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION));
            var bytes = context.getValidator().fromComparableBytes(peekableBytes, TypeUtil.BYTE_COMPARABLE_VERSION);
            return value.compareTo(bytes) == 0;
        }
    }

    @Override
    public int compareTo(PrimaryKey o)
    {
        if (!(o instanceof PrimaryKeyWithByteComparable))
            throw new IllegalArgumentException("Cannot compare PrimaryKeyWithByteComparable with " + o.getClass().getSimpleName());

        return ByteComparable.compare(byteComparable, ((PrimaryKeyWithByteComparable) o).byteComparable, TypeUtil.BYTE_COMPARABLE_VERSION);
    }

    @Override
    public long ramBytesUsed()
    {
        return super.ramBytesUsed() + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    }
}
