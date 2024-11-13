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

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * A PrimaryKey with one piece of metadata. Subclasses define the metadata, and to prevent unnecessary boxing, the
 * metadata is not referenced in this calss. The metadata is not used to determine equality or hash code, but it is used
 * to compare the PrimaryKey objects.
 * Note: this class has a natural ordering that is inconsistent with equals.
 */
public abstract class PrimaryKeyWithSortKey implements PrimaryKey
{
    protected final IndexContext context;
    private final PrimaryKey primaryKey;
    // Either a Memtable reference or an SSTableId reference
    private final Object sourceTable;

    protected PrimaryKeyWithSortKey(IndexContext context, Object sourceTable, PrimaryKey primaryKey)
    {
        this.context = context;
        this.sourceTable = sourceTable;
        this.primaryKey = primaryKey;
    }

    public PrimaryKey primaryKey()
    {
        return primaryKey;
    }

    public boolean isIndexDataValid(Row row, int nowInSecs)
    {
        assert context.getDefinition().isRegular() : "Only regular columns are supported, got " + context.getDefinition();
        var cell = row.getCell(context.getDefinition());
        if (!cell.isLive(nowInSecs))
            return false;
        assert cell instanceof CellWithSourceTable : "Expected CellWithSource, got " + cell.getClass();
        return sourceTable.equals(((CellWithSourceTable<?>) cell).sourceTable())
               && isIndexDataEqualToLiveData(cell.buffer());
    }

    /**
     * Compares the index data to the live data to ensure that the index data is still valid. This is only
     * necessary when an index allows one row to have multiple values associated with it.
     */
    abstract protected boolean isIndexDataEqualToLiveData(ByteBuffer value);

    /**
     * Tie-breaker for comparing two PrimaryKeyWithSortKey objects. We want newer elements sorted first in cases of a
     * tied score, so we compare the {@link #sourceTable}. If both are memtables, we don't care about the order. If one
     * is a memtable and the other is an SSTable, the memtable should be sorted first. If both are SSTables, we compare
     * the SSTableId.
     *
     * @param o the object to compare to
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the
     *        specified object.
     */
    protected int compareSourceTables(PrimaryKeyWithSortKey o)
    {
        var sourceTableIsMemtable = sourceTable instanceof Memtable;
        var oSourceTableIsMemtable = o.sourceTable instanceof Memtable;
        if (sourceTableIsMemtable && oSourceTableIsMemtable)
            return 0;
        if (sourceTableIsMemtable)
            return -1;
        if (oSourceTableIsMemtable)
            return 1;

        // Not enough information to compare the SSTableId objects, so we don't break the tie.
        if (sourceTable.getClass() != o.sourceTable.getClass())
            return 0;

        var cmp = ((SSTableId) sourceTable).compareTo(o.sourceTable);
        // Descending order
        return -cmp;
    }

    @Override
    public final int hashCode()
    {
        // The sort key must not affect the hash code because
        // the same Primary Key could have different scores depending
        // on the source sstable/index, and we store this object
        // in a HashMap to prevent loading the same row multiple times.
        return primaryKey.hashCode();
    }

    @Override
    public final boolean equals(Object obj)
    {
        if (!(obj instanceof PrimaryKeyWithSortKey))
            return false;

        // The sort key must not affect the equality because
        // the same Primary Key could have different scores depending
        // on the source sstable/index, and we store this object
        // in a HashMap to prevent loading the same row multiple times.
        return primaryKey.equals(((PrimaryKeyWithSortKey) obj).primaryKey());
    }


    // Generic primary key wrapper methods:
    @Override
    public Token token()
    {
        return primaryKey.token();
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return primaryKey.partitionKey();
    }

    @Override
    public Clustering<?> clustering()
    {
        return primaryKey.clustering();
    }

    @Override
    public PrimaryKey loadDeferred()
    {
        return primaryKey.loadDeferred();
    }

    @Override
    public ByteSource asComparableBytes(ByteComparable.Version version)
    {
        return primaryKey.asComparableBytes(version);
    }

    @Override
    public ByteSource asComparableBytesMinPrefix(ByteComparable.Version version)
    {
        return primaryKey.asComparableBytesMinPrefix(version);
    }

    @Override
    public ByteSource asComparableBytesMaxPrefix(ByteComparable.Version version)
    {
        return primaryKey.asComparableBytesMaxPrefix(version);
    }

}
