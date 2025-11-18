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

import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.io.sstable.SSTableId;

/**
 * A {@link PrimaryKey} that includes a score from a source index.
 * Note: this class has a natural ordering that is inconsistent with equals.
 */
public class PrimaryKeyWithScore extends PrimaryKeyWithSortKey
{
    public final float indexScore;
    public final boolean isScoreApproximate;

    /**
     * Constructs a new {@link PrimaryKeyWithScore} for a memtable source. Memtables always have exact scores, so
     * we do not parameterize the isScoreApproximate flag.
     */
    public PrimaryKeyWithScore(IndexContext context, Memtable source, PrimaryKey primaryKey, float indexScore)
    {
        this(context, (Object) source, primaryKey, indexScore, false);
    }

    /**
     * Constructs a new {@link PrimaryKeyWithScore} for an sstable source. SStables may have approximate scores, so
     * we parameterize the isScoreApproximate flag. Setting the isScoreApproximate flag to true will cause the score to be
     * recalculated from the live data when the row is read.
     */
    public PrimaryKeyWithScore(IndexContext context, SSTableId<?> source, PrimaryKey primaryKey, float indexScore, boolean isScoreApproximate)
    {
        this(context, (Object) source, primaryKey, indexScore, isScoreApproximate);
    }

    private PrimaryKeyWithScore(IndexContext context, Object source, PrimaryKey primaryKey, float indexScore, boolean isScoreApproximate)
    {
        super(context, source, primaryKey);
        this.indexScore = indexScore;
        this.isScoreApproximate = isScoreApproximate;
    }

    @Override
    public PrimaryKeyWithScore forStaticRow()
    {
        return new PrimaryKeyWithScore(context, sourceTable, primaryKey.forStaticRow(), indexScore, isScoreApproximate);
    }

    @Override
    protected boolean isIndexDataEqualToLiveData(ByteBuffer value)
    {
        // Vector indexes handle updated rows properly and not allow a row to have more than one value in the same
        // index segment. Therefore, there is no need to validate the index data against the live data.
        return true;
    }

    public float getExactScore(Orderer orderer, Row row)
    {
        if (!isScoreApproximate)
            return indexScore;
        return orderer.score(row.getCell(context.getDefinition()).buffer());
    }

    @Override
    public int compareTo(PrimaryKey o)
    {
        if (!(o instanceof PrimaryKeyWithScore))
            throw new IllegalArgumentException("Cannot compare PrimaryKeyWithScore with " + o.getClass().getSimpleName());

        // Descending order
        return Float.compare(((PrimaryKeyWithScore) o).indexScore, indexScore);
    }

    @Override
    public long ramBytesUsed()
    {
        // Include super class fields plus float value
        return super.ramBytesUsed() + Float.BYTES;
    }
}
