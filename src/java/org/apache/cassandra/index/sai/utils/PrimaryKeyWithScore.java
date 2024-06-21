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

import org.apache.cassandra.index.sai.IndexContext;

/**
 * A {@link PrimaryKey} that includes a score from a source index.
 * Note: this class has a natural ordering that is inconsistent with equals.
 */
public class PrimaryKeyWithScore extends PrimaryKeyWithSortKey
{
    private final float indexScore;

    public PrimaryKeyWithScore(IndexContext context, Object source, PrimaryKey primaryKey, float indexScore)
    {
        super(context, source, primaryKey);
        this.indexScore = indexScore;
    }

    @Override
    protected boolean isIndexDataEqualToLiveData(ByteBuffer value)
    {
        // Vector indexes handle updated rows properly and not allow a row to have more than one value in the same
        // index segment. Therefore, there is no need to validate the index data against the live data.
        return true;
    }

    @Override
    public int compareTo(PrimaryKey o)
    {
        if (!(o instanceof PrimaryKeyWithScore))
            throw new IllegalArgumentException("Cannot compare PrimaryKeyWithScore with " + o.getClass().getSimpleName());

        return Float.compare(indexScore, ((PrimaryKeyWithScore) o).indexScore);
    }
}
