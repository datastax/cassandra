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

package org.apache.cassandra.index.sai.disk.v7;

import java.util.EnumSet;
import java.util.Set;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.v6.V6OnDiskFormat;

public class V7OnDiskFormat extends V6OnDiskFormat
{
    public static final V7OnDiskFormat instance = new V7OnDiskFormat();

    private static final IndexFeatureSet v7IndexFeatureSet = new IndexFeatureSet()
    {
        @Override
        public boolean isRowAware()
        {
            return true;
        }

        @Override
        public boolean hasVectorIndexChecksum()
        {
            return false;
        }

        @Override
        public boolean hasTermsHistogram()
        {
            return true;
        }

        @Override
        public boolean hasNullIndex()
        {
            return true;
        }
    };


    private static final Set<IndexComponentType> LITERAL_COMPONENTS = EnumSet.of(IndexComponentType.COLUMN_COMPLETION_MARKER,
                                                                                 IndexComponentType.META,
                                                                                 IndexComponentType.TERMS_DATA,
                                                                                 IndexComponentType.POSTING_LISTS,
                                                                                 IndexComponentType.NULL_POSTING_LIST);

    private static final Set<IndexComponentType> NUMERIC_COMPONENTS = EnumSet.of(IndexComponentType.COLUMN_COMPLETION_MARKER,
                                                                                 IndexComponentType.META,
                                                                                 IndexComponentType.KD_TREE,
                                                                                 IndexComponentType.KD_TREE_POSTING_LISTS,
                                                                                 IndexComponentType.NULL_POSTING_LIST);

    private static final Set<IndexComponentType> VECTOR_COMPONENTS = EnumSet.of(IndexComponentType.COLUMN_COMPLETION_MARKER,
                                                                                IndexComponentType.META,
                                                                                IndexComponentType.PQ,
                                                                                IndexComponentType.TERMS_DATA,
                                                                                IndexComponentType.POSTING_LISTS,
                                                                                IndexComponentType.NULL_POSTING_LIST);

    @Override
    public Set<IndexComponentType> perIndexComponentTypes(IndexContext context)
    {
        return context.isLiteral() ? LITERAL_COMPONENTS : context.isVector() ? VECTOR_COMPONENTS : NUMERIC_COMPONENTS;
    }

    @Override
    public IndexFeatureSet indexFeatureSet()
    {
        return v7IndexFeatureSet;
    }
}