/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.disk.v8;

import java.util.EnumSet;
import java.util.Set;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v7.V7OnDiskFormat;
import org.apache.cassandra.index.sai.utils.TypeUtil;

public class V8OnDiskFormat extends V7OnDiskFormat
{
    public static final V8OnDiskFormat instance = new V8OnDiskFormat();

    private static final Set<IndexComponentType> LITERAL_COMPONENTS = EnumSet.of(IndexComponentType.COLUMN_COMPLETION_MARKER,
                                                                                 IndexComponentType.META,
                                                                                 IndexComponentType.TERMS_DATA,
                                                                                 IndexComponentType.POSTING_LISTS,
                                                                                 IndexComponentType.DOC_LENGTHS);
    
    private static final IndexFeatureSet v8IndexFeatureSet = new IndexFeatureSet()
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
    };

    @Override
    public IndexFeatureSet indexFeatureSet()
    {
        return v8IndexFeatureSet;
    }

    @Override
    public Set<IndexComponentType> perIndexComponentTypes(AbstractType<?> validator)
    {
        if (validator.isVector())
            return V3OnDiskFormat.VECTOR_COMPONENTS_V3;
        if (TypeUtil.isLiteral(validator))
            return LITERAL_COMPONENTS;
        return V1OnDiskFormat.NUMERIC_COMPONENTS;
    }
}
