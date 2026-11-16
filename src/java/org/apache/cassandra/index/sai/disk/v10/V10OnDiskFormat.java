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

package org.apache.cassandra.index.sai.disk.v10;

import java.util.EnumSet;
import java.util.Set;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.v9.V9OnDiskFormat;

public class V10OnDiskFormat extends V9OnDiskFormat
{
    public static final V10OnDiskFormat instance = new V10OnDiskFormat();

    /** The components of a vector index with {@code enable_ash}: its ASH codes in place of PQ. */
    public static final Set<IndexComponentType> ASH_VECTOR_COMPONENTS = EnumSet.of(IndexComponentType.COLUMN_COMPLETION_MARKER,
                                                                                   IndexComponentType.META,
                                                                                   IndexComponentType.ASH,
                                                                                   IndexComponentType.TERMS_DATA,
                                                                                   IndexComponentType.POSTING_LISTS);

    @Override
    public Set<IndexComponentType> perIndexComponentTypes(IndexContext indexContext)
    {
        if (indexContext.isVector() && indexContext.getIndexWriterConfig().isAshEnabled())
            return ASH_VECTOR_COMPONENTS;
        return super.perIndexComponentTypes(indexContext);
    }

    /**
     * JVector file format 7, the first that reads and writes page-aligned records.
     */
    @Override
    public int jvectorFileFormatVersion()
    {
        return 7;
    }
}
