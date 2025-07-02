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

package org.apache.cassandra.index.sai.disk.v3;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v2.V2OnDiskFormat;

import static org.apache.cassandra.config.CassandraRelevantProperties.*;

/**
 * Different vector components compared to V2OnDiskFormat (supporting DiskANN/jvector instead of HNSW/lucene).
 */
public class V3OnDiskFormat extends V2OnDiskFormat
{
    public static final boolean REDUCE_TOPK_ACROSS_SSTABLES = SAI_REDUCE_TOPK_ACROSS_SSTABLES.getBoolean();
    public static final boolean ENABLE_RERANK_FLOOR = SAI_ENABLE_RERANK_FLOOR.getBoolean();
    public static final boolean ENABLE_EDGES_CACHE = SAI_ENABLE_EDGES_CACHE.getBoolean();
    public static final boolean ENABLE_JVECTOR_DELETES = SAI_ENABLE_JVECTOR_DELETES.getBoolean();

    public static volatile boolean WRITE_JVECTOR3_FORMAT = SAI_WRITE_JVECTOR3_FORMAT.getBoolean();
    public static final boolean ENABLE_LTM_CONSTRUCTION = SAI_ENABLE_LTM_CONSTRUCTION.getBoolean();
    // JVector doesn't give us a way to access its default, so we set it here, but allow it to be overridden.
    public static boolean JVECTOR_USE_PRUNING_DEFAULT = SAI_VECTOR_USE_PRUNING_DEFAULT.getBoolean();

    // We allow the version to be configured via a system property because of some legacy use cases, but it is
    // generally not recommended to change this directly. Instead, use the cassandra.sai.latest.version system property
    // to control the on-disk format version.
    private final static int JVECTOR_VERSION = SAI_JVECTOR_VERSION.getInt();
    static
    {
        // JVector 3 is not compatible with the latest jvector changes, so we fail fast if the config is enabled.
        assert JVECTOR_VERSION != 3 : "JVector version 3 is no longer suppoerted";
        assert !WRITE_JVECTOR3_FORMAT : "JVector version 3 is no longer suppoerted";
    }

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final V3OnDiskFormat instance = new V3OnDiskFormat();

    public static final Set<IndexComponentType> VECTOR_COMPONENTS_V3 = EnumSet.of(IndexComponentType.COLUMN_COMPLETION_MARKER,
                                                                                  IndexComponentType.META,
                                                                                  IndexComponentType.PQ,
                                                                                  IndexComponentType.TERMS_DATA,
                                                                                  IndexComponentType.POSTING_LISTS);

    private static final IndexFeatureSet v3IndexFeatureSet = new IndexFeatureSet()
    {
        @Override
        public boolean isRowAware()
        {
            return true;
        }

        @Override
        public boolean hasTermsHistogram()
        {
            return false;
        }
    };

    @Override
    public IndexFeatureSet indexFeatureSet()
    {
        return v3IndexFeatureSet;
    }

    @Override
    public IndexSearcher newIndexSearcher(SSTableContext sstableContext,
                                          IndexContext indexContext,
                                          PerIndexFiles indexFiles,
                                          SegmentMetadata segmentMetadata) throws IOException
    {
        if (indexContext.isVector())
            return new V3VectorIndexSearcher(sstableContext, indexFiles, segmentMetadata, indexContext);
        if (indexContext.isLiteral())
            return new V3InvertedIndexSearcher(sstableContext, indexFiles, segmentMetadata, indexContext);
        return super.newIndexSearcher(sstableContext, indexContext, indexFiles, segmentMetadata);
    }

    @Override
    public Set<IndexComponentType> perIndexComponentTypes(AbstractType<?> validator)
    {
        // VSTODO add checksums and actual validation
        if (validator.isVector())
            return VECTOR_COMPONENTS_V3;
        return super.perIndexComponentTypes(validator);
    }

    @Override
    public int jvectorFileFormatVersion()
    {
        return JVECTOR_VERSION;
    }
}
