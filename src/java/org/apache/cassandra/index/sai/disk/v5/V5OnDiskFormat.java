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

package org.apache.cassandra.index.sai.disk.v5;

import java.io.IOException;

import org.slf4j.Logger;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat;

public class V5OnDiskFormat extends V3OnDiskFormat
{
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(V5OnDiskFormat.class);

    public static final V5OnDiskFormat instance = new V5OnDiskFormat();

    private static final boolean WRITE_V5_VECTOR_POSTINGS = Boolean.parseBoolean(System.getProperty("cassandra.sai.write_v5_vector_postings", "true"));
    public static boolean writeV5VectorPostings()
    {
        // circular reference from Version -> V5OnDiskFormat means we can't just evaluate this in a static block
        if (WRITE_V5_VECTOR_POSTINGS && !Version.latest().onOrAfter(Version.DC))
        {
            logger.error("V5 vector postings are configured but latest version does not support them.  Disabling V5 vector postings.");
            return false;
        }
        return WRITE_V5_VECTOR_POSTINGS;
    }

    @Override
    public IndexSearcher newIndexSearcher(SSTableContext sstableContext,
                                          IndexContext indexContext,
                                          PerIndexFiles indexFiles,
                                          SegmentMetadata segmentMetadata) throws IOException
    {
        if (indexContext.isVector())
            return new V5VectorIndexSearcher(sstableContext.primaryKeyMapFactory(), indexFiles, segmentMetadata, indexContext);
        return super.newIndexSearcher(sstableContext, indexContext, indexFiles, segmentMetadata);
    }
}