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

package org.apache.cassandra.index.sai.disk.vector;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.disk.format.Version;

public class JVectorVersionUtil
{
    /** Whether to fuse quantized vectors into the graph when writing indexes, assuming all other conditions are met */
    public static final boolean ENABLE_FUSED = CassandraRelevantProperties.SAI_VECTOR_ENABLE_FUSED.getBoolean();
    public static final boolean ENABLE_NVQ = CassandraRelevantProperties.SAI_VECTOR_ENABLE_NVQ.getBoolean();
    public static final int NUM_SUB_VECTORS = CassandraRelevantProperties.SAI_VECTOR_NVQ_NUM_SUB_VECTORS.getInt();

    /**
     * Decide whether we should write NVQ vectors to disk.
     * With NVQ, we use M * (7 + D / M) bytes, where D is the number of dimensions and M is the number of subvectors.
     * For FP vectors, we trivially use 4D bytes
     * @param dimension vector dimension for the index
     * @param version SAI on disk version, which internally determines the jvector version
     * @return true if NVQ should be used for the graph or false otherwise
     */
    public static boolean shouldWriteNVQ(int dimension, Version version)
    {
        return ENABLE_NVQ && versionSupportsNVQ(version) && NUM_SUB_VECTORS * (7 + dimension / NUM_SUB_VECTORS) < 4 * dimension;
    }

    public static boolean versionSupportsNVQ(Version version)
    {
        return version.onDiskFormat().jvectorFileFormatVersion() >= 4;
    }

    /**
     * Decide whether to attempt to write the quantized vectors as fused parts of the graph. Note that this method
     * does not take into account whether the graph has enough information to build a quantization, as that depends on
     * external factors.
     * @param version the SAI on disk format to use when writing to disk
     * @return true if conditions are met, false otherwise
     */
    public static boolean shouldWriteFused(Version version)
    {
        return ENABLE_FUSED && versionSupportsFused(version);
    }

    public static boolean versionSupportsFused(Version version)
    {
        return version.onDiskFormat().jvectorFileFormatVersion() >= 6;
    }
}
