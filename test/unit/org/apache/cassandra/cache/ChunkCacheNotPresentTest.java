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

package org.apache.cassandra.cache;


import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionRealm;
import org.apache.cassandra.db.compaction.unified.RealEnvironment;
import org.apache.cassandra.schema.CompressionParams;
import org.mockito.Mockito;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ChunkCacheNotPresentTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.disableChunkCache();
    }

    @Test
    public void testCompressionParams()
    {
        assertNull(ChunkCache.instance);
        CompressionParams params = CompressionParams.fromMap(ImmutableMap.of("class", "LZ4Compressor", "chunk_length_in_kb", "1"));
        assertNotNull(params);
    }

    @Test
    public void testRealEnvironment()
    {
        assertNull(ChunkCache.instance);
        CompactionRealm mockRealm = Mockito.mock(CompactionRealm.class);
        RealEnvironment env = new RealEnvironment(mockRealm);
        env.cacheMissRatio();
    }
}
