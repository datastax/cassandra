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

package org.apache.cassandra.index.sai.cql;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;

// We do not parameterize this test because it is not intended to run multiple versions at once.
public class VectorIndexMixedVersionTest extends VectorTester
{
    // Versions in random order
    final static List<Version> VERSIONS = getVersions();

    private static List<Version> getVersions()
    {
        var versions = Lists.newArrayList(Version.CA, Version.DC, Version.EB);
        Collections.shuffle(versions, getRandom().getRandom());
        return versions;
    }

    @Test
    public void testMultiVersionJVectorCompatibility() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, vec vector<float, 4>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");

        // Note that we do not test the multi-version path where compaction produces different sstables, which is
        // the norm in CNDB. If we had a way to compact individual sstables, we could.
        disableCompaction();

        for (var version : VERSIONS)
        {
            SAIUtil.setCurrentVersion(version);
            for (int i = 0; i < CassandraOnHeapGraph.MIN_PQ_ROWS; i++)
                execute("INSERT INTO %s (pk, vec) VALUES (?, ?)", i, randomVectorBoxed(4));
            flush();
        }

        // Run basic query to confirm we can, no need to validate results
        execute("SELECT pk FROM %s ORDER BY vec ANN OF [2.0, 2.0, 3.0, 4.0] LIMIT 2");
    }
}
