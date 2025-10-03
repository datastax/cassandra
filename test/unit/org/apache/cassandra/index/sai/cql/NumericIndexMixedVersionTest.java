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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.junit.Assert.assertEquals;

public class NumericIndexMixedVersionTest extends SAITester
{
    // Versions in random order
    final static List<Version> VERSIONS = getVersions();

    private static List<Version> getVersions()
    {
        var versions = new ArrayList<>(Version.ALL);
        Collections.reverse(versions);
        // AA is the earliest version and produces different data for flush vs compaction, so we have
        // special logic to hit that and make this first.
        assert versions.get(0).equals(Version.AA);
        logger.info("Running mixed version test with versions: {}", versions);
        return versions;
    }


    // This test does not trigger an issue. It simply confirms that we can query across versions.
    @Test
    public void testMultiVersionCompatibilityNoClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, val int, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // Note that we do not test the multi-version path where compaction produces different sstables, which is
        // the norm in CNDB. If we had a way to compnact individual sstables, we could.
        disableCompaction();

        SAIUtil.setCurrentVersion(Version.AA);
        for (int j = 0; j < 500; j++)
            execute("INSERT INTO %s (pk, val) VALUES (?, ?)", j, j);
        flush();
        compact();

        // Insert 500 rows per version, each with a unique pk but overlapping values.
        int pk = 0;
        for (var version : VERSIONS)
        {
            SAIUtil.setCurrentVersion(version);
            for (int i = 0; i < 500; i++)
                execute("INSERT INTO %s (pk, val) VALUES (?, ?)", pk++, i);
            flush();
        }

        // Confirm that compaction (aka rebuilding all indexes onto same version) also produces correct results
        final int expectedRows = pk;
        runThenFlushThenCompact(() -> {
            var batchLimit = CassandraRelevantProperties.SAI_PARTITION_ROW_BATCH_SIZE.getInt();
            // Query that will hit all sstables and exceed the cassandra.sai.partition_row_batch_size limit
            var rows = executeNetWithPaging("SELECT pk FROM %s WHERE val >= 0 LIMIT 10000", batchLimit / 2);
            assertEquals(expectedRows, rows.all().size());

            rows = executeNetWithPaging("SELECT pk FROM %s WHERE val >= 0 LIMIT 10000", batchLimit);
            assertEquals(expectedRows, rows.all().size());

            rows = executeNetWithPaging("SELECT pk FROM %s WHERE val >= 0 LIMIT 10000", batchLimit * 2);
            assertEquals(expectedRows, rows.all().size());

            // Test without paging
            assertNumRows(expectedRows, "SELECT pk FROM %%s WHERE val >= 0 LIMIT 10000");
        });
    }

    @Test
    public void testMultiVersionCompatibilityWithClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, val int, PRIMARY KEY(pk, ck)) WITH CLUSTERING ORDER BY (ck ASC)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // Note that we do not test the multi-version path where compaction produces different sstables, which is
        // the norm in CNDB. If we had a way to compact individual sstables, we could.
        disableCompaction();

        SAIUtil.setCurrentVersion(Version.AA);
        int ck = 0;
        for (int j = 0; j < 500; j++)
            execute("INSERT INTO %s (pk, ck, val) VALUES (1, ?, ?)", ck++, j);
        flush();
        compact();

        // Insert 500 rows per version
        for (var version : VERSIONS)
        {
            SAIUtil.setCurrentVersion(version);
            for (int j = 0; j < 500; j++)
                execute("INSERT INTO %s (pk, ck, val) VALUES (1, ?, ?)", ck++, j);
            flush();
        }

        // Confirm that compaction (aka rebuilding all indexes onto same version) also produces correct results
        final int expectedRows = ck;
        runThenFlushThenCompact(() -> {
            // When using paging, we get an excessive number of results because of logic within the contoller.select
            // method that short circuits when one of the indexes is aa (not row aware).
            var batchLimit = CassandraRelevantProperties.SAI_PARTITION_ROW_BATCH_SIZE.getInt();
            var rows = executeNetWithPaging("SELECT ck FROM %s WHERE val >= 0 LIMIT 10000", batchLimit / 2);
            assertEquals(expectedRows, rows.all().size());

            rows = executeNetWithPaging("SELECT ck FROM %s WHERE val >= 0 LIMIT 10000", batchLimit);
            assertEquals(expectedRows, rows.all().size());

            rows = executeNetWithPaging("SELECT ck FROM %s WHERE val >= 0 LIMIT 10000", batchLimit * 2);
            assertEquals(expectedRows, rows.all().size());

            // Test without paging. This test actually fails by producing fewer than expected rows because of an issue in
            // partition-only primary keys and row aware primary keys that are considered equal. When they are unioned
            // in the iterator, we take one and leave the other (they evaluate to equal after all) but this behavior
            // filters out a result that would have loaded the whole partition and might have returned a unique result.
            assertNumRows(expectedRows, "SELECT ck FROM %%s WHERE val >= 0 LIMIT 10000");
        });
    }

}
