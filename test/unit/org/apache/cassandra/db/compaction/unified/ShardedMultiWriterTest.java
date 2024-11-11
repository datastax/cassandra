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

package org.apache.cassandra.db.compaction.unified;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.ShardManager;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ShardedMultiWriterTest extends CQLTester
{
    private static final int ROW_PER_PARTITION = 10;

    @Parameterized.Parameter
    public boolean isReplicaAware;

    @Parameterized.Parameters(name = "isReplicaAware={0}")
    public static Object[] parameters()
    {
        return new Object[] { true, false };
    }

    @BeforeClass
    public static void beforeClass()
    {
        System.setProperty("unified_compaction.l0_shards_enabled", "true");
        CQLTester.setUpClass();
        StorageService.instance.initServer();
    }

    @Test
    public void testShardedCompactionWriter_fiveShards() throws Throwable
    {
        int numShards = 5;
        int minSSTableSizeMB = 2;
        long totSizeBytes = ((minSSTableSizeMB << 20) * numShards) * 2;

        // We have double the data required for 5 shards so we should get 5 shards
        testShardedCompactionWriter(numShards, totSizeBytes, numShards);
    }

    @Test
    public void testShardedCompactionWriter_oneShard() throws Throwable
    {
        int numShards = 1;
        int minSSTableSizeMB = 2;
        long totSizeBytes = (minSSTableSizeMB << 20);

        // there should be only 1 shard if there is <= minSSTableSize
        testShardedCompactionWriter(numShards, totSizeBytes, 1);
    }

    @Test
    public void testShardedCompactionWriter_threeShard() throws Throwable
    {
        int numShards = 3;
        int minSSTableSizeMB = 2;
        long totSizeBytes = (minSSTableSizeMB << 20) * 3;

        // there should be only 3 shards if there is minSSTableSize * 3 data
        testShardedCompactionWriter(numShards, totSizeBytes, 3);
    }

    private void testShardedCompactionWriter(int numShards, long totSizeBytes, int numOutputSSTables) throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (k int, t int, v blob, PRIMARY KEY (k, t)) with compaction = " +
                                  "{'class':'UnifiedCompactionStrategy', 'base_shard_count' : '%d', " +
                                  "'min_sstable_size' : '0B', 'is_replica_aware': '%s'} ", numShards, isReplicaAware));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        int rowCount = insertData(totSizeBytes);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        assertEquals(numOutputSSTables, cfs.getLiveSSTables().size());

        if (isReplicaAware)
        {
            // Assert that the space does not cross token boundaries
            var tokenMetadata = StorageService.instance.getTokenMetadataForKeyspace(keyspace());
            var tokenSpaceCoverage = 0d;
            var spannedTokens = 0;
            for (SSTableReader rdr : cfs.getLiveSSTables())
            {
                final double coverage = rdr.first.getToken().size(rdr.last.getToken());
                // the coverage reported by rdr.tokenSpaceCoverage() may be adjusted upwards if the sstable spans too
                // few partitions
                if (rdr.estimatedKeys() >= ShardManager.PER_PARTITION_SPAN_THRESHOLD
                    && coverage >= ShardManager.MINIMUM_TOKEN_COVERAGE)
                    assertEquals(coverage, rdr.tokenSpaceCoverage(), 0.01);

                tokenSpaceCoverage += coverage;
                for (var token : tokenMetadata.sortedTokens())
                    if (rdr.getBounds().contains(token))
                        spannedTokens++;
            }
            // We don't have an even distribution because the first token is selected at random and we split along
            // token boundaries, so we don't assert even distribution. We do however konw that the coverage should
            // add up to about 1 without crossing that boundary. The coverage is measured by measuring the distance
            // between the min and the max token in each shard, so we have a large delta in the assertion.
            assertTrue(tokenSpaceCoverage <= 1.0);
            assertEquals(1.0, tokenSpaceCoverage, 0.1);
            // If we have more split points than tokens, the sstables must be split along token boundaries
            var numSplitPoints = numShards - 1;
            var expectedSpannedTokens = Math.max(0, tokenMetadata.sortedTokens().size() - numSplitPoints);
            // There is a chance that the sstable bounds don't contain a token boundary due to the random selection
            // of the first token, so we can only assert that we don't have more spanned tokens than expected.
            assertTrue(expectedSpannedTokens >= spannedTokens);
        }
        else
        {
            for (SSTableReader rdr : cfs.getLiveSSTables())
            {
                assertEquals(1.0 / numOutputSSTables, rdr.tokenSpaceCoverage(), 0.05);
            }
        }

        validateData(rowCount);
        cfs.truncateBlocking();
    }

    private int insertData(long totSizeBytes) throws Throwable
    {
        byte [] payload = new byte[5000];
        ByteBuffer b = ByteBuffer.wrap(payload);
        int rowCount = (int) Math.ceil((double) totSizeBytes / (8 + ROW_PER_PARTITION * payload.length));

        for (int i = 0; i < rowCount; i++)
        {
            for (int j = 0; j < ROW_PER_PARTITION; j++)
            {
                new Random(42 + i * ROW_PER_PARTITION + j).nextBytes(payload); // write different data each time to make non-compressible
                execute("INSERT INTO %s(k, t, v) VALUES (?, ?, ?)", i, j, b);
            }
        }

        return rowCount;
    }

    private void validateData(int rowCount) throws Throwable
    {
        for (int i = 0; i < rowCount; i++)
        {
            Object[][] expected = new Object[ROW_PER_PARTITION][];
            for (int j = 0; j < ROW_PER_PARTITION; j++)
                expected[j] = row(i, j);

            assertRows(execute("SELECT k, t FROM %s WHERE k = :i", i), expected);
        }
    }

    @Override
    public UntypedResultSet execute(String query, Object... values)
    {
        return super.executeFormattedQuery(formatQuery(KEYSPACE_PER_TEST, query), values);
    }

    @Override
    public String createTable(String query)
    {
        return super.createTable(KEYSPACE_PER_TEST, query);
    }

    @Override
    public ColumnFamilyStore getCurrentColumnFamilyStore()
    {
        return super.getCurrentColumnFamilyStore(KEYSPACE_PER_TEST);
    }
}