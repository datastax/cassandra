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
package org.apache.cassandra.index.sai.virtual;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.SAIUtil;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the virtual table exposing SSTable index segment metadata.
 */
@RunWith(Parameterized.class)
public class SegmentsSystemViewTest extends SAITester
{
    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Version.ALL.stream().map(v -> new Object[]{ v }).collect(Collectors.toList());
    }

    @Before
    public void setupVersion()
    {
        SAIUtil.setCurrentVersion(version);
    }

    private static final String SELECT = String.format("SELECT %s, %s, %s, %s " +
                                                       "FROM %s.%s WHERE %s = '%s' AND %s = ?",
                                                       SegmentsSystemView.SEGMENT_ROW_ID_OFFSET,
                                                       SegmentsSystemView.CELL_COUNT,
                                                       SegmentsSystemView.MIN_SSTABLE_ROW_ID,
                                                       SegmentsSystemView.MAX_SSTABLE_ROW_ID,
                                                       SchemaConstants.VIRTUAL_VIEWS,
                                                       SegmentsSystemView.NAME,
                                                       SegmentsSystemView.KEYSPACE_NAME,
                                                       KEYSPACE,
                                                       SegmentsSystemView.INDEX_NAME);


    private static final String SELECT_INDEX_METADATA = String.format("SELECT %s, %s, %s " +
                                                                      "FROM %s.%s WHERE %s = '%s'",
                                                                      SegmentsSystemView.COMPONENT_METADATA,
                                                                      SegmentsSystemView.MIN_TERM,
                                                                      SegmentsSystemView.MAX_TERM,
                                                                      SchemaConstants.VIRTUAL_VIEWS,
                                                                      SegmentsSystemView.NAME,
                                                                      SegmentsSystemView.KEYSPACE_NAME,
                                                                      KEYSPACE);

    @BeforeClass
    public static void setup() throws Exception
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(SchemaConstants.VIRTUAL_VIEWS, ImmutableList.of(new SegmentsSystemView(SchemaConstants.VIRTUAL_VIEWS))));

        requireNetwork();
    }

    @Test
    public void testSegmentsMetadata() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v1 int, v2 text, PRIMARY KEY (k, c))");
        String numericIndex = createIndex("CREATE CUSTOM INDEX ON %s(v1) USING 'StorageAttachedIndex' WITH OPTIONS = {'enable_segment_compaction':true}");
        String stringIndex = createIndex("CREATE CUSTOM INDEX ON %s(v2) USING 'StorageAttachedIndex' WITH OPTIONS = {'enable_segment_compaction':true}");

        int num = 100;

        String insert = "INSERT INTO %s(k, c, v1, v2) VALUES (?, ?, ?, ?)";

        // the virtual table should be empty before adding contents
        assertEmpty(execute(SELECT, numericIndex));
        assertEmpty(execute(SELECT, stringIndex));

        // insert rows and verify that the virtual table is empty before flushing
        for (int i = 0; i < num / 2; i++)
            execute(insert, i, 10, 100, "1000");
        assertEmpty(execute(SELECT, numericIndex));
        assertEmpty(execute(SELECT, stringIndex));

        // flush the memtable and verify the new record in the virtual table
        flush();
        Object[] row1 = row(0L, (long)(num / 2), 0L, (long)(num / 2 - 1));
        assertRows(execute(SELECT, numericIndex), row1);
        assertRows(execute(SELECT, stringIndex), row1);

        // flush a second memtable and verify both the old and the new record in the virtual table
        for (int i = num / 2; i < num; i++)
            execute(insert, i, 20, 200, "2000");
        flush();
        Object[] row2 = row(0L, (long)(num / 2), 0L, (long)(num / 2 - 1));
        assertRows(execute(SELECT, numericIndex), row1, row2);
        assertRows(execute(SELECT, stringIndex), row1, row2);

        // force compaction, there is only 1 sstable
        compact();
        waitForCompactions();
        Object[] row3 = row(0L, (long)num, 0L, (long)(num - 1));
        assertRows(execute(SELECT, numericIndex), row3);
        assertRows(execute(SELECT, stringIndex), row3);

        for (int lastValidSegmentRowId : Arrays.asList(0, 1, 2, 3, 5, 9, 25, 49, 59, 99, 101))
        {
            SegmentBuilder.updateLastValidSegmentRowId(lastValidSegmentRowId);

            // compaction to rewrite segments
            StorageService.instance.upgradeSSTables(KEYSPACE, false, new String[] { currentTable() });
            // segment compaction is now disabled
            int segmentCount = (int) Math.ceil(num * 1.0 / (lastValidSegmentRowId + 1));
            assertRowCount(execute(SELECT, numericIndex), segmentCount);
            assertRowCount(execute(SELECT, stringIndex), segmentCount);

            // verify index metadata length
            Map<String, Long> indexLengths = new HashMap<>();
            for (UntypedResultSet.Row row : execute(SELECT_INDEX_METADATA))
            {
                int minTerm = Integer.parseInt(row.getString(SegmentsSystemView.MIN_TERM));
                int maxTerm = Integer.parseInt(row.getString(SegmentsSystemView.MAX_TERM));

                assertTrue(minTerm >= 100);
                assertTrue(maxTerm <= 2000);

                Map<String, Map<String, String>> indexMetadatas = row.getMap(SegmentsSystemView.COMPONENT_METADATA,
                                                                             UTF8Type.instance,
                                                                             MapType.getInstance(UTF8Type.instance, UTF8Type.instance, true));

                for (Map.Entry<String, Map<String, String>> entry : indexMetadatas.entrySet())
                {
                    final String indexType = entry.getKey();
                    final String str = entry.getValue().getOrDefault(SegmentMetadata.ComponentMetadata.LENGTH, "0");

                    if (indexType.equals(IndexComponentType.KD_TREE.toString()))
                    {
                        int maxPointsInLeafNode = Integer.parseInt(entry.getValue().get("max_points_in_leaf_node"));

                        assertEquals(1024, maxPointsInLeafNode);
                    }
                    else if (indexType.equals(IndexComponentType.KD_TREE_POSTING_LISTS.toString()))
                    {
                        int numLeafPostings = Integer.parseInt(entry.getValue().get("num_leaf_postings"));

                        assertTrue(numLeafPostings > 0);
                    }

                    final long length = Long.parseLong(str);

                    final long value = indexLengths.getOrDefault(indexType, 0L);
                    indexLengths.put(indexType, value + length);
                }
            }
            if (!Boolean.parseBoolean(System.getProperty("cassandra.test.encryption", "false")))
                assertEquals(indexFileLengths(currentTable()), indexLengths);
        }

        // drop the numeric index and verify that there are not entries for it in the table
        dropIndex("DROP INDEX %s." + numericIndex);
        assertEmpty(execute(SELECT, numericIndex));
        assertNotEquals(0, execute(SELECT, stringIndex).size());

        // drop the string index and verify that there are not entries for it in the table
        dropIndex("DROP INDEX %s." +  stringIndex);
        assertEmpty(execute(SELECT, numericIndex));
        assertEmpty(execute(SELECT, stringIndex));
    }

    private HashMap<String, Long> indexFileLengths(String table) throws Exception
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        HashMap<String, Long> lengths = new HashMap<>();
        for (Index idx : cfs.indexManager.listIndexes())
        {
            StorageAttachedIndex index = (StorageAttachedIndex) idx;

            for (SSTableIndex sstableIndex : index.getIndexContext().getView().getIndexes())
            {
                SSTableReader sstable = sstableIndex.getSSTable();

                IndexDescriptor indexDescriptor = loadDescriptor(sstable, cfs);

                if (TypeUtil.isLiteral(sstableIndex.getIndexContext().getValidator()))
                {
                    addComponentSizeToMap(lengths, IndexComponentType.TERMS_DATA, index.getIndexContext(), indexDescriptor);
                    addComponentSizeToMap(lengths, IndexComponentType.POSTING_LISTS, index.getIndexContext(), indexDescriptor);
                    if (version.onOrAfter(Version.BM25_EARLIEST))
                    {
                        addComponentSizeToMap(lengths, IndexComponentType.DOC_LENGTHS, index.getIndexContext(), indexDescriptor);
                        // Version EC does not count the length of the segment header in the DOC_LENGTHS file, so
                        // we do a special adjustment here
                        if (version.equals(Version.EC))
                        {
                            var error = sstableIndex.getSegments().size() * SAICodecUtils.headerSize();
                            lengths.computeIfPresent(IndexComponentType.DOC_LENGTHS.name(), (typeName, acc) -> acc - error);
                        }
                    }
                }
                else
                {
                    addComponentSizeToMap(lengths, IndexComponentType.KD_TREE, index.getIndexContext(), indexDescriptor);
                    addComponentSizeToMap(lengths, IndexComponentType.KD_TREE_POSTING_LISTS, index.getIndexContext(), indexDescriptor);
                }
            }
        }

        return lengths;
    }

    private void addComponentSizeToMap(HashMap<String, Long> map, IndexComponentType key, IndexContext indexContext, IndexDescriptor indexDescriptor)
    {
        map.compute(key.name(), (typeName, acc) -> {
            final long size = indexDescriptor.perIndexComponents(indexContext).get(key).file().length();
            return acc == null ? size : size + acc;
        });
    }
}
