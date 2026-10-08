/*
 * Copyright IBM Corp.
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

package org.apache.cassandra.cql3.validation.operations;

import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;

/**
 * Tests reads that select only a subset of the static columns of a table on which the row cache caches only
 * a limited number of rows per partition.
 * <p>
 * When the row cache does not hold the full partition, the rows that are not cached are served from a full
 * partition read whose iterator advertises all the table columns. The result of the query is the concatenation
 * of the cached rows and of those extra rows, and it used to advertise all the static columns of the table even
 * though the column filter of the query only fetched some of them, which made the serialization of the read
 * response fail with an {@code IllegalStateException} ("... is not a subset of ...").
 * <p>
 * The queries are executed through the native protocol so that they go through {@code StorageProxy} and the
 * read response serialization, which the internal execution path skips.
 */
public class SelectWithPartialRowCacheTest extends CQLTester
{
    private static final UUID ENTITY = UUID.fromString("7b44ee6b-ba13-4be4-9ade-46776304494b");
    private static final UUID ITEM_1 = UUID.fromString("68ffcfc1-e426-436f-bfb2-d60e4c89f975");
    private static final UUID ITEM_2 = UUID.fromString("babb7d3c-4bdd-4e8e-af82-001228b7fdfe");
    private static final UUID GROUP = UUID.fromString("9ac7702c-75f2-46a6-b123-39cd4c5a5dbd");

    private static final String SCHEMA = "CREATE TABLE %s (" +
                                         "entity_id uuid, item_id uuid, " +
                                         "label text static, region text static, created_at timestamp static, " +
                                         "finished_at timestamp static, weight double static, last_note text static, " +
                                         "assignee text static, paused_at timestamp static, mode text static, " +
                                         "group_id uuid static, item_count int static, started_at timestamp static, " +
                                         "status text static, tags set<text> static, " +
                                         "host text, range_end varint, retries int, item_finished_at timestamp, " +
                                         "item_started_at timestamp, item_status int, range_start varint, ranges text, " +
                                         "PRIMARY KEY (entity_id, item_id))";

    private static final String PARTIAL_STATICS_QUERY = "SELECT entity_id, group_id, item_id, item_status " +
                                                        "FROM %s WHERE entity_id = " + ENTITY + " AND item_status = 1 ALLOW FILTERING";

    private static final String NO_STATICS_QUERY = "SELECT entity_id, item_id, item_status " +
                                                   "FROM %s WHERE entity_id = " + ENTITY + " AND item_status = 1 ALLOW FILTERING";

    private static final String ALL_STATICS_QUERY = "SELECT entity_id, item_id, item_status, group_id, label, region, status " +
                                                    "FROM %s WHERE entity_id = " + ENTITY + " AND item_status = 1 ALLOW FILTERING";

    private void insert()
    {
        execute("INSERT INTO %s (entity_id, item_id, label, region, group_id, status, item_status) " +
                "VALUES (?, ?, 'first batch', 'region-a', ?, 'ACTIVE', 1)", ENTITY, ITEM_1, GROUP);
        execute("INSERT INTO %s (entity_id, item_id, item_status) VALUES (?, ?, 2)", ENTITY, ITEM_2);
    }

    private void check(String label)
    {
        checkQuery(label + " (partial statics)", PARTIAL_STATICS_QUERY, row(ENTITY, GROUP, ITEM_1, 1));
        checkQuery(label + " (no statics)", NO_STATICS_QUERY, row(ENTITY, ITEM_1, 1));
        checkQuery(label + " (all selected statics)", ALL_STATICS_QUERY,
                   row(ENTITY, ITEM_1, 1, GROUP, "first batch", "region-a", "ACTIVE"));
    }

    private void checkQuery(String label, String query, Object[] expected)
    {
        assertRowsNet(executeNet(query), expected);

        SimpleStatement paged = new SimpleStatement(formatQuery(query));
        paged.setFetchSize(1);
        List<Row> rows = executeNet(paged).all();
        assertEquals(label + " paged", 1, rows.size());

        PreparedStatement prepared = sessionNet().prepare(formatQuery(query));
        assertRowsNet(executeNet(prepared.bind()), expected);
    }

    @Test
    public void testWithoutRowCache()
    {
        createTable(SCHEMA);
        insert();
        check("memtable");
        flush();
        check("sstable");
        insert();
        check("memtable and sstable");
    }

    @Test
    public void testWithFullRowCache()
    {
        createTable(SCHEMA + " WITH caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}");
        insert();
        check("memtable");
        flush();
        check("sstable, cache miss");
        check("sstable, cache hit");
    }

    @Test
    public void testWithPartialRowCache()
    {
        createTable(SCHEMA + " WITH caching = {'keys': 'ALL', 'rows_per_partition': '1'}");
        insert();
        check("memtable");
        flush();
        check("sstable, cache miss");
        check("sstable, cache hit");
    }

    @Test
    public void testWithPartialRowCacheLargerThanPartition()
    {
        createTable(SCHEMA + " WITH caching = {'keys': 'ALL', 'rows_per_partition': '10'}");
        insert();
        check("memtable");
        flush();
        check("sstable, cache miss");
        check("sstable, cache hit");
    }
}
