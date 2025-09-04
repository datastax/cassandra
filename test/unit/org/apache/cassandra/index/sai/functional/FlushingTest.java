/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.sai.functional;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.kdtree.NumericIndexWriter;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class FlushingTest extends SAITester
{
    @Test
    public void testFlushingLargeStaleMemtableIndex() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        // BDKWriter#valueCount is updated when leaf values are written at BKDWriter#writeLeakBlock on every
        // BKDWriter#DEFAULT_MAX_POINTS_IN_LEAF_NODE (1024) number of points, see LUCENE-8765
        int overwrites = NumericIndexWriter.MAX_POINTS_IN_LEAF_NODE + 1;
        for (int j = 0; j < overwrites; j++)
        {
            execute("INSERT INTO %s (id1, v1) VALUES ('1', ?)", j);
        }

        flush();

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(1, rows.all().size());

        assertIndexFilesInToc(indexFiles());
    }

    @Test
    public void testFlushingOverwriteDelete() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        IndexContext numericIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")), Int32Type.instance);

        int sstables = 3;
        for (int j = 0; j < sstables; j++)
        {
            execute("INSERT INTO %s (id1, v1) VALUES (?, 1)", Integer.toString(j));
            execute("DELETE FROM %s WHERE id1 = ?", Integer.toString(j));
            flush();
        }

        assertIndexFilesInToc(indexFiles());

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1 >= 0");
        assertEquals(0, rows.all().size());
        verifyIndexFiles(numericIndexContext, null, sstables, 0, 0, sstables, 0);
        verifySSTableIndexes(numericIndexContext.getIndexName(), sstables, sstables);

        compact();
        waitForAssert(() -> verifyIndexFiles(numericIndexContext, null, 1, 0, 0, 1, 0));

        rows = executeNet("SELECT id1 FROM %s WHERE v1 >= 0");
        assertEquals(0, rows.all().size());
        verifySSTableIndexes(numericIndexContext.getIndexName(), 1, 1);

        assertIndexFilesInToc(indexFiles());
    }

    @Test
    public void testMemtableIndexFlushFailure() throws Throwable
    {
        Injection failMemtableComplete = Injections.newCustom("FailMemtableIndexWriterComplete")
                                                   .add(InvokePointBuilder.newInvokePoint()
                                                                          .onClass("org.apache.cassandra.index.sai.disk.v1.MemtableIndexWriter")
                                                                          .onMethod("complete", "com.google.common.base.Stopwatch")
                                                   )
                                                   .add(ActionBuilder.newActionBuilder().actions()
                                                                     .doThrow(java.io.IOException.class, Expression.quote("Byteman-injected fault in MemtableIndexWriter.complete"))
                                                   )
                                                   .build();
        Injections.inject(failMemtableComplete);

        createTable(CREATE_TABLE_TEMPLATE);
        String indexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        String pkValue = "key_bm_flush_fail";
        int indexedValue = 456;

        execute("INSERT INTO %s (id1, v1) VALUES (?, ?)", pkValue, indexedValue);

        long oldCommitLogSize = CommitLog.instance.getActiveContentSize();

        // The Byteman rule will cause the SAI part of the flush to fail
        assertThrows(RuntimeException.class, this::flush);

        // Assert that the index is still queryable for the inserted data despite the injected fault.
        assertEquals(0, getNotQueryableIndexes().size());
        ResultSet indexQueryResults = executeNet("SELECT id1 FROM %s WHERE v1 = ?", indexedValue);
        assertEquals("The index should be still usable despite flush failure", 1, indexQueryResults.all().size());

        // Assert that the table is still queryable by primary key.
        ResultSet pkQueryResults = executeNet("SELECT v1 FROM %s WHERE id1 = ?", pkValue);
        assertEquals("The table should still be queryable by primary key", 1, pkQueryResults.all().size());

        // Make sure no sstable has been created:
        assertEquals(0, getCurrentColumnFamilyStore().getLiveSSTables().size());
        verifySSTableIndexes(indexName, 0, 0);
        assertEquals(oldCommitLogSize, CommitLog.instance.getActiveContentSize());
    }
}
