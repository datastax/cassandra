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

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import static org.apache.cassandra.index.sai.cql.AbstractQueryTester.INDEX_QUERY_COUNTER;
import static org.apache.cassandra.index.sai.disk.v2.blockindex.BytesUtil.gatherBytes;
import static org.junit.Assert.assertEquals;

public class SimpleQueryTest extends SAITester
{
    @Test
    public void test() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, value int)");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (id, value) VALUES(?, ?)", 1, 1);
        flush();
//        execute("INSERT INTO %s (id, value) VALUES(?, ?)", 10, "10");
//        flush();
//        compact();
        assertEquals(1, execute("SELECT * FROM %s WHERE value = 1").size());
    }

    @Test
    public void literalTest() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, value text)");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (id, value) VALUES(?, ?)", 1, "1");
        flush();
        execute("INSERT INTO %s (id, value) VALUES(?, ?)", 10, "10");
        flush();
        compact();
        assertEquals(1, execute("SELECT * FROM %s WHERE value = '1'").size());
    }

    @Test
    public void stringTest() throws Throwable
    {
        String string = "1";
        ByteBuffer term = UTF8Type.instance.decompose(string);

        final ByteComparable byteComparable = v -> UTF8Type.instance.asComparableBytes(term.duplicate(), v);

        BytesRefBuilder builder = new BytesRefBuilder();


//        byte[] bytes = ByteSourceInverse.readBytes(byteSource);
//
//        gatherBytes(byteSource, builder);

        BytesRef bytesRef = builder.toBytesRef();

        System.out.println();
    }

    @Test
    public void intTest() throws Throwable
    {
        int value = 10;
        ByteBuffer term = Int32Type.instance.decompose(value);

        final ByteSource byteSource = Int32Type.instance.asComparableBytes(term.duplicate(), ByteComparable.Version.OSS41);

        BytesRefBuilder builder = new BytesRefBuilder();


        byte[] bytes = ByteSourceInverse.readBytes(byteSource);

        gatherBytes(byteSource, builder);

        BytesRef bytesRef = builder.toBytesRef();

        System.out.println();

    }

    @Test
    public void rangeTest() throws Throwable
    {
        DataModel dataModel = new DataModel.BaseDataModel(DataModel.NORMAL_COLUMNS, DataModel.NORMAL_COLUMN_DATA);
        DataModel.Executor executor = new SingleNodeExecutor(this, INDEX_QUERY_COUNTER);
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", DataModel.KEYSPACE));
        dataModel.createTables(executor);
        dataModel.disableCompaction(executor);
        dataModel.createIndexes(executor);
        dataModel.insertRows(executor);
        dataModel.flush(executor);

//        UntypedResultSet result = execute("SELECT abbreviation FROM " + DataModel.KEYSPACE + "." + dataModel.indexedTable() + " WHERE murders_per_year <= 126 AND tiny_murders_per_year <= 9");
//
//        System.out.println(result);

        List<Object> result2 = dataModel.executeIndexed(executor, "SELECT abbreviation FROM " +
                                                                  DataModel.KEYSPACE + "." +
                                                                  dataModel.indexedTable() +
                                                                  " WHERE murders_per_year <= ? AND tiny_murders_per_year <= ?",
                                                        4 ,
                                                        (short)126,
                                                        (byte)9);

        System.out.println(result2);
    }
}
