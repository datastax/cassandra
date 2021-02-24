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

import java.util.List;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

public class SimpleQueryTest extends SAITester
{
    @Test
    public void skinnyTest() throws Throwable
    {
        createTable("CREATE TABLE %s(id int, value text, primary key(id))");
        createIndex("CREATE CUSTOM INDEX ON %s(value) using 'StorageAttachedIndex'");
        waitForIndexQueryable();
        execute("INSERT INTO %s (id, value) VALUES (1, '1')");
        execute("INSERT INTO %s (id, value) VALUES (2, '2')");
        execute("INSERT INTO %s (id, value) VALUES (3, '2')");
        flush();
        execute("INSERT INTO %s (id, value) VALUES (1, '1')");
        execute("INSERT INTO %s (id, value) VALUES (2, '2')");
        execute("INSERT INTO %s (id, value) VALUES (3, '2')");
//        assertRows(execute("SELECT * FROM %s WHERE value = '2' AND id = 2"), row(2, "2"), row(3, "2"));
        assertRows(execute("SELECT * FROM %s WHERE value = '2' AND id = 2"), row(2, "2"));
    }

    @Test
    public void wideTest() throws Throwable
    {
        createTable("CREATE TABLE %s(pk int, ck int, value int, primary key(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(value) using 'StorageAttachedIndex'");
        waitForIndexQueryable();
        execute("INSERT INTO %s (pk, ck, value) VALUES (1, 1, 1)");
        execute("INSERT INTO %s (pk, ck, value) VALUES (1, 2, 2)");
        execute("INSERT INTO %s (pk, ck, value) VALUES (2, 1, 2)");
        flush();
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE value = 2"), row(1, 2, 2), row(2, 1, 2));
    }

    @Test
    public void lessThanTest() throws Throwable
    {
        requireNetwork();
        DataModel model = new DataModel.BaseDataModel(DataModel.NORMAL_COLUMNS, DataModel.NORMAL_COLUMN_DATA);

        model.createTables(this);
        model.disableCompaction(this);
        model.createIndexes(this);
        model.insertRows(this);
        model.flush(this);

        Object[] values = new Object[] { 5000000000L, 10};
        ResultSet resultSet = model.executeIndexed(this, "SELECT abbreviation FROM %s WHERE gdp < ? LIMIT ?", 5, values);

        System.out.println(resultSet.all().size());
//        UntypedResultSet resultSet = model.executeLocalIndexed(this, "SELECT abbreviation FROM %s WHERE gdp < ? LIMIT ?", 5000000000L, 10);
//
//        List<String> rows = makeRowStrings(resultSet);
//
//        rows.forEach(System.out::println);


    }

    @Test
    public void compoundAndStaticsTest() throws Throwable
    {
        requireNetwork();

        DataModel model = new DataModel.CompoundKeyWithStaticsDataModel(DataModel.STATIC_COLUMNS, DataModel.STATIC_COLUMN_DATA);

        model.createTables(this);
        model.disableCompaction(this);
        model.createIndexes(this);
        model.insertRows(this);
        model.flush(this);

        Object[] values = new Object[] { 1845, 10};
        ResultSet resultSet = model.executeIndexed(this, "SELECT abbreviation FROM %s WHERE entered < ? LIMIT ?", 5, values);

        System.out.println(resultSet.all().size());

    }
}
