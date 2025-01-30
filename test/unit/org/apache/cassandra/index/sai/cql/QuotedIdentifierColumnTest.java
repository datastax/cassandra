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

import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

public class QuotedIdentifierColumnTest extends SAITester
{
    @Test
    public void testSimpleQuotedIdentifierColumn()
    {
        createTable("CREATE TABLE %s (\"key\" int PRIMARY KEY, \"value\" text)");
        createIndex("CREATE CUSTOM INDEX ON %s(\"value\") USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (\"key\", \"value\") VALUES (1, 'value1')");
        execute("INSERT INTO %s (\"key\", \"value\") VALUES (2, 'value2')");

        assertRows(execute("SELECT * FROM %s WHERE \"value\" = 'value1'"), row(1, "value1"));
        assertRows(execute("SELECT * FROM %s WHERE \"value\" = 'value2'"), row(2, "value2"));
    }

    private String intoColumnDefs(String[] columnNames)
    {
        StringBuilder sb = new StringBuilder();
        for (String columnName : columnNames)
        {
            sb.append(columnName).append(" int,");
        }
        return sb.toString();
    }

    @Test
    public void testQuotedIdentifierWithSpecialCharsAndLong()
    {
        String[] columnNames
        = new String[]{ "\"user name\"",
                        "\"userCountry\"",
                        "\"user-age\"",
                        "\"/user/age\"",
                        "\"userage\"",
                        "\"a very very very very very very very very long field\"",
                        "\"   a_very_very_very_very_very_very_very_very_"
                        + "very_very_very_very_very_very_very_very_very_"
                        + "very_very_very_very(very)very_very "
                        + "_very_very_very_very_very_very_very_"
                        + "very_very_very_very_very_very_very_very_"
                        + "very_very_very_very_very_very_very_very_very \""
        };

        createTable("CREATE TABLE %s (key int,"
                    + intoColumnDefs(columnNames)
                    + "PRIMARY KEY (key))");
        for (String columnName : columnNames)
            execute("CREATE CUSTOM INDEX ON %s (" + columnName + ") USING 'StorageAttachedIndex'");
        for (int i = 0; i < columnNames.length; i++)
            execute("INSERT INTO %s (key, " + columnNames[i] + ") VALUES (" + i + ", " + i + ')');
        for (int i = 0; i < columnNames.length; i++)
            assertRows(execute("SELECT key, " + columnNames[i] + " FROM %s WHERE " + columnNames[i] + " = " + i), row(i, i));

        flush();

        for (int i = 0; i < columnNames.length; i++)
            assertRows(execute("SELECT key, " + columnNames[i] + " FROM %s WHERE " + columnNames[i] + " = " + i), row(i, i));
    }
}
