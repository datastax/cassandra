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

package org.apache.cassandra.index;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.schema.SchemaConstants;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class IndexNameTest extends CQLTester
{
    @Parameterized.Parameter()
    public String createIndexQuery;

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters()
    {
        return List.of(
        new Object[]{ "CREATE INDEX %s ON %s(%s)" },
        new Object[]{ "CREATE CUSTOM INDEX %s ON %s(%s) USING 'StorageAttachedIndex'" }
        );
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
    public void testQuotedAndLongColumnNames() throws Throwable
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
                        + "very_very_very_very_very_very_very_very_very \"",
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"
        };

        createTable("CREATE TABLE %s (key int,"
                    + intoColumnDefs(columnNames)
                    + "PRIMARY KEY (key))");
        for (String columnName : columnNames)
            createIndex(String.format(createIndexQuery, "", "%s", columnName));

        for (int i = 0; i < columnNames.length; i++)
            execute("INSERT INTO %s (key, " + columnNames[i] + ") VALUES (" + i + ", " + i + ')');

        beforeAndAfterFlush(() -> {
            for (int i = 0; i < columnNames.length; i++)
                assertRows(execute("SELECT key, " + columnNames[i] +
                                   " FROM %s WHERE " + columnNames[i] + " = " + i),
                           row(i, i));
        });
    }

    @Test
    public void testAllLongNames()
    {
        String longName = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s with replication = " +
                              "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
                              longName));
        execute(String.format("DROP TABLE IF EXISTS %s.%<s", longName));
        createTable(String.format("CREATE TABLE %s.%<s (" +
                                  "key int PRIMARY KEY," +
                                  "%<s int)",
                                  longName));
        createIndex(String.format(createIndexQuery, "", longName + '.' + longName, longName));
        execute(String.format("INSERT INTO %s.%<s (\"key\", %<s) VALUES (1, 1)", longName));
        execute(String.format("INSERT INTO %s.%<s (\"key\", %<s) VALUES (2, 2)", longName));

        assertRows(execute(String.format("SELECT key, %s FROM %<s.%<s WHERE %<s = 1", longName)), row(1, 1));

        flush(longName, longName);

        assertRows(execute(String.format("SELECT key, %s FROM %<s.%<s WHERE %<s = 1", longName)), row(1, 1));
    }

    @Test
    public void failOnBadCharIndexName()
    {
        String columnName = "value";
        createTable(String.format("CREATE TABLE %%s (key int PRIMARY KEY, %s int)", columnName));
        assertThatThrownBy(() -> execute(String.format(createIndexQuery, "\"unacceptable index name\"", "%s", columnName)))
        .isInstanceOf(ConfigurationException.class);
    }

    @Test
    public void testTooLongNamesInternal() throws Throwable
    {
        String longName = "a".repeat(183);

        createTable("CREATE TABLE %s (" +
                    "key int PRIMARY KEY," +
                    "value int)"
        );
        createIndex(String.format(createIndexQuery, longName, "%s", "value"));
        execute(String.format("INSERT INTO %%s (\"key\", %s) VALUES (1, 1)", "value"));
        execute(String.format("INSERT INTO %%s (\"key\", %s) VALUES (2, 2)", "value"));

        beforeAndAfterFlush(() -> assertRows(execute(String.format("SELECT key, %s FROM %%s WHERE %<s = 1", "value")), row(1, 1)));
    }

    @Test
    public void testMaxAcceptableLongNamesNewIndex() throws Throwable
    {
        assertEquals(182, Version.calculateIndexNameAllowedLength(KEYSPACE));
        String longName = "a".repeat(182);
        createTable("CREATE TABLE %s (" +
                    "key int PRIMARY KEY," +
                    "value int)"
        );
        executeNet(String.format(createIndexQuery, longName, "%s", "value"));

        execute(String.format("INSERT INTO %%s (\"key\", %s) VALUES (1, 1)", "value"));
        execute(String.format("INSERT INTO %%s (\"key\", %s) VALUES (2, 2)", "value"));

        beforeAndAfterFlush(() -> assertRows(execute(String.format("SELECT key, %s FROM %%s WHERE %<s = 1", "value")), row(1, 1)));
    }

    @Test
    public void failTooLongNamesNewIndex()
    {
        String longName = "a".repeat(183);
        createTable("CREATE TABLE %s (" +
                    "key int PRIMARY KEY," +
                    "value int)"
        );
        assertThatThrownBy(() -> executeNet(String.format(createIndexQuery, longName, "%s", "value")))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessage(String.format("Index name shouldn't be more than %s characters long (got %s chars for %s)",
                                  SchemaConstants.INDEX_NAME_LENGTH, longName.length(), longName));
    }
}
