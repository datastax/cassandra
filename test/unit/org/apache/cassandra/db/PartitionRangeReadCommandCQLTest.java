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
package org.apache.cassandra.db;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.assertj.core.api.Assertions;

public class PartitionRangeReadCommandCQLTest extends CQLTester
{
    @Test
    public void testToCQLString()
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");

        testToCQLString("SELECT * FROM %s", "SELECT * FROM %s");

        testToCQLString("SELECT * FROM %s WHERE c = 0 ALLOW FILTERING", "SELECT * FROM %s WHERE (c) = (0)");
        testToCQLString("SELECT * FROM %s WHERE (c) = (0) ALLOW FILTERING", "SELECT * FROM %s WHERE (c) = (0)");
        testToCQLString("SELECT * FROM %s WHERE c > 0 ALLOW FILTERING", "SELECT * FROM %s WHERE c > 0");
        testToCQLString("SELECT * FROM %s WHERE c < 0 ALLOW FILTERING", "SELECT * FROM %s WHERE c < 0");
        testToCQLString("SELECT * FROM %s WHERE c >= 0 ALLOW FILTERING", "SELECT * FROM %s WHERE c >= 0");
        testToCQLString("SELECT * FROM %s WHERE c <= 0 ALLOW FILTERING", "SELECT * FROM %s WHERE c <= 0");

        testToCQLString("SELECT * FROM %s WHERE v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE v = 1");
        testToCQLString("SELECT * FROM %s WHERE c = 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE v = 1 AND (c) = (0)");
        testToCQLString("SELECT * FROM %s WHERE c > 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE v = 1 AND c > 0");
        testToCQLString("SELECT * FROM %s WHERE c < 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE v = 1 AND c < 0");
        testToCQLString("SELECT * FROM %s WHERE c >= 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE v = 1 AND c >= 0");
        testToCQLString("SELECT * FROM %s WHERE c <= 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE v = 1 AND c <= 0");
    }

    private void testToCQLString(String query, String expected)
    {
        ReadCommand command = parseReadCommand(query);
        Assertions.assertThat(command.toCQLString())
                  .isEqualTo(formatQuery(expected));
    }
}

