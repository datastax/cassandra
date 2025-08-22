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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.assertj.core.api.Assertions;

public class PartitionRangeReadCommandCQLTest extends ReadCommandCQLTester<PartitionRangeReadCommand>
{
    @Test
    public void testToCQLString()
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");

        assertToCQLString("SELECT * FROM %s", "SELECT * FROM %s");

        assertToCQLString("SELECT * FROM %s WHERE c = 0 ALLOW FILTERING", "SELECT * FROM %s WHERE c = 0");
        assertToCQLString("SELECT * FROM %s WHERE (c) = (0) ALLOW FILTERING", "SELECT * FROM %s WHERE c = 0");
        assertToCQLString("SELECT * FROM %s WHERE c > 0 ALLOW FILTERING", "SELECT * FROM %s WHERE c > 0");
        assertToCQLString("SELECT * FROM %s WHERE c < 0 ALLOW FILTERING", "SELECT * FROM %s WHERE c < 0");
        assertToCQLString("SELECT * FROM %s WHERE c >= 0 ALLOW FILTERING", "SELECT * FROM %s WHERE c >= 0");
        assertToCQLString("SELECT * FROM %s WHERE c <= 0 ALLOW FILTERING", "SELECT * FROM %s WHERE c <= 0");

        assertToCQLString("SELECT * FROM %s WHERE v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE v = 1");
        assertToCQLString("SELECT * FROM %s WHERE c = 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE v = 1 AND c = 0");
        assertToCQLString("SELECT * FROM %s WHERE c > 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE v = 1 AND c > 0");
        assertToCQLString("SELECT * FROM %s WHERE c < 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE v = 1 AND c < 0");
        assertToCQLString("SELECT * FROM %s WHERE c >= 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE v = 1 AND c >= 0");
        assertToCQLString("SELECT * FROM %s WHERE c <= 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE v = 1 AND c <= 0");

        // test with token restrictions
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        String token = partitioner.getToken(Int32Type.instance.decompose(0)).toString();
        assertToCQLString("SELECT * FROM %s WHERE token(k) > token(0)",
                          "SELECT * FROM %s WHERE token(k) > " + token);
        assertToCQLString("SELECT * FROM %s WHERE token(k) >= token(0)",
                          "SELECT * FROM %s WHERE token(k) >= " + token);
        assertToCQLString("SELECT * FROM %s WHERE token(k) >= token(0) AND token(k) <= token(0)",
                          "SELECT * FROM %s WHERE token(k) >= " + token + " AND token(k) <= " + token);

        // test with a secondary index (indexed queries are always mapped to range commands)
        createIndex("CREATE INDEX ON %s(v)");
        assertToCQLString("SELECT * FROM %s WHERE v = 0", "SELECT * FROM %s WHERE v = 0");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0", "SELECT * FROM %s WHERE v = 0 AND k = 0");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0 AND c = 0", "SELECT * FROM %s WHERE v = 0 AND k = 0 AND c = 0");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0 AND c > 0", "SELECT * FROM %s WHERE v = 0 AND k = 0 AND c > 0");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0 AND c < 0", "SELECT * FROM %s WHERE v = 0 AND k = 0 AND c < 0");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0 AND c >= 0", "SELECT * FROM %s WHERE v = 0 AND k = 0 AND c >= 0");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0 AND c <= 0", "SELECT * FROM %s WHERE v = 0 AND k = 0 AND c <= 0");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0 AND c IN (0)", "SELECT * FROM %s WHERE v = 0 AND k = 0 AND c = 0");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0 AND c IN (0, 1)", "SELECT * FROM %s WHERE v = 0 AND k = 0 AND c IN (0, 1)");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND token(k) > token(0)",
                          "SELECT * FROM %s WHERE v = 0 AND token(k) > " + token);
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND token(k) >= token(0)",
                          "SELECT * FROM %s WHERE v = 0 AND token(k) >= " + token);
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND token(k) >= token(0) AND token(k) <= token(0)",
                          "SELECT * FROM %s WHERE v = 0 AND token(k) >= " + token + " AND token(k) <= " + token);

        // test with index and multi-column clustering
        createTable("CREATE TABLE %s (k int, c1 int, c2 int,v int, PRIMARY KEY (k, c1, c2))");
        createIndex("CREATE INDEX ON %s(v)");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0", "SELECT * FROM %s WHERE v = 0 AND k = 0");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 = 1", "SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 = 1");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 > 1", "SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 > 1");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 < 1", "SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 < 1");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 >= 1", "SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 >= 1");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 <= 1", "SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 <= 1");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 = 1 AND c2 = 2", "SELECT * FROM %s WHERE v = 0 AND k = 0 AND (c1, c2) = (1, 2)");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 = 1 AND c2 > 2", "SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 = 1 AND c2 > 2");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 = 1 AND c2 < 2", "SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 = 1 AND c2 < 2");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 = 1 AND c2 >= 2", "SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 = 1 AND c2 >= 2");
        assertToCQLString("SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 = 1 AND c2 <= 2", "SELECT * FROM %s WHERE v = 0 AND k = 0 AND c1 = 1 AND c2 <= 2");
    }

    @Override
    protected PartitionRangeReadCommand parseCommand(String query)
    {
        ReadCommand command = parseReadCommand(query);
        Assertions.assertThat(command).isInstanceOf(PartitionRangeReadCommand.class);
        return (PartitionRangeReadCommand) command;
    }
}
