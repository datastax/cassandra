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

        assertToCQLString("SELECT * FROM %s",
                          "SELECT * FROM %s ALLOW FILTERING");

        assertToCQLString("SELECT * FROM %s WHERE c = 0 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE c = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE (c) = (0) ALLOW FILTERING",
                          "SELECT * FROM %s WHERE c = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c > 0 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE c > 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c < 0 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE c < 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c >= 0 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE c >= 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c <= 0 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE c <= 0 ALLOW FILTERING");

        assertToCQLString("SELECT * FROM %s WHERE v = 1 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE v = 1 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c = 0 AND v = 1 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE c = 0 AND v = 1 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c > 0 AND v = 1 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE c > 0 AND v = 1 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c < 0 AND v = 1 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE c < 0 AND v = 1 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c >= 0 AND v = 1 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE c >= 0 AND v = 1 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c <= 0 AND v = 1 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE c <= 0 AND v = 1 ALLOW FILTERING");

        // test with token restrictions
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        String token = partitioner.getToken(Int32Type.instance.decompose(0)).toString();
        assertToCQLString("SELECT * FROM %s WHERE token(k) > token(0)",
                          "SELECT * FROM %s WHERE token(k) > " + token + " ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE token(k) >= token(0)",
                          "SELECT * FROM %s WHERE token(k) >= " + token + " ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE token(k) >= token(0) AND token(k) <= token(0)",
                          "SELECT * FROM %s WHERE token(k) >= " + token + " AND token(k) <= " + token + " ALLOW FILTERING");

        // test with a secondary index (indexed queries are always mapped to range commands)
        createIndex("CREATE INDEX ON %s(v)");
        assertToCQLString("SELECT * FROM %s WHERE v = 0",
                          "SELECT * FROM %s WHERE v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND  v = 0",
                          "SELECT * FROM %s WHERE k = 0 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c = 0 AND v = 0 ",
                          "SELECT * FROM %s WHERE k = 0 AND c = 0 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c > 0 AND v = 0 ",
                          "SELECT * FROM %s WHERE k = 0 AND c > 0 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c < 0 AND v = 0 ",
                          "SELECT * FROM %s WHERE k = 0 AND c < 0 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c >= 0 AND v = 0 ",
                          "SELECT * FROM %s WHERE k = 0 AND c >= 0 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c <= 0 AND v = 0",
                          "SELECT * FROM %s WHERE k = 0 AND c <= 0 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c IN (0) AND v = 0",
                          "SELECT * FROM %s WHERE k = 0 AND c = 0 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c IN (0, 1) AND v = 0",
                          "SELECT * FROM %s WHERE k = 0 AND c IN (0, 1) AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE token(k) > token(0) AND v = 0",
                          "SELECT * FROM %s WHERE token(k) > " + token + " AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE token(k) >= token(0) AND v = 0",
                          "SELECT * FROM %s WHERE token(k) >= " + token + " AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE token(k) >= token(0) AND token(k) <= token(0) AND v = 0",
                          "SELECT * FROM %s WHERE token(k) >= " + token + " AND token(k) <= " + token + " AND v = 0 ALLOW FILTERING");

        // test with multi-column partition key and index
        createTable("CREATE TABLE %s (k1 int, k2 int, c int, v int, PRIMARY KEY ((k1, k2), c))");
        createIndex("CREATE INDEX ON %s(v)");
        assertToCQLString("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND v = 1",
                          "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND v = 1 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c = 3 AND v = 1",
                          "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c = 3 AND v = 1 ALLOW FILTERING");

        // test with index and multi-column clustering
        createTable("CREATE TABLE %s (k int, c1 int, c2 int,v int, PRIMARY KEY (k, c1, c2))");
        createIndex("CREATE INDEX ON %s(v)");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND v = 0",
                          "SELECT * FROM %s WHERE k = 0 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c1 = 1 AND v = 0",
                          "SELECT * FROM %s WHERE k = 0 AND c1 = 1 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c1 > 1 AND v = 0",
                          "SELECT * FROM %s WHERE k = 0 AND c1 > 1 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c1 < 1 AND v = 0",
                          "SELECT * FROM %s WHERE k = 0 AND c1 < 1 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c1 >= 1 AND v = 0",
                          "SELECT * FROM %s WHERE k = 0 AND c1 >= 1 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c1 <= 1 AND v = 0",
                          "SELECT * FROM %s WHERE k = 0 AND c1 <= 1 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c1 = 1 AND c2 = 2 AND v = 0",
                          "SELECT * FROM %s WHERE k = 0 AND (c1, c2) = (1, 2) AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c1 = 1 AND c2 > 2 AND v = 0",
                          "SELECT * FROM %s WHERE k = 0 AND c1 = 1 AND c2 > 2 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c1 = 1 AND c2 < 2 AND v = 0",
                          "SELECT * FROM %s WHERE k = 0 AND c1 = 1 AND c2 < 2 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c1 = 1 AND c2 >= 2 AND v = 0",
                          "SELECT * FROM %s WHERE k = 0 AND c1 = 1 AND c2 >= 2 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c1 = 1 AND c2 <= 2 AND v = 0",
                          "SELECT * FROM %s WHERE k = 0 AND c1 = 1 AND c2 <= 2 AND v = 0 ALLOW FILTERING");

        // test generic index-based ORDER BY
        createTable("CREATE TABLE %s (k int, c int, n int, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
        assertToCQLString("SELECT * FROM %s ORDER BY n LIMIT 10",
                          "SELECT * FROM %s ORDER BY n ASC LIMIT 10 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s ORDER BY n DESC LIMIT 10",
                          "SELECT * FROM %s ORDER BY n DESC LIMIT 10 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 ORDER BY n LIMIT 10",
                          "SELECT * FROM %s WHERE k = 0 ORDER BY n ASC LIMIT 10 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c = 0 ORDER BY n LIMIT 10",
                          "SELECT * FROM %s WHERE k = 0 AND c = 0 ORDER BY n ASC LIMIT 10 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c IN (0, 1) ORDER BY n LIMIT 10",
                          "SELECT * FROM %s WHERE k = 0 AND c IN (0, 1) ORDER BY n ASC LIMIT 10 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE n = 0 ORDER BY n LIMIT 10",
                          "SELECT * FROM %s WHERE n = 0 ORDER BY n ASC LIMIT 10 ALLOW FILTERING");

        // test ANN index-based ORDER BY
        createTable("CREATE TABLE %s (k int, c int, n int, v vector<float, 2>, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        String truncationError = "no viable alternative at input '..'";
        assertToCQLString("SELECT * FROM %s ORDER BY v ANN OF [1, 2] LIMIT 10",
                          "SELECT * FROM %s ORDER BY v ANN OF [1.0, ... LIMIT 10 ALLOW FILTERING",
                          truncationError);
        assertToCQLString("SELECT * FROM %s WHERE k = 0 ORDER BY v ANN OF [1, 2] LIMIT 10",
                          "SELECT * FROM %s WHERE k = 0 ORDER BY v ANN OF [1.0, ... LIMIT 10 ALLOW FILTERING",
                          truncationError);
        assertToCQLString("SELECT * FROM %s WHERE n = 0 ORDER BY v ANN OF [1, 2] LIMIT 10",
                          "SELECT * FROM %s WHERE n = 0 ORDER BY v ANN OF [1.0, ... LIMIT 10 ALLOW FILTERING",
                          truncationError);

        // test literals
        createTable("CREATE TABLE %s (k text, c text, m map<text, text>, PRIMARY KEY (k, c))");
        assertToCQLString("SELECT m['key'] FROM %s WHERE m['key'] = 'value' ALLOW FILTERING",
                          "SELECT m FROM %s WHERE m['key'] = 'value' ALLOW FILTERING");
        assertToCQLString("SELECT m['key'] FROM %s WHERE m['key'] < 'value' ALLOW FILTERING",
                          "SELECT m FROM %s WHERE m['key'] < 'value' ALLOW FILTERING");
        assertToCQLString("SELECT m['key'] FROM %s WHERE m['key'] > 'value' ALLOW FILTERING",
                          "SELECT m FROM %s WHERE m['key'] > 'value' ALLOW FILTERING");
        assertToCQLString("SELECT m['key'] FROM %s WHERE m['key'] <= 'value' ALLOW FILTERING",
                          "SELECT m FROM %s WHERE m['key'] <= 'value' ALLOW FILTERING");
        assertToCQLString("SELECT m['key'] FROM %s WHERE m['key'] >= 'value' ALLOW FILTERING",
                          "SELECT m FROM %s WHERE m['key'] >= 'value' ALLOW FILTERING");

        // test with quoted identifiers
        createKeyspace("CREATE KEYSPACE \"K\" WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        createTable("CREATE TABLE \"K\".\"T\" (\"K\" int, \"C\" int, \"S\" int static, \"V\" int, PRIMARY KEY(\"K\", \"C\"))");
        assertToCQLString("SELECT * FROM \"K\".\"T\"",
                          "SELECT * FROM \"K\".\"T\" ALLOW FILTERING");
        assertToCQLString("SELECT \"K\" FROM \"K\".\"T\"",
                          "SELECT * FROM \"K\".\"T\" ALLOW FILTERING");
        assertToCQLString("SELECT \"S\" FROM \"K\".\"T\"",
                          "SELECT \"S\" FROM \"K\".\"T\" ALLOW FILTERING");
        assertToCQLString("SELECT \"V\" FROM \"K\".\"T\"",
                          "SELECT \"V\" FROM \"K\".\"T\" ALLOW FILTERING");
        assertToCQLString("SELECT \"K\", \"C\", \"S\", \"V\" FROM \"K\".\"T\"",
                          "SELECT \"S\", \"V\" FROM \"K\".\"T\" ALLOW FILTERING");
        assertToCQLString("SELECT * FROM \"K\".\"T\" WHERE \"V\" = 0 ALLOW FILTERING",
                          "SELECT * FROM \"K\".\"T\" WHERE \"V\" = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM \"K\".\"T\" WHERE \"S\" = 0 ALLOW FILTERING",
                          "SELECT * FROM \"K\".\"T\" WHERE \"S\" = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM \"K\".\"T\" WHERE \"C\" = 0 ALLOW FILTERING",
                          "SELECT * FROM \"K\".\"T\" WHERE \"C\" = 0 ALLOW FILTERING");
    }

    @Override
    protected PartitionRangeReadCommand parseCommand(String query)
    {
        ReadCommand command = parseReadCommand(query);
        Assertions.assertThat(command).isInstanceOf(PartitionRangeReadCommand.class);
        return (PartitionRangeReadCommand) command;
    }
}
