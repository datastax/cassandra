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

package org.apache.cassandra.index.sai.plan;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.transport.ProtocolVersion;

public class QueryPlanRedactionTest extends SAITester
{
    @Before
    public void setup()
    {
        requireNetwork();
    }

    private static final String KEYSPACE = "prepared_stmt_cleanup";
    private static final String createKsStatement = "CREATE KEYSPACE " + KEYSPACE +
                                                    " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";

    @Test
    public void testQuery()
    {
        String selectCQL2 = "SELECT * FROM t WHERE val : 'missing' OR (val : ? AND val : ?)";
        Session session = getSession(ProtocolVersion.V5);
        session.execute(createKsStatement);
        session.execute("Use prepared_stmt_cleanup");
        session.execute("CREATE TABLE t (id text PRIMARY KEY, val text)");
        session.execute("CREATE CUSTOM INDEX ON t(val) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                        "'index_analyzer': '{\n" +
                        "\t\"tokenizer\":{\"name\":\"ngram\", \"args\":{\"minGramSize\":\"2\", \"maxGramSize\":\"3\"}}," +
                        "\t\"filters\":[{\"name\":\"lowercase\"}]\n" +
                        "}'," +
                        "'query_analyzer': '{\n" +
                        "\t\"tokenizer\":{\"name\":\"whitespace\"},\n" +
                        "\t\"filters\":[{\"name\":\"porterstem\"}]\n" +
                        "}'};");

        PreparedStatement preparedSelect2 = session.prepare(selectCQL2);
        session.execute(preparedSelect2.bind("'quick'", "'dog'")).all().size();
    }
}
