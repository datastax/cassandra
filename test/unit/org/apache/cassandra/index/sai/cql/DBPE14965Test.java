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

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.assertj.core.api.Assertions;

public class DBPE14965Test extends SAITester
{
    @Test
    public void testDBPE14965()
    {
        createTable("CREATE TABLE %s (" +
                    "    pk1 int," +
                    "    pk2 int," +
                    "    ck int," +
                    "    r int," +
                    "    s int static," +
                    "    PRIMARY KEY ((pk1, pk2), ck))");

        createIndex("CREATE CUSTOM INDEX ON %s (pk1) USING 'StorageAttachedIndex'");

        disableCompaction();
        execute("INSERT INTO %s (pk1, pk2, ck, r, s) VALUES (0, ?, ?, ?, ?)", 1, 1, 1, 1);
        flush();
        compact();

        Assertions.assertThat(execute("SELECT * FROM %s WHERE pk1=0").size()).isEqualTo(1); // finds 2!!
    }
}