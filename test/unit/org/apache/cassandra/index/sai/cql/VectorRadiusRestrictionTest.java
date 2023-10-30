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

public class VectorRadiusRestrictionTest extends VectorTester
{
    @Test
    public void testTwoPredicates()
    {
        createTable("CREATE TABLE %s (pk int, b boolean, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, v) VALUES (0, [1.0, 1.0])");
        execute("INSERT INTO %s (pk, v) VALUES (1, [10.0, 10.0])");
        execute("INSERT INTO %s (pk, v) VALUES (2, [5.0, 5.0])");

        // todo figure out how to remove limit
        assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [2,2]) < 3 LIMIT 100"), row(0));
    }
}
