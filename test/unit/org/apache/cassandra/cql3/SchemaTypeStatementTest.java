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

package org.apache.cassandra.cql3;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.schema.SchemaType;

public class SchemaTypeStatementTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    @Test
    public void testSetOnCreate() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, val int, primary key(key)) WITH schema_type = 'collection';");
        Assert.assertEquals(currentTableMetadata().params.schemaType, SchemaType.COLLECTION);
    }

    @Test
    public void testDefault() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, val int, primary key(key));");
        Assert.assertEquals(currentTableMetadata().params.schemaType, SchemaType.TABLE);
    }

    @Test
    public void testAlter() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, val int, primary key(key));");
        Assert.assertEquals(currentTableMetadata().params.schemaType, SchemaType.TABLE);
        execute("ALTER TABLE %s WITH schema_type = 'collection';");
        Assert.assertEquals(currentTableMetadata().params.schemaType, SchemaType.COLLECTION);
    }

}