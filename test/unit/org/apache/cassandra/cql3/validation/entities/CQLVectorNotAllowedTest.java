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

package org.apache.cassandra.cql3.validation.entities;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class CQLVectorNotAllowedTest extends CQLTester
{
    @BeforeClass
    public static void setupClass()
    {
        System.setProperty("cassandra.float_only_vectors", "false");
        System.setProperty("cassandra.vector_type_allowed", "false");
    }

    @Test(expected = InvalidRequestException.class)
    public void testCreateTableVectorPresentNotAllowed() throws Throwable
    {
        createTableMayThrow("CREATE TABLE %s (a int, b int, pk vector<int, 2> primary key)");
    }

    @Test(expected = InvalidRequestException.class)
    public void testCreateTableVectorInTupleNotAllowed() throws Throwable
    {
        createTableMayThrow("CREATE TABLE %s (a int, b int, pk tuple<int, vector<int, 2>> primary key)");
    }

    @Test(expected = InvalidRequestException.class)
    public void testCreateUdtWithVectorNotAllowed() throws Throwable
    {
        createType("CREATE TYPE %s (uuid_type uuid, text_type text, vec vector<int, 2>)");
    }

    @Test(expected = InvalidRequestException.class)
    public void testAlterTableAddVectorNotAllowed() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY(a, b))");
        alterTableMayThrow("ALTER TABLE %s ADD v3 vector<int, 2>;");
    }

    @Test
    public void testCreateUdtWithoutVectorNotAllowed() throws Throwable
    {
        createType("CREATE TYPE %s (uuid_type uuid, text_type text)");
    }

    @Test
    public void testCreateTableVectorNotPresentNotAllowed() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c tuple<int, int>, PRIMARY KEY(a, b))");
    }

}
