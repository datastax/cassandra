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
package org.apache.cassandra.auth;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertTrue;

public class GrantAndRevokeTest extends CQLTester
{
    private static final String user = "user";
    private static final String pass = "12345";

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.setPermissionsValidity(0);
        CQLTester.setUpClass();
        requireAuthentication();
        requireNetwork();
        StorageService.instance.doAuthSetup(true);
    }

    @After
    public void tearDown() throws Throwable
    {
        useSuperUser();
        executeNet("DROP ROLE " + user);
    }

    @Test
    public void testWarnings() throws Throwable
    {
        useSuperUser();

        executeNet("CREATE KEYSPACE revoke_yeah WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        executeNet("CREATE TABLE revoke_yeah.t1 (id int PRIMARY KEY, val text)");
        executeNet("CREATE USER '" + user + "' WITH PASSWORD '" + pass + "'");

        ResultSet res = executeNet("REVOKE CREATE ON KEYSPACE revoke_yeah FROM " + user);
        assertWarningsContain(res.getExecutionInfo().getWarnings(), "Role '" + user + "' was not granted CREATE on <keyspace revoke_yeah>");

        res = executeNet("GRANT SELECT ON KEYSPACE revoke_yeah TO " + user);
        assertTrue(res.getExecutionInfo().getWarnings().isEmpty());

        res = executeNet("GRANT SELECT ON KEYSPACE revoke_yeah TO " + user);
        assertWarningsContain(res.getExecutionInfo().getWarnings(), "Role '" + user + "' was already granted SELECT on <keyspace revoke_yeah>");

        res = executeNet("REVOKE SELECT ON TABLE revoke_yeah.t1 FROM " + user);
        assertWarningsContain(res.getExecutionInfo().getWarnings(), "Role '" + user + "' was not granted SELECT on <table revoke_yeah.t1>");

        res = executeNet("REVOKE MODIFY ON KEYSPACE revoke_yeah FROM " + user);
        assertWarningsContain(res.getExecutionInfo().getWarnings(), "Role '" + user + "' was not granted MODIFY on <keyspace revoke_yeah>");
    }
}
