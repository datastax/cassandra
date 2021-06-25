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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.exceptions.UnauthorizedException;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static org.apache.cassandra.auth.Permission.EXECUTE;
import static org.apache.cassandra.auth.Permission.MODIFY;
import static org.apache.cassandra.auth.Permission.SELECT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AuthCacheInvalidationTest extends CQLTester
{
    private static final String ks = "acinv_ks";
    private static final String tab = "acinv_tab";
    private static final String tab2 = "acinv_tab2";
    private static final String func = "acinv_func";
    private static final String aggr = "acinv_aggr";
    private static final RoleResource user1 = RoleResource.role("acinv_user1");
    private static final String userPw = "12345";
    private static final RoleResource role1 = RoleResource.role("acinv_role1");
    private static final RoleResource role2 = RoleResource.role("acinv_role2");
    private static final RoleResource role3 = RoleResource.role("acinv_role3");

    @BeforeClass
    public static void setup()
    {
        requireAuthentication();
        DatabaseDescriptor.setPermissionsValidity(9999);
        DatabaseDescriptor.setPermissionsUpdateInterval(9999);
        requireNetwork();
    }

    @Before
    public void prepare() throws Throwable
    {
        useSuperUser();

        executeNet("DROP ROLE IF EXISTS " + user1.getRoleName());
        executeNet("DROP ROLE IF EXISTS " + role1.getRoleName());
        executeNet("DROP ROLE IF EXISTS " + role2.getRoleName());
        executeNet("DROP ROLE IF EXISTS " + role3.getRoleName());
        executeNet("DROP KEYSPACE IF EXISTS " + ks);

        executeNet("CREATE USER " + user1.getRoleName() + " WITH PASSWORD '" + userPw + '\'');
        createRole(role1);
        createRole(role2);
        executeNet("GRANT " + role1.getRoleName() + " TO " + user1.getRoleName());
        executeNet("GRANT " + role2.getRoleName() + " TO " + role1.getRoleName());

        execute("CREATE KEYSPACE " + ks + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        execute("CREATE TABLE " + ks + '.' + tab + " (id int PRIMARY KEY)");
        execute("CREATE TABLE " + ks + '.' + tab2 + " (id int PRIMARY KEY)");

        executeNet("GRANT SELECT ON TABLE " + ks + '.' + tab + " TO " + role1.getRoleName());
        executeNet("GRANT MODIFY ON TABLE " + ks + '.' + tab + " TO " + role1.getRoleName());
        executeNet("GRANT MODIFY ON KEYSPACE " + ks + " TO " + role2.getRoleName());

        Roles.cache.invalidate();
        AuthenticatedUser.permissionsCache.invalidate();
    }

    private void createRole(RoleResource role) throws Throwable
    {
        executeNet(format("CREATE ROLE %s WITH LOGIN = true AND password = '%s'", role.getRoleName(), userPw));
    }

    @Test
    public void testInvalidationOnGrant() throws Throwable
    {
        populateAndCheckCaches();
        
        // Checks that the user has no SELECT permissions on tab2
        assertSelectNotAllowed(user1, ks, tab2);
        assertSelectNotAllowed(role1, ks, tab2);
        assertSelectNotAllowed(role2, ks, tab2);

        // Checks that the caches have been populated by the query.
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyPermissionPerResource(DataResource.table(ks, tab2)));
        assertPermissionsCacheContains(user1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), MODIFY));

        // Allow the user to execute SELECT on tab2
        useSuperUser();
        executeNet("GRANT SELECT ON KEYSPACE " + ks + " TO " + role2.getRoleName());

        // Checks that all role data have been removed from the caches
        assertRolesCacheDoesNotContains(user1);
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheDoesNotContains(role2);

        assertPermissionsCacheDoesNotContains(user1, DataResource.keyspace(ks));
        assertPermissionsCacheDoesNotContains(user1, DataResource.table(ks, tab2));
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheDoesNotContains(role2);

        // Checks that the user is now allowed to execute SELECT queries on tab2
        assertSelectAllowed(user1, ks, tab2);
        assertSelectAllowed(role1, ks, tab2);
        assertSelectAllowed(role2, ks, tab2);

        // Checks that role2 data been re-populated by the query.
        assertRolesCacheContains(role2, setOf());
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));

        assertPermissionsCacheContains(user1, permissionPerResource(DataResource.keyspace(ks), SELECT, MODIFY));
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.keyspace(ks), SELECT, MODIFY));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), SELECT, MODIFY));
    }

    @Test
    public void testInvalidationOnRevoke() throws Throwable
    {
        populateAndCheckCaches();
        
        // Checks that the user has SELECT permissions on tab
        assertSelectAllowed(user1, ks, tab);

        // Checks that the caches have been populated by the query.
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), MODIFY));

        // Revoke SELECT on tab
        useSuperUser();

        executeNet("REVOKE SELECT ON TABLE " + ks + '.' + tab + " FROM " + role1.getRoleName());

        // Checks that only the role1 data have been removed from the caches
        assertRolesCacheDoesNotContains(user1);
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheDoesNotContains(user1);
        assertPermissionsCacheDoesNotContains(role1);
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), MODIFY));

        // Checks that the user has no SELECT permissions on tab
        assertSelectNotAllowed(user1, ks, tab);
        assertSelectNotAllowed(role1, ks, tab);
        assertSelectNotAllowed(role2, ks, tab);
        
        // Checks that the cache content is the expected one
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, permissionPerResource(DataResource.table(ks, tab), MODIFY));
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), MODIFY));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), MODIFY));

        // Revoke SELECT on tab
        useSuperUser();

        executeNet("REVOKE ALL ON TABLE " + ks + '.' + tab + " FROM " + role1.getRoleName());

        // Checks that only the user1 and role1 data have been removed from the caches
        assertRolesCacheDoesNotContains(user1);
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheDoesNotContains(user1, DataResource.table(ks, tab));
        assertPermissionsCacheDoesNotContains(role1, DataResource.table(ks, tab));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), MODIFY));
    }

    @Test
    public void testInvalidationOnDropRole() throws Throwable
    {
        populateAndCheckCaches();

        // Checks that the user has no SELECT permissions on tab2
        assertSelectNotAllowed(user1, ks, tab2);
        assertSelectNotAllowed(role1, ks, tab2);
        assertSelectNotAllowed(role2, ks, tab2);

        // Checks that the caches have been populated by the query.
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyPermissionPerResource(DataResource.table(ks, tab2)));
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), MODIFY));

        // Create new ROLE with SELECT permission on tab2 and grant it to the user
        useSuperUser();

        createRole(role3);
        executeNet("GRANT SELECT ON KEYSPACE " + ks + " TO " + role3.getRoleName());
        executeNet("GRANT " + role3.getRoleName() + " TO " + user1.getRoleName());

        // Checks that only the user1 has been removed from the caches
        assertRolesCacheDoesNotContains(user1);
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());
        assertRolesCacheDoesNotContains(role3);

        assertPermissionsCacheDoesNotContains(user1);
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheDoesNotContains(role2, DataResource.keyspace(ks));
        assertPermissionsCacheDoesNotContains(role3);

        // Checks the the user is now allowed to perform the query
        assertSelectAllowed(user1, ks, tab2);
        assertSelectNotAllowed(role2, ks, tab2);
        assertSelectAllowed(role3, ks, tab2);

        // Checks that the caches have been populated by the query correctly.
        assertRolesCacheContains(user1, setOf(role1, role3));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());
        assertRolesCacheContains(role3, setOf());

        assertPermissionsCacheContains(user1, emptyPermissionPerResource(DataResource.table(ks, tab2)));
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), MODIFY));
        assertPermissionsCacheContains(role3, permissionPerResource(DataResource.keyspace(ks), SELECT));

        // Drop the role3
        useSuperUser();
        executeNet("DROP ROLE " + role3.getRoleName());

        // Checks that role3 and its children have been removed from the cache correctly.
        assertRolesCacheDoesNotContains(user1);
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());
        assertRolesCacheDoesNotContains(role3);

        assertPermissionsCacheDoesNotContains(user1);
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), MODIFY));
        assertPermissionsCacheDoesNotContains(role3);

        // Re-create role3 without granting it to the user
        createRole(role3);
        executeNet("GRANT SELECT ON KEYSPACE " + ks + " TO " + role3.getRoleName());

        // Checks that the user has no SELECT permissions on tab2
        useUser(user1.getRoleName(), userPw);

        assertSelectNotAllowed(user1, ks, tab2);

        // Checks the cache
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());
        assertRolesCacheDoesNotContains(role3);

        assertPermissionsCacheContains(user1, emptyPermissionPerResource(DataResource.table(ks, tab2)));
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheDoesNotContains(role2);
        assertPermissionsCacheDoesNotContains(role3);
    }

    @Test
    public void testInvalidationOnRevokeRole() throws Throwable
    {
        populateAndCheckCaches();
        
        // Checks that the user has no SELECT permissions on tab2
        assertSelectNotAllowed(user1, ks, tab2);

        // Checks that the caches have been populated by the query.
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyPermissionPerResource(DataResource.table(ks, tab2)));
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), MODIFY));

        // Create new ROLE with SELECT permission on tab2 and grant it to the user
        useSuperUser();

        createRole(role3);
        executeNet("GRANT SELECT ON KEYSPACE " + ks + " TO " + role3.getRoleName());
        executeNet("GRANT " + role3.getRoleName() + " TO " + user1.getRoleName());

        // Checks that only the user1 has been removed from the caches
        assertRolesCacheDoesNotContains(user1);
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());
        assertRolesCacheDoesNotContains(role3);

        assertPermissionsCacheDoesNotContains(user1);
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheDoesNotContains(role2);
        assertPermissionsCacheDoesNotContains(role3);

        // Checks the the user is now allowed to perform the query
        assertSelectAllowed(user1, ks, tab2);
        assertSelectNotAllowed(role2, ks, tab);
        assertSelectAllowed(role3, ks, tab2);

        // Checks that the caches have been populated by the query correctly.
        assertRolesCacheContains(user1, setOf(role1, role3));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());
        assertRolesCacheContains(role3, setOf());

        assertPermissionsCacheContains(user1, emptyPermissionPerResource(DataResource.table(ks, tab2)));
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), MODIFY));
        assertPermissionsCacheContains(role3, permissionPerResource(DataResource.keyspace(ks), SELECT));

        // Revoke the role3
        useSuperUser();
        executeNet("REVOKE " + role3.getRoleName() + " FROM " + user1.getRoleName());

        // Checks that the user has been removed from the cache correctly.
        assertRolesCacheDoesNotContains(user1);
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());
        assertRolesCacheContains(role3, setOf());

        assertPermissionsCacheDoesNotContains(user1);
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), MODIFY));
        assertPermissionsCacheContains(role3, permissionPerResource(DataResource.keyspace(ks), SELECT));

        // Checks that the user has no SELECT permissions on tab2
        assertSelectNotAllowed(user1, ks, tab2);
    }

    @Test
    public void testInvalidationOnDropRoleWithNonDirectChildren() throws Throwable
    {
        populateAndCheckCaches();
        
        // Checks that the user has no SELECT permissions on tab2
        assertSelectNotAllowed(user1, ks, tab2);

        // Checks that the caches have been populated by the query.
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyPermissionPerResource(DataResource.table(ks, tab2)));
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), MODIFY));

        // Create new ROLE with SELECT permission on tab2 and grant it to the user
        useSuperUser();

        createRole(role3);
        executeNet("GRANT SELECT ON KEYSPACE " + ks + " TO " + role3.getRoleName());
        executeNet("GRANT " + role3.getRoleName() + " TO " + role2.getRoleName());

        // Checks that all roles have been removed from the caches
        assertRolesCacheDoesNotContains(user1);
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheDoesNotContains(role2);
        assertRolesCacheDoesNotContains(role3);

        assertPermissionsCacheContains(user1, emptyPermissionPerResource(DataResource.table(ks, tab2)));
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheDoesNotContains(role2);
        assertPermissionsCacheDoesNotContains(role3);

        // Checks the the user is now allowed to perform the query
        assertSelectAllowed(user1, ks, tab2);

        // Checks that the caches have been populated by the query correctly.
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheDoesNotContains(role2);
        assertRolesCacheDoesNotContains(role3);

        assertPermissionsCacheContains(user1, emptyPermissionPerResource(DataResource.table(ks, tab2)));
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheDoesNotContains(role2);
        assertPermissionsCacheDoesNotContains(role3);

        // Drop the role3
        useSuperUser();
        executeNet("DROP ROLE " + role3.getRoleName());

        // Checks that role3 and its direct children have been removed from the cache correctly.
        assertRolesCacheDoesNotContains(user1);
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheDoesNotContains(role2);
        assertRolesCacheDoesNotContains(role3);

        assertPermissionsCacheContains(user1, emptyPermissionPerResource(DataResource.table(ks, tab2)));
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheDoesNotContains(role2);
        assertPermissionsCacheDoesNotContains(role3);

        // Re-create role3 without granting it to role2
        createRole(role3);
        executeNet("GRANT SELECT ON KEYSPACE " + ks + " TO " + role3.getRoleName());

        // Checks that the user has no SELECT permissions on tab2
        assertSelectNotAllowed(user1, ks, tab2);

        // Checks the cache
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheDoesNotContains(role2);
        assertRolesCacheDoesNotContains(role3);

        assertPermissionsCacheContains(user1, emptyPermissionPerResource(DataResource.table(ks, tab2)));
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheDoesNotContains(role2);
        assertPermissionsCacheDoesNotContains(role3);
    }

    private void populateAndCheckCaches() throws Throwable
    {
        populateCaches();

        // roles cache should be populated for user1 + assigned roles role1 + role2
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        // permissions are checked against all roles
        assertPermissionsCacheContains(user1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, MODIFY));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), MODIFY));
    }

    private void populateCaches() throws Throwable
    {
        String query = format("SELECT * FROM %s.%s", ks, tab);
        executeIgnoreError(role1, query);
        executeIgnoreError(role2, query);
        executeIgnoreError(user1, query);
    }

    @Test
    public void testInvalidationOnDropTable() throws Throwable
    {
        populateAndCheckCaches();

        useSuperUser();

        executeNet("DROP TABLE " + ks + '.' + tab);

        // permissions were only granted to role1 - so only role1 and user1 are invalidated
        assertRolesCacheDoesNotContains(user1);
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheDoesNotContains(user1);
        assertPermissionsCacheDoesNotContains(role1);
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), MODIFY));

        // Re-create the dropped table
        execute("CREATE TABLE " + ks + '.' + tab + " (id int PRIMARY KEY)");

        // Checks that the user has no SELECT permissions on tab
        assertSelectNotAllowed(user1, ks, tab);
        assertSelectNotAllowed(role1, ks, tab);
        assertSelectNotAllowed(role2, ks, tab);

        // checks the caches
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheDoesNotContains(user1, DataResource.table(ks, tab));
        assertPermissionsCacheDoesNotContains(role1, DataResource.table(ks, tab));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), MODIFY));
    }

    @Test
    public void testInvalidationOnDropKeyspace() throws Throwable
    {
        populateAndCheckCaches();

        useSuperUser();

        executeNet("DROP KEYSPACE " + ks);

        // permissions were only granted to role1 - so only role1 and user1 are invalidated
        assertRolesCacheDoesNotContains(user1);
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheDoesNotContains(role2);

        assertPermissionsCacheDoesNotContains(user1);
        assertPermissionsCacheDoesNotContains(role1);
        assertPermissionsCacheDoesNotContains(role2);

        // Re-create keyspace and table
        executeNet("CREATE KEYSPACE " + ks + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        executeNet("CREATE TABLE " + ks + '.' + tab + " (id int PRIMARY KEY)");

        // Re-populate caches
        populateCaches();

        // Checks that the user has no SELECT permissions on tab
        assertSelectNotAllowed(user1, ks, tab);

        // checks the caches
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheDoesNotContains(user1);
        assertPermissionsCacheDoesNotContains(role1);
        assertPermissionsCacheDoesNotContains(role2);
    }

    @Test
    public void testInvalidationOnDropAggregateAndFunction() throws Throwable
    {
        execute("CREATE FUNCTION " + ks + '.' + func + " ( input1 int, input2 int ) " +
                "CALLED ON NULL INPUT " +
                "RETURNS int " +
                "LANGUAGE java AS 'return input1 + input2;'");

        execute("CREATE AGGREGATE " + ks + '.' + aggr + "(int)" +
                "SFUNC " + func + ' ' +
                "STYPE int " +
                "INITCOND 0");

        executeNet("GRANT EXECUTE ON FUNCTION " + ks + '.' + func + "(int,int) TO " + role2.getRoleName());
        executeNet("GRANT EXECUTE ON FUNCTION " + ks + '.' + aggr + "(int) TO " + role1.getRoleName());

        IResource functionResource = FunctionResource.function(ks, func, Arrays.asList(Int32Type.instance, Int32Type.instance));
        IResource aggregateResource = FunctionResource.function(ks, aggr, Arrays.asList(Int32Type.instance));

        Map<IResource, Set<Permission>> role1Permissions = new HashMap<>();
        role1Permissions.put(DataResource.table(ks, tab), permissionSet(SELECT, MODIFY));
        role1Permissions.put(aggregateResource, permissionSet(EXECUTE));

        Map<IResource, Set<Permission>> role2Permissions = new HashMap<>();
        role2Permissions.put(DataResource.keyspace(ks), permissionSet(MODIFY));
        role2Permissions.put(functionResource, permissionSet(EXECUTE));

        Map<IResource, Set<Permission>> user1Permissions = new HashMap<>();
        user1Permissions.put(DataResource.table(ks, tab), permissionSet(SELECT, MODIFY));
        user1Permissions.put(aggregateResource, permissionSet(EXECUTE));
        user1Permissions.put(functionResource, permissionSet(EXECUTE));

        populateCaches();
        
        String funcQuery = format("SELECT %s.%s(listen_port, listen_port) FROM system.local", ks, func);
        assertQueryAllowed(user1, funcQuery);
        assertQueryAllowed(role1, funcQuery);
        assertQueryAllowed(role2, funcQuery);

        String aggrQuery = format("SELECT %s.%s(listen_port) FROM system.local", ks, aggr);
        assertQueryAllowed(user1, aggrQuery);
        assertQueryAllowed(role1, aggrQuery);
        String expected = format("User %s has no EXECUTE permission on <function %s.%s(int)> or any of its parents", role2.getRoleName(), ks, aggr);
        assertQueryNotAllowed(role2, aggrQuery, expected);

        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, user1Permissions);
        assertPermissionsCacheContains(role1, role1Permissions);
        assertPermissionsCacheContains(role2, role2Permissions);

        useSuperUser();

        executeNet("DROP AGGREGATE " + ks + '.' + aggr + "(int)");

        assertRolesCacheDoesNotContains(user1);
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheContains(role2, setOf());

        role1Permissions.remove(aggregateResource);
        user1Permissions.remove(aggregateResource);

        assertPermissionsCacheContains(user1, user1Permissions);
        assertPermissionsCacheDoesNotContains(user1, aggregateResource);
        assertPermissionsCacheContains(role1, role1Permissions);
        assertPermissionsCacheDoesNotContains(role1, aggregateResource);
        assertPermissionsCacheContains(role2, role2Permissions);

        populateCaches();

        assertRolesCacheDoesNotContains(user1);
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, user1Permissions);
        assertPermissionsCacheDoesNotContains(user1, aggregateResource);
        assertPermissionsCacheContains(role1, role1Permissions);
        assertPermissionsCacheDoesNotContains(role1, aggregateResource);
        assertPermissionsCacheContains(role2, role2Permissions);

        useSuperUser();

        executeNet("DROP FUNCTION " + ks + '.' + func + "(int,int)");

        assertRolesCacheDoesNotContains(user1);
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheDoesNotContains(role2);

        role2Permissions.remove(functionResource);
        user1Permissions.remove(functionResource);

        assertPermissionsCacheContains(user1, user1Permissions);
        assertPermissionsCacheDoesNotContains(user1, functionResource);
        assertPermissionsCacheContains(role1, role1Permissions);
        assertPermissionsCacheDoesNotContains(role1, functionResource);
        assertPermissionsCacheDoesNotContains(role2, FunctionResource.function(ks, func, Arrays.asList(Int32Type.instance, Int32Type.instance)));

        populateCaches();

        assertRolesCacheDoesNotContains(user1);
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheDoesNotContains(role2);

        assertPermissionsCacheContains(user1, user1Permissions);
        assertPermissionsCacheDoesNotContains(user1, functionResource);
        assertPermissionsCacheContains(role1, role1Permissions);
        assertPermissionsCacheDoesNotContains(role1, functionResource);
        assertPermissionsCacheContains(role2, role2Permissions);
    }

    private void assertSelectAllowed(RoleResource role, String ks, String table) throws Throwable
    {
        assertQueryAllowed(role, format("SELECT * FROM %s.%s", ks, table));
    }
    
    private void assertQueryAllowed(RoleResource role, String query) throws Throwable
    {
        useUser(role.getRoleName(), userPw);
        executeNet(query);
    }
    
    private void assertSelectNotAllowed(RoleResource role, String ks, String table) throws Throwable
    {
        String query = format("SELECT * FROM %s.%s", ks, table);
        String expected = format("User %s has no SELECT permission on <table %s.%s> or any of its parents", role.getRoleName(), ks, table);
        assertQueryNotAllowed(role, query, expected);
    }

    private void assertQueryNotAllowed(RoleResource role, String query, String expected) throws Throwable
    {
        useUser(role.getRoleName(), userPw);
        try
        {
            executeNet(query);
        }
        catch (UnauthorizedException e)
        {
            assertEquals(expected, e.getMessage());
        }
    }
    
    private void executeIgnoreError(RoleResource role, String query) throws Throwable
    {
        useUser(role.getRoleName(), userPw);
        try
        {
            executeNet(query);
        }
        catch (Exception e)
        {
            // ignore
        }
    }

    private static void assertRolesCacheContains(RoleResource resource, Set<RoleResource> memberOf)
    {
        tryWithBackoff(() -> {
            Set<Role> roles = Roles.cache.getIfPresent(resource);
            assertNotNull("The role " + resource + " should be in the role cache but is not.", roles);

            Role role = roles.stream().filter(r -> r.resource.equals(resource)).findFirst().orElse(null);
            assertNotNull("The role " + resource + " should be in the role cache but is not.", role);

            Set<String> memberOfStr = memberOf.stream().map(roleResource -> roleResource.getRoleName()).collect(Collectors.toSet());
            assertEquals("The role " + resource + " is not a member of the expected roles.", memberOfStr, role.memberOf);
        });
    }

    private static void assertRolesCacheDoesNotContains(RoleResource resource)
    {
        tryWithBackoff(() -> {
            Set<Role> roles = Roles.cache.getIfPresent(resource);
            assertNull(format("The role %s is in the role cache but should not.", resource), roles);
        });
    }

    private void assertPermissionsCacheContains(RoleResource roleResource, Map<IResource, Set<Permission>> expectedPermissions)
    {
        assertFalse("Pass emptyPermissionPerResource or use assertPermissionsCacheNotContains", expectedPermissions.isEmpty());
        AuthenticatedUser authUser = new AuthenticatedUser(roleResource.getRoleName());
        for (Map.Entry<IResource, Set<Permission>> entry : expectedPermissions.entrySet())
        {
            assertPermissionsCacheContains(authUser, entry.getKey(), entry.getValue());
        }
    }

    private void assertPermissionsCacheContains(AuthenticatedUser user, IResource resource, Set<Permission> expectedPermissions)
    {
        tryWithBackoff(() -> {
            Set<Permission> actual = AuthenticatedUser.permissionsCache.getIfPresent(Pair.create(user, resource));
            assertNotNull("AuthUser " + user + " resource " + resource + " is not in the permissions cache.", actual);
            assertEquals("The expected permissions for user " + user + " resource " + resource + " do not match the ones of the permissions cache.", expectedPermissions, actual);
        });
    }

    private void assertPermissionsCacheDoesNotContains(RoleResource resource)
    {
        assertPermissionsCacheDoesNotContains(resource, null);
    }
    
    private void assertPermissionsCacheDoesNotContains(RoleResource roleResource, IResource resource)
    {
        tryWithBackoff(() -> {
            AuthenticatedUser user = new AuthenticatedUser(roleResource.getRoleName());
            Set<Map.Entry<Pair<AuthenticatedUser, IResource>, Set<Permission>>> entries = 
            AuthenticatedUser.permissionsCache.cache.asMap().entrySet().stream().filter(entry -> {
                boolean userMatches = Objects.equals(entry.getKey().left(), user);
                boolean resourceMatches = resource == null || Objects.equals(entry.getKey().right, resource);
                boolean permissionsNotEmpty = !entry.getValue().isEmpty();
                return userMatches && resourceMatches && permissionsNotEmpty;
            }).collect(Collectors.toSet());
            String message = format("The role %s has %s permissions in the cache: %s", roleResource, resource == null ? "" : resource, entries);
            assertFalse(message, !entries.isEmpty());
        });
    }

    /**
     * Try the assertion in the given function a few times.
     * Reason for this retry functionality is that the caches might not be updated immediately or there are
     * some "visibility issues" in the cache. For production code, that is usually fine.
     * But the test code relies on "immediate-ish" cache updates.
     *
     * See DB-2740
     */
    private static void tryWithBackoff(Runnable f)
    {
        AssertionError err = null;
        for (int i = 0; i < 10; i++)
        {
            try
            {
                f.run();
                break;
            }
            catch (AssertionError e)
            {
                logger.warn("An assertion failed in try #{}: {}", i + 1, e.toString());
                err = e;
            }

            Thread.yield();
        }
        if (err != null)
            throw err;
    }

    private static <T> Set<T> setOf(T... elem)
    {
        return new HashSet<>(Arrays.asList(elem));
    }

    private static Map<IResource, Set<Permission>> emptyPermissionPerResource(IResource resource)
    {
        return Collections.singletonMap(resource, ImmutableSet.of());
    }

    private static Map<IResource, Set<Permission>> permissionPerResource(IResource resource, Permission... granted)
    {
        assertTrue("Use emptyPermissionPerResource", granted.length > 0);
        return Collections.singletonMap(resource, permissionSet(granted));
    }

    private static Set<Permission> permissionSet(Permission... granted)
    {
        Set<Permission> permissions = new HashSet<>();
        Collections.addAll(permissions, granted);
        return permissions;
    }
}
