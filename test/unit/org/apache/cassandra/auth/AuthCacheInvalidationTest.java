/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.auth;

import java.util.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.exceptions.UnauthorizedException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.utils.MapsFactory;
import org.apache.cassandra.utils.Pair;

import static java.util.Collections.emptyMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import static org.apache.cassandra.auth.permission.CorePermission.EXECUTE;
import static org.apache.cassandra.auth.permission.CorePermission.UPDATE;
import static org.apache.cassandra.auth.permission.CorePermission.SELECT;

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
        executeNet("CREATE ROLE " + role1.getRoleName());
        executeNet("CREATE ROLE " + role2.getRoleName());
        executeNet("GRANT " + role1.getRoleName() + " TO " + user1.getRoleName());
        executeNet("GRANT " + role2.getRoleName() + " TO " + role1.getRoleName());

        execute("CREATE KEYSPACE " + ks + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        execute("CREATE TABLE " + ks + '.' + tab + " (id int PRIMARY KEY)");
        execute("CREATE TABLE " + ks + '.' + tab2 + " (id int PRIMARY KEY)");

        executeNet("GRANT SELECT, UPDATE ON TABLE " + ks + '.' + tab + " TO " + role1.getRoleName());
        executeNet("GRANT UPDATE ON KEYSPACE " + ks + " TO " + role2.getRoleName());

        DatabaseDescriptor.getAuthManager().invalidateCaches();
    }

    @Test
    public void testInvalidationOnGrant() throws Throwable
    {
        // Checks that the user has no SELECT permissions on tab2

        useUser(user1.getRoleName(), userPw);

        assertUnauthorized("SELECT * FROM " + ks + '.' + tab2,
                           "User " + user1.getRoleName() + " has no SELECT permission on <table " + ks + '.' + tab2 + "> or any of its parents");

        // Checks that the caches have been populated by the query.
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));

        // Allow the user to execute SELECT on tab2
        useSuperUser();
        executeNet("GRANT SELECT ON KEYSPACE " + ks + " TO " + role2.getRoleName());

        // Checks that only the role2 data have been removed from the caches
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheDoesNotContains(role2);

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheDoesNotContains(role2);

        // Checks that the user is now allowed to execute SELECT queries on tab2
        useUser(user1.getRoleName(), userPw);

        executeNet("SELECT * FROM " + ks + '.' + tab2);

        // Checks that role2 data been re-populated by the query.
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), SELECT, UPDATE));
    }

    @Test
    public void testInvalidationOnRevoke() throws Throwable
    {
        // Checks that the user has SELECT permissions on tab
        useUser(user1.getRoleName(), userPw);

        executeNet("SELECT * FROM " + ks + '.' + tab);

        // Checks that the caches have been populated by the query.
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));

        // Revoke SELECT on tab
        useSuperUser();

        executeNet("REVOKE SELECT ON TABLE " + ks + '.' + tab + " FROM " + role1.getRoleName());

        // Checks that only the role1 data have been removed from the caches
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheDoesNotContains(role1);
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));

        // Checks that the user has no SELECT permissions on tab
        useUser(user1.getRoleName(), userPw);
        assertUnauthorized("SELECT * FROM " + ks + '.' + tab,
                           "User " + user1.getRoleName() + " has no SELECT permission on <table " + ks + '.' + tab + "> or any of its parents");

        // Checks that the cache content is the expected one
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), UPDATE));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));

        // Revoke SELECT on tab
        useSuperUser();

        executeNet("REVOKE ALL ON TABLE " + ks + '.' + tab + " FROM " + role1.getRoleName());

        // Checks that only the role1 data have been removed from the caches
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheDoesNotContains(role1);
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));
    }

    @Test
    public void testInvalidationOnDropRole() throws Throwable
    {
        // Checks that the user has no SELECT permissions on tab2
        useUser(user1.getRoleName(), userPw);

        assertUnauthorized("SELECT * FROM " + ks + '.' + tab2,
                           "User " + user1.getRoleName() + " has no SELECT permission on <table " + ks + '.' + tab2 + "> or any of its parents");

        // Checks that the caches have been populated by the query.
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));

        // Create new ROLE with SELECT permission on tab2 and grant it to the user
        useSuperUser();

        executeNet("CREATE ROLE " + role3.getRoleName());
        executeNet("GRANT SELECT ON KEYSPACE " + ks + " TO " + role3.getRoleName());
        executeNet("GRANT " + role3.getRoleName() + " TO " + user1.getRoleName());

        // Checks that only the user1 has been removed from the caches
        assertRolesCacheDoesNotContains(user1);
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());
        assertRolesCacheDoesNotContains(role3);

        assertPermissionsCacheDoesNotContains(user1);
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));
        assertPermissionsCacheDoesNotContains(role3);

        // Checks the the user is now allowed to perform the query
        useUser(user1.getRoleName(), userPw);
        executeNet("SELECT * FROM " + ks + '.' + tab2);

        // Checks that the caches have been populated by the query correctly.
        assertRolesCacheContains(user1, setOf(role1, role3));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());
        assertRolesCacheContains(role3, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));
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
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));
        assertPermissionsCacheDoesNotContains(role3);

        // Re-create role3 without granting it to the user
        executeNet("CREATE ROLE " + role3.getRoleName());
        executeNet("GRANT SELECT ON KEYSPACE " + ks + " TO " + role3.getRoleName());

        // Checks that the user has no SELECT permissions on tab2
        useUser(user1.getRoleName(), userPw);

        assertUnauthorized("SELECT * FROM " + ks + '.' + tab2,
                           "User " + user1.getRoleName() + " has no SELECT permission on <table " + ks + '.' + tab2 + "> or any of its parents");

        // Checks the cache
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());
        assertRolesCacheDoesNotContains(role3);

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));
        assertPermissionsCacheDoesNotContains(role3);
    }

    @Test
    public void testInvalidationOnRevokeRole() throws Throwable
    {
        // Checks that the user has no SELECT permissions on tab2
        useUser(user1.getRoleName(), userPw);

        assertUnauthorized("SELECT * FROM " + ks + '.' + tab2,
                           "User " + user1.getRoleName() + " has no SELECT permission on <table " + ks + '.' + tab2 + "> or any of its parents");

        // Checks that the caches have been populated by the query.
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));

        // Create new ROLE with SELECT permission on tab2 and grant it to the user
        useSuperUser();

        executeNet("CREATE ROLE " + role3.getRoleName());
        executeNet("GRANT SELECT ON KEYSPACE " + ks + " TO " + role3.getRoleName());
        executeNet("GRANT " + role3.getRoleName() + " TO " + user1.getRoleName());

        // Checks that only the user1 has been removed from the caches
        assertRolesCacheDoesNotContains(user1);
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());
        assertRolesCacheDoesNotContains(role3);

        assertPermissionsCacheDoesNotContains(user1);
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));
        assertPermissionsCacheDoesNotContains(role3);

        // Checks the the user is now allowed to perform the query
        useUser(user1.getRoleName(), userPw);
        executeNet("SELECT * FROM " + ks + '.' + tab2);

        // Checks that the caches have been populated by the query correctly.
        assertRolesCacheContains(user1, setOf(role1, role3));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());
        assertRolesCacheContains(role3, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));
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
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));
        assertPermissionsCacheContains(role3, permissionPerResource(DataResource.keyspace(ks), SELECT));

        // Checks that the user has no SELECT permissions on tab2
        useUser(user1.getRoleName(), userPw);

        assertUnauthorized("SELECT * FROM " + ks + '.' + tab2,
                           "User " + user1.getRoleName() + " has no SELECT permission on <table " + ks + '.' + tab2 + "> or any of its parents");
    }

    @Test
    public void testInvalidationOnDropRoleWithNonDirectChildren() throws Throwable
    {
        // Checks that the user has no SELECT permissions on tab2
        useUser(user1.getRoleName(), userPw);

        assertUnauthorized("SELECT * FROM " + ks + '.' + tab2,
                           "User " + user1.getRoleName() + " has no SELECT permission on <table " + ks + '.' + tab2 + "> or any of its parents");

        // Checks that the caches have been populated by the query.
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));

        // Create new ROLE with SELECT permission on tab2 and grant it to the user
        useSuperUser();

        executeNet("CREATE ROLE " + role3.getRoleName());
        executeNet("GRANT SELECT ON KEYSPACE " + ks + " TO " + role3.getRoleName());
        executeNet("GRANT " + role3.getRoleName() + " TO " + role2.getRoleName());

        // Checks that only the user1 has been removed from the caches
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheDoesNotContains(role2);
        assertRolesCacheDoesNotContains(role3);

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheDoesNotContains(role2);
        assertPermissionsCacheDoesNotContains(role3);

        // Checks the the user is now allowed to perform the query
        useUser(user1.getRoleName(), userPw);
        executeNet("SELECT * FROM " + ks + '.' + tab2);

        // Checks that the caches have been populated by the query correctly.
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf(role3));
        assertRolesCacheContains(role3, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));
        assertPermissionsCacheContains(role3, permissionPerResource(DataResource.keyspace(ks), SELECT));

        // Drop the role3
        useSuperUser();
        executeNet("DROP ROLE " + role3.getRoleName());

        // Checks that role3 and its direct children have been removed from the cache correctly.
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheDoesNotContains(role2);
        assertRolesCacheDoesNotContains(role3);

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheDoesNotContains(role2);
        assertPermissionsCacheDoesNotContains(role3);

        // Re-create role3 without granting it to role2
        executeNet("CREATE ROLE " + role3.getRoleName());
        executeNet("GRANT SELECT ON KEYSPACE " + ks + " TO " + role3.getRoleName());

        // Checks that the user has no SELECT permissions on tab2
        useUser(user1.getRoleName(), userPw);

        assertUnauthorized("SELECT * FROM " + ks + '.' + tab2,
                           "User " + user1.getRoleName() + " has no SELECT permission on <table " + ks + '.' + tab2 + "> or any of its parents");

        // Checks the cache
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());
        assertRolesCacheDoesNotContains(role3);

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));
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
        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, permissionPerResource(DataResource.table(ks, tab), SELECT, UPDATE));
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));
    }

    private void populateCaches() throws Throwable
    {
        useUser(user1.getRoleName(), userPw);

        // perform a query to populate the permissions cache for user1 and all assigned roles
        executeNet("SELECT * FROM " + ks + '.' + tab);
    }

    @Test
    public void testInvalidationOnDropTable() throws Throwable
    {
        populateAndCheckCaches();

        useSuperUser();

        executeNet("DROP TABLE " + ks + '.' + tab);

        // permissions were only granted to role1 - so only role1 needs to be invalidated
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheDoesNotContains(role1);
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));

        // Re-create the dropped table
        execute("CREATE TABLE " + ks + '.' + tab + " (id int PRIMARY KEY)");

        // Checks that the user has no SELECT permissions on tab
        useUser(user1.getRoleName(), userPw);

        assertUnauthorized("SELECT * FROM " + ks + '.' + tab,
                           "User " + user1.getRoleName() + " has no SELECT permission on <table " + ks + '.' + tab + "> or any of its parents");

        // checks the caches
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, emptyMap());
        assertPermissionsCacheContains(role2, permissionPerResource(DataResource.keyspace(ks), UPDATE));
    }

    @Test
    public void testInvalidationOnDropKeyspace() throws Throwable
    {
        populateAndCheckCaches();

        useSuperUser();

        executeNet("DROP KEYSPACE " + ks);

        // permissions were only granted to role1 - so only role1 needs to be invalidated
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheDoesNotContains(role2);

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheDoesNotContains(role1);
        assertPermissionsCacheDoesNotContains(role2);

        // Re-create keyspace and table
        executeNet("CREATE KEYSPACE " + ks + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        executeNet("CREATE TABLE " + ks + '.' + tab + " (id int PRIMARY KEY)");

        // Checks that the user has no SELECT permissions on tab
        useUser(user1.getRoleName(), userPw);

        assertUnauthorized("SELECT * FROM " + ks + '.' + tab,
                           "User " + user1.getRoleName() + " has no SELECT permission on <table " + ks + '.' + tab + "> or any of its parents");

        // checks the caches
        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, emptyMap());
        assertPermissionsCacheContains(role2, emptyMap());
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

        Map<IResource, PermissionSets> role1Permissions = MapsFactory.newMap();
        role1Permissions.put(DataResource.table(ks, tab), permissionSet(SELECT, UPDATE));
        role1Permissions.put(aggregateResource, permissionSet(EXECUTE));

        Map<IResource, PermissionSets> role2Permissions = MapsFactory.newMap();
        role2Permissions.put(DataResource.keyspace(ks), permissionSet(UPDATE));
        role2Permissions.put(functionResource, permissionSet(EXECUTE));

        populateCaches();

        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, role1Permissions);
        assertPermissionsCacheContains(role2, role2Permissions);

        useSuperUser();

        executeNet("DROP AGGREGATE " + ks + '.' + aggr + "(int)");

        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheDoesNotContains(role1);
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheDoesNotContains(role1);
        assertPermissionsCacheContains(role2, role2Permissions);

        populateCaches();

        role1Permissions.remove(aggregateResource);

        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, role1Permissions);
        assertPermissionsCacheContains(role2, role2Permissions);

        useSuperUser();

        executeNet("DROP FUNCTION " + ks + '.' + func + "(int,int)");

        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheDoesNotContains(role2);

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, role1Permissions);
        assertPermissionsCacheDoesNotContains(role2);

        populateCaches();

        role2Permissions.remove(functionResource);

        assertRolesCacheContains(user1, setOf(role1));
        assertRolesCacheContains(role1, setOf(role2));
        assertRolesCacheContains(role2, setOf());

        assertPermissionsCacheContains(user1, emptyMap());
        assertPermissionsCacheContains(role1, role1Permissions);
        assertPermissionsCacheContains(role2, role2Permissions);
    }

    private void assertUnauthorized(String query, String errorMessage) throws Throwable
    {
        try
        {
            executeNet(query);
        }
        catch (UnauthorizedException e)
        {
            assertEquals(errorMessage, e.getMessage());
        }
    }

    private static void assertRolesCacheContains(RoleResource resource, Set<RoleResource> memberOf)
    {
        tryWithBackoff(() -> {
            Role role = DatabaseDescriptor.getAuthManager().rolesCache.getIfPresent(Pair.create(resource, null));
            assertNotNull("The role " + resource + " should be in the role cache but is not.", role);
            assertEquals("The role " + resource + " is not a member of the expected roles.", memberOf, role.memberOf);
        });
    }

    private static void assertRolesCacheDoesNotContains(RoleResource resource)
    {
        tryWithBackoff(() -> {
            Role role = DatabaseDescriptor.getAuthManager().rolesCache.getIfPresent(Pair.create(resource, null));
            assertNull("The role " + resource + " is in the role cache but should not.", role);
        });
    }

    private void assertPermissionsCacheContains(RoleResource resource, Map<IResource, PermissionSets> expectedPermissions)
    {
        tryWithBackoff(() -> {
            Map<IResource, PermissionSets> actual = DatabaseDescriptor.getAuthManager().permissionsCache.getIfPresent(Pair.create(resource, null));
            assertNotNull("The resource " + resource + " is not in the permissions cache.", actual);
            assertEquals("The expected permissions for the role " + resource + " do not match the ones of the permissions cache.", expectedPermissions, actual);
        });
    }

    private void assertPermissionsCacheDoesNotContains(RoleResource resource)
    {
        tryWithBackoff(() -> {
            Map<IResource, PermissionSets> permissions = DatabaseDescriptor.getAuthManager().permissionsCache.getIfPresent(Pair.create(resource, null));
            assertNull("The role " + resource + " has some permissions int the cache.", permissions);
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

    private static Map<IResource, PermissionSets> permissionPerResource(IResource resource, Permission... granted)
    {
        return Collections.singletonMap(resource, permissionSet(granted));
    }

    private static PermissionSets permissionSet(Permission... granted)
    {
        PermissionSets.Builder builder = PermissionSets.builder();
        for (Permission permission : granted)
        {
            builder.addGranted(permission);
        }
        return builder.build();
    }
}
