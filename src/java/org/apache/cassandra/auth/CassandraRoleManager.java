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

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.mindrot.jbcrypt.BCrypt;

/**
 * Responsible for the creation, maintenance and deletion of roles
 * for the purposes of authentication and authorization.
 * Role data is stored internally, using the roles and role_members tables
 * in the system_auth keyspace.
 *
 * Additionally, if org.apache.cassandra.auth.PasswordAuthenticator is used,
 * encrypted passwords are also stored in the system_auth.roles table. This
 * coupling between the IAuthenticator and IRoleManager implementations exists
 * because setting a role's password via CQL is done with a CREATE ROLE or
 * ALTER ROLE statement, the processing of which is handled by IRoleManager.
 * As IAuthenticator is concerned only with credentials checking and has no
 * means to modify passwords, PasswordAuthenticator depends on
 * CassandraRoleManager for those functions.
 *
 * Alternative IAuthenticator implementations may be used in conjunction with
 * CassandraRoleManager, but WITH PASSWORD = 'password' will not be supported
 * in CREATE/ALTER ROLE statements.
 *
 * Such a configuration could be implemented using a custom IRoleManager that
 * extends CassandraRoleManager and which includes Option.PASSWORD in the {@code Set<Option>}
 * returned from supportedOptions/alterableOptions. Any additional processing
 * of the password itself (such as storing it in an alternative location) would
 * be added in overridden createRole and alterRole implementations.
 */
public class CassandraRoleManager implements IRoleManager
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraRoleManager.class);

    static final String DEFAULT_SUPERUSER_NAME = "cassandra";
    static final String DEFAULT_SUPERUSER_PASSWORD = "cassandra";

    // In some cases we want to avoid creating default super user accounts (for security purposes). For example CNDB
    // can create roles without requiring default SU account. So we need a flag to tell C* to skip creating this role.
    // Even though this says SKIP it's actually safer to create the user as non super user with no login creds
    // and a timestamp of 1 in case a user forgets the flag on another node.
    private static final boolean SKIP_DEFAULT_ROLE_SETUP = Boolean.getBoolean(Config.PROPERTY_PREFIX + "skip_default_role_setup");

    // Transform a row in the AuthKeyspace.ROLES to a Role instance
    private static final Function<UntypedResultSet.Row, Role> ROW_TO_ROLE = row ->
    {
        try
        {
            return new Role(row.getString("role"),
                            row.getBoolean("is_superuser"),
                            row.getBoolean("can_login"),
                            Collections.emptyMap(),
                            row.has("member_of") ? row.getSet("member_of", UTF8Type.instance)
                                                 : Collections.<String>emptySet());
        }
        // Failing to deserialize a boolean in is_superuser or can_login will throw an NPE
        catch (NullPointerException e)
        {
            logger.warn("An invalid value has been detected in the {} table for role {}. If you are " +
                        "unable to login, you may need to disable authentication and confirm " +
                        "that values in that table are accurate", AuthKeyspace.ROLES, row.getString("role"));
            throw new RuntimeException(String.format("Invalid metadata has been detected for role %s", row.getString("role")), e);
        }
    };

    // 2 ** GENSALT_LOG2_ROUNDS rounds of hashing will be performed.
    @VisibleForTesting
    public static final String GENSALT_LOG2_ROUNDS_PROPERTY = Config.PROPERTY_PREFIX + "auth_bcrypt_gensalt_log2_rounds";
    private static final int GENSALT_LOG2_ROUNDS = getGensaltLogRounds();

    static int getGensaltLogRounds()
    {
         int rounds = Integer.getInteger(GENSALT_LOG2_ROUNDS_PROPERTY, 10);
         if (rounds < 4 || rounds > 30)
         throw new ConfigurationException(String.format("Bad value for system property -D%s." +
                                                        "Please use a value between 4 and 30 inclusively",
                                                        GENSALT_LOG2_ROUNDS_PROPERTY));
         return rounds;
    }

    private SelectStatement loadRoleStatement;
    private SelectStatement loadRoleMembersStatement;

    private final Set<Option> supportedOptions;
    private final Set<Option> alterableOptions;

    // Will be set to true when all nodes in the cluster are on a version which supports roles (i.e. 2.2+)
    private volatile boolean isClusterReady = false;

    public CassandraRoleManager()
    {
        supportedOptions = DatabaseDescriptor.getAuthenticator().getClass() == PasswordAuthenticator.class
                         ? ImmutableSet.of(Option.LOGIN, Option.SUPERUSER, Option.PASSWORD)
                         : ImmutableSet.of(Option.LOGIN, Option.SUPERUSER);
        alterableOptions = DatabaseDescriptor.getAuthenticator().getClass().equals(PasswordAuthenticator.class)
                         ? ImmutableSet.of(Option.PASSWORD)
                         : ImmutableSet.<Option>of();
    }

    public void setup()
    {
        loadRoleStatement = (SelectStatement) prepare("SELECT * from %s.%s WHERE role = ?",
                                                      SchemaConstants.AUTH_KEYSPACE_NAME,
                                                      AuthKeyspace.ROLES);

        loadRoleMembersStatement = (SelectStatement) prepare("SELECT member FROM %s.%s WHERE role = ?",
                                                      SchemaConstants.AUTH_KEYSPACE_NAME,
                                                      AuthKeyspace.ROLE_MEMBERS);
        scheduleSetupTask(() -> {
            setupDefaultRole();
            return null;
        });
    }

    public Set<Option> supportedOptions()
    {
        return supportedOptions;
    }

    public Set<Option> alterableOptions()
    {
        return alterableOptions;
    }

    public void createRole(AuthenticatedUser performer, RoleResource role, RoleOptions options)
    throws RequestValidationException, RequestExecutionException
    {
        String insertCql = options.getPassword().isPresent()
                         ? String.format("INSERT INTO %s.%s (role, is_superuser, can_login, salted_hash) VALUES ('%s', %s, %s, '%s')",
                                         SchemaConstants.AUTH_KEYSPACE_NAME,
                                         AuthKeyspace.ROLES,
                                         escape(role.getRoleName()),
                                         options.getSuperuser().or(false),
                                         options.getLogin().or(false),
                                         escape(hashpw(options.getPassword().get())))
                         : String.format("INSERT INTO %s.%s (role, is_superuser, can_login) VALUES ('%s', %s, %s)",
                                         SchemaConstants.AUTH_KEYSPACE_NAME,
                                         AuthKeyspace.ROLES,
                                         escape(role.getRoleName()),
                                         options.getSuperuser().or(false),
                                         options.getLogin().or(false));
        process(insertCql, consistencyForRole(role.getRoleName()));
    }

    public void dropRole(AuthenticatedUser performer, RoleResource role) throws RequestValidationException, RequestExecutionException
    {
        process(String.format("DELETE FROM %s.%s WHERE role = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.ROLES,
                              escape(role.getRoleName())),
                consistencyForRole(role.getRoleName()));
        removeAllMembers(role.getRoleName());
    }

    public void alterRole(AuthenticatedUser performer, RoleResource role, RoleOptions options)
    {
        // Unlike most of the other data access methods here, this does not use a
        // prepared statement in order to allow the set of assignments to be variable.
        String assignments = optionsToAssignments(options.getOptions());
        if (!Strings.isNullOrEmpty(assignments))
        {
            process(String.format("UPDATE %s.%s SET %s WHERE role = '%s'",
                                  SchemaConstants.AUTH_KEYSPACE_NAME,
                                  AuthKeyspace.ROLES,
                                  assignments,
                                  escape(role.getRoleName())),
                    consistencyForRole(role.getRoleName()));
        }
    }

    public void grantRole(AuthenticatedUser performer, RoleResource role, RoleResource grantee)
    throws RequestValidationException, RequestExecutionException
    {
        if (getRoles(grantee, true).contains(role))
            throw new InvalidRequestException(String.format("%s is a member of %s",
                                                            grantee.getRoleName(),
                                                            role.getRoleName()));
        if (getRoles(role, true).contains(grantee))
            throw new InvalidRequestException(String.format("%s is a member of %s",
                                                            role.getRoleName(),
                                                            grantee.getRoleName()));

        modifyRoleMembership(grantee.getRoleName(), role.getRoleName(), "+");
        process(String.format("INSERT INTO %s.%s (role, member) values ('%s', '%s')",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.ROLE_MEMBERS,
                              escape(role.getRoleName()),
                              escape(grantee.getRoleName())),
                consistencyForRole(role.getRoleName()));
    }

    public void revokeRole(AuthenticatedUser performer, RoleResource role, RoleResource revokee)
    throws RequestValidationException, RequestExecutionException
    {
        if (!getRoles(revokee, false).contains(role))
            throw new InvalidRequestException(String.format("%s is not a member of %s",
                                                            revokee.getRoleName(),
                                                            role.getRoleName()));

        modifyRoleMembership(revokee.getRoleName(), role.getRoleName(), "-");
        process(String.format("DELETE FROM %s.%s WHERE role = '%s' and member = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.ROLE_MEMBERS,
                              escape(role.getRoleName()),
                              escape(revokee.getRoleName())),
                consistencyForRole(role.getRoleName()));
    }

    public Set<RoleResource> getRoles(RoleResource grantee, boolean includeInherited)
    throws RequestValidationException, RequestExecutionException
    {
        return collectRoles(getRole(grantee.getRoleName()),
                            includeInherited,
                            filter())
               .map(r -> r.resource)
               .collect(Collectors.toSet());
    }

    public Set<Role> getRoleDetails(RoleResource grantee)
    {
        return collectRoles(getRole(grantee.getRoleName()),
                            true,
                            filter())
               .collect(Collectors.toSet());
    }

    public Set<RoleResource> getMembersOf(RoleResource role)
    {
        // Get the membership list of the given role
        QueryOptions options = QueryOptions.forInternalCalls(consistencyForRole(role.getRoleName()),
                                                             Collections.singletonList(ByteBufferUtil.bytes(role.getRoleName())));
        ResultMessage.Rows rows = select(loadRoleMembersStatement, options);
        UntypedResultSet resultSet = UntypedResultSet.create(rows.result);

        return StreamSupport.stream(resultSet.spliterator(), false)
               .map(row -> RoleResource.role(row.getString("member")))
               .collect(Collectors.toSet());
    }

    public Set<RoleResource> getAllRoles() throws RequestValidationException, RequestExecutionException
    {
        ImmutableSet.Builder<RoleResource> builder = ImmutableSet.builder();
        UntypedResultSet rows = process(String.format("SELECT role from %s.%s",
                                                      SchemaConstants.AUTH_KEYSPACE_NAME,
                                                      AuthKeyspace.ROLES),
                                        ConsistencyLevel.QUORUM);
        rows.forEach(row -> builder.add(RoleResource.role(row.getString("role"))));
        return builder.build();
    }

    public boolean isSuper(RoleResource role)
    {
        try
        {
            return getRole(role.getRoleName()).isSuper;
        }
        catch (RequestExecutionException e)
        {
            logger.debug("Failed to authorize {} for super-user permission", role.getRoleName());
            throw new UnauthorizedException("Unable to perform authorization of super-user permission: " + e.getMessage(), e);
        }
    }

    public boolean canLogin(RoleResource role)
    {
        try
        {
            return getRole(role.getRoleName()).canLogin;
        }
        catch (RequestExecutionException e)
        {
            logger.debug("Failed to authorize {} for login permission", role.getRoleName());
            throw new UnauthorizedException("Unable to perform authorization of login permission: " + e.getMessage(), e);
        }
    }

    public Map<String, String> getCustomOptions(RoleResource role)
    {
        return Collections.emptyMap();
    }

    public boolean isExistingRole(RoleResource role)
    {
        return !Roles.isNullRole(getRole(role.getRoleName()));
    }

    public Set<? extends IResource> protectedResources()
    {
        return ImmutableSet.of(DataResource.table(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES),
                               DataResource.table(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_MEMBERS));
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    /*
     * Create the default superuser role to bootstrap role creation on a clean system. Preemptively
     * gives the role the default password so PasswordAuthenticator can be used to log in (if
     * configured)
     */
    private static void setupDefaultRole()
    {
        if (StorageService.instance.getTokenMetadata().sortedTokens().isEmpty())
            throw new IllegalStateException("CassandraRoleManager skipped default role setup: no known tokens in ring");

        try
        {
            if (!hasExistingRoles())
            {
                QueryProcessor.process(String.format("INSERT INTO %s.%s (role, is_superuser, can_login, salted_hash) " +
                                                     "VALUES ('%s', %s, %s, '%s') USING TIMESTAMP %s",
                                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                                     AuthKeyspace.ROLES,
                                                     DEFAULT_SUPERUSER_NAME,
                                                     SKIP_DEFAULT_ROLE_SETUP ? "false" : "true",
                                                     SKIP_DEFAULT_ROLE_SETUP ? "false" : "true",
                                                     SKIP_DEFAULT_ROLE_SETUP ? "" : escape(hashpw(DEFAULT_SUPERUSER_PASSWORD)),
                                                     SKIP_DEFAULT_ROLE_SETUP ? "1" : "0"),
                                       consistencyForRole(DEFAULT_SUPERUSER_NAME));
                logger.info("Created default superuser role '{}'", DEFAULT_SUPERUSER_NAME);
            }
        }
        catch (RequestExecutionException e)
        {
            logger.warn("CassandraRoleManager skipped default role setup: some nodes were not ready");
            throw e;
        }
    }

    @VisibleForTesting
    public static boolean hasExistingRoles() throws RequestExecutionException
    {
        // Try looking up the 'cassandra' default role first, to avoid the range query if possible.
        String defaultSUQuery = String.format("SELECT * FROM %s.%s WHERE role = '%s'", SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES, DEFAULT_SUPERUSER_NAME);
        String allUsersQuery = String.format("SELECT * FROM %s.%s LIMIT 1", SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES);
        return !QueryProcessor.process(defaultSUQuery, ConsistencyLevel.ONE).isEmpty()
               || !QueryProcessor.process(defaultSUQuery, ConsistencyLevel.QUORUM).isEmpty()
               || !QueryProcessor.process(allUsersQuery, ConsistencyLevel.QUORUM).isEmpty();
    }

    protected void scheduleSetupTask(final Callable<Void> setupTask)
    {
        // The delay is to give the node a chance to see its peers before attempting the operation
        ScheduledExecutors.optionalTasks.schedule(() -> {
            isClusterReady = true;
            try
            {
                setupTask.call();
            }
            catch (Exception e)
            {
                logger.info("Setup task failed with error, rescheduling");
                scheduleSetupTask(setupTask);
            }
        }, AuthKeyspace.SUPERUSER_SETUP_DELAY, TimeUnit.MILLISECONDS);
    }

    private CQLStatement prepare(String template, String keyspace, String table)
    {
        try
        {
            return QueryProcessor.parseStatement(String.format(template, keyspace, table)).prepare(ClientState.forInternalCalls());
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
    }

    private Stream<Role> collectRoles(Role role, boolean includeInherited, Predicate<String> distinctFilter)
    {
        if (Roles.isNullRole(role))
            return Stream.empty();

        if (!includeInherited)
            return Stream.concat(Stream.of(role), role.memberOf.stream().map(this::getRole));


        return Stream.concat(Stream.of(role),
                             role.memberOf.stream()
                                          .filter(distinctFilter)
                                          .flatMap(r -> collectRoles(getRole(r), true, distinctFilter)));
    }

    // Used as a stateful filtering function when recursively collecting granted roles
    private static Predicate<String> filter()
    {
        final Set<String> seen = new HashSet<>();
        return seen::add;
    }

    /*
     * Get a single Role instance given the role name. This never returns null, instead it
     * uses a null object when a role with the given name cannot be found. So
     * it's always safe to call methods on the returned object without risk of NPE.
     */
    private Role getRole(String name)
    {
        QueryOptions options = QueryOptions.forInternalCalls(consistencyForRole(name),
                                                             Collections.singletonList(ByteBufferUtil.bytes(name)));
        ResultMessage.Rows rows = select(loadRoleStatement, options);
        if (rows.result.isEmpty())
            return Roles.nullRole();

        return ROW_TO_ROLE.apply(UntypedResultSet.create(rows.result).one());
    }

    /*
     * Adds or removes a role name from the membership list of an entry in the roles table table
     * (adds if op is "+", removes if op is "-")
     */
    private void modifyRoleMembership(String grantee, String role, String op)
    throws RequestExecutionException
    {
        process(String.format("UPDATE %s.%s SET member_of = member_of %s {'%s'} WHERE role = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.ROLES,
                              op,
                              escape(role),
                              escape(grantee)),
                consistencyForRole(grantee));
    }

    /*
     * Clear the membership list of the given role
     */
    private void removeAllMembers(String role) throws RequestValidationException, RequestExecutionException
    {
        // Get the membership list of the the given role
        UntypedResultSet rows = process(String.format("SELECT member FROM %s.%s WHERE role = '%s'",
                                                      SchemaConstants.AUTH_KEYSPACE_NAME,
                                                      AuthKeyspace.ROLE_MEMBERS,
                                                      escape(role)),
                                        consistencyForRole(role));
        if (rows.isEmpty())
            return;

        // Update each member in the list, removing this role from its own list of granted roles
        for (UntypedResultSet.Row row : rows)
            modifyRoleMembership(row.getString("member"), role, "-");

        // Finally, remove the membership list for the dropped role
        process(String.format("DELETE FROM %s.%s WHERE role = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.ROLE_MEMBERS,
                              escape(role)),
                consistencyForRole(role));
    }

    /*
     * Convert a map of Options from a CREATE/ALTER statement into
     * assignment clauses used to construct a CQL UPDATE statement
     */
    private String optionsToAssignments(Map<Option, Object> options)
    {
        return options.entrySet()
                      .stream()
                      .map(entry ->
                           {
                               switch (entry.getKey())
                               {
                                   case LOGIN:
                                       return String.format("can_login = %s", entry.getValue());
                                   case SUPERUSER:
                                       return String.format("is_superuser = %s", entry.getValue());
                                   case PASSWORD:
                                       return String.format("salted_hash = '%s'", escape(hashpw((String) entry.getValue())));
                                   default:
                                       return null;
                               }
                           })
                      .filter(Objects::nonNull)
                      .collect(Collectors.joining(","));
    }

    protected static ConsistencyLevel consistencyForRole(String role)
    {
        if (role.equals(DEFAULT_SUPERUSER_NAME))
            return ConsistencyLevel.QUORUM;
        else
            return ConsistencyLevel.LOCAL_ONE;
    }

    private static String hashpw(String password)
    {
        return BCrypt.hashpw(password, BCrypt.gensalt(GENSALT_LOG2_ROUNDS));
    }

    private static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    /**
     * Executes the provided query.
     * This shouldn't be used during setup as this will directly return an error if the manager is not setup yet. Setup tasks
     * should use QueryProcessor.process directly.
     */
    @VisibleForTesting
    UntypedResultSet process(String query, ConsistencyLevel consistencyLevel)
    throws RequestValidationException, RequestExecutionException
    {
        if (!isClusterReady)
            throw new InvalidRequestException("Cannot process role related query as the role manager isn't yet setup. "
                                            + "This is likely because some of nodes in the cluster are on version 2.1 or earlier. "
                                            + "You need to upgrade all nodes to Cassandra 2.2 or more to use roles.");

        return QueryProcessor.process(query, consistencyLevel);
    }

    @VisibleForTesting
    ResultMessage.Rows select(SelectStatement statement, QueryOptions options)
    {
        return statement.execute(QueryState.forInternalCalls(), options, System.nanoTime());
    }

}
