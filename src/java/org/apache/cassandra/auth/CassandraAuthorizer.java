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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

/**
 * CassandraAuthorizer is an IAuthorizer implementation that keeps
 * user permissions internally in C* using the system_auth.role_permissions
 * table.
 */
public class CassandraAuthorizer implements IAuthorizer
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraAuthorizer.class);

    // Permission.valueOf without the exception (and its stack trace) for every unknown name on the authorization path
    private static final Map<String, Permission> PERMISSIONS_BY_NAME = Arrays.stream(Permission.values())
                                                                             .collect(Collectors.toMap(Enum::name, p -> p));
    // names found in role_permissions that this software does not know, logged once each (see permissions/knownResource)
    private static final Set<String> unknownPermissionsLogged = ConcurrentHashMap.newKeySet();
    private static final Set<String> unknownResourcePrefixesLogged = ConcurrentHashMap.newKeySet();
    private static final Set<String> unparseableResourcesLogged = ConcurrentHashMap.newKeySet();

    // DataStax Enterprise 6.8 and 6.9 have these two permissions besides MODIFY, and write them together on a data
    // resource for GRANT ALL and for the permissions of a resource's creator (the creator's rows of the DSE 6.8.65 test
    // fixtures; GRANT ALL, and DSE 6.9, observed on single nodes); DSE's GRANT MODIFY writes the name MODIFY, its
    // REVOKE MODIFY removes that name only, and its REVOKE UPDATE and REVOKE TRUNCATE never remove MODIFY. On DSE,
    // UPDATE alone allows inserts but not truncate, TRUNCATE alone truncate but not inserts. Here, together they are
    // read as MODIFY, one alone is dropped (MODIFY would also grant the other) and listed, see permissions(); in the
    // DSE 6 compatibility mode GRANT MODIFY writes the two, see grant()
    private static final String DSE_UPDATE = "UPDATE";
    private static final String DSE_TRUNCATE = "TRUNCATE";
    private static final List<String> DSE_MODIFY = ImmutableList.of(DSE_UPDATE, DSE_TRUNCATE);
    // the permissions of this software that DSE 6 does not have: DSE's own are CREATE, ALTER, DROP, SELECT, MODIFY,
    // AUTHORIZE, DESCRIBE, EXECUTE, UPDATE, TRUNCATE and BACKUP, and how it reads a name it does not have is not known,
    // so they are not written on data resources, the only ones with such permissions, to the table DSE nodes read in
    // the DSE 6 compatibility mode, see grant()
    private static final Set<Permission> UNKNOWN_TO_DSE = Sets.immutableEnumSet(EnumSet.complementOf(EnumSet.of(Permission.CREATE,
                                                                                                                 Permission.ALTER,
                                                                                                                 Permission.DROP,
                                                                                                                 Permission.SELECT,
                                                                                                                 Permission.MODIFY,
                                                                                                                 Permission.AUTHORIZE,
                                                                                                                 Permission.DESCRIBE,
                                                                                                                 Permission.EXECUTE)));
    private static final AtomicBoolean dseModifyPairLogged = new AtomicBoolean();
    // (role, resource) -> the DSE name granted alone on it, as last read on this node
    private static final ConcurrentMap<Pair<String, String>, String> dseLoneWriteGrants = new ConcurrentHashMap<>();
    // how dseLoneWriteGrants was filled, for nodetool compatstatus (see loneDseWriteGrantsSource): set by each scan of
    // role_permissions (see dseLoneWriteGrantsScanned); null before the first one
    private static volatile String dseLoneWriteGrantsScan;

    private static final String ROLE = "role";
    private static final String RESOURCE = "resource";
    private static final String PERMISSIONS = "permissions";

    private SelectStatement authorizeRoleStatement;

    public CassandraAuthorizer()
    {
    }

    /**
     * Whether the reverse index {@code system_auth.resource_role_permissons_index} is kept up to date. In the DSE 6
     * compatibility mode {@code system_auth} keeps the definition DSE 6 wrote (see {@code DseSystemKeyspaces}), which
     * may lack the table (the 6.9.26 schema capture has none): then GRANT and REVOKE update the permissions only, DROP
     * ROLE deletes them, and the roles holding permissions on a dropped resource are found by filtering the
     * permissions. When DSE's definition has the table, it is maintained as without the mode, so that DSE's own index
     * stays complete.
     * <p>
     * An index created at the first start with {@code internode_compatibility_mode: NONE} holds none of the grants made
     * before (on DSE or in the mode): until it is rebuilt, dropping a table, keyspace or function leaves those grants
     * in {@code role_permissions}, and a resource created again with the same name gets them back. It must be rebuilt
     * after the last restart with {@code NONE} and before any DROP, for instance with cqlsh:
     * {@code COPY system_auth.role_permissions (resource, role) TO 'f.csv'} then
     * {@code COPY system_auth.resource_role_permissons_index (resource, role) FROM 'f.csv'}.
     */
    private static boolean maintainsResourceIndex()
    {
        return !DatabaseDescriptor.isDse6CompatibilityMode() || Schema.instance.getTableMetadata(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.RESOURCE_ROLE_INDEX) != null;
    }

    // Returns every permission on the resource granted to the user either directly
    // or indirectly via roles granted to the user.
    public Set<Permission> authorize(AuthenticatedUser user, IResource resource)
    {
        try
        {
            if (user.isSuper())
                return resource.applicablePermissions();

            Set<Permission> permissions = EnumSet.noneOf(Permission.class);

            // Even though we only care about the RoleResource here, we use getRoleDetails as
            // it saves a Set creation in RolesCache
            for (Role role: user.getRoleDetails())
                addPermissionsForRole(permissions, resource, role.resource);
            return permissions;
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            logger.debug("Failed to authorize {} for {}", user, resource);
            throw new UnauthorizedException("Unable to perform authorization of permissions: " + e.getMessage(), e);
        }
    }

    /**
     * Grants the permissions the role does not have yet. The permissions {@code UPDATE} + {@code TRUNCATE} on a data
     * resource, which DataStax Enterprise writes together for {@code GRANT ALL} and a creator's permissions, are read
     * as {@code MODIFY}, which then counts as held. In the DSE 6 compatibility mode, a {@code MODIFY} granted on a data
     * resource is written as {@code UPDATE} + {@code TRUNCATE} (DSE's own {@code GRANT MODIFY} writes the name
     * {@code MODIFY}), which this software reads as {@code MODIFY} in every mode: the DSE nodes of the cluster, which
     * read the same replicated table, and a node rolled back to DSE grant it too. DSE's {@code REVOKE MODIFY} removes
     * the name {@code MODIFY} only and leaves the two names, so the role keeps its writes and truncate there and here:
     * a {@code MODIFY} granted by this node in the mode is revoked by {@code REVOKE MODIFY} on a node of this software
     * (see {@link #revoke} for what it removes), or on a DSE node by {@code REVOKE UPDATE} and {@code REVOKE TRUNCATE}
     * (and {@code REVOKE MODIFY}, when the row also holds the name {@code MODIFY}). The two names are written whenever
     * {@code MODIFY} is granted in the mode, even if it looks held, since the row is read at {@code LOCAL_ONE}.
     * <p>
     * In the mode, the permissions DSE 6 does not have ({@code UNMASK}, {@code SELECT_MASKED}) are not written on a data
     * resource either, since how a DSE node reads a name it does not have is not known: {@code GRANT ALL}, and the
     * permissions granted to the creator of a keyspace or table, grant the others, and the client gets a warning naming
     * those not granted, also logged at INFO (they are returned as granted, so that {@code GRANT} does not report them
     * as already granted: the other callers ignore the result); a {@code GRANT} naming them is refused, unless it names
     * every permission of the resource, which is {@code GRANT ALL}. Without the mode only this software's names are
     * written, as before.
     * <p>
     * The row read to find the permissions held leaves the list of lone grants as it is, and does not log the
     * {@code UPDATE} or {@code TRUNCATE} it holds alone nor the pair it reads as {@code MODIFY}: the authorizer's next
     * read of the row lists and logs a lone name (unless this grant covers it), and logs the pair if that one-off
     * message has not been logged yet.
     *
     * @throws InvalidRequestException in the DSE 6 compatibility mode, for a {@code GRANT} naming a permission DSE 6
     *                                 does not have on a data resource, other than {@code GRANT ALL} (or a
     *                                 {@code GRANT} naming every permission of the resource, the same set)
     */
    public Set<Permission> grant(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource grantee)
    throws RequestValidationException, RequestExecutionException
    {
        Set<Permission> unknownToDse = writesDseModify(resource) ? Sets.intersection(permissions, UNKNOWN_TO_DSE) : Collections.emptySet();
        if (!unknownToDse.isEmpty() && !permissions.equals(resource.applicablePermissions()))
            throw new InvalidRequestException(String.format("%s cannot be granted with internode_compatibility_mode DSE_6: DataStax Enterprise, which reads " +
                                                            "the same %s.%s, does not have %s. Grant %s once the upgrade is complete " +
                                                            "(internode_compatibility_mode NONE on every node)",
                                                            joined(unknownToDse), SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS,
                                                            unknownToDse.size() == 1 ? "it" : "them", unknownToDse.size() == 1 ? "it" : "them"));

        Set<String> storedNames = storedPermissionNames(grantee, resource);
        Set<Permission> existingPermissions = existingPermissions(grantee, resource, storedNames, permissions);
        Set<Permission> nonExistingPermissions = Sets.difference(permissions, existingPermissions);

        boolean dseForm = permissions.contains(Permission.MODIFY) && writesDseModify(resource);
        List<String> names = new ArrayList<>(nonExistingPermissions.size() + DSE_MODIFY.size());
        Set<Permission> notGranted = EnumSet.noneOf(Permission.class);
        for (Permission permission : nonExistingPermissions)
        {
            if (unknownToDse.contains(permission))
                notGranted.add(permission);
            else if (!dseForm || permission != Permission.MODIFY)
                names.add(permission.name());
        }
        if (dseForm)
            names.addAll(DSE_MODIFY);
        if (!notGranted.isEmpty())
        {
            String message = String.format("%s not granted to role '%s' on %s: with internode_compatibility_mode DSE_6 only the " +
                                           "permissions DataStax Enterprise also has are written. Grant %s once the upgrade is complete " +
                                           "(internode_compatibility_mode NONE on every node)",
                                           joined(notGranted), grantee.getRoleName(), resource.getName(), notGranted.size() == 1 ? "it" : "them");
            logger.info(message);
            ClientWarn.instance.warn(message);
        }

        if (!names.isEmpty())
        {
            modifyRolePermissions(names, resource, grantee, "+");
            if (maintainsResourceIndex())
                addLookupEntry(resource, grantee);
            // the row now holds MODIFY: a DSE name it held alone is covered
            if (permissions.contains(Permission.MODIFY))
                forgetLoneDseWriteGrant(grantee.getRoleName(), resource.getName());
        }

        return nonExistingPermissions;
    }

    /**
     * Revokes the permissions the role has. Revoking {@code MODIFY} on a data resource also removes DSE's
     * {@code UPDATE} and {@code TRUNCATE} from the row, together or alone: otherwise the DSE nodes of the cluster (and
     * a node rolled back to DSE) would keep granting them, and a pair left behind would still read as {@code MODIFY}
     * here. In the DSE 6 compatibility mode both names, and the name {@code MODIFY} (what DSE's {@code GRANT MODIFY}
     * writes), are always removed, because the row is read at {@code LOCAL_ONE} and may miss what a DSE node just
     * granted: every {@code REVOKE MODIFY} on a data resource then writes the removal of the three names, a role
     * without them included (a write and its cell tombstones for nothing). Without the mode only the
     * names read are removed, so a row without them is written as before. DSE names removed while the role did not
     * hold {@code MODIFY} here are not reported as revoked, but the client gets a warning naming them, which is also
     * logged at INFO; the row read leaves the list of lone grants as it is, as for {@link #grant}. On other
     * resources, where whether DSE writes the two is not known, the two names are left alone like any unknown
     * name.
     */
    public Set<Permission> revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource revokee)
    throws RequestValidationException, RequestExecutionException
    {
        Set<String> storedNames = storedPermissionNames(revokee, resource);
        Set<Permission> existingPermissions = existingPermissions(revokee, resource, storedNames, permissions);

        List<String> names = names(existingPermissions);
        List<String> unhonouredDseNames = new ArrayList<>(DSE_MODIFY.size());
        if (permissions.contains(Permission.MODIFY) && resource instanceof DataResource)
        {
            boolean always = writesDseModify(resource);
            if (always && !existingPermissions.contains(Permission.MODIFY))
                names.add(Permission.MODIFY.name());
            for (String dseName : DSE_MODIFY)
            {
                boolean stored = storedNames.contains(dseName);
                if (always || stored)
                    names.add(dseName);
                if (stored && !existingPermissions.contains(Permission.MODIFY))
                    unhonouredDseNames.add(dseName);
            }
        }

        if (!names.isEmpty())
        {
            modifyRolePermissions(names, resource, revokee, "-");
            // as before when a permission of this software was revoked; also when DSE names alone were, and nothing is left
            if ((!existingPermissions.isEmpty() || (!unhonouredDseNames.isEmpty() && names.containsAll(storedNames))) && maintainsResourceIndex())
                removeLookupEntry(resource, revokee);
            if (permissions.contains(Permission.MODIFY) && resource instanceof DataResource)
                forgetLoneDseWriteGrant(revokee.getRoleName(), resource.getName());
        }

        // at most one name: UPDATE and TRUNCATE stored together are read as MODIFY, which the role then holds
        if (!unhonouredDseNames.isEmpty())
        {
            String message = String.format("Revoked %s (a DataStax Enterprise permission), granted alone to role '%s' on %s, which this software did not honour",
                                           String.join(" and ", unhonouredDseNames), revokee.getRoleName(), resource.getName());
            logger.info(message);
            ClientWarn.instance.warn(message);
        }

        return existingPermissions;
    }

    // the names of the permissions, in their order, joined by "and": "UNMASK and SELECT_MASKED"
    private static String joined(Set<Permission> permissions)
    {
        return permissions.stream().sorted().map(Permission::name).collect(Collectors.joining(" and "));
    }

    // whether MODIFY is written as UPDATE + TRUNCATE (what DSE writes for GRANT ALL and a creator's permissions), and
    // only the permissions DSE has are written: in the DSE 6 compatibility mode, on data resources
    private static boolean writesDseModify(IResource resource)
    {
        return DatabaseDescriptor.isDse6CompatibilityMode() && resource instanceof DataResource;
    }

    // Called when deleting a role with DROP ROLE query.
    // Internal hook, so no permission checks are needed here.
    // Executes a logged batch removing the granted premissions
    // for the role as well as the entries from the reverse index
    // table
    public void revokeAllFrom(RoleResource revokee)
    {
        try
        {
            List<CQLStatement> statements = new ArrayList<>();
            // without the reverse index (DSE 6 compatibility mode on a DSE system_auth that has none) only the permissions are deleted
            if (maintainsResourceIndex())
            {
                UntypedResultSet rows = process(String.format("SELECT resource FROM %s.%s WHERE role = '%s'",
                                                              SchemaConstants.AUTH_KEYSPACE_NAME,
                                                              AuthKeyspace.ROLE_PERMISSIONS,
                                                              escape(revokee.getRoleName())),
                                                authReadConsistencyLevel());

                for (UntypedResultSet.Row row : rows)
                {
                    statements.add(
                        QueryProcessor.getStatement(String.format("DELETE FROM %s.%s WHERE resource = '%s' AND role = '%s'",
                                                                  SchemaConstants.AUTH_KEYSPACE_NAME,
                                                                  AuthKeyspace.RESOURCE_ROLE_INDEX,
                                                                  escape(row.getString("resource")),
                                                                  escape(revokee.getRoleName())),
                                                    ClientState.forInternalCalls()));

                }
            }

            statements.add(QueryProcessor.getStatement(String.format("DELETE FROM %s.%s WHERE role = '%s'",
                                                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                                                     AuthKeyspace.ROLE_PERMISSIONS,
                                                                     escape(revokee.getRoleName())),
                                                       ClientState.forInternalCalls()));

            executeLoggedBatch(statements);
            if (!dseLoneWriteGrants.isEmpty())
                dseLoneWriteGrants.keySet().removeIf(key -> key.left.equals(revokee.getRoleName()));
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            logger.warn(String.format("CassandraAuthorizer failed to revoke all permissions of %s", revokee.getRoleName()), e);
        }
    }

    // Called after a resource is removed (DROP KEYSPACE, DROP TABLE, etc.).
    // Execute a logged batch removing all the permissions for the resource
    // as well as the index table entry
    @Override
    public Set<RoleResource> revokeAllOn(IResource droppedResource)
    {
        Set<RoleResource> affectedRoles = new HashSet<>();
        try
        {
            // without the reverse index (DSE 6 compatibility mode on a DSE system_auth that has none) the roles are found in the permissions
            boolean resourceIndex = maintainsResourceIndex();
            UntypedResultSet rows = resourceIndex
                                    ? process(String.format("SELECT role FROM %s.%s WHERE resource = '%s'",
                                                            SchemaConstants.AUTH_KEYSPACE_NAME,
                                                            AuthKeyspace.RESOURCE_ROLE_INDEX,
                                                            escape(droppedResource.getName())),
                                              authReadConsistencyLevel())
                                    : process(String.format("SELECT role FROM %s.%s WHERE resource = '%s' ALLOW FILTERING",
                                                            SchemaConstants.AUTH_KEYSPACE_NAME,
                                                            AuthKeyspace.ROLE_PERMISSIONS,
                                                            escape(droppedResource.getName())),
                                              authReadConsistencyLevel());

            List<CQLStatement> statements = new ArrayList<>();
            for (UntypedResultSet.Row row : rows)
            {
                String role = row.getString("role");
                affectedRoles.add(RoleResource.role(role));
                statements.add(QueryProcessor.getStatement(String.format("DELETE FROM %s.%s WHERE role = '%s' AND resource = '%s'",
                                                                         SchemaConstants.AUTH_KEYSPACE_NAME,
                                                                         AuthKeyspace.ROLE_PERMISSIONS,
                                                                         escape(role),
                                                                         escape(droppedResource.getName())),
                                                           ClientState.forInternalCalls()));
            }

            if (resourceIndex)
                statements.add(QueryProcessor.getStatement(String.format("DELETE FROM %s.%s WHERE resource = '%s'",
                                                                         SchemaConstants.AUTH_KEYSPACE_NAME,
                                                                         AuthKeyspace.RESOURCE_ROLE_INDEX,
                                                                         escape(droppedResource.getName())),
                                                           ClientState.forInternalCalls()));

            if (!statements.isEmpty())
                executeLoggedBatch(statements);
            if (!dseLoneWriteGrants.isEmpty())
                dseLoneWriteGrants.keySet().removeIf(key -> key.right.equals(droppedResource.getName()));
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            logger.warn(String.format("CassandraAuthorizer failed to revoke all permissions on %s", droppedResource), e);
        }

        return affectedRoles;
    }

    /**
     * The permission names stored for the role on the resource, as written (DataStax Enterprise's names included),
     * or an empty set when there is no row or the row has no permissions (DSE writes its rows with INSERT, so a row
     * can exist with a null set: it used to fail GRANT and REVOKE with a NullPointerException).
     */
    private Set<String> storedPermissionNames(RoleResource role, IResource resource)
    {
        UntypedResultSet rs = process(String.format("SELECT permissions FROM %s.%s WHERE role = '%s' AND resource = '%s'",
                                                    SchemaConstants.AUTH_KEYSPACE_NAME,
                                                    AuthKeyspace.ROLE_PERMISSIONS,
                                                    escape(role.getRoleName()),
                                                    escape(resource.getName())),
                                      ConsistencyLevel.LOCAL_ONE);

        if (rs.isEmpty())
            return Collections.emptySet();

        Row one = rs.one();
        return one.has(PERMISSIONS) ? one.getSet(PERMISSIONS, UTF8Type.instance) : Collections.emptySet();
    }

    /**
     * The expected permissions that the role has on the resource.
     *
     * @param role the role
     * @param resource the resource
     * @param storedNames the permission names stored for the role on the resource, see {@link #storedPermissionNames}
     * @param expectedPermissions the permissions to check for
     * @return The existing permissions
     */
    private static Set<Permission> existingPermissions(RoleResource role,
                                                       IResource resource,
                                                       Set<String> storedNames,
                                                       Set<Permission> expectedPermissions)
    {
        if (storedNames.isEmpty())
            return Collections.emptySet();

        Set<Permission> existingPermissions = Sets.newHashSetWithExpectedSize(expectedPermissions.size());
        // a read for the statement, at LOCAL_ONE: the list of lone DSE names is left as it is, and neither a lone name nor
        // the pair is logged (the authorizer's next read of the row does that; GRANT and REVOKE of MODIFY forget the
        // row's entry themselves)
        for (Permission permission : permissions(role.getRoleName(), resource.getName(), storedNames, STATEMENT_READ))
        {
            if (expectedPermissions.contains(permission))
                existingPermissions.add(permission);
        }
        return existingPermissions;
    }

    // the names of the permissions, in their iteration order, in a list the caller can extend
    private static List<String> names(Set<Permission> permissions)
    {
        List<String> names = new ArrayList<>(permissions.size() + DSE_MODIFY.size());
        for (Permission permission : permissions)
            names.add(permission.name());
        return names;
    }

    private void executeLoggedBatch(List<CQLStatement> statements)
    throws RequestExecutionException, RequestValidationException
    {
        BatchStatement batch = new BatchStatement(null,
                BatchStatement.Type.LOGGED,
                VariableSpecifications.empty(),
                Lists.newArrayList(Iterables.filter(statements, ModificationStatement.class)),
                Attributes.none());
        processBatch(batch);
    }

    // Add every permission on the resource granted to the role
    private void addPermissionsForRole(Set<Permission> permissions, IResource resource, RoleResource role)
    throws RequestExecutionException, RequestValidationException
    {
        QueryOptions options = QueryOptions.forInternalCalls(authReadConsistencyLevel(),
                                                             Lists.newArrayList(ByteBufferUtil.bytes(role.getRoleName()),
                                                                                ByteBufferUtil.bytes(resource.getName())));

        ResultMessage.Rows rows = select(authorizeRoleStatement, options);

        UntypedResultSet result = UntypedResultSet.create(rows.result);

        if (!result.isEmpty() && result.one().has(PERMISSIONS))
            permissions.addAll(permissions(role.getRoleName(), resource.getName(), result.one().getSet(PERMISSIONS, UTF8Type.instance), null));
    }

    // Adds or removes permissions, by name, from a role_permissions table (adds if op is "+", removes if op is "-")
    private void modifyRolePermissions(Collection<String> permissions, IResource resource, RoleResource role, String op)
            throws RequestExecutionException
    {
        process(String.format("UPDATE %s.%s SET permissions = permissions %s {%s} WHERE role = '%s' AND resource = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.ROLE_PERMISSIONS,
                              op,
                              "'" + StringUtils.join(permissions, "','") + "'",
                              escape(role.getRoleName()),
                              escape(resource.getName())),
                authWriteConsistencyLevel());
    }

    // Removes an entry from the inverted index table (from resource -> role with defined permissions)
    private void removeLookupEntry(IResource resource, RoleResource role) throws RequestExecutionException
    {
        process(String.format("DELETE FROM %s.%s WHERE resource = '%s' and role = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.RESOURCE_ROLE_INDEX,
                              escape(resource.getName()),
                              escape(role.getRoleName())),
                authWriteConsistencyLevel());
    }

    // Adds an entry to the inverted index table (from resource -> role with defined permissions)
    private void addLookupEntry(IResource resource, RoleResource role) throws RequestExecutionException
    {
        process(String.format("INSERT INTO %s.%s (resource, role) VALUES ('%s','%s')",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.RESOURCE_ROLE_INDEX,
                              escape(resource.getName()),
                              escape(role.getRoleName())),
                authWriteConsistencyLevel());
    }

    // 'grantee' can be null - in that case everyone's permissions have been requested. Otherwise, only single user's.
    // If the 'performer' requesting 'LIST PERMISSIONS' is not a superuser OR their username doesn't match 'grantee' OR
    // they have no permission to describe all roles OR they have no permission to describe 'grantee', then we throw
    // UnauthorizedException.
    public Set<PermissionDetails> list(AuthenticatedUser performer,
                                       Set<Permission> permissions,
                                       IResource resource,
                                       RoleResource grantee)
    throws RequestValidationException, RequestExecutionException
    {
        if (!performer.isSuper()
            && !performer.isSystem()
            && !performer.getRoles().contains(grantee)
            && !performer.getPermissions(RoleResource.root()).contains(Permission.DESCRIBE)
            && (grantee == null || !performer.getPermissions(grantee).contains(Permission.DESCRIBE)))
            throw new UnauthorizedException(String.format("You are not authorized to view %s's permissions",
                                                          grantee == null ? "everyone" : grantee.getRoleName()));

        if (null == grantee)
            return listPermissionsForRole(permissions, resource, null);

        Set<RoleResource> roles = DatabaseDescriptor.getRoleManager().getRoles(grantee, true);
        Set<PermissionDetails> details = new HashSet<>();
        for (RoleResource role : roles)
            details.addAll(listPermissionsForRole(permissions, resource, role));

        return details;
    }

    private Set<PermissionDetails> listPermissionsForRole(Set<Permission> permissions,
                                                          IResource resource,
                                                          RoleResource role)
    throws RequestExecutionException
    {
        Set<PermissionDetails> details = new HashSet<>();
        for (UntypedResultSet.Row row : process(buildListQuery(resource, role), authReadConsistencyLevel()))
        {
            if (row.has(PERMISSIONS))
            {
                String rowRole = row.getString(ROLE);
                String rowResourceName = row.getString(RESOURCE);
                IResource rowResource = knownResource(rowRole, rowResourceName);
                if (rowResource == null)
                    continue;
                for (Permission permission : permissions(rowRole, rowResourceName, row.getSet(PERMISSIONS, UTF8Type.instance), null))
                {
                    if (permissions.contains(permission))
                        details.add(new PermissionDetails(rowRole, rowResource, permission));
                }
            }
        }
        return details;
    }

    private String buildListQuery(IResource resource, RoleResource grantee)
    {
        List<String> vars = Lists.newArrayList(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS);
        List<String> conditions = new ArrayList<>();

        if (resource != null)
        {
            conditions.add("resource = '%s'");
            vars.add(escape(resource.getName()));
        }

        if (grantee != null)
        {
            conditions.add(ROLE + " = '%s'");
            vars.add(escape(grantee.getRoleName()));
        }

        String query = "SELECT " + ROLE + ", resource, permissions FROM %s.%s";

        if (!conditions.isEmpty())
            query += " WHERE " + StringUtils.join(conditions, " AND ");

        if (resource != null && grantee == null)
            query += " ALLOW FILTERING";

        return String.format(query, vars.toArray());
    }


    public Set<DataResource> protectedResources()
    {
        return ImmutableSet.of(DataResource.table(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS));
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    public void setup()
    {
        authorizeRoleStatement = prepare(ROLE, AuthKeyspace.ROLE_PERMISSIONS);
    }

    /**
     * Reads the permission names of a {@code role_permissions} row as the authorizer does (see {@link #permissions}),
     * without the warning of each lone grant, for a scan of the table that logs them itself, in one warning per pass
     * (those it lists first): the lone {@code UPDATE} or {@code TRUNCATE} the row grants on a data resource is listed,
     * or forgotten when the row no longer holds one alone. A row with no permissions is passed as an empty set.
     *
     * @param newlyListed where the lone grant is added when this read lists it, or lists another name for the row
     *                    (decided by the update of the list itself): like the authorizer's own read, which logs what
     *                    it lists first, the scan then logs it
     * @return the lone grant this read found, {@code role '<role>' on <resource>: <name>}, or null when the row holds
     *         none, or names a resource this software cannot parse
     */
    public static String readDseLoneWriteGrant(String role, String resourceName, Set<String> permissionNames, Collection<String> newlyListed)
    {
        if (knownResource(role, resourceName) == null)
            return null;
        LoneGrantRead read = new LoneGrantRead(false);
        permissions(role, resourceName, permissionNames, read);
        if (read.entry != null && read.newlyListed)
            newlyListed.add(read.entry);
        return read.entry;
    }

    /**
     * A read other than the authorizer's: for a scan, what it found (the lone grant of the row, and whether the read
     * listed it first); for GRANT and REVOKE ({@link #STATEMENT_READ}), nothing: that read leaves the list alone and
     * logs nothing, the pair included.
     */
    private static final class LoneGrantRead
    {
        private final boolean statement;
        private String entry;
        private boolean newlyListed;

        private LoneGrantRead(boolean statement)
        {
            this.statement = statement;
        }
    }

    /** The read of GRANT and REVOKE, which leaves the list of lone grants alone and logs neither a lone grant nor the pair: never written to. */
    private static final LoneGrantRead STATEMENT_READ = new LoneGrantRead(true);

    private static boolean isStatementRead(LoneGrantRead read)
    {
        return read != null && read.statement;
    }

    /**
     * Forgets the lone grants listed by a scan that failed before it could log them, so that the next read of their
     * rows, by the authorizer or a scan, lists and logs them.
     *
     * @param entries the lone grants, as {@link #readDseLoneWriteGrant} returns them
     */
    public static void forgetDseLoneWriteGrants(Collection<String> entries)
    {
        if (entries.isEmpty())
            return;
        Set<String> forgotten = new HashSet<>(entries);
        dseLoneWriteGrants.entrySet().removeIf(e -> forgotten.contains(loneDseWriteGrant(e.getKey(), e.getValue())));
    }

    /**
     * Records how the lone grants were last read by a scan of {@code role_permissions}, for {@code nodetool
     * compatstatus}. A complete scan, a read of the whole table at the authorizer's consistency level, also forgets
     * the entries of the rows it did not find granting one alone (a row that a DSE node deleted, for instance, which
     * no read would otherwise remove); an entry added meanwhile for a row the scan read before a grant is forgotten
     * too, until the row is read again. A lone grant held only in a partition this node keeps but no longer owns
     * (until {@code nodetool cleanup}) is forgotten by each complete scan, until a read of this node's replica lists it
     * again.
     *
     * @param how how the scan was made, as shown by compatstatus
     * @param found the lone grants the scan found, see {@link #readDseLoneWriteGrant}
     * @param complete whether the scan read the whole table at the authorizer's consistency level
     */
    public static void dseLoneWriteGrantsScanned(String how, Collection<String> found, boolean complete)
    {
        if (complete)
        {
            Set<String> seen = new HashSet<>(found);
            dseLoneWriteGrants.entrySet().removeIf(e -> !seen.contains(loneDseWriteGrant(e.getKey(), e.getValue())));
        }
        dseLoneWriteGrantsScan = how;
    }

    private static String loneDseWriteGrant(Pair<String, String> key, String name)
    {
        return String.format("role '%s' on %s: %s", key.left, key.right, name);
    }

    /**
     * The grants of DataStax Enterprise's {@code UPDATE} or {@code TRUNCATE} alone on data resources that this node
     * drops, as last read on this node, as {@code role '<role>' on <resource>: <name>}, sorted; see
     * {@link #loneDseWriteGrantsSource} for how complete it is. Being the last read, it can lag behind the table: a
     * change made through another node shows once this node reads the row again (a GRANT here that races with a read of
     * the row by the permissions cache can leave the entry listed until then), and DROP ROLE and the drop of a resource
     * forget their entries even when rows are left behind (where the reverse index is maintained, a drop finds the rows
     * through it, and it lacks DSE's rows until it is rebuilt). Conversely, an entry a scan adds for a row that a
     * concurrent drop deletes stays listed until the next complete scan (see {@link #dseLoneWriteGrantsScanned}).
     */
    public static List<String> loneDseWriteGrants()
    {
        List<String> result = new ArrayList<>(dseLoneWriteGrants.size());
        for (Map.Entry<Pair<String, String>, String> e : dseLoneWriteGrants.entrySet())
            result.add(loneDseWriteGrant(e.getKey(), e.getValue()));
        Collections.sort(result);
        return result;
    }

    /**
     * How the list of {@link #loneDseWriteGrants} was made: the last scan of {@code role_permissions}, as recorded by
     * {@link #dseLoneWriteGrantsScanned}, then the rows read since; before any scan, only the rows this node has read
     * since it started.
     */
    public static String loneDseWriteGrantsSource()
    {
        String scan = dseLoneWriteGrantsScan;
        return scan != null ? scan : "the rows read since startup";
    }

    private static void forgetLoneDseWriteGrant(String role, String resource)
    {
        if (!dseLoneWriteGrants.isEmpty())
            dseLoneWriteGrants.remove(Pair.create(role, resource));
    }

    @VisibleForTesting
    public static void resetLoneDseWriteGrants()
    {
        dseLoneWriteGrants.clear();
        dseLoneWriteGrantsScan = null;
        dseModifyPairLogged.set(false);
    }

    private SelectStatement prepare(String entityname, String permissionsTable)
    {
        String query = String.format("SELECT permissions FROM %s.%s WHERE %s = ? AND resource = ?",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     permissionsTable,
                                     entityname);
        return (SelectStatement) QueryProcessor.getStatement(query, ClientState.forInternalCalls());
    }

    // We only worry about one character ('). Make sure it's properly escaped.
    private String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    ResultMessage.Rows select(SelectStatement statement, QueryOptions options)
    {
        return statement.execute(QueryState.forInternalCalls(), options, Dispatcher.RequestTime.forImmediateExecution());
    }

    /**
     * This is exposed so we can override the consistency level for tests that are single node
     */
    @VisibleForTesting
    UntypedResultSet process(String query, ConsistencyLevel cl) throws RequestExecutionException
    {
        return QueryProcessor.process(query, cl);
    }

    void processBatch(BatchStatement statement)
    {
        QueryOptions options = QueryOptions.forInternalCalls(authWriteConsistencyLevel(), Collections.emptyList());
        QueryProcessor.instance.processBatch(statement,
                                             QueryState.forInternalCalls(),
                                             BatchQueryOptions.withoutPerStatementVariables(options),
                                             Dispatcher.RequestTime.forImmediateExecution());
    }

    public static ConsistencyLevel authWriteConsistencyLevel()
    {
        return AuthProperties.instance.getWriteConsistencyLevel();
    }

    public static ConsistencyLevel authReadConsistencyLevel()
    {
        return AuthProperties.instance.getReadConsistencyLevel();
    }

    /**
     * Get an initial set of permissions to load into the PermissionsCache at startup
     * @return map of User/Resource -> Permissions for cache initialisation
     */
    public Supplier<Map<Pair<AuthenticatedUser, IResource>, Set<Permission>>> bulkLoader()
    {
        return () ->
        {
            Map<Pair<AuthenticatedUser, IResource>, Set<Permission>> entries = new HashMap<>();
            String cqlTemplate = "SELECT %s, %s, %s FROM %s.%s";

            logger.info("Warming permissions cache from role_permissions table");
            UntypedResultSet results = process(String.format(cqlTemplate,
                                                             ROLE, RESOURCE, PERMISSIONS,
                                                             SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS),
                                                             AuthProperties.instance.getReadConsistencyLevel());

            // role_name -> (resource, permissions)
            Table<String, IResource, Set<Permission>> individualRolePermissions = HashBasedTable.create();
            results.forEach(row -> {
                if (row.has(PERMISSIONS))
                {
                    String role = row.getString(ROLE);
                    String resourceName = row.getString(RESOURCE);
                    IResource resource = knownResource(role, resourceName);
                    if (resource != null)
                        individualRolePermissions.put(role,
                                                      resource,
                                                      permissions(role, resourceName, row.getSet(PERMISSIONS, UTF8Type.instance), null));
                }
            });

            // Iterate all user level roles in the system and accumulate the permissions of their granted roles
            Roles.getAllRoles().forEach(roleResource -> {
                // If the role has login priv, accumulate the permissions of all its granted roles
                if (Roles.canLogin(roleResource))
                {
                    // Structure to accumulate the resource -> permission mappings for the closure of granted roles
                    Map<IResource, ImmutableSet.Builder<Permission>> userPermissions = new HashMap<>();
                    BiConsumer<IResource, Set<Permission>> accumulator = accumulator(userPermissions);

                    // For each role granted to this primary, lookup the specific resource/permissions grants
                    // we read in the first step. We'll accumlate those in the userPermissions map, which we'll turn
                    // into cache entries when we're done.
                    // Note: we need to provide a default empty set of permissions for roles without any explicitly
                    // granted to them (e.g. superusers or roles with no direct perms).
                    Roles.getRoleDetails(roleResource).forEach(grantedRole ->
                                                               individualRolePermissions.rowMap()
                                                                                        .getOrDefault(grantedRole.resource.getRoleName(), Collections.emptyMap())
                                                                                        .forEach(accumulator));

                    // Having iterated all the roles granted to this user, finalize the transitive permissions
                    // (i.e. turn them into entries for the PermissionsCache)
                    userPermissions.forEach((resource, builder) -> entries.put(cacheKey(roleResource, resource),
                                                                               builder.build()));
                }
            });

            return entries;
        };
    }

    // Helper function to group the transitive set of permissions granted
    // to user by the specific resources to which they apply
    private static BiConsumer<IResource, Set<Permission>> accumulator(Map<IResource, ImmutableSet.Builder<Permission>> accumulator)
    {
        return (resource, permissions) -> accumulator.computeIfAbsent(resource, k -> new ImmutableSet.Builder<>()).addAll(permissions);
    }

    /**
     * The permissions of a {@code role_permissions} row that this software knows. Rows written by DataStax Enterprise
     * before an in-place upgrade can name permissions it does not have ({@code BACKUP}, {@code PROXY.LOGIN},
     * {@code SEARCH.*}, …); they are skipped, and logged once per name, instead of failing every authorization of the
     * role. On a data resource DSE writes {@code UPDATE} + {@code TRUNCATE} for {@code GRANT ALL} and a creator's
     * permissions (its {@code GRANT MODIFY} writes the name {@code MODIFY}; see the comment of {@code DSE_UPDATE}): the
     * two together are read as {@code MODIFY}, which covers exactly what they grant (inserts, updates, deletes,
     * truncate). One alone is never widened to {@code MODIFY}, which would also grant the other: it is dropped, logged
     * once per role and resource, and listed by {@code nodetool compatstatus} ({@link #loneDseWriteGrants}), unless the
     * row holds {@code MODIFY} as well. On other resources, where whether DSE writes the two is not known, the two names
     * are unknown names like the others. This applies in every {@code internode_compatibility_mode}: the rows stay in
     * {@code system_auth} after the upgrade is complete; a row without DSE's names is read as before.
     * <p>
     * DSE's {@code RESTRICT}, stored in the {@code restricted} column, is not read: this software has no such control,
     * so a restriction DSE enforces on a role is not enforced here, and the permissions the role's other rows grant
     * apply in full.
     *
     * @param read null for a read of the authorizer, which lists a lone {@code UPDATE} or {@code TRUNCATE} and logs
     *             it when it lists it first; for a read of a scan ({@link #readDseLoneWriteGrant}), which logs them in
     *             one warning per pass instead, where the lone grant found is recorded; {@link #STATEMENT_READ} for the
     *             read of GRANT and REVOKE, which leaves the list as it is and logs nothing, the pair included
     */
    private static Set<Permission> permissions(String role, String resource, Set<String> permissionNames, LoneGrantRead read)
    {
        Set<Permission> permissions = EnumSet.noneOf(Permission.class);
        boolean dseUpdate = false;
        boolean dseTruncate = false;
        boolean dataResource = isDataResourceName(resource);
        for (String name : permissionNames)
        {
            Permission permission = PERMISSIONS_BY_NAME.get(name);
            if (permission != null)
                permissions.add(permission);
            else if (dataResource && DSE_UPDATE.equals(name))
                dseUpdate = true;
            else if (dataResource && DSE_TRUNCATE.equals(name))
                dseTruncate = true;
            else if (unknownPermissionsLogged.add(name))
                logger.warn("Ignoring the unknown permission '{}' granted to role '{}' on resource '{}' in {}.{}: it was probably " +
                            "granted by DataStax Enterprise and does not exist here, so the access it gave is not granted; where an equivalent " +
                            "exists, grant it instead (logged once per permission name)",
                            name, role, resource, SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS);
        }
        if (dseUpdate || dseTruncate)
            mapDseWriteGrants(role, resource, permissions, dseUpdate, dseTruncate, read);
        else if (!isStatementRead(read))
            forgetLoneDseWriteGrant(role, resource);
        return permissions;
    }

    /**
     * The DSE name, {@code UPDATE} or {@code TRUNCATE}, that a {@code role_permissions} row grants alone on a data
     * resource, which {@link #permissions} drops when it reads the row; null when the row grants both, or
     * {@code MODIFY}, or neither, or when the resource is not a data resource. Nothing is read, logged or recorded: for
     * a caller that reads DSE's rows itself, offline for instance, before the upgrade.
     */
    public static String dseLoneWriteGrantName(String resource, Set<String> permissionNames)
    {
        if (!isDataResourceName(resource) || permissionNames.contains(Permission.MODIFY.name()))
            return null;
        boolean update = permissionNames.contains(DSE_UPDATE);
        boolean truncate = permissionNames.contains(DSE_TRUNCATE);
        if (update == truncate)
            return null;
        return update ? DSE_UPDATE : DSE_TRUNCATE;
    }

    /**
     * The names of a {@code role_permissions} row that {@link #permissions} skips as unknown: every name but this
     * software's permissions and, on a data resource, DSE's {@code UPDATE} and {@code TRUNCATE}
     * ({@link #dseLoneWriteGrantName}); sorted. Nothing is logged: for a caller that reads DSE's rows itself.
     */
    public static List<String> unknownPermissionNames(String resource, Set<String> permissionNames)
    {
        boolean dataResource = isDataResourceName(resource);
        List<String> unknown = new ArrayList<>();
        for (String name : permissionNames)
        {
            if (!PERMISSIONS_BY_NAME.containsKey(name) && !(dataResource && DSE_MODIFY.contains(name)))
                unknown.add(name);
        }
        Collections.sort(unknown);
        return unknown;
    }

    /**
     * Whether this software parses the resource name of a {@code role_permissions} row; false for a resource type it
     * does not have or a name it cannot parse, whose rows {@link #knownResource} skips. Nothing is logged: for a caller
     * that reads DSE's rows itself.
     */
    public static boolean isKnownResourceName(String resourceName)
    {
        try
        {
            return Resources.tryFromName(resourceName) != null;
        }
        catch (RuntimeException e)
        {
            // any failure to parse a foreign name, IllegalArgumentException and RequestValidationException included
            return false;
        }
    }

    // the names DataResource gives: "data", "data/<keyspace>", "data/<keyspace>/*", "data/<keyspace>/<table>"
    private static boolean isDataResourceName(String resource)
    {
        return resource.equals("data") || resource.startsWith("data/");
    }

    private static void mapDseWriteGrants(String role, String resource, Set<Permission> permissions, boolean dseUpdate, boolean dseTruncate, LoneGrantRead read)
    {
        if (dseUpdate && dseTruncate)
        {
            // logged only when the pair is what grants MODIFY, and not by the read of GRANT and REVOKE
            if (permissions.add(Permission.MODIFY) && !isStatementRead(read) && dseModifyPairLogged.compareAndSet(false, true))
                logger.info("Reading the permissions UPDATE and TRUNCATE granted together to role '{}' on resource '{}' in {}.{} as MODIFY: " +
                            "DataStax Enterprise writes the two for GRANT ALL and a creator's permissions, and this software for every grant " +
                            "of MODIFY (GRANT MODIFY, GRANT ALL, a creator's permissions) with internode_compatibility_mode DSE_6 (logged once)",
                            role, resource, SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS);
            if (!isStatementRead(read))
                forgetLoneDseWriteGrant(role, resource);
            return;
        }
        if (permissions.contains(Permission.MODIFY))
        {
            if (!isStatementRead(read))
                forgetLoneDseWriteGrant(role, resource);
            return;
        }

        if (isStatementRead(read))
            return;
        String name = dseUpdate ? DSE_UPDATE : DSE_TRUNCATE;
        Pair<String, String> key = Pair.create(role, resource);
        String listed = dseLoneWriteGrants.put(key, name);
        if (read != null)
        {
            read.entry = loneDseWriteGrant(key, name);
            read.newlyListed = !name.equals(listed);
        }
        else if (!name.equals(listed))
            logger.warn("Ignoring the permission '{}' granted alone to role '{}' on resource '{}' in {}.{}: DataStax Enterprise grants it " +
                        "without {}, and this software has MODIFY only, which grants both, so the role loses {}. GRANT MODIFY if the role " +
                        "should also {}, or accept the loss (logged once per role and resource; nodetool compatstatus lists them)",
                        name, role, resource, SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS,
                        dseUpdate ? DSE_TRUNCATE : DSE_UPDATE,
                        dseUpdate ? "inserts, updates and deletes" : "truncate",
                        dseUpdate ? "truncate" : "insert, update and delete");
    }

    /**
     * The resource named by a {@code role_permissions} row, or null for a name this software cannot parse: a resource
     * type it does not have (logged once per type), or a name of a known type it cannot parse (logged once per name),
     * both typically left by DataStax Enterprise. Like {@link #permissions}, this applies in every mode.
     */
    private static IResource knownResource(String role, String resourceName)
    {
        IResource resource;
        try
        {
            resource = Resources.tryFromName(resourceName);
        }
        catch (RuntimeException e)
        {
            // any failure to parse a foreign name, IllegalArgumentException and RequestValidationException included:
            // the row grants nothing, rather than failing the read of every other row
            if (unparseableResourcesLogged.add(resourceName))
                logger.warn("Ignoring the grants to role '{}' on resource '{}' found in {}.{}: the name cannot be parsed ({}) " +
                            "(it was probably granted by DataStax Enterprise; logged once per resource name)",
                            role, resourceName, SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS, e.getMessage());
            return null;
        }
        if (resource == null)
        {
            String prefix = resourceName.contains("/") ? resourceName.substring(0, resourceName.indexOf('/')) : resourceName;
            if (unknownResourcePrefixesLogged.add(prefix))
                logger.warn("Ignoring the grants to role '{}' on resource '{}' found in {}.{}: resources of type '{}' do not exist here " +
                            "(they were probably granted by DataStax Enterprise; logged once per resource type)", role, resourceName,
                            SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS, prefix);
        }
        return resource;
    }

    private static Pair<AuthenticatedUser, IResource> cacheKey(RoleResource role, IResource resource)
    {
        return cacheKey(role.getRoleName(), resource);
    }

    private static Pair<AuthenticatedUser, IResource> cacheKey(String roleName, IResource resource)
    {
        return Pair.create(new AuthenticatedUser(roleName), resource);
    }
}
