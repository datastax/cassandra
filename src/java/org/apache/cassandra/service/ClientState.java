/**
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

package org.apache.cassandra.service;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql.CQLStatement;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SemanticVersion;

/**
 * A container for per-client, thread-local state that Avro/Thrift threads must hold.
 * TODO: Kill thrift exceptions
 */
public class ClientState
{
    private static final int MAX_CACHE_PREPARED = 10000;    // Enough to keep buggy clients from OOM'ing us
    private static Logger logger = LoggerFactory.getLogger(ClientState.class);
    public static final SemanticVersion DEFAULT_CQL_VERSION = org.apache.cassandra.cql.QueryProcessor.CQL_VERSION;

    private static final Set<IResource> READABLE_SYSTEM_RESOURCES = new HashSet<IResource>();
    private static final Set<IResource> PROTECTED_AUTH_RESOURCES = new HashSet<IResource>();

    // Permissions cache.
    private static final LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> permissionsCache;

    static
    {
        // We want these system cfs to be always readable since many tools rely on them (nodetool, cqlsh, bulkloader, etc.)
        String[] cfs =  new String[] { SystemTable.SCHEMA_KEYSPACES_CF,
                                       SystemTable.SCHEMA_COLUMNFAMILIES_CF,
                                       SystemTable.SCHEMA_COLUMNS_CF,
                                       SystemTable.VERSION_CF,
                                       SystemTable.INDEX_CF,
                                       SystemTable.NODE_ID_CF };
        for (String cf : cfs)
            READABLE_SYSTEM_RESOURCES.add(DataResource.columnFamily(Table.SYSTEM_TABLE, cf));

        PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getAuthenticator().protectedResources());
        PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getAuthorizer().protectedResources());

        permissionsCache = initializePermissionsCache();
    }

    // Current user for the session
    private volatile AuthenticatedUser user;
    private String keyspace;
    // Reusable array for authorization
    private SemanticVersion cqlVersion = DEFAULT_CQL_VERSION;

    // The remote address of the client - maybe null for internal clients
    private SocketAddress remoteAddress;

    // An LRU map of prepared statements
    private Map<Integer, CQLStatement> prepared = new LinkedHashMap<Integer, CQLStatement>(16, 0.75f, true) {
        protected boolean removeEldestEntry(Map.Entry<Integer, CQLStatement> eldest) {
            return size() > MAX_CACHE_PREPARED;
        }
    };

    private Map<Integer, ParsedStatement.Prepared> cql3Prepared = new LinkedHashMap<Integer, ParsedStatement.Prepared>(16, 0.75f, true) {
        protected boolean removeEldestEntry(Map.Entry<Integer, ParsedStatement.Prepared> eldest) {
            return size() > MAX_CACHE_PREPARED;
        }
    };

    private long clock;
    private final boolean internalCall;

    /**
     * Construct a new, empty ClientState: can be reused after logout() or reset().
     */
    public ClientState()
    {
        this(false);
    }

    public ClientState(boolean internalCall)
    {
        reset();
        this.internalCall = internalCall;
        if (!internalCall)
            setRemoteAddress(SocketSessionManagementService.remoteSocket.get());
    }

    public Map<Integer, CQLStatement> getPrepared()
    {
        return prepared;
    }

    public Map<Integer, ParsedStatement.Prepared> getCQL3Prepared()
    {
        return cql3Prepared;
    }

    public String getRawKeyspace()
    {
        return keyspace;
    }

    public String getKeyspace() throws InvalidRequestException
    {
        if (keyspace == null)
            throw new InvalidRequestException("no keyspace has been specified");
        return keyspace;
    }

    public void setKeyspace(String ks) throws InvalidRequestException
    {
        // Skip keyspace validation for non-authenticated users. Apparently, some client libraries
        // call set_keyspace() before calling login(), and we have to handle that.
        if (user != null && Schema.instance.getKSMetaData(ks) == null)
            throw new InvalidRequestException("Keyspace '" + ks + "' does not exist");
        keyspace = ks;
    }

    public String getSchedulingValue()
    {
        switch(DatabaseDescriptor.getRequestSchedulerId())
        {
            case keyspace: return keyspace;
        }
        return "default";
    }

    /**
     * Attempts to login this client with the given credentials map.
     */
    public void login(Map<String, String> credentials) throws AuthenticationException
    {
        AuthenticatedUser user = DatabaseDescriptor.getAuthenticator().authenticate(credentials);

        if (!user.isAnonymous())
        {
            boolean isExisting;
            try
            {
                isExisting = Auth.isExistingUser(user.getName());
            }
            catch (Exception e)
            {
                throw new AuthenticationException(e.toString());
            }

            if (!isExisting)
                throw new AuthenticationException(String.format("User %s doesn't exist - create it with CREATE USER query first",
                                                                user.getName()));
        }

        logger.debug("logged in: {}", user);

        this.user = user;
    }

    public void logout()
    {
        logger.debug("logged out: {}", user);
        reset();
    }

    public void hasAllKeyspacesAccess(Permission perm) throws InvalidRequestException
    {
        if (internalCall)
            return;
        validateLogin();
        ensureHasPermission(perm, DataResource.root());
    }

    public void hasKeyspaceAccess(String keyspace, Permission perm) throws InvalidRequestException
    {
        hasAccess(keyspace, perm, DataResource.keyspace(keyspace));
    }

    public void hasColumnFamilyAccess(String keyspace, String columnFamily, Permission perm)
    throws InvalidRequestException
    {
        hasAccess(keyspace, perm, DataResource.columnFamily(keyspace, columnFamily));
    }

    private void hasAccess(String keyspace, Permission perm, DataResource resource)
    throws InvalidRequestException
    {
        validateKeyspace(keyspace);
        if (internalCall)
            return;
        validateLogin();
        preventSystemKSSchemaModification(resource, perm);
        if (perm.equals(Permission.SELECT) && READABLE_SYSTEM_RESOURCES.contains(resource))
            return;
        if (PROTECTED_AUTH_RESOURCES.contains(resource) && (perm.equals(Permission.CREATE) || perm.equals(Permission.ALTER) || perm.equals(Permission.DROP)))
            throw new UnauthorizedException(String.format("Can't %s protected %s", perm, resource));
        ensureHasPermission(perm, resource);
    }

    public void ensureHasPermission(Permission perm, IResource resource) throws UnauthorizedException
    {
        for (IResource r : Resources.chain(resource))
        {
            if (authorize(r).contains(perm))
                return;
        }
        throw new UnauthorizedException(String.format("User %s has no %s permission on %s or any of its parents",
                                                      user.getName(),
                                                      perm,
                                                      resource));
    }

    private void preventSystemKSSchemaModification(DataResource resource, Permission perm) throws UnauthorizedException
    {
        if (!Schema.systemKeyspaceNames.contains(resource.getKeyspace().toLowerCase()))
            return;

        // only forbid system schema modification.
        if (perm.equals(Permission.CREATE) || perm.equals(Permission.ALTER) || perm.equals(Permission.DROP))
            throw new UnauthorizedException(resource.getKeyspace() + " keyspace schema is not user-modifiable.");
    }


    public void reset()
    {
        if (!DatabaseDescriptor.getAuthenticator().requireAuthentication())
            this.user = AuthenticatedUser.ANONYMOUS_USER;
        keyspace = null;
        prepared.clear();
        cql3Prepared.clear();
        cqlVersion = DEFAULT_CQL_VERSION;
    }

    public void validateLogin() throws UnauthorizedException
    {
        if (user == null)
            throw new UnauthorizedException("You have not logged in");
    }

    public void ensureNotAnonymous() throws UnauthorizedException
    {
        validateLogin();
        if (user.isAnonymous())
            throw new UnauthorizedException("You have to be logged in and not anonymous to perform this query");
    }

    private static void validateKeyspace(String keyspace) throws InvalidRequestException
    {
        if (keyspace == null)
        {
            throw new InvalidRequestException("You have not set a keyspace for this session");
        }
    }

    /**
     * This clock guarantees that updates from a given client will be ordered in the sequence seen,
     * even if multiple updates happen in the same millisecond.  This can be useful when a client
     * wants to perform multiple updates to a single column.
     */
    public long getTimestamp()
    {
        long current = System.currentTimeMillis() * 1000;
        clock = clock >= current ? clock + 1 : current;
        return clock;
    }

    public void setCQLVersion(String str) throws InvalidRequestException
    {
        SemanticVersion version;
        try
        {
            version = new SemanticVersion(str);
        }
        catch (IllegalArgumentException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }

        SemanticVersion cql = org.apache.cassandra.cql.QueryProcessor.CQL_VERSION;
        SemanticVersion cql3 = org.apache.cassandra.cql3.QueryProcessor.CQL_VERSION;

        if (version.isSupportedBy(cql))
            cqlVersion = cql;
        else if (version.isSupportedBy(cql3))
            cqlVersion = cql3;
        else
            throw new InvalidRequestException(String.format("Provided version %s is not supported by this server (supported: %s)",
                                                            version,
                                                            StringUtils.join(getCQLSupportedVersion(), ", ")));
    }

    public AuthenticatedUser getUser()
    {
        return user;
    }

    public SemanticVersion getCQLVersion()
    {
        return cqlVersion;
    }

    public static SemanticVersion[] getCQLSupportedVersion()
    {
        SemanticVersion cql = org.apache.cassandra.cql.QueryProcessor.CQL_VERSION;
        SemanticVersion cql3 = org.apache.cassandra.cql3.QueryProcessor.CQL_VERSION;

        return new SemanticVersion[]{ cql, cql3 };
    }

    private static LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> initializePermissionsCache()
    {
        if (DatabaseDescriptor.getAuthorizer() instanceof AllowAllAuthorizer)
            return null;

        int validityPeriod = DatabaseDescriptor.getPermissionsValidity();
        if (validityPeriod <= 0)
            return null;

        return CacheBuilder.newBuilder().expireAfterWrite(validityPeriod, TimeUnit.MILLISECONDS)
                                        .build(new CacheLoader<Pair<AuthenticatedUser, IResource>, Set<Permission>>()
                                        {
                                            public Set<Permission> load(Pair<AuthenticatedUser, IResource> userResource)
                                            {
                                                return DatabaseDescriptor.getAuthorizer().authorize(userResource.left,
                                                                                                    userResource.right);
                                            }
                                        });
    }

    public SocketAddress getRemoteAddress()
    {
        return remoteAddress;
    }

    public void setRemoteAddress(SocketAddress address)
    {
        remoteAddress = address;
    }

    public Set<Permission> authorize(IResource resource)
    {
        // AllowAllAuthenticator or manually disabled caching.
        if (permissionsCache == null)
            return DatabaseDescriptor.getAuthorizer().authorize(user, resource);

        try
        {
            return permissionsCache.get(Pair.create(user, resource));
        }
        catch (ExecutionException e)
        {
            throw Throwables.propagate(e);
        }
    }
}
