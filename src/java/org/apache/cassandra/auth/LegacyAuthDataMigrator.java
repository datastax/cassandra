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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Recreates the legacy dse_auth keyspace and uses any data it contains
 * to populate the system_auth keyspace. Works around the fact that
 * system_auth will be unavailable for client auth on non-upgraded nodes
 * by forcing rf=ring size.
 */
public class LegacyAuthDataMigrator
{

    private static final Logger logger = LoggerFactory.getLogger(LegacyAuthDataMigrator.class);

    public static final String DSE_AUTH_KS = "dse_auth";
    public static final String USERS_CF = "users";
    public static final Integer USERS_CF_ID = 101;
    public static final String CREDENTIALS_CF = "credentials";
    public static final Integer CREDENTIALS_CF_ID = 103;
    public static final String PERMISSIONS_CF = "permissions";
    public static final Integer PERMISSIONS_CF_ID = 102;

    private final Class<? extends AbstractReplicationStrategy> replicationStrategy;
    private final Map<String, String> replicationOptions;

    public LegacyAuthDataMigrator(Class<? extends AbstractReplicationStrategy> replicationStrategy,
                                  Map<String, String> replicationOptions)
    {
        this.replicationStrategy = replicationStrategy;
        this.replicationOptions = replicationOptions;
    }

    public void migrateCredentials() throws Exception
    {
        logger.info("Migrating legacy user credentials");
        setAuthKsReplicationFactor();
        loadOldCredentialsDefinitions();
        copyUsers();
        copyCredentials();
    }

    public void migratePermissions() throws Exception
    {
        logger.info("Migrating legacy user permissions");
        setAuthKsReplicationFactor();
        loadOldPermissionsDefinitions();
        copyPermissions();
    }

    // increase the RF of system_auth to match the number of nodes
    // to ensure that during a rolling upgrade every read can be
    // satisified by the coordinator. Because schema changes from
    // the 1.2 nodes will not be processed by 1.1 node, so they
    // will never hold the system_auth tables.
    // After upgrade, this can be tuned down if required
    private void setAuthKsReplicationFactor() throws ConfigurationException
    {
        int rfAll = StorageService.instance.getLiveNodes().size();
        logger.info(String.format("During migration of legacy auth data, increasing rf " +
                                  "of system_auth to match size of the ring (%s)",
                                   rfAll));
        logger.info("Following a successful upgrade & migration this can be lowered if required");

        KSMetaData ksm = KSMetaData.newKeyspace(Auth.AUTH_KS,
                                                SimpleStrategy.class.getName(),
                                                ImmutableMap.of("replication_factor", "" + rfAll),
                                                true);
        MigrationManager.announceKeyspaceUpdate(ksm);
    }

    private void loadOldKeyspaceDefinition() throws Exception
    {
        if (null == Schema.instance.getKSMetaData(DSE_AUTH_KS))
        {
            logger.info("Re-creating dse_auth keyspace");
            KSMetaData dseAuth = KSMetaData.newKeyspace(DSE_AUTH_KS,
                                                        replicationStrategy,
                                                        replicationOptions,
                                                        true,
                                                        Collections.EMPTY_LIST);
            MigrationManager.announceNewKeyspace(dseAuth);
        }
    }

    private void maybeCreateColumnFamily(CFMetaData cfm, Integer oldCfId) throws Exception
    {
        if (null == Schema.instance.getCFMetaData(cfm.ksName, cfm.cfName))
        {
            MigrationManager.announceNewColumnFamily(cfm);
        }
        logger.debug("Mapping old CfId {} to new cf {}", oldCfId, cfm.cfName);
        Schema.instance.addOldCfIdMapping(oldCfId, cfm.cfId);
    }

    private void loadOldCredentialsDefinitions() throws Exception
    {
        loadOldKeyspaceDefinition();

        logger.info("Re-creating dse_auth user tables");
        maybeCreateColumnFamily(getDseUsers(), USERS_CF_ID);
        maybeCreateColumnFamily(getDseCredentials(), CREDENTIALS_CF_ID);
    }

    private void loadOldPermissionsDefinitions() throws Exception
    {
        loadOldKeyspaceDefinition();

        logger.info("Re-creating dse_auth permissions table");
        maybeCreateColumnFamily(getDsePermissions(), PERMISSIONS_CF_ID);
    }

    private CFMetaData getDseUsers()
    {
        CFMetaData dseUsers = new CFMetaData(DSE_AUTH_KS,
                                             USERS_CF,
                                             ColumnFamilyType.Standard,
                                             UTF8Type.instance,
                                             null);
        dseUsers.addColumnDefinition(new ColumnDefinition(ByteBuffer.wrap("super".getBytes()),
                                                          BooleanType.instance,
                                                          null,
                                                          null,
                                                          null,
                                                          null));
        dseUsers.keyAliases(Lists.newArrayList(ByteBuffer.wrap("name".getBytes())));
        dseUsers.compressionParameters(getCompressionParams());
        return dseUsers;

    }

    private CFMetaData getDseCredentials()
    {
        CFMetaData dseCredentials = new CFMetaData(DSE_AUTH_KS,
                                                   CREDENTIALS_CF,
                                                   ColumnFamilyType.Standard,
                                                   UTF8Type.instance,
                                                   null);
        dseCredentials.addColumnDefinition(new ColumnDefinition(ByteBuffer.wrap("salted_hash".getBytes()),
                                                                UTF8Type.instance,
                                                                null,
                                                                null,
                                                                null,
                                                                null));
        dseCredentials.keyAliases(Lists.newArrayList(ByteBuffer.wrap("username".getBytes())));
        dseCredentials.compressionParameters(getCompressionParams());
        return dseCredentials;
    }

    private CFMetaData getDsePermissions()
    {
        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>() {{
            add(UTF8Type.instance);
            add(UTF8Type.instance);
            add(UTF8Type.instance);
        }};
        CFMetaData dsePermissions = new CFMetaData(DSE_AUTH_KS,
                                                   PERMISSIONS_CF,
                                                   ColumnFamilyType.Standard,
                                                   CompositeType.getInstance(types),
                                                   null);
        dsePermissions.addColumnDefinition(new ColumnDefinition(ByteBuffer.wrap("x".getBytes()),
                                                                Int32Type.instance,
                                                                null,
                                                                null,
                                                                null,
                                                                2));
        dsePermissions.keyAliases(Lists.newArrayList(ByteBuffer.wrap("username".getBytes())));
        dsePermissions.columnAliases(Lists.newArrayList(ByteBuffer.wrap("resource".getBytes()),
                                                        ByteBuffer.wrap("permission".getBytes())));
        dsePermissions.compressionParameters(getCompressionParams());
        return dsePermissions;
    }

    private CompressionParameters getCompressionParams()
    {
        try
        {
            return (! SnappyCompressor.isAvailable())
                   ? CompressionParameters.create(Collections.EMPTY_MAP)
                   : CompressionParameters.create(
                           Collections.singletonMap(CompressionParameters.SSTABLE_COMPRESSION,
                                   SnappyCompressor.class.getName()));
        }
        catch(ConfigurationException e)
        {
            logger.warn("Error initialising compression parameters for legacy auth ks", e);
            return null;
        }
    }

    /**
     * users & credentials tables should already be setup by this point
     */
    private void copyUsers()
    {
        logger.debug("Fetching legacy user records to migrate");
        if (null == Schema.instance.getCFMetaData(DSE_AUTH_KS, USERS_CF))
        {
            logger.debug("No pre-existing user info to migrate");
            return;
        }

        UntypedResultSet source = process(String.format("SELECT * FROM %s.%s", DSE_AUTH_KS, USERS_CF));
        if (source == null)
            return;

        for (UntypedResultSet.Row row : source)
        {
            copyUser(row.getString("name"), row.getBoolean("super"));
        }
    }

    private void copyUser(String username, boolean isSuper)
    {
        try
        {
            logger.debug(String.format("Migrating user record %s", username));
            processInternal(String.format("INSERT INTO %s.%s (name, super) VALUES ('%s', %s)",
                                          Auth.AUTH_KS,
                                          USERS_CF,
                                          username,
                                          isSuper));
            logger.debug(String.format("User record %s updated", username));
        }
        catch (Error e)
        {
            logger.info("Exception when migrating user record from dse_system to system_auth keyspace", e);
        }
    }

    private void copyCredentials()
    {
        logger.debug("Fetching legacy credentials info to migrate");
        if (null == Schema.instance.getCFMetaData(DSE_AUTH_KS, CREDENTIALS_CF))
        {
            logger.debug("No pre-existing credentials info to migrate");
            return;
        }
        UntypedResultSet source = process(String.format("SELECT * FROM %s.%s", DSE_AUTH_KS, CREDENTIALS_CF));
        if (source == null)
            return;

        for (UntypedResultSet.Row row : source)
        {
            copyUserCredentials(row.getString("username"), row.getString("salted_hash"));
        }
    }

    /**
     * We don't use an instance of PasswordAuthenticator to insert the users
     * into the system table because the password we have is already hashed
     */
    private void copyUserCredentials(String username, String passwordHash)
    {
        try
        {
            logger.debug(String.format("Migrating credentials for user %s", username));
            processInternal(String.format("INSERT INTO %s.%s (username, salted_hash) VALUES ('%s', '%s')",
                                          Auth.AUTH_KS,
                                          CREDENTIALS_CF,
                                          username,
                                          passwordHash));
            logger.debug(String.format("Credentials for user %s updated", username));
        }
        catch (Exception e)
        {
            logger.info("Exception when migrating user credentials from dse_system to system_auth keyspace", e);
        }
    }

    private void copyPermissions()
    {
        logger.debug("Fetching legacy permissions info to migrate");
        if (null == Schema.instance.getCFMetaData(DSE_AUTH_KS, PERMISSIONS_CF))
        {
            logger.debug("No pre-existing permissions info to migrate");
            return;
        }
        CassandraAuthorizer authorizer = new CassandraAuthorizer();

        UntypedResultSet source = process(String.format("SELECT * FROM %s.%s", DSE_AUTH_KS, PERMISSIONS_CF));
        if (source == null)
            return;

        for (UntypedResultSet.Row row : source)
        {
            copyUserPermission(authorizer, row.getString("username"), row.getString("resource"), row.getString("permission"));
        }
    }

    private void copyUserPermission(CassandraAuthorizer authorizer,
                                    String username,
                                    String resource,
                                    String permission)
    {
        try
        {
            logger.debug(String.format("Migrating single permissions entry for user %s", username));
            authorizer.grant(AuthenticatedUser.ANONYMOUS_USER,
                             Sets.newHashSet(Permission.valueOf(permission)),
                             DataResource.fromName(resource),
                             username);
            logger.debug(String.format("Permission for user %s updated", username));
        }
        catch (Exception e)
        {
            logger.debug("Exception when migrating user permissions from dse_system to system_auth keyspace", e);
        }
    }

    private static UntypedResultSet process(String query)
    {
        UntypedResultSet result = null;
        try
        {
            result = QueryProcessor.process(query, ConsistencyLevel.ONE);
            if (result.isEmpty())
                result = null;
        } catch (RequestExecutionException e)
        {
            logger.info("Error performing query", e);
        }
        return result;
    }

    private static UntypedResultSet processInternal(String query)
    {
        UntypedResultSet result = QueryProcessor.processInternal(query);
        if (null == result || result.isEmpty())
            return null;
        return result;
    }
}
