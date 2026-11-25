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
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.DseSystemKeyspaces;
import org.apache.cassandra.schema.SchemaConstants;

/**
 * Only purpose is to Initialize authentication/authorization via {@link #applyAuth()}.
 * This is in this separate class as it implicitly initializes schema stuff (via classes referenced in here).
 */
public final class AuthConfig
{
    private static final Logger logger = LoggerFactory.getLogger(AuthConfig.class);

    private static boolean initialized;

    public static void applyAuth()
    {
        // some tests need this
        if (initialized)
            return;

        initialized = true;

        Config conf = DatabaseDescriptor.getRawConfig();


        /* Authentication, authorization and role management backend, implementing IAuthenticator, I*Authorizer & IRoleManager */

        IAuthenticator authenticator = authInstantiate(conf.authenticator, AllowAllAuthenticator.class);

        // the configuration options regarding credentials caching are only guaranteed to
        // work with PasswordAuthenticator, so log a message if some other authenticator
        // is in use and non-default values are detected
        if (!(authenticator instanceof PasswordAuthenticator || authenticator instanceof MutualTlsAuthenticator)
            && (conf.credentials_update_interval != null
                || conf.credentials_validity.toMilliseconds() != 2000
                || conf.credentials_cache_max_entries != 1000))
        {
            logger.info("Configuration options credentials_update_interval, credentials_validity and " +
                        "credentials_cache_max_entries may not be applicable for the configured authenticator ({})",
                        authenticator.getClass().getName());
        }

        DatabaseDescriptor.setAuthenticator(authenticator);

        // authorizer

        IAuthorizer authorizer = authInstantiate(conf.authorizer, AllowAllAuthorizer.class);

        if (!authenticator.requireAuthentication() && authorizer.requireAuthorization())
        {
            throw new ConfigurationException(authorizer.getClass().getName() + " has authorization enabled which requires " +
                                             authenticator.getClass().getName() + " to enable authentication", false);
        }

        DatabaseDescriptor.setAuthorizer(authorizer);

        // role manager

        IRoleManager roleManager = authInstantiate(conf.role_manager, CassandraRoleManager.class);

        if (authenticator instanceof PasswordAuthenticator && !(roleManager instanceof CassandraRoleManager))
            throw new ConfigurationException(authenticator.getClass().getName() + " requires " + CassandraRoleManager.class.getName(), false);

        DatabaseDescriptor.setRoleManager(roleManager);

        // authenticator

        IInternodeAuthenticator internodeAuthenticator = authInstantiate(conf.internode_authenticator,
                                                                         AllowAllInternodeAuthenticator.class);
        DatabaseDescriptor.setInternodeAuthenticator(internodeAuthenticator);

        // network authorizer

        INetworkAuthorizer networkAuthorizer = authInstantiate(conf.network_authorizer, AllowAllNetworkAuthorizer.class);

        if (networkAuthorizer.requireAuthorization() && !authenticator.requireAuthentication())
        {
            throw new ConfigurationException(conf.network_authorizer + " can't be used with " + conf.authenticator.class_name, false);
        }

        DatabaseDescriptor.setNetworkAuthorizer(networkAuthorizer);

        // cidr authorizer

        ICIDRAuthorizer cidrAuthorizer = authInstantiate(conf.cidr_authorizer, AllowAllCIDRAuthorizer.class);

        if (cidrAuthorizer.requireAuthorization() && !authenticator.requireAuthentication())
        {
            throw new ConfigurationException(conf.cidr_authorizer + " can't be used with " + conf.authenticator, false);
        }

        DatabaseDescriptor.setCIDRAuthorizer(cidrAuthorizer);

        checkDse6CompatibilityMode(authenticator, networkAuthorizer, cidrAuthorizer);
        checkDse51CompatibilityMode(authenticator, networkAuthorizer, cidrAuthorizer);

        // Validate at last to have authenticator, authorizer, role-manager and internode-auth setup
        // in case these rely on each other.

        authenticator.validateConfiguration();
        authorizer.validateConfiguration();
        roleManager.validateConfiguration();
        networkAuthorizer.validateConfiguration();
        cidrAuthorizer.validateConfiguration();
        DatabaseDescriptor.getInternodeAuthenticator().validateConfiguration();
    }

    /**
     * In the DSE 6 compatibility mode, refuses the implementations that read tables of {@code system_auth} that DSE 6
     * does not have (see {@link #refuseWhatNeedsTablesDseLacks}); a no-op without the mode.
     */
    @VisibleForTesting
    static void checkDse6CompatibilityMode(IAuthenticator authenticator, INetworkAuthorizer networkAuthorizer, ICIDRAuthorizer cidrAuthorizer)
    {
        if (DatabaseDescriptor.isDse6CompatibilityMode())
            refuseWhatNeedsTablesDseLacks(authenticator, networkAuthorizer, cidrAuthorizer);
    }

    /**
     * In the DSE 5.1 compatibility mode, refuses the implementations that read tables of {@code system_auth} that
     * DSE 5.1 does not have, the same ones as in the DSE 6 mode (see {@link #refuseWhatNeedsTablesDseLacks}); a no-op
     * without the mode.
     */
    @VisibleForTesting
    static void checkDse51CompatibilityMode(IAuthenticator authenticator,
                                            INetworkAuthorizer networkAuthorizer,
                                            ICIDRAuthorizer cidrAuthorizer)
    {
        if (DatabaseDescriptor.isDse51CompatibilityMode())
            refuseWhatNeedsTablesDseLacks(InternodeCompatibilityMode.DSE_5_1, authenticator, networkAuthorizer,
                                          cidrAuthorizer);
    }

    /**
     * Refuses the implementations that read tables of {@code system_auth} that DSE 6 does not have: in the DSE 6
     * compatibility mode {@code system_auth} keeps DSE's definition until the mode is dropped ({@code DseSystemKeyspaces}),
     * and silently not enforcing a network or CIDR restriction would open access. The CIDR and identity tables are
     * not even created then while {@code storage_compatibility_mode} stays before 5.0, as the mode requires. Every
     * offending setting is named in the one exception.
     *
     * @throws ConfigurationException naming each setting, the tables it reads and the implementation to use meanwhile
     */
    @VisibleForTesting
    static void refuseWhatNeedsTablesDseLacks(IAuthenticator authenticator, INetworkAuthorizer networkAuthorizer, ICIDRAuthorizer cidrAuthorizer)
    {
        refuseWhatNeedsTablesDseLacks(InternodeCompatibilityMode.DSE_6, authenticator, networkAuthorizer,
                                      cidrAuthorizer);
    }

    /**
     * Refuses the implementations that read tables of {@code system_auth} that the DSE release of {@code mode} does
     * not have, as {@link #refuseWhatNeedsTablesDseLacks(IAuthenticator, INetworkAuthorizer, ICIDRAuthorizer)} does
     * for DSE 6. DSE 5.1's {@code system_auth} has the tables {@code roles}, {@code role_members} and
     * {@code role_permissions} only, so the same three settings are refused in the DSE 5.1 mode.
     *
     * @throws ConfigurationException naming each setting, the tables it reads and the implementation to use meanwhile
     */
    @VisibleForTesting
    static void refuseWhatNeedsTablesDseLacks(InternodeCompatibilityMode mode,
                                              IAuthenticator authenticator,
                                              INetworkAuthorizer networkAuthorizer,
                                              ICIDRAuthorizer cidrAuthorizer)
    {
        Preconditions.checkArgument(mode.isDse(), "not a mode of an upgrade from DSE: %s", mode);
        List<String> refusals = new ArrayList<>();
        if (networkAuthorizer instanceof CassandraNetworkAuthorizer)
        {
            String setting = "network_authorizer " + networkAuthorizer.getClass().getName();
            String table = authTable(AuthKeyspace.NETWORK_PERMISSIONS);
            String meanwhile = AllowAllNetworkAuthorizer.class.getSimpleName();
            refusals.add(mode.isDse6()
                         ? dseRefusal(setting, table + ", which DSE 6 does not have: " + DseSystemKeyspaces.DEFERRED,
                                      meanwhile, "then")
                         : dseRefusal(setting, table + ", which " + mode.source() + " does not have",
                                      meanwhile, "the mode is dropped"));
        }
        if (cidrAuthorizer instanceof CassandraCIDRAuthorizer)
            refusals.add(dseRefusal("cidr_authorizer " + cidrAuthorizer.getClass().getName(),
                                    authTable(AuthKeyspace.CIDR_PERMISSIONS) + " and " +
                                    authTable(AuthKeyspace.CIDR_GROUPS) + storage50Tables(mode, "them"),
                                    AllowAllCIDRAuthorizer.class.getSimpleName(), "then"));
        if (authenticator instanceof MutualTlsAuthenticator ||
            authenticator instanceof MutualTlsWithPasswordFallbackAuthenticator)
            refusals.add(dseRefusal("authenticator " + authenticator.getClass().getName(),
                                    authTable(AuthKeyspace.IDENTITY_TO_ROLES) + storage50Tables(mode, "it"),
                                    PasswordAuthenticator.class.getSimpleName(), "then"));
        if (!refusals.isEmpty())
            throw new ConfigurationException("internode_compatibility_mode " + mode + " refused: " +
                                             String.join(". ", refusals), false);
    }

    /**
     * Why the tables of {@code system_auth} that need storage_compatibility_mode 5.0 are missing in the compatibility
     * mode of an upgrade from DSE.
     */
    private static String storage50Tables(InternodeCompatibilityMode mode, String pronoun)
    {
        return ", which " + mode.source() + " does not have: this software creates " + pronoun + " only with " +
               "storage_compatibility_mode UPGRADING or NONE, after the upgrade (internode_compatibility_mode " + mode +
               " requires HCD_1)";
    }

    private static String authTable(String table)
    {
        return SchemaConstants.AUTH_KEYSPACE_NAME + '.' + table;
    }

    private static String dseRefusal(String setting, String reads, String meanwhile, String until)
    {
        return setting + " reads " + reads + "; use " + meanwhile + " until " + until;
    }

    private static <T> T authInstantiate(ParameterizedClass authCls, Class<T> defaultCls) {
        if (authCls != null && authCls.class_name != null)
        {
            String authPackage = AuthConfig.class.getPackage().getName();
            return ParameterizedClass.newInstance(authCls, List.of("", authPackage));
        }

        // for now, this has to stay and can not be replaced by ParameterizedClass.newInstance as above
        // due to that failing for simulator dtests. See CASSANDRA-20450 for more information.
        try
        {
            return defaultCls.newInstance();
        }
        catch (InstantiationException | IllegalAccessException  e)
        {
            throw new ConfigurationException("Failed to instantiate " + defaultCls.getName(), e);
        }
    }
}
