/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.auth;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * In the DSE 6 compatibility mode the node refuses to start with an authenticator or authorizer that reads a table of
 * {@code system_auth} DSE 6 does not have: silently not enforcing a network or CIDR restriction would open access.
 * Without the mode they are accepted as before.
 */
public class AuthConfigDse6Test
{
    private static final IAuthenticator PASSWORD = mock(PasswordAuthenticator.class);
    private static final INetworkAuthorizer ALLOW_ALL_NETWORK = new AllowAllNetworkAuthorizer();
    private static final ICIDRAuthorizer ALLOW_ALL_CIDR = new AllowAllCIDRAuthorizer();
    private static final INetworkAuthorizer NETWORK = mock(CassandraNetworkAuthorizer.class);
    private static final ICIDRAuthorizer CIDR = mock(CassandraCIDRAuthorizer.class);

    private static final String NETWORK_REFUSAL = "network_authorizer " + NETWORK.getClass().getName() + " reads system_auth.network_permissions, " +
                                                  "which DSE 6 does not have: the replicated system keyspaces keep DSE 6's definitions during the " +
                                                  "upgrade, and this software creates its own tables in them at the first start with " +
                                                  "internode_compatibility_mode NONE; use AllowAllNetworkAuthorizer until then";
    private static final String CIDR_REFUSAL = "cidr_authorizer " + CIDR.getClass().getName() + " reads system_auth.cidr_permissions and " +
                                               "system_auth.cidr_groups, which DSE 6 does not have: this software creates them only with " +
                                               "storage_compatibility_mode UPGRADING or NONE, after the upgrade (internode_compatibility_mode DSE_6 " +
                                               "requires HCD_1); use AllowAllCIDRAuthorizer until then";

    private InternodeCompatibilityMode modeBefore;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void saveMode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
    }

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    @Test
    public void defaultsAndPasswordAuthenticationAreAccepted()
    {
        assertThatCode(() -> AuthConfig.refuseWhatNeedsTablesDseLacks(new AllowAllAuthenticator(), ALLOW_ALL_NETWORK, ALLOW_ALL_CIDR)).doesNotThrowAnyException();
        assertThatCode(() -> AuthConfig.refuseWhatNeedsTablesDseLacks(PASSWORD, ALLOW_ALL_NETWORK, ALLOW_ALL_CIDR)).doesNotThrowAnyException();
    }

    @Test
    public void networkAuthorizerIsRefused()
    {
        assertThatThrownBy(() -> AuthConfig.refuseWhatNeedsTablesDseLacks(PASSWORD, NETWORK, ALLOW_ALL_CIDR))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("internode_compatibility_mode DSE_6 refused: " + NETWORK_REFUSAL);
    }

    @Test
    public void cidrAuthorizerIsRefused()
    {
        assertThatThrownBy(() -> AuthConfig.refuseWhatNeedsTablesDseLacks(PASSWORD, ALLOW_ALL_NETWORK, CIDR))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("internode_compatibility_mode DSE_6 refused: " + CIDR_REFUSAL);
    }

    @Test
    public void mutualTlsAuthenticatorsAreRefused()
    {
        for (IAuthenticator authenticator : new IAuthenticator[]{ mock(MutualTlsAuthenticator.class), mock(MutualTlsWithPasswordFallbackAuthenticator.class) })
            assertThatThrownBy(() -> AuthConfig.refuseWhatNeedsTablesDseLacks(authenticator, ALLOW_ALL_NETWORK, ALLOW_ALL_CIDR))
            .isInstanceOf(ConfigurationException.class)
            .hasMessage("internode_compatibility_mode DSE_6 refused: " + authenticatorRefusal(authenticator));
    }

    /** The refusal of a mutual TLS authenticator, as part of the message. */
    private static String authenticatorRefusal(IAuthenticator authenticator)
    {
        return "authenticator " + authenticator.getClass().getName() + " reads system_auth.identity_to_role, which DSE 6 does not have: " +
               "this software creates it only with storage_compatibility_mode UPGRADING or NONE, after the upgrade " +
               "(internode_compatibility_mode DSE_6 requires HCD_1); use PasswordAuthenticator until then";
    }

    @Test
    public void everyRefusedSettingIsNamedAtOnce()
    {
        assertThatThrownBy(() -> AuthConfig.refuseWhatNeedsTablesDseLacks(PASSWORD, NETWORK, CIDR))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("internode_compatibility_mode DSE_6 refused: " + NETWORK_REFUSAL + ". " + CIDR_REFUSAL);
    }

    /** The check applyAuth runs refuses in DSE_6 mode only. */
    @Test
    public void refusedInDse6ModeOnly()
    {
        IAuthenticator mutualTls = mock(MutualTlsAuthenticator.class);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        assertThatCode(() -> AuthConfig.checkDse6CompatibilityMode(mutualTls, NETWORK, CIDR)).doesNotThrowAnyException();

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertThatThrownBy(() -> AuthConfig.checkDse6CompatibilityMode(mutualTls, NETWORK, CIDR))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("internode_compatibility_mode DSE_6 refused: " + NETWORK_REFUSAL + ". " + CIDR_REFUSAL + ". " + authenticatorRefusal(mutualTls));
    }
}
