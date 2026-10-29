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

package org.apache.cassandra.distributed.test.dsecompat;

import java.lang.reflect.InvocationTargetException;

import org.junit.Test;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cluster-wide pass of the DSE access-control check ({@code Dse6AccessControlCheck.verifyAfterJoin}) on a node
 * upgraded to {@code DSE_6} next to an HCD 1.2 node speaking the pre-4.0 dialect: its read of
 * {@code system_auth.role_permissions} at the authorizer's consistency level goes to the peer through the legacy
 * codec, a clean table passes, and a row-level grant written by the peer (the DSE side) refuses the start with the
 * refusal text. {@code system_auth} has one replica here: the role name is chosen so that the HCD 1.2 node holds
 * the grant, and the refusing row crosses the legacy codec.
 */
public class Dse6AccessControlMixedTest extends DseDialectTestBase
{
    private static final int UPGRADED = 1;
    private static final int HCD = 2;

    @Test
    public void theClusterWidePassReadsThroughTheDseDialect() throws Throwable
    {
        dse6TestCase(2)
        .nodesToUpgrade(UPGRADED)
        .setup(cluster -> {})
        .runAfterNodeUpgrade((cluster, node) -> {
            assertThat(clusterWidePass()).isNull();

            // a role whose partition lives on the HCD 1.2 node only
            String role = null;
            for (int i = 0; i < 64 && role == null; i++)
            {
                String candidate = "analyst" + i;
                cluster.coordinator(HCD).execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) " +
                                                 "VALUES (?, 'rows/top_secret/data/ks1/table1', {'SELECT'})", ALL, candidate);
                String select = "SELECT role FROM system_auth.role_permissions WHERE role = '" + candidate + '\'';
                if (cluster.get(HCD).executeInternal(select).length == 1 && cluster.get(UPGRADED).executeInternal(select).length == 0)
                    role = candidate;
                else
                    cluster.coordinator(HCD).execute("DELETE FROM system_auth.role_permissions WHERE role = ?", ALL, candidate);
            }
            assertThat(role).as("a role held by the HCD 1.2 node only").isNotNull();
            String refusal = clusterWidePass();
            assertThat(refusal).isEqualTo("internode_compatibility_mode DSE_6 refused: DSE access controls that this software does not " +
                                          "enforce would grant access if ignored (found by a LOCAL_QUORUM read): row-level grants [role '" +
                                          role + "' on rows/top_secret/data/ks1/table1: [SELECT]]. Roll this node back to DSE, remove them " +
                                          "there (REVOKE the row-level grants) " +
                                          "and upgrade it again; or start with -Dcassandra.internode_compatibility_mode.ignore_dse_restrictions=true " +
                                          "to drop them knowingly");

            cluster.coordinator(HCD).execute("DELETE FROM system_auth.role_permissions WHERE role = ?", ALL, role);
            assertThat(clusterWidePass()).isNull();
        })
        .run();
    }

    /**
     * Runs the cluster-wide pass inside the upgraded instance (by reflection: test classes are not shared with the
     * instances of this harness) and returns the refusal text, or null when the pass accepts the start.
     */
    private static String clusterWidePass()
    {
        ClassLoader classLoader = instanceClassLoader(UPGRADED);
        try
        {
            Class<?> check = Class.forName("org.apache.cassandra.service.Dse6AccessControlCheck", true, classLoader);
            check.getMethod("verifyAfterJoin").invoke(null);
            return null;
        }
        catch (InvocationTargetException e)
        {
            Throwable cause = e.getCause();
            if (cause.getClass().getName().equals("org.apache.cassandra.exceptions.StartupException"))
                return cause.getMessage();
            throw new AssertionError("the cluster-wide pass failed", cause);
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError(e);
        }
    }
}
