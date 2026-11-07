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

import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The pure readings of DataStax Enterprise's {@code role_permissions} names that the DSE 6 preflight inspection uses
 * ({@link CassandraAuthorizer#dseLoneWriteGrantName}, {@link CassandraAuthorizer#unknownPermissionNames},
 * {@link CassandraAuthorizer#isKnownResourceName}): the same rules as the authorizer's own reading of the rows, without
 * reading, logging or recording anything.
 */
public class CassandraAuthorizerDseNamesTest
{
    @BeforeClass
    public static void initialise()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void aDseWriteGrantAloneOnADataResourceIsTheLoneOne()
    {
        assertThat(CassandraAuthorizer.dseLoneWriteGrantName("data/ks/t", Set.of("UPDATE"))).isEqualTo("UPDATE");
        assertThat(CassandraAuthorizer.dseLoneWriteGrantName("data", Set.of("TRUNCATE", "SELECT"))).isEqualTo("TRUNCATE");
        assertThat(CassandraAuthorizer.dseLoneWriteGrantName("data/ks", Set.of("UPDATE", "TRUNCATE"))).as("DSE's GRANT ALL").isNull();
        assertThat(CassandraAuthorizer.dseLoneWriteGrantName("data/ks/t", Set.of("UPDATE", "MODIFY"))).as("MODIFY covers it").isNull();
        assertThat(CassandraAuthorizer.dseLoneWriteGrantName("data/ks/t", Set.of("SELECT"))).isNull();
        assertThat(CassandraAuthorizer.dseLoneWriteGrantName("roles/r", Set.of("UPDATE"))).as("not a data resource").isNull();
    }

    @Test
    public void theNamesThisSoftwareDoesNotHaveAreUnknown()
    {
        assertThat(CassandraAuthorizer.unknownPermissionNames("data/ks", Set.of("SELECT", "BACKUP", "UPDATE"))).containsExactly("BACKUP");
        assertThat(CassandraAuthorizer.unknownPermissionNames("roles/r", Set.of("UPDATE", "PROXY.LOGIN", "AUTHORIZE")))
            .as("UPDATE is DSE's only on data resources").containsExactly("PROXY.LOGIN", "UPDATE");
        assertThat(CassandraAuthorizer.unknownPermissionNames("data", Set.of("MODIFY", "SELECT"))).isEmpty();
    }

    @Test
    public void theResourcesThisSoftwareParsesAreKnown()
    {
        assertThat(CassandraAuthorizer.isKnownResourceName("data")).isTrue();
        assertThat(CassandraAuthorizer.isKnownResourceName("data/ks/t")).isTrue();
        assertThat(CassandraAuthorizer.isKnownResourceName("roles/dseadmin")).isTrue();
        assertThat(CassandraAuthorizer.isKnownResourceName("mbean")).isTrue();
        assertThat(CassandraAuthorizer.isKnownResourceName("rows/tenant1/data/ks/t")).isFalse();
        assertThat(CassandraAuthorizer.isKnownResourceName("search/ks/t")).isFalse();
        assertThat(CassandraAuthorizer.isKnownResourceName("data/ks/t/extra")).as("a data name that does not parse").isFalse();
    }

    /**
     * DataStax Enterprise 5.1's names: no {@code UPDATE} or {@code TRUNCATE}, so nothing is granted alone; its
     * {@code PROXY.*} and {@code SEARCH.*} permissions are unknown names, and its {@code search}, {@code rows},
     * {@code rpc}, {@code authentication_schemes}, {@code any_work_pool} and {@code any_submission} resources are not
     * resources of this software, so their rows grant nothing.
     */
    @Test
    public void theNamesOfDse51ReadAsNoGrant()
    {
        assertThat(CassandraAuthorizer.dseLoneWriteGrantName("data/ks/t", Set.of("MODIFY", "SELECT"))).isNull();
        Set<String> proxy = Set.of("ALTER", "PROXY.EXECUTE", "PROXY.LOGIN");
        assertThat(CassandraAuthorizer.unknownPermissionNames("roles/r", proxy))
        .containsExactly("PROXY.EXECUTE", "PROXY.LOGIN");
        Set<String> search = Set.of("AUTHORIZE", "SEARCH.CREATE", "SEARCH.ALTER", "SEARCH.DROP", "SEARCH.RELOAD",
                                    "SEARCH.REBUILD", "SEARCH.COMMIT");
        assertThat(CassandraAuthorizer.unknownPermissionNames("search/ks.t", search))
        .containsExactly("SEARCH.ALTER", "SEARCH.COMMIT", "SEARCH.CREATE", "SEARCH.DROP", "SEARCH.REBUILD",
                         "SEARCH.RELOAD");
        String[] resources = { "search", "search/ks", "search/ks.t", "rows/EU/data/ks/t", "rpc", "rpc/DseClientTool",
                               "rpc/DseClientTool/getData", "authentication_schemes", "authentication_schemes/internal",
                               "any_work_pool", "any_work_pool/default", "any_submission",
                               "any_submission/default/submission1" };
        for (String resource : resources)
            assertThat(CassandraAuthorizer.isKnownResourceName(resource)).as(resource).isFalse();
    }
}
