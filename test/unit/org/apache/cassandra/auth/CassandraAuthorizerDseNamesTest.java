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
 * The pure readings of DataStax Enterprise's {@code role_permissions} names, for callers that read the rows themselves
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
}
