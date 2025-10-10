/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.index.sai;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.Ordering;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.schema.ColumnMetadata;
import org.assertj.core.api.Assertions;

@RunWith(Parameterized.class)
public class SAIUtilTest
{
    @Parameterized.Parameter
    public Version currentVersion;

    @Parameterized.Parameter(1)
    public Version keyspaceVersion;

    private String keyspaceWithVersion;

    @Parameterized.Parameters(name = "currentVersion={0} keyspaceVersion={1}")
    public static Collection<Object[]> data()
    {
        Collection<Object[]> parameters = new ArrayList<>();
        for (Version currentVersion : Version.ALL)
        {
            for (Version keyspaceVersion : Version.ALL)
            {
                parameters.add(new Object[]{currentVersion, keyspaceVersion});
            }
        }
        return parameters;
    }

    @Before
    public void setVersionSelector()
    {
        keyspaceWithVersion = "ks_" + keyspaceVersion;
        Map<String, Version> versionsPerKeyspace = new HashMap<>() {{ put(keyspaceWithVersion, keyspaceVersion); }};
        SAIUtil.setCurrentVersion(currentVersion, versionsPerKeyspace);
    }

    @Test
    public void testCurrentVersion()
    {
        Assertions.assertThat(SAIUtil.currentVersion())
                  .isEqualTo(currentVersion);
        Assertions.assertThat(Version.current("ks"))
                  .isEqualTo(currentVersion);
        Assertions.assertThat(Version.current(keyspaceWithVersion))
                  .isEqualTo(keyspaceVersion);
    }

    @Test
    public void testANNUseSyntheticScoreProperty()
    {
        // with a column belonging to a keyspace without a specific version
        ColumnMetadata column = ColumnMetadata.regularColumn("ks", "cf", "col", FloatType.instance);
        Assertions.assertThat(new Ordering.Ann(column, null, null).isScored())
                  .isEqualTo(Ordering.Ann.useSyntheticScore(currentVersion));

        // with a column belonging to the keyspace with a specific version
        column = ColumnMetadata.regularColumn(keyspaceWithVersion, "cf", "col", FloatType.instance);
        Assertions.assertThat(new Ordering.Ann(column, null, null).isScored())
                  .isEqualTo(Ordering.Ann.useSyntheticScore(keyspaceVersion));
    }
}
