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

package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that creating a new index with a different version than an existing index works as expected:
 * <ul>
 *     <li>If the sstable doesn't have any other index components, the index should be created with the current
 *     version.</li>
 *     <li>If the sstable already has other index components belonging to another index, the new index should be created
 *     with the version of those already existing components, regardless of the current version.</li>
 * </ul>
 * See CNDB-8756 for further details.
 * See {@link FeaturesVersionSupportTest} and {@link VersionSelectorTest} for related tests.
 */
@RunWith(Parameterized.class)
public class CreateIndexWithNewVersionTest extends SAITester
{
    @Parameterized.Parameter
    public boolean useImmutableComponents;

    @Parameterized.Parameter(1)
    public Version initialVersion;

    @Parameterized.Parameter(2)
    public Version upgradeVersion;

    // The smallest between initialVersion and upgradeVersion, since we're testing both upgrades and downgrades.
    private Version oldestVersion;

    @Parameterized.Parameters(name = "immutable={0} initial={1} upgrade={2}")
    public static Collection<Object[]> data()
    {
        List<Object[]> data = new ArrayList<>();
        for (boolean useImmutableComponents : new boolean[]{ true, false })
        {
            for (Version oldVersion : Version.ALL)
            {
                for (Version newVersion : Version.ALL)
                {
                    data.add(new Object[]{ useImmutableComponents, oldVersion, newVersion });
                }
            }
        }
        return data;
    }

    @Before
    public void setup()
    {
        SAIUtil.setCurrentVersion(initialVersion);
        CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS.setBoolean(useImmutableComponents);
        oldestVersion = initialVersion.after(upgradeVersion) ? upgradeVersion : initialVersion;
    }

    @Test
    public void testCreateIndexWithNewVersion()
    {
        // Create a table and insert some data.
        createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY (k, c))");
        execute("INSERT INTO %s (k, c, a, b) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        execute("INSERT INTO %s (k, c, a, b) VALUES (?, ?, ?, ?)", 0, 1, 0, 1);
        execute("INSERT INTO %s (k, c, a, b) VALUES (?, ?, ?, ?)", 0, 2, 1, 0);
        execute("INSERT INTO %s (k, c, a, b) VALUES (?, ?, ?, ?)", 0, 3, 1, 1);
        execute("INSERT INTO %s (k, c, a, b) VALUES (?, ?, ?, ?)", 1, 0, 0, 0);
        execute("INSERT INTO %s (k, c, a, b) VALUES (?, ?, ?, ?)", 1, 1, 0, 1);
        execute("INSERT INTO %s (k, c, a, b) VALUES (?, ?, ?, ?)", 1, 2, 1, 0);
        execute("INSERT INTO %s (k, c, a, b) VALUES (?, ?, ?, ?)", 1, 3, 1, 1);
        flush();

        // Create an index with the initial version.
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        verifySAIVersionInUse(initialVersion);
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore());
        assertThat(group).isNotNull();
        assertThat(group.getMinVersion()).isEqualTo(initialVersion);

        // Create a new index with a different version.
        // The per-sstable components and the existing per-index components will remain in the initial version.
        // The new per-index components will be created in the new version only if it's the same as the initial version.
        // Otherwise, they will use the old version, so all the index components of a sstable are in the same version.
        SAIUtil.setCurrentVersion(upgradeVersion);
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        verifySAIVersionInUse(initialVersion, initialVersion);
        assertThat(group.getMinVersion()).isEqualTo(initialVersion);

        // Add some more data, and flush it to a new sstable.
        execute("INSERT INTO %s (k, c, a, b) VALUES (?, ?, ?, ?)", 2, 0, 0, 0);
        execute("INSERT INTO %s (k, c, a, b) VALUES (?, ?, ?, ?)", 2, 1, 0, 1);
        execute("INSERT INTO %s (k, c, a, b) VALUES (?, ?, ?, ?)", 2, 2, 1, 0);
        execute("INSERT INTO %s (k, c, a, b) VALUES (?, ?, ?, ?)", 2, 3, 1, 1);
        flush();

        // The new sstable should create new per-sstable components in the new version, for both indexes.
        // The old sstable should keep their components in the old version.
        verifySAIVersionInUse(initialVersion, upgradeVersion);
        assertThat(group.getMinVersion()).isEqualTo(oldestVersion);
        verifyQueries();

        // Rebuild should rebuild the sstable indexes keeping the exact same versions we were using.
        rebuildTableIndexes();
        verifySAIVersionInUse(initialVersion, upgradeVersion);
        assertThat(group.getMinVersion()).isEqualTo(oldestVersion);
        verifyQueries();

        // Upgrade sstables should upgrade all sstable indexes to the new version.
        upgradeSSTables();
        verifySAIVersionInUse(upgradeVersion);
        verifyQueries();
    }

    private void verifyQueries()
    {
        // test old index on column a
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a = 0"),
                                row(0, 0, 0, 0),
                                row(0, 1, 0, 1),
                                row(1, 0, 0, 0),
                                row(1, 1, 0, 1),
                                row(2, 0, 0, 0),
                                row(2, 1, 0, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a = 1"),
                                row(0, 2, 1, 0),
                                row(0, 3, 1, 1),
                                row(1, 2, 1, 0),
                                row(1, 3, 1, 1),
                                row(2, 2, 1, 0),
                                row(2, 3, 1, 1));

        // test new index on column b
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE b = 0"),
                                row(0, 0, 0, 0),
                                row(0, 2, 1, 0),
                                row(1, 0, 0, 0),
                                row(1, 2, 1, 0),
                                row(2, 0, 0, 0),
                                row(2, 2, 1, 0));
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE b = 1"),
                                row(0, 1, 0, 1),
                                row(0, 3, 1, 1),
                                row(1, 1, 0, 1),
                                row(1, 3, 1, 1),
                                row(2, 1, 0, 1),
                                row(2, 3, 1, 1));

        // test both indexes combined
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a = 0 AND b = 0"),
                                row(0, 0, 0, 0),
                                row(1, 0, 0, 0),
                                row(2, 0, 0, 0));
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a = 0 AND b = 1"),
                                row(0, 1, 0, 1),
                                row(1, 1, 0, 1),
                                row(2, 1, 0, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a = 1 AND b = 0"),
                                row(0, 2, 1, 0),
                                row(1, 2, 1, 0),
                                row(2, 2, 1, 0));
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a = 1 AND b = 1"),
                                row(0, 3, 1, 1),
                                row(1, 3, 1, 1),
                                row(2, 3, 1, 1));
    }
}
