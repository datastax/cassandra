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
package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.inject.Expression.expr;
import static org.junit.Assert.assertEquals;

/**
 * Test to verify that we can query a sstable with multiple indexes on different versions.
 * </p>
 * We shouldn't generate such a scenario after CNDB-8756, which makes all the indexes of a sstable use the same version.
 * However, we can have old sstables created before that fix was introduced, and we should be able to deal with them
 * when possible.
 *
 * @see CreateIndexWithNewVersionTest
 */
@RunWith(Parameterized.class)
public class MixedVersionsInSSTableTest extends SAITester
{
    @Parameterized.Parameter
    public Version initialVersion;

    @Parameterized.Parameter(1)
    public Version upgradeVersion;

    private Version oldestVersion;
    private Injection injection;

    @Parameterized.Parameters(name = "initial={0} upgrade={1}")
    public static Collection<Object[]> data()
    {
        List<Object[]> data = new ArrayList<>();
        for (Version oldVersion : Version.ALL)
        {
            for (Version newVersion : Version.ALL)
            {
                if (oldVersion == newVersion)
                    continue;

                data.add(new Object[]{ oldVersion, newVersion });
            }
        }
        return data;
    }

    @Before
    public void before() throws Throwable
    {
        oldestVersion = initialVersion.after(upgradeVersion) ? upgradeVersion : initialVersion;

        // Inject a rule to skip CNDB-8756, so new indexes are created with the new version regardless of having an
        // index using the old version.
        Expression expression = expr("org.apache.cassandra.index.sai.disk.format.Version." + upgradeVersion.toString().toUpperCase());
        injection = Injections.newCustom("use_current_version")
                              .add(InvokePointBuilder.newInvokePoint()
                                                     .onClass(IndexDescriptor.class)
                                                     .onMethod("versionForNewComponents")
                                                     .atEntry())
                              .add(ActionBuilder.newActionBuilder().actions().doReturn(expression))
                              .build();
        Injections.inject(injection);
        injection.disable();
    }

    @After
    public void after()
    {
        injection.disable();
    }

    @Test
    public void testMixedVersionsInSSTable()
    {
        createTable("CREATE TABLE %s(k int, c int, a int, b int, v vector<float, 2>, PRIMARY KEY(k, c))");

        execute("INSERT INTO %s (k, c, a, b, v) VALUES (0, 0, 0, 0, [0, 1])");
        execute("INSERT INTO %s (k, c, a, b, v) VALUES (0, 1, 1, 0, [0, 2])");
        execute("INSERT INTO %s (k, c, a, b, v) VALUES (0, 2, 0, 0, [0, 3])");
        execute("INSERT INTO %s (k, c, a, b, v) VALUES (1, 0, 1, 0, [0, 4])");
        execute("INSERT INTO %s (k, c, a, b, v) VALUES (1, 1, 0, 1, [0, 5])");
        execute("INSERT INTO %s (k, c, a, b, v) VALUES (1, 2, 1, 1, [0, 6])");
        execute("INSERT INTO %s (k, c, a, b, v) VALUES (2, 0, 0, 1, [0, 7])");
        execute("INSERT INTO %s (k, c, a, b, v) VALUES (2, 1, 1, 1, [0, 8])");
        execute("INSERT INTO %s (k, c, a, b, v) VALUES (2, 2, 0, 1, [0, 9])");
        flush();

        SAIUtil.setCurrentVersion(initialVersion);
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        verifySAIVersionInUse(initialVersion);

        injection.enable();

        SAIUtil.setCurrentVersion(upgradeVersion);
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        if (oldestVersion.onOrAfter(Version.JVECTOR_EARLIEST))
        {
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        }
        verifySAIVersionInUse(KEYSPACE, currentTable(), false, initialVersion, upgradeVersion);

        assertEquals(5, execute("SELECT * FROM %s WHERE a = 0").size());
        assertEquals(4, execute("SELECT * FROM %s WHERE a = 1").size());

        // queries only for the index on the new version break when the older index is in AA if they find results
        if (initialVersion == Version.AA)
        {
            assertFailsDueToPrimaryKey("SELECT * FROM %s WHERE b = 0");
            assertFailsDueToPrimaryKey("SELECT * FROM %s WHERE b = 1");
        }
        else
        {
            assertEquals(4, execute("SELECT * FROM %s WHERE b = 0").size());
            assertEquals(5, execute("SELECT * FROM %s WHERE b = 1").size());
        }
        assertEmpty(execute("SELECT * FROM %s WHERE b = 2"));

        assertEquals(2, execute("SELECT * FROM %s WHERE a = 0 AND b = 0").size());
        assertEquals(3, execute("SELECT * FROM %s WHERE a = 0 AND b = 1").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE a = 1 AND b = 0").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE a = 1 AND b = 1").size());
        assertEquals(7, execute("SELECT * FROM %s WHERE a = 0 OR b = 0").size());
        assertEquals(7, execute("SELECT * FROM %s WHERE a = 0 OR b = 1").size());
        assertEquals(6, execute("SELECT * FROM %s WHERE a = 1 OR b = 0").size());
        assertEquals(7, execute("SELECT * FROM %s WHERE a = 1 OR b = 1").size());

        if (oldestVersion.onOrAfter(Version.JVECTOR_EARLIEST))
        {
            assertEquals(2, execute("SELECT k, c FROM %s ORDER BY v ANN OF [0, 1] LIMIT 2").size());
            assertEquals(9, execute("SELECT k, c FROM %s ORDER BY v ANN OF [0, 1] LIMIT 10").size());
        }
    }

    private void assertFailsDueToPrimaryKey(String query)
    {
        Assertions.assertThatThrownBy(() -> executeInternal(query))
                  .isInstanceOf(AssertionError.class)
                  .hasMessageContaining("PrimaryKey");
    }
}
