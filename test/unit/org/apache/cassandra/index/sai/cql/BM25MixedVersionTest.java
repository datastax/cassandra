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

import javax.annotation.Nullable;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

/**
 * Tests BM25 with sstables using different index versions.
 * This verifies that the scores given to the results coming from each sstables are compatible with each other.
 * See <a href="https://github.com/riptano/cndb/issues/17887">CNDB-17887</a> for further details.
 */
@RunWith(Parameterized.class)
public class BM25MixedVersionTest extends SAITester
{
    /** The version of a first sstable. */
    @Parameterized.Parameter
    public Version oldVersion;

    /** The version of a second sstable, or null if the second set of rows is not flushed. */
    @Parameterized.Parameter(1)
    @Nullable
    public Version newVersion;

    @Parameterized.Parameters(name = "old={0} new={1}")
    public static Collection<Object[]> parameters()
    {
        List<Object[]> params = new ArrayList<>();
        for (Version oldVersion : Version.ALL)
        {
            if (!oldVersion.onOrAfter(Version.BM25_EARLIEST))
                continue;

            for (Version newVersion : Version.ALL)
            {
                if (!newVersion.after(oldVersion))
                    continue;

                params.add(new Object[]{ oldVersion, newVersion });
            }

            params.add(new Object[]{ oldVersion, null });
        }
        return params;
    }

    @Test
    public void testBM25WithMixedVersions()
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'english'}");

        SAIUtil.setCurrentVersion(oldVersion);
        execute("INSERT INTO %s(k, c, v) VALUES (0, 1, 'bananas bananas bananas ORANGE')");
        execute("INSERT INTO %s(k, c, v) VALUES (0, 2, 'bananas bananas ORANGE ORANGE')");
        execute("INSERT INTO %s(k, c, v) VALUES (0, 3, 'bananas ORANGE ORANGE ORANGE')");
        flush();

        if (newVersion != null)
            SAIUtil.setCurrentVersion(newVersion);
        execute("INSERT INTO %s(k, c, v) VALUES (0, 4, 'bananas ORANGE ORANGE ORANGE')");
        execute("INSERT INTO %s(k, c, v) VALUES (0, 5, 'bananas bananas ORANGE ORANGE')");
        execute("INSERT INTO %s(k, c, v) VALUES (0, 6, 'bananas bananas bananas ORANGE')");
        if (newVersion != null)
            flush();

        assertRows(execute("SELECT c FROM %s ORDER BY v BM25 OF 'banana' LIMIT 10"),
                   row(1), row(6), row(2), row(5), row(3), row(4));
        assertRows(execute("SELECT c FROM %s ORDER BY v BM25 OF 'orange' LIMIT 10"),
                   row(3), row(4), row(2), row(5), row(1), row(6));
    }
}
