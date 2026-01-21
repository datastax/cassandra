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

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

@RunWith(Parameterized.class)
public class VectorCompaction2dTest extends VectorCompactionTest
{
    @Override
    public int dimension()
    {
        return 2;
    }

    @Parameterized.Parameter(0)
    public Version version;

    @Parameterized.Parameter(1)
    public boolean enableNVQ;

    @Parameterized.Parameters(name = "{0} {1}")
    public static Collection<Object[]> data()
    {
        // See Version file for explanation of changes associated with each version
        return Version.ALL.stream()
                          .filter(v -> v.onOrAfter(Version.JVECTOR_EARLIEST))
                          .flatMap(vd -> {
                              // NVQ is only relevant some of the time, but we always pass it so that the test
                              // could be broken up into multiple tests, and would finish before timeouts.
                              return Arrays.stream(new Boolean[]{ true, false }).map(b -> new Object[]{ vd, b });
                          })
                          .collect(Collectors.toList());
    }

    @Before
    public void setCurrentVersion() throws Throwable
    {
        SAIUtil.setCurrentVersion(version);
    }

    @Before
    public void setEnableNVQ() throws Throwable
    {
        SAIUtil.setEnableNVQ(enableNVQ);
    }
}
