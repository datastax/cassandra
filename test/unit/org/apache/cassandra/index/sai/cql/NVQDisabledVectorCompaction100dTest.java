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

import java.util.Collection;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

@RunWith(Parameterized.class)
public class NVQDisabledVectorCompaction100dTest extends VectorCompaction100dTest
{

    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        // we don't require support for these, even though it isn't always relevant
        return allVersions();
    }

    @Before
    public void setCurrentVersion() throws Throwable
    {
        SAIUtil.setCurrentVersion(version);
    }

    @Before
    public void setEnableNVQ()
    {
        SAIUtil.setEnableNVQ(false);
    }
}
