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

import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.disk.format.Version;

public class VectorCompaction100dTest extends VectorCompactionTest
{
    // The full version matrix takes longer than the test fork timeout on slow CI hosts, so the
    // 100d suite is sharded by version: this class covers FB and later, older versions are covered
    // by VectorCompaction100dEdFaTest, VectorCompaction100dEbEcTest and
    // VectorCompaction100dLegacyTest.
    @Parameterized.Parameters(name = "version={0} enableNVQ={1}")
    public static Collection<Object[]> data()
    {
        return data(v -> v.onOrAfter(Version.FB));
    }

    @Override
    public int dimension()
    {
        return 100;
    }
}
