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

import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.Ordering;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.assertj.core.api.Assertions;

@RunWith(Parameterized.class)
public class SAIUtilTest
{
    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Version.ALL.stream()
                          .map(v -> new Object[]{ v })
                          .collect(Collectors.toList());
    }

    @Before
    public void setCurrentVersion() throws Throwable
    {
        SAIUtil.setCurrentVersion(version);
    }

    @Test
    public void testCurrentVersion()
    {
        Assertions.assertThat(Version.current())
                  .isEqualTo(version);
    }

    @Test
    public void testANNUseSyntheticScoreProperty()
    {
        Assertions.assertThat(Ordering.Ann.useSyntheticScore())
                  .isEqualTo(Ordering.Ann.useSyntheticScore(version));
    }
}
