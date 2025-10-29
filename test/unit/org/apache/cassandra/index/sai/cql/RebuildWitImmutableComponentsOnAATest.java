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

import org.junit.After;
import org.junit.Before;

import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.apache.cassandra.config.CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS_MIN_VERSION;

public class RebuildWitImmutableComponentsOnAATest extends RebuildWithImmutableComponentsTest
{
    private String defaultImmutableMinVersionSetting;

    @Before
    public void setupAAVersion()
    {
        SAIUtil.setCurrentVersion(Version.AA);
        defaultImmutableMinVersionSetting = IMMUTABLE_SAI_COMPONENTS_MIN_VERSION.getString();
        IMMUTABLE_SAI_COMPONENTS_MIN_VERSION.setString("aa");
    }

    @After
    public void restoreVersionSettings()
    {
        SAIUtil.setCurrentVersion(Version.LATEST);
        if (defaultImmutableMinVersionSetting != null)
            IMMUTABLE_SAI_COMPONENTS_MIN_VERSION.setString(defaultImmutableMinVersionSetting);
    }
}
