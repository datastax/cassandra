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

package org.apache.cassandra.distributed.test.sai.features;

import java.io.IOException;

import org.junit.Assume;
import org.junit.BeforeClass;

import org.apache.cassandra.index.sai.disk.format.Version;

/**
 * {@link FeaturesVersionSupportTester} for {@link Version#ED}.
 */
public class FeaturesVersionSupportEDTest extends FeaturesVersionSupportTester
{
    @BeforeClass
    public static void setup() throws IOException
    {
        // Skip this test only when ED is the LATEST version
        // The test framework is designed to test older versions against LATEST
        // When a newer version is added (e.g., EE becomes LATEST), this test will 
        // automatically start running again to test ED compatibility
        Assume.assumeFalse("Skipping test because Version.ED is currently the LATEST version", 
                          Version.ED.equals(Version.LATEST));
        
        initCluster(Version.ED);
    }
}
