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
package org.apache.cassandra.crypto;

import com.google.common.annotations.VisibleForTesting;

public class TDEConfigurationProvider
{
    private static String systemKeyDirectoryProperty = System.getProperty("cassandra.system_key_directory", "/etc/cassandra/conf");

    public static TDEConfiguration getConfiguration()
    {
        //TODO TDE porting: for now, just return a default configuration
        return new TDEConfiguration(systemKeyDirectoryProperty);
    }

    @VisibleForTesting
    public static void setSystemKeyDirectoryProperty(String value)
    {
        systemKeyDirectoryProperty = value;
    }
}
