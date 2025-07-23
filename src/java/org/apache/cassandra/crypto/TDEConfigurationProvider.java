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

import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_KEY_DIRECTORY;

public class TDEConfigurationProvider
{
    private static String systemKeyDirectoryProperty = SYSTEM_KEY_DIRECTORY.getString();

    public static TDEConfiguration getConfiguration()
    {
        //TODO replace with reading the system key directory from config file
        return new TDEConfiguration(systemKeyDirectoryProperty);
    }

    @VisibleForTesting
    public static void setSystemKeyDirectoryProperty(String value)
    {
        systemKeyDirectoryProperty = value;
    }
}
