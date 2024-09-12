/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction.unified;

import static org.apache.cassandra.db.compaction.unified.Controller.LEGACY_PREFIX;
import static org.apache.cassandra.db.compaction.unified.Controller.PREFIX;

public class DSECompatibilityUtils
{
    protected static String getSystemProperty(String option)
    {
        return getSystemProperty(option, null);
    }

    /**
     * Tries to get system property with PREFIX first. If it doesn't find it,
     * looks for system property with LEGACY_PREFIX. If both are missing it
     * returns the default value.
     */
    protected static String getSystemProperty(String option, String defaultValue) {
        String value = System.getProperty(PREFIX + option);
        if (value == null)
            value = System.getProperty(LEGACY_PREFIX + option, defaultValue);
        return value;
    }

    protected static Boolean getBooleanSystemProperty(String option)
    {
        return Boolean.parseBoolean(getSystemProperty(option));
    }

    public static Integer getIntegerSystemProperty(String option, int defaultValue)
    {
        String value = getSystemProperty(option, String.valueOf(defaultValue));
        return Integer.valueOf(value);
    }
}
