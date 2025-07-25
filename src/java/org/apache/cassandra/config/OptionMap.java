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
package org.apache.cassandra.config;

import java.util.Map;

public class OptionMap
{
    private final Map<String, String> options;

    public OptionMap(Map<String, String> options)
    {
        this.options = options;
    }

    public String get(String key, String defaultValue)
    {
        return options.containsKey(key)
                ? options.get(key)
                : defaultValue;
    }

    public int get(String key, int defaultValue)
    {
        return options.containsKey(key)
                ? Integer.parseInt(options.get(key))
                : defaultValue;
    }

    public Class<?> get(String key, Class<?> defaultValue) throws ClassNotFoundException
    {
        return options.containsKey(key)
                ? Class.forName(options.get(key))
                : defaultValue;
    }

    public Map<String, String> getOptions()
    {
        return options;
    }
}
