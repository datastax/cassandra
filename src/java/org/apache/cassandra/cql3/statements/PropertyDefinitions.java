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
package org.apache.cassandra.cql3.statements;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.exceptions.SyntaxException;

public class PropertyDefinitions
{
    public static final String MULTIPLE_DEFINITIONS_ERROR = "Multiple definitions for property '%s'";
    protected static final Logger logger = LoggerFactory.getLogger(PropertyDefinitions.class);
    private static final Pattern POSITIVE_PATTERN = Pattern.compile("(1|true|yes)");
    private static final Pattern NEGATIVE_PATTERN = Pattern.compile("(0|false|no)");
    private static final Map<String, Long> OBSOLETE_PROPERTY_LOG_TIME = new ConcurrentHashMap<>();
    private static long OBSOLETE_PROPERTY_LOG_INTERVAL_MS = 30_000;
    protected final Map<String, Object> properties = new HashMap<>();

    public static boolean parseBoolean(String key, String value) throws SyntaxException
    {
        if (null == value)
            throw new IllegalArgumentException("value argument can't be null");

        String lowerCasedValue = value.toLowerCase();

        if (POSITIVE_PATTERN.matcher(lowerCasedValue).matches())
            return true;
        else if (NEGATIVE_PATTERN.matcher(lowerCasedValue).matches())
            return false;

        throw new SyntaxException(String.format("Invalid boolean value %s for '%s'. " +
                                                "Positive values can be '1', 'true' or 'yes'. " +
                                                "Negative values can be '0', 'false' or 'no'.",
                                                value, key));
    }

    public static Integer toInt(String key, String value, Integer defaultValue) throws SyntaxException
    {
        if (value == null)
        {
            return defaultValue;
        }
        else
        {
            try
            {
                return Integer.valueOf(value);
            }
            catch (NumberFormatException e)
            {
                throw new SyntaxException(String.format("Invalid integer value %s for '%s'", value, key));
            }
        }
    }

    public void addProperty(String name, String value) throws SyntaxException
    {
        if (properties.putIfAbsent(name, value) != null)
            throw new SyntaxException(String.format(MULTIPLE_DEFINITIONS_ERROR, name));
    }

    public void addProperty(String name, Map<String, String> value) throws SyntaxException
    {
        if (properties.putIfAbsent(name, value) != null)
            throw new SyntaxException(String.format(MULTIPLE_DEFINITIONS_ERROR, name));
    }

    public void addProperty(String name, Set<QualifiedName> value) throws SyntaxException
    {
        if (properties.putIfAbsent(name, value) != null)
            throw new SyntaxException(String.format(MULTIPLE_DEFINITIONS_ERROR, name));
    }

    public void validate(Set<String> keywords, Set<String> obsolete) throws SyntaxException
    {
        for (String name : properties.keySet())
        {
            if (keywords.contains(name))
                continue;

            if (obsolete.contains(name))
            {
                long now = System.currentTimeMillis();
                Long lastLogged = OBSOLETE_PROPERTY_LOG_TIME.get(name);

                if (lastLogged == null || (now - lastLogged) >= OBSOLETE_PROPERTY_LOG_INTERVAL_MS)
                {
                    logger.warn("Ignoring obsolete property {}", name);
                    OBSOLETE_PROPERTY_LOG_TIME.put(name, now);
                }
            }
            else
                throw new SyntaxException(String.format("Unknown property '%s'", name));
        }
    }

    @Nullable
    protected String getSimple(String name) throws SyntaxException
    {
        Object val = properties.get(name);
        if (val == null)
            return null;
        if (!(val instanceof String))
            throw new SyntaxException(String.format("Invalid value for property '%s'. It should be a string", name));
        return (String) val;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public Set<QualifiedName> getQualifiedNames(String name) throws SyntaxException
    {
        Object val = properties.get(name);
        if (val == null)
            return null;
        if (val instanceof Map && ((Map<?, ?>) val).isEmpty()) // to solve the ambiguity between empty map and empty set
            return Collections.emptySet();
        if (!(val instanceof Set))
            throw new SyntaxException(String.format("Invalid value for property '%s'. It should be a set of identifiers.", name));
        return (Set<QualifiedName>) val;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public Map<String, String> getMap(String name) throws SyntaxException
    {
        Object val = properties.get(name);
        if (val == null)
            return null;
        if (val instanceof Set && ((Set<?>) val).isEmpty()) // to solve the ambiguity between empty map and empty set
            return Collections.emptyMap();
        if (!(val instanceof Map))
            throw new SyntaxException(String.format("Invalid value for property '%s'. It should be a map.", name));
        return (Map<String, String>) val;
    }

    public Boolean hasProperty(String name)
    {
        return properties.containsKey(name);
    }

    // Return a property value, typed as a Boolean
    public Boolean getBoolean(String key, Boolean defaultValue) throws SyntaxException
    {
        String value = getSimple(key);
        return (value == null) ? defaultValue : parseBoolean(key, value);
    }

    // Return a property value, typed as a double
    public double getDouble(String key, double defaultValue) throws SyntaxException
    {
        String value = getSimple(key);
        if (value == null)
        {
            return defaultValue;
        }
        else
        {
            try
            {
                return Double.parseDouble(value);
            }
            catch (NumberFormatException e)
            {
                throw new SyntaxException(String.format("Invalid double value %s for '%s'", value, key));
            }
        }
    }

    // Return a property value, typed as an Integer
    public Integer getInt(String key, Integer defaultValue) throws SyntaxException
    {
        String value = getSimple(key);
        return toInt(key, value, defaultValue);
    }

    /**
     * Returns the name of all the properties that are updated by this object.
     */
    public Set<String> updatedProperties()
    {
        return properties.keySet();
    }

    public void removeProperty(String name)
    {
        properties.remove(name);
    }

    public Object getProperty(String name)
    {
        Object ret = properties.get(name);
        if (ret == null)
            throw new SyntaxException(String.format("Invalid value for property '%s'. It should not be null.", name));

        return ret;
    }
}
