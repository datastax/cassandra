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
package org.apache.cassandra.schema;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InheritingClass;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.TrieMemtableFactory;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.StorageCompatibilityMode;

/**
 * Memtable types and options are specified with these parameters. Memtable classes must either contain a static
 * {@code FACTORY} field (if they take no arguments other than class), or implement a
 * {@code factory(Map<String, String>)} method.
 *
 * The latter should consume any further options (using {@code map.remove}).
 *
 * See Memtable_API.md for further details on the configuration and usage of memtable implementations.
  */
public final class MemtableParams
{
    private static final Logger logger = LoggerFactory.getLogger(MemtableParams.class);
    public final Memtable.Factory factory;
    private final String configurationKey;

    private MemtableParams(Memtable.Factory factory, String configurationKey)
    {
        this.configurationKey = configurationKey;
        this.factory = factory;
    }

    public String configurationKey()
    {
        return configurationKey;
    }

    public Memtable.Factory factory()
    {
        return factory;
    }

    /**
     * Returns a map representation of the memtable configuration for backward compatibility with CC 4.0.
     * This is used when outputting schema in a format compatible with CC 4.0.
     *
     * For the "default" configuration key, we output an empty map {} to let each Cassandra version
     * interpret "default" according to its own configuration. This ensures backward compatibility
     * with CC 4.0 which uses an empty map to represent the default memtable configuration.
     *
     * For other configurations, CC 4.0 accepts both short class names (e.g., 'TrieMemtable') and
     * fully qualified names (e.g., 'org.apache.cassandra.db.memtable.TrieMemtable'). For standard
     * Cassandra memtables in the org.apache.cassandra.db.memtable package, we use short names and
     * for custom memtables from other packages, we preserve the fully qualified class name.
     */
    public Map<String, String> toMapForCC4()
    {
        if ("default".equals(configurationKey))
            return ImmutableMap.of();

        ParameterizedClass definition = CONFIGURATION_DEFINITIONS.get(configurationKey);
        if (definition != null && definition.class_name != null)
        {
            Map<String, String> map = new HashMap<>();
            String className = definition.class_name;

            if (className.startsWith("org.apache.cassandra.db.memtable."))
            {
                className = className.substring("org.apache.cassandra.db.memtable.".length());
            }

            map.put("class", className);
            if (definition.parameters != null)
                map.putAll(definition.parameters);
            return map;
        }
        // Fallback for unknown configurations
        return ImmutableMap.of("class", configurationKey);
    }

    @Override
    public String toString()
    {
        return configurationKey;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof MemtableParams))
            return false;

        MemtableParams c = (MemtableParams) o;

        return Objects.equal(configurationKey, c.configurationKey);
    }

    @Override
    public int hashCode()
    {
        return configurationKey.hashCode();
    }

    private static final String DEFAULT_CONFIGURATION_KEY = "default";
    private static final Memtable.Factory DEFAULT_MEMTABLE_FACTORY = TrieMemtableFactory.INSTANCE;
    private static final ParameterizedClass DEFAULT_CONFIGURATION = TrieMemtableFactory.CONFIGURATION;
    private static final Map<String, ParameterizedClass>
        CONFIGURATION_DEFINITIONS = expandDefinitions(DatabaseDescriptor.getMemtableConfigurations());
    private static final Map<String, MemtableParams> CONFIGURATIONS = new HashMap<>();
    public static final MemtableParams DEFAULT = get(null);

    public static Set<String> knownDefinitions()
    {
        return CONFIGURATION_DEFINITIONS.keySet();
    }

    public static MemtableParams get(String key)
    {
        if (key == null)
            key = DEFAULT_CONFIGURATION_KEY;

        synchronized (CONFIGURATIONS)
        {
            return CONFIGURATIONS.computeIfAbsent(key, MemtableParams::parseConfiguration);
        }
    }

    public static MemtableParams getWithFallback(String key)
    {
        try
        {
            return get(key);
        }
        catch (ConfigurationException e)
        {
            logger.error("Invalid memtable configuration \"" + key + "\" in schema. " +
                         "Falling back to default to avoid schema mismatch.\n" +
                         "Please ensure the correct definition is given in cassandra.yaml.",
                         e);
            return new MemtableParams(DEFAULT.factory(), key);
        }
    }

    /**
     * Useful for testing where we can provide a factory that produces spied instances of memtable so that we can
     * modify behaviour of certains methods.
     */
    // Used by CNDB
    @VisibleForTesting
    public static MemtableParams forTesting(Memtable.Factory factory, String configurationKey)
    {
        return new MemtableParams(factory, configurationKey);
    }

    @VisibleForTesting
    static Map<String, ParameterizedClass> expandDefinitions(Map<String, InheritingClass> memtableConfigurations)
    {
        if (memtableConfigurations == null)
            return ImmutableMap.of(DEFAULT_CONFIGURATION_KEY, DEFAULT_CONFIGURATION);

        LinkedHashMap<String, ParameterizedClass> configs = new LinkedHashMap<>(memtableConfigurations.size() + 1);

        // If default is not overridden, add an entry first so that other configurations can inherit from it.
        // If it is, process it in its point of definition, so that the default can inherit from another configuration.
        if (!memtableConfigurations.containsKey(DEFAULT_CONFIGURATION_KEY))
            configs.put(DEFAULT_CONFIGURATION_KEY, DEFAULT_CONFIGURATION);

        Map<String, InheritingClass> inheritingClasses = new LinkedHashMap<>();

        for (Map.Entry<String, InheritingClass> entry : memtableConfigurations.entrySet())
        {
            if (entry.getValue().inherits != null)
            {
                if (entry.getKey().equals(entry.getValue().inherits))
                    throw new ConfigurationException(String.format("Configuration entry %s can not inherit itself.", entry.getKey()));

                if (memtableConfigurations.get(entry.getValue().inherits) == null && !entry.getValue().inherits.equals(DEFAULT_CONFIGURATION_KEY))
                    throw new ConfigurationException(String.format("Configuration entry %s inherits non-existing entry %s.",
                                                                   entry.getKey(), entry.getValue().inherits));

                inheritingClasses.put(entry.getKey(), entry.getValue());
            }
            else
                configs.put(entry.getKey(), entry.getValue().resolve(configs));
        }

        for (Map.Entry<String, InheritingClass> inheritingEntry : inheritingClasses.entrySet())
        {
            String inherits = inheritingEntry.getValue().inherits;
            while (inherits != null)
            {
                InheritingClass nextInheritance = inheritingClasses.get(inherits);
                if (nextInheritance == null)
                    inherits = null;
                else
                    inherits = nextInheritance.inherits;

                if (inherits != null && inherits.equals(inheritingEntry.getKey()))
                    throw new ConfigurationException(String.format("Detected loop when processing key %s", inheritingEntry.getKey()));
            }
        }

        while (!inheritingClasses.isEmpty())
        {
            Set<String> forRemoval = new HashSet<>();
            for (Map.Entry<String, InheritingClass> inheritingEntry : inheritingClasses.entrySet())
            {
                if (configs.get(inheritingEntry.getValue().inherits) != null)
                {
                    configs.put(inheritingEntry.getKey(), inheritingEntry.getValue().resolve(configs));
                    forRemoval.add(inheritingEntry.getKey());
                }
            }

            assert !forRemoval.isEmpty();

            for (String toRemove : forRemoval)
                inheritingClasses.remove(toRemove);
        }

        return ImmutableMap.copyOf(configs);
    }

    private static MemtableParams parseConfiguration(String configurationKey)
    {
        ParameterizedClass definition = CONFIGURATION_DEFINITIONS.get(configurationKey);

        if (definition == null)
            throw new ConfigurationException("Memtable configuration \"" + configurationKey + "\" not found.");
        return new MemtableParams(getMemtableFactory(definition), configurationKey);
    }


    private static Memtable.Factory getMemtableFactory(ParameterizedClass options)
    {
        // Special-case this so that we don't initialize memtable class for tests that need to delay that.
        if (options == DEFAULT_CONFIGURATION)
            return DEFAULT_MEMTABLE_FACTORY;

        String className = options.class_name;
        if (className == null || className.isEmpty())
            throw new ConfigurationException("The 'class_name' option must be specified.");

        className = className.contains(".") ? className : "org.apache.cassandra.db.memtable." + className;
        try
        {
            Memtable.Factory factory;
            Class<?> clazz = Class.forName(className);
            final Map<String, String> parametersCopy = options.parameters != null
                                                       ? new HashMap<>(options.parameters)
                                                       : new HashMap<>();
            try
            {
                Method factoryMethod = clazz.getDeclaredMethod("factory", Map.class);
                factory = (Memtable.Factory) factoryMethod.invoke(null, parametersCopy);
            }
            catch (NoSuchMethodException e)
            {
                // continue with FACTORY field
                Field factoryField = clazz.getDeclaredField("FACTORY");
                factory = (Memtable.Factory) factoryField.get(null);
            }
            if (!parametersCopy.isEmpty())
                throw new ConfigurationException("Memtable class " + className + " does not accept any futher parameters, but " +
                                                 parametersCopy + " were given.");
            return factory;
        }
        catch (NoSuchFieldException | ClassNotFoundException | IllegalAccessException | InvocationTargetException | ClassCastException e)
        {
            if (e.getCause() instanceof ConfigurationException)
                throw (ConfigurationException) e.getCause();
            throw new ConfigurationException("Could not create memtable factory for class " + options, e);
        }
    }

    /**
     * Attempts to read memtable configuration, with fallback for CC4 upgrade compatibility.
     *
     * CC4 stored memtable as {@code frozen<map<text, text>>}, while CC5 uses text.
     * During upgrades or in mixed clusters, the column may contain either format.
     *
     * This method uses byte-sniffing (detecting null bytes) to determine the actual
     * format of the stored data. The storage_compatibility_mode determines the format
     * to write, but doesn't guarantee the format of existing stored data.
     *
     * This defensive approach handles:
     * <ul>
     *   <li>Upgrading from CC4 to CC5 (reads CC4 format, writes CC5 format)</li>
     *   <li>Running CC5 in CC_4 mode (reads either format, writes CC4 format)</li>
     *   <li>Mixed clusters during rolling upgrades (reads both formats)</li>
     * </ul>
     *
     * @param row The row containing the memtable column
     * @param columnName The name of the memtable column
     * @return MemtableParams instance, or DEFAULT if column is missing or invalid
     */
    public static MemtableParams getWithCC4Fallback(UntypedResultSet.Row row, String columnName)
    {
        if (!row.has(columnName))
            return DEFAULT;

        String stringValue;
        try
        {
            stringValue = row.getString(columnName);
        }
        catch (MarshalException e)
        {
            // CC4 map data may not be valid UTF-8, fall back to map parsing
            return parseCC4MapFormat(row, columnName);
        }

        // Check if this looks like binary data (contains null bytes from CC4's map serialization)
        if (stringValue != null && stringValue.indexOf('\0') >= 0)
        {
            return parseCC4MapFormat(row, columnName);
        }

        // Normal CC5 string value
        return getWithFallback(stringValue);
    }

    private static MemtableParams parseCC4MapFormat(UntypedResultSet.Row row, String columnName)
    {
        // This is likely CC4's frozen<map<text, text>> serialization
        // Try to read it as a map instead
        try
        {
            ByteBuffer raw = row.getBytes(columnName);
            Map<String, String> cc4Map = MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false).compose(raw);

            if (cc4Map == null || cc4Map.isEmpty())
            {
                // Empty map in CC4 means "default"
                logger.info("Detected CC4 empty memtable map for upgrade compatibility, using default");
                return DEFAULT;
            }

            // Convert CC4 map format to CC5 configuration key
            String className = cc4Map.get("class");
            if (className != null)
            {
                // CC4 used class names like "SkipListMemtable" or "TrieMemtable"
                // Try to map to CC5 configuration keys
                String configKey = mapCC4ClassNameToCC5Key(className);
                logger.info("Detected CC4 memtable configuration '{}', mapped to CC5 key '{}'",
                            className, configKey);
                return getWithFallback(configKey);
            }
            else
            {
                // CC4 map exists but has no "class" key - likely corrupted data
                logger.warn("Detected CC4 memtable map without 'class' key, falling back to default");
                return DEFAULT;
            }
        }
        catch (Exception e)
        {
            logger.warn("Failed to parse memtable column as CC4 map format, falling back to default", e);
            return DEFAULT;
        }
    }

    private static String mapCC4ClassNameToCC5Key(String cc4ClassName)
    {
        // Handle both short names and fully qualified names
        String shortName = cc4ClassName.contains(".")
                           ? cc4ClassName.substring(cc4ClassName.lastIndexOf('.') + 1)
                           : cc4ClassName;

        // Map common CC4 class names to CC5 configuration keys
        switch (shortName)
        {
            case "SkipListMemtable":
                return "skiplist";
            case "TrieMemtable":
                return "trie";
            default:
                // For unknown types, try the short name as-is
                logger.warn("Unknown CC4 memtable class '{}', attempting to use as configuration key", shortName);
                return shortName.toLowerCase();
        }
    }

    /**
     * Returns the memtable value as a map for CC4 compatibility mode.
     * Used when storage_compatibility_mode is CC_4 or CASSANDRA_4.
     *
     * @return Map representation for CC4 schema (frozen&lt;map&lt;text,text&gt;&gt;)
     * @throws ConfigurationException if the memtable type is not compatible with CC4
     */
    public Map<String, String> asSchemaValueMap()
    {
        return asSchemaValueMap(DatabaseDescriptor.getStorageCompatibilityMode());
    }

    /**
     * Returns the memtable value as a map for CC4 compatibility mode.
     * This overload exists for testing purposes.
     *
     * @param mode The storage compatibility mode to use
     * @return Map representation for CC4 schema (frozen&lt;map&lt;text,text&gt;&gt;)
     * @throws ConfigurationException if the memtable type is not compatible with CC4
     */
    @VisibleForTesting
    Map<String, String> asSchemaValueMap(StorageCompatibilityMode mode)
    {
        if (!mode.isBefore(CassandraVersion.CASSANDRA_5_0.major))
            throw new IllegalStateException("Cannot get map value in CC5 mode. Use asSchemaValueText() instead.");

        // CC4 writes empty map {} for "default" configuration
        if ("default".equals(configurationKey))
            return ImmutableMap.of();

        // Validate and map the configuration key to CC4 class name
        // This also validates CC4 compatibility (rejects sharded types, unknown configs)
        String className = mapCC5KeyToCC4ClassName(configurationKey);

        // Get the configuration definition to access parameters
        ParameterizedClass definition = CONFIGURATION_DEFINITIONS.get(configurationKey);

        // Build the map with class name and any additional parameters
        Map<String, String> map = new HashMap<>();
        map.put("class", className);
        if (definition != null && definition.parameters != null)
            map.putAll(definition.parameters);

        return map;
    }

    /**
     * Returns the memtable value as text for CC5 mode.
     * Used when storage_compatibility_mode is NONE.
     *
     * @return String representation for CC5 schema (text)
     */
    public String asSchemaValueText()
    {
        return asSchemaValueText(DatabaseDescriptor.getStorageCompatibilityMode());
    }

    /**
     * Returns the memtable value as text for CC5 mode.
     * This overload exists for testing purposes.
     *
     * @param mode The storage compatibility mode to use
     * @return String representation for CC5 schema (text)
     * @throws IllegalStateException if called in CC4 compatibility mode
     */
    @VisibleForTesting
    String asSchemaValueText(StorageCompatibilityMode mode)
    {
        if (mode.isBefore(CassandraVersion.CASSANDRA_5_0.major))
            throw new IllegalStateException("Cannot get text value in CC4 compatibility mode. Use asSchemaValueMap() instead.");

        return configurationKey;
    }

    /**
     * Maps CC5 configuration key to CC4 class name, validating CC4 compatibility.
     * This method combines validation and mapping for use in CC4 compatibility mode.
     *
     * @param configKey The CC5 configuration key (e.g., "trie", "skiplist")
     * @return The corresponding CC4 class name (e.g., "TrieMemtable")
     * @throws ConfigurationException if the configuration is not compatible with CC4
     */
    private static String mapCC5KeyToCC4ClassName(String configKey)
    {
        if (configKey == null || configKey.isEmpty())
            throw new ConfigurationException("Configuration key cannot be null or empty");

        // Check if this is a CC5-only memtable type
        // ShardedSkipListMemtable and related sharded types don't exist in CC4
        String lowerKey = configKey.toLowerCase();
        if (lowerKey.contains("sharded"))
        {
            throw new ConfigurationException(
                String.format("Memtable configuration '%s' is not compatible with CC4. " +
                             "Sharded memtable types were introduced in CC5. " +
                             "Please use 'skiplist' or 'trie' when storage_compatibility_mode is CC_4 or CASSANDRA_4.",
                             configKey));
        }

        // Check if the configuration key exists in CONFIGURATION_DEFINITIONS
        // This ensures we're not trying to write an unknown/invalid configuration
        ParameterizedClass definition = CONFIGURATION_DEFINITIONS.get(configKey);
        if (definition == null)
        {
            throw new ConfigurationException(
                String.format("Memtable configuration '%s' not found in cassandra.yaml. " +
                             "Cannot write to schema in CC4 compatibility mode.",
                             configKey));
        }

        // Get the class name from the definition and strip the package prefix
        // CC4 accepts both short names (e.g., 'TrieMemtable') and fully qualified names,
        // but we use short names for standard Cassandra memtables
        String className = definition.class_name;
        if (className == null || className.isEmpty())
        {
            throw new ConfigurationException(
                String.format("Memtable configuration '%s' has no class name defined.",
                             configKey));
        }

        // Strip the standard Cassandra memtable package prefix
        if (className.startsWith("org.apache.cassandra.db.memtable."))
        {
            className = className.substring("org.apache.cassandra.db.memtable.".length());
        }

        return className;
    }
}
