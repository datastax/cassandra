/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.io.compress;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Optional;

public class OptionMap
{
    private final Map<String, String> options;

    public OptionMap() {
        this(new HashMap<String, String>());
    }

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

    public Optional<Integer> getOptionalInteger(String key)
    {
        String value = options.get(key);
        return value == null ? Optional.<Integer>absent() : Optional.of(Integer.valueOf(value));
    }

    public Optional<Double> getOptionalDouble(String key)
    {
        String value = options.get(key);
        return value == null ? Optional.<Double>absent() : Optional.of(Double.valueOf(value));
    }

    public Map<String, String> getOptions()
    {
        return options;
    }

    public void putIfPresent(String key, Optional<?> value)
    {
        if (value.isPresent())
        {
            options.put(key, String.valueOf(value.get()));
        }
    }
}
