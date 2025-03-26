/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.io.compress;


import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Factory for obtaining secret key providers.
 * Instances implementing IKeyProviderFactory are created by reflection by calling a default constructor.
 */
public interface IKeyProviderFactory
{
    /**
     * Returns a key provider configured with the given options.
     * It is allowed to return the same instance for the same options.
     * @param options options dependent on the actual key provider type
     * @return secret key provider
     * @throws IOException if key provider could not be contacted
     */
    IKeyProvider getKeyProvider(Map<String, String> options) throws IOException;

    /**
     * @return a list of options supported by getKeyProvider
     */
    Set<String> supportedOptions();

}
