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
