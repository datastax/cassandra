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

package org.apache.cassandra.index.sai.disk.format;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.FBUtilities;

/**
 * This is an interface for providing whether to use immutable component files for new builds of index components.
 * This is going to be used by CNDB to enable immutable components for a given tenant.
 * <p>
 * Note that it may be called multiple times for the same build, so implementations should avoid
 * switching from {@code true} to {@code false} for a keyspace while index builds are in progress.
 */
public interface ImmutableSAIComponentsConfigProvider
{
    Logger logger = LoggerFactory.getLogger(ImmutableSAIComponentsConfigProvider.class);

    ImmutableSAIComponentsConfigProvider INSTANCE = ImmutableSAIComponentsConfigProvider.fromProperty();

    /**
     * @param keyspace the keyspace for which to provide the config, assumed to belong to an existing keyspace.
     * @return whether immutable component files should be used for new builds of index components of the provided
     * keyspace. Note that {@link CassandraRelevantProperties#IMMUTABLE_SAI_COMPONENTS_MIN_VERSION} still applies.
     */
    boolean useImmutableComponentsForKeyspace(String keyspace);

    class Default implements ImmutableSAIComponentsConfigProvider
    {

        @Override
        public boolean useImmutableComponentsForKeyspace(String keyspace)
        {
            return CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS.getBoolean();
        }
    }

    static ImmutableSAIComponentsConfigProvider fromProperty()
    {
        try
        {
            String providerClass = CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS_CONFIG_PROVIDER_CLASS.getString();
            if (providerClass.isEmpty())
            {
                return new Default();
            }
            else
            {
                logger.info("Using SAI immutable components config provider: {}", providerClass);
                return FBUtilities.construct(providerClass, "SAI immutable components config provider");
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
