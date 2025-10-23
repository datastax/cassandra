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

package org.apache.cassandra.index.sai;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.MBeanWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@inheritDoc}
 */
public class StorageAttachedIndexConfig implements StorageAttachedIndexConfigMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.index.sai:type=StorageAttachedIndexConfig";
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageAttachedIndexConfig.class);
    private static final StorageAttachedIndexConfig INSTANCE = new StorageAttachedIndexConfig();

    public static StorageAttachedIndexConfig instance()
    {
        return INSTANCE;
    }

    private StorageAttachedIndexConfig()
    {
    }

    public void registerMBean()
    {
        // We do registering here instead of the constructor to avoid issues with the lazily building of the singleton
        // instance, and the inlining of the calls to CassandraRelevantProperties, which might lead to never actually
        // building the instance.
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    @Override
    public boolean isSlowQueryLogExecutionInfoEnabled()
    {
        return CassandraRelevantProperties.SAI_SLOW_QUERY_LOG_EXECUTION_INFO_ENABLED.getBoolean();
    }

    @Override
    public void setSlowQueryLogExecutionInfoEnabled(boolean enabled)
    {
        LOGGER.info("Setting {} from {} to {}",
                CassandraRelevantProperties.SAI_SLOW_QUERY_LOG_EXECUTION_INFO_ENABLED.getKey(),
                CassandraRelevantProperties.SAI_SLOW_QUERY_LOG_EXECUTION_INFO_ENABLED.getBoolean(),
                enabled);
        CassandraRelevantProperties.SAI_SLOW_QUERY_LOG_EXECUTION_INFO_ENABLED.setBoolean(enabled);
    }
}
