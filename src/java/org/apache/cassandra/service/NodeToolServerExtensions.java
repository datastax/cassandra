/*
 * Copyright IBM Corp.
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
package org.apache.cassandra.service;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_NODETOOL_SERVER_EXTENSION_CLASSES;

/**
 * Loads and sets up the {@link NodeToolServerExtension}s configured through
 * {@link org.apache.cassandra.config.CassandraRelevantProperties#CUSTOM_NODETOOL_SERVER_EXTENSION_CLASSES}.
 */
public final class NodeToolServerExtensions
{
    private static final Logger logger = LoggerFactory.getLogger(NodeToolServerExtensions.class);

    private NodeToolServerExtensions()
    {
    }

    /**
     * Instantiates the configured extensions, in the order they are listed.
     *
     * @throws ConfigurationException if a listed class cannot be found or instantiated, is not a
     * {@link NodeToolServerExtension}, or is listed twice
     */
    public static List<NodeToolServerExtension> load() throws ConfigurationException
    {
        return FBUtilities.constructAll(CUSTOM_NODETOOL_SERVER_EXTENSION_CLASSES.getString(),
                                        NodeToolServerExtension.class,
                                        "nodetool server extension (set with " + CUSTOM_NODETOOL_SERVER_EXTENSION_CLASSES.getKey() + ')');
    }

    /**
     * Calls {@link NodeToolServerExtension#setup()} on each of the given extensions, in order. Called once, by the
     * daemon, during the startup of the node, with the extensions returned by {@link #load()}.
     *
     * @throws RuntimeException whatever a {@link NodeToolServerExtension#setup()} throws
     */
    public static void setupAll(List<NodeToolServerExtension> extensions)
    {
        for (NodeToolServerExtension extension : extensions)
        {
            logger.info("Setting up nodetool server extension {}", extension.getClass().getName());
            extension.setup();
        }
    }
}
