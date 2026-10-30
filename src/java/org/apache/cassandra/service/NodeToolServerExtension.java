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

/**
 * Server-side hook that lets a distribution bundling Cassandra (for instance HCD) set up, when the node starts, the
 * server-side half of its nodetool extensions: typically the MBeans that the commands contributed through a
 * {@link org.apache.cassandra.tools.NodeToolExtension} talk to over JMX.
 * <p>
 * Implementations are listed, comma-separated, in the
 * {@link org.apache.cassandra.config.CassandraRelevantProperties#CUSTOM_NODETOOL_SERVER_EXTENSION_CLASSES} system
 * property of the server JVM. They are instantiated through their public no-arg constructor early in the startup of
 * the node, so that a misconfigured property aborts the startup before the node touches the ring; a class listed
 * twice is a misconfiguration too. Then {@link #setup()} is called once each, in the order they are listed, on the
 * startup thread, after the JMX server is started and the commit log replayed, and <em>before</em> the node joins
 * the ring ({@link StorageService#initServer()}), so that the commands of the extension are available while the node
 * bootstraps, like the built-in ones. {@link #setup()} must therefore not assume anything about the ring, the state of
 * the other nodes, or authentication: an MBean that needs them checks them when it is invoked.
 * <p>
 * A {@link #setup()} that throws aborts the startup of the node. There is no teardown callback: an extension that
 * starts threads or holds resources releases them through a shutdown hook (for instance
 * {@link StorageService#addPostShutdownHook(Runnable)}), and must be prepared to be set up again in the same JVM,
 * which happens when an in-JVM dtest instance restarts. Nothing restricts what {@link #setup()} does, but it is meant
 * for the nodetool integration, not as a general-purpose startup hook.
 */
public interface NodeToolServerExtension
{
    /**
     * Sets up the server-side half of the extension, for instance by registering MBeans through
     * {@link org.apache.cassandra.utils.MBeanWrapper#instance}.
     */
    void setup();
}
