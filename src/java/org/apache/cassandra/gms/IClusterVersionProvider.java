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

package org.apache.cassandra.gms;

import org.apache.cassandra.utils.CassandraVersion;

public interface IClusterVersionProvider
{
    /**
     * Returns the minimum Cassandra version of the nodes in the cluster. The method skips the nodes that have no or
     * invalid version. However, if such nodes are present, {@link #isUpgradeInProgress()}} returns {@code true}.
     */
    CassandraVersion getMinClusterVersion();

    /**
     * Resets the provider to its initial state. This is called by the {@link Gossiper} when nodes join or becomes alive.
     * It can be also called by the tests to verify the behaviour.
     */
    void reset();

    /**
     * Returns {@code true} if the cluster is in the middle of an upgrade. This is the case when the provider has
     * detected that some nodes have no or invalid version. May also implement some grace period to avoid flapping.
     */
    boolean isUpgradeInProgress();
}
