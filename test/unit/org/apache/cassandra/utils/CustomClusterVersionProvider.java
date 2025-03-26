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

package org.apache.cassandra.utils;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.gms.IClusterVersionProvider;

public class CustomClusterVersionProvider implements IClusterVersionProvider
{
    public static void set()
    {
        CassandraRelevantProperties.CLUSTER_VERSION_PROVIDER_CLASS_NAME.setString(CustomClusterVersionProvider.class.getName());
    }

    public final static CustomClusterVersionProvider instance = new CustomClusterVersionProvider();

    public volatile CassandraVersion version = new CassandraVersion(FBUtilities.getReleaseVersionString());

    public volatile boolean upgradeInProgress = false;

    public volatile Runnable onReset = null;

    private volatile boolean wasUsed = false;

    @Override
    public CassandraVersion getMinClusterVersion()
    {
        wasUsed = true;
        return version;
    }

    @Override
    public void reset()
    {
        wasUsed = true;
        if (onReset != null)
            onReset.run();
    }

    @Override
    public boolean isUpgradeInProgress()
    {
        wasUsed = true;
        return upgradeInProgress;
    }

    public void assertUsed()
    {
        assert wasUsed;
    }
}
