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
package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.List;

/**
 * A simple endpoint snitch implementation that treats Strategy order as proximity,
 * allowing non-read-repaired reads to prefer a single endpoint, which improves
 * cache locality.
 */
public class SimpleSnitch extends AbstractEndpointSnitch
{
    private static final String DEFAULT_DC = "datacenter1";
    private static final String DEFAULT_RACK = "rack1";
    public String getRack(InetAddress endpoint)
    {
        return DEFAULT_RACK;
    }

    public String getDatacenter(InetAddress endpoint)
    {
        return DEFAULT_DC;
    }

    @Override
    public void sortByProximity(final InetAddress address, List<InetAddress> addresses)
    {
        // Optimization to avoid walking the list
    }

    public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
    {
        // Making all endpoints equal ensures we won't change the original ordering (since
        // Collections.sort is guaranteed to be stable)
        return 0;
    }

    @Override
    public boolean isDefaultDC(String dc)
    {
        assert dc != null;
        return dc == DEFAULT_DC;
    }
}
