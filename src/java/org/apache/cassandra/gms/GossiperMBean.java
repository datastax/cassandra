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

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

public interface GossiperMBean
{
    public long getEndpointDowntime(String address) throws UnknownHostException;

    public int getCurrentGenerationNumber(String address) throws UnknownHostException;

    public void unsafeAssassinateEndpoint(String address) throws UnknownHostException;

    public void assassinateEndpoint(String address) throws UnknownHostException;

    /**
     * Do not call this method unless you know what you are doing.
     * In case a node went into a hibernate state - i.e. replacing a node with the <em>same</em> address
     * or bootstrapping a node without letting join the ring - and it's required to bring that node back
     * to a normal status (e.g. for a failed replace operation), use this method.
     * It can be called on any node, prefer a seed node, to set the status back to {@code normal}.
     *
     * @param address endpoint to revive
     */
    public void reviveEndpoint(String address) throws UnknownHostException;

    public List<String> reloadSeeds();

    public List<String> getSeeds();

    /** Returns each node's database release version */
    public Map<String, List<String>> getReleaseVersionsWithPort();

}
