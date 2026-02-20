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

package org.apache.cassandra.nodes;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link NodesPersistence} subclass that migrates CC4 file-based node metadata
 * to CC5 system tables on first load.
 *
 * This class is instantiated by {@link Nodes} when a CC4 nodes directory is detected.
 * It reads CC4's MessagePack metadata via {@link CC4NodesFileReader}, persists the
 * migrated data to the system tables, and returns it.
 */
public class CC4UpgradeNodesPersistence extends NodesPersistence
{
    private static final Logger logger = LoggerFactory.getLogger(CC4UpgradeNodesPersistence.class);

    @Override
    public LocalInfo loadLocal()
    {
        LocalInfo migrated = CC4NodesFileReader.tryReadLocalInfo();
        if (migrated != null)
        {
            logger.info("Migrating local node metadata from CC4 file-based store (host_id={})", migrated.getHostId());
            saveLocal(migrated);
            return migrated;
        }
        logger.debug("No CC4 local metadata found to migrate");
        return null;
    }

    @Override
    public Stream<PeerInfo> loadPeers()
    {
        List<PeerInfo> migrated = CC4NodesFileReader.tryReadPeers().collect(Collectors.toList());
        if (!migrated.isEmpty())
        {
            logger.info("Migrating {} peer entries from CC4 file-based store", migrated.size());
            for (PeerInfo peer : migrated)
                savePeer(peer);
        }

        CC4NodesFileReader.archiveCC4NodesDirectory();

        return migrated.stream();
    }
}
