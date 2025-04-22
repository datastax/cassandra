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

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

public class DefaultIndexComponentDiscovery extends IndexComponentDiscovery
{
    @Override
    public SSTableIndexComponentsState discoverComponents(SSTableReader sstable) {
        Descriptor descriptor = sstable.getDescriptor();

        // Older versions might not have all components in the TOC, we should not trust it (fix for CNDB-13582):
        if (descriptor.version.getVersion().compareTo("ca") < 0)
           return discoverComponentsFromDiskFallback(descriptor);

        SSTableIndexComponentsState groups = tryDiscoverComponentsFromTOC(descriptor);
        return groups == null
               ? discoverComponentsFromDiskFallback(descriptor)
               : groups;
    }
}
