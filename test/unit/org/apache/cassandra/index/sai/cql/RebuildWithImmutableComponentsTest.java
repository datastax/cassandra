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

package org.apache.cassandra.index.sai.cql;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.PathUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RebuildWithImmutableComponentsTest extends AbstractRebuildAndImmutableComponentsTester
{
    @Override
    protected boolean useImmutableComponents()
    {
        return true;
    }

    @Override
    protected void validateSSTables(ColumnFamilyStore cfs, IndexContext context) throws Exception
    {
        Index.Group indexGroup = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assert indexGroup != null;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            // Make sure the TOC has all the active components; this is somewhat indirectly tested by the few next
            // assertions, but good sanity check (and somewhat test the `activeComponents` method too).
            assertTrue(SSTable.readTOC(sstable.descriptor).containsAll(indexGroup.activeComponents(sstable)));

            // verify TOC only includes latest SAI generation
            Set<Component> saiComponents = SSTable.readTOC(sstable.descriptor).stream().filter(c -> c.name.contains("SAI")).collect(Collectors.toSet());
            assertEquals(saiComponents, indexGroup.activeComponents(sstable));

            // verify SSTable#components only includes latest SAI generation
            saiComponents = sstable.components().stream().filter(c -> c.name.contains("SAI")).collect(Collectors.toSet());
            assertEquals(saiComponents, indexGroup.activeComponents(sstable));

            IndexDescriptor descriptor = IndexDescriptor.load(sstable, Set.of(context));
            assertEquals(1, descriptor.perSSTableComponents().buildId().generation());
            assertEquals(1, descriptor.perIndexComponents(context).buildId().generation());

            Set<String> files = allSSTableFilenames(sstable);
            for (var components : List.of(descriptor.perSSTableComponents(), descriptor.perIndexComponents(context)))
            {
                for (var component : components.all())
                {
                    String gen0Component = components.version().fileNameFormatter().format(component.componentType(), components.context(), 0);
                    String expectedFilename = sstable.descriptor.fileFor(new Component(Component.Type.CUSTOM, gen0Component)).name();
                    assertTrue( "File " + expectedFilename + " not found in " + files, files.contains(expectedFilename));
                }
            }
        }
    }

    private static Set<String> allSSTableFilenames(SSTableReader sstable)
    {
        Set<String> files = new HashSet<>();
        PathUtils.forEach(sstable.descriptor.directory.toPath(), path -> {
            String filename = path.getFileName().toString();
            if (filename.startsWith(sstable.descriptor.filenamePart()))
                files.add(filename);
        });
        return files;
    }
}
