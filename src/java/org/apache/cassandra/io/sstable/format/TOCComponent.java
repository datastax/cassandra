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

package org.apache.cassandra.io.sstable.format;

import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableWatcher;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;

import static org.apache.cassandra.io.util.File.WriteMode.APPEND;
import static org.apache.cassandra.io.util.File.WriteMode.OVERWRITE;

public class TOCComponent
{
    private static final Logger logger = LoggerFactory.getLogger(TOCComponent.class);

    /**
     * Reads the list of components from the TOC component.
     *
     * @return set of components found in the TOC
     */
    public static Set<Component> loadTOC(Descriptor descriptor) throws IOException
    {
        return loadTOC(descriptor, true);
    }

    /**
     * Reads the list of components from the TOC component.
     *
     * @param skipMissing skip adding the component to the returned set if the corresponding file is missing.
     * @return set of components found in the TOC
     */
    public static Set<Component> loadTOC(Descriptor descriptor, boolean skipMissing) throws IOException
    {
        File tocFile = descriptor.fileFor(Components.TOC);
        List<String> componentNames = Files.readAllLines(tocFile.toPath());
        Set<Component> components = Sets.newHashSetWithExpectedSize(componentNames.size());
        for (String componentName : componentNames)
        {
            Component component = Component.parse(componentName, descriptor.version.format);
            if (skipMissing && !descriptor.fileFor(component).exists())
                logger.error("Missing component: {}", descriptor.fileFor(component));
            else
                components.add(component);
        }
        return components;
    }

    /**
     * Write TOC file with given components and write mode
     */
    public static void writeTOC(File tocFile, Collection<Component> components, File.WriteMode writeMode)
    {
        try (FileOutputStreamPlus out = tocFile.newOutputStream(writeMode);
             PrintWriter w = new PrintWriter(out))
        {
            for (Component component : components)
                w.println(component.name);
            w.flush();
            out.sync();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, tocFile);
        }
    }

    /**
     * Appends new component names to the TOC component.
     */
    @SuppressWarnings("resource")
    public static void appendTOC(Descriptor descriptor, Collection<Component> components)
    {
        File tocFile = descriptor.fileFor(Components.TOC);
        writeTOC(tocFile, components, APPEND);
    }

    public static Set<Component> loadOrCreate(Descriptor descriptor)
    {
        try
        {
            try
            {
                // Try loading TOC first without discovering components or checking file existence.
                return TOCComponent.loadTOC(descriptor, false);
            }
            catch (FileNotFoundException | NoSuchFileException e)
            {
                SSTableWatcher.instance.discoverComponents(descriptor);

                // Try loading TOC again after discovering components, still without existence checks
                try
                {
                    return TOCComponent.loadTOC(descriptor, false);
                }
                catch (FileNotFoundException | NoSuchFileException e2)
                {
                    // Still no TOC, create it from discovered components
                    Set<Component> components = descriptor.discoverComponents();
                    if (components.isEmpty())
                        return components; // sstable doesn't exist yet

                    components.add(Components.TOC);
                    TOCComponent.appendTOC(descriptor, components);
                    return components;
                }
            }
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /**
     * Rewrite TOC components by deleting existing TOC file and append new components
     */
    public static void rewriteTOC(Descriptor descriptor, Collection<Component> components)
    {
        File tocFile = descriptor.fileFor(Components.TOC);
        // As this method *re*-write the TOC (and is currently only called by "unregisterComponents"), it should only
        // be called in contexts where the TOC is expected to exist. If it doesn't, there is probably something
        // unexpected happening, so we log relevant information to help diagnose a potential earlier problem.
        // But in principle, this isn't a big deal for this method, and we still end up with the TOC in the state we
        // expect.
        if (!tocFile.exists())
        {
            // Note: we pass a dummy runtime exception as a simple way to get a stack-trace. Knowing from where this
            // is called in this case is likely useful information.
            logger.warn("Was asked to 'rewrite' TOC file {} for sstable {}, but it does not exists. The file will be created but this is unexpected. The components to 'overwrite' are: {}", tocFile, descriptor, components, new RuntimeException());
        }

        writeTOC(tocFile, components, OVERWRITE);
    }

    public static void maybeAdd(Descriptor descriptor, Component component) throws IOException
    {
        Set<Component> toc = TOCComponent.loadOrCreate(descriptor);
        if (!toc.isEmpty() && toc.add(component))
            TOCComponent.rewriteTOC(descriptor, toc);
    }
}
