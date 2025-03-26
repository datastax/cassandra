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

import java.util.Objects;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.index.sai.IndexContext;

/**
 * Identifies a particular build of a per-sstable or per-index group of SAI index components, aka a pair of the
 * {@link Version} built and the generation.
 */
public class ComponentsBuildId implements Comparable<ComponentsBuildId>
{
    private static final ComponentsBuildId FOR_NEW_SSTABLE = ComponentsBuildId.latest(0);

    private final Version version;
    private final int generation;

    private ComponentsBuildId(Version version, int generation)
    {
        this.version = version;
        this.generation = generation;
    }

    public static ComponentsBuildId of(Version version, int generation)
    {
        return new ComponentsBuildId(version, generation);
    }

    public static ComponentsBuildId latest(int generation)
    {
        return of(Version.latest(), generation);
    }

    public static ComponentsBuildId forNewSSTable()
    {
        return FOR_NEW_SSTABLE;
    }

    public static ComponentsBuildId forNewBuild(@Nullable ComponentsBuildId previousBuild, Predicate<ComponentsBuildId> newBuildIsUsablePredicate)
    {
        Version version = Version.latest();
        // If we're not using immutable components, we always use generation 0, and we're fine if that overrides existing files
        if (!version.useImmutableComponentFiles())
            return new ComponentsBuildId(version, 0);

        // Otherwise, if there is no previous build or the new build is for a new version, then we can "tentatively"
        // use generation 0, but if not, we need to bump the generation.
        int generation = previousBuild != null && previousBuild.version.equals(version) ? previousBuild.generation + 1 : 0;
        var candidate = new ComponentsBuildId(version, generation);

        // Usually, the candidate above is fine, but we want to avoid overriding existing file (it's theoretically
        // possible that the next generation was created at some other point, but then corrupted, and so we falled back
        // on the previous generation but some of those file for the next generation still exists). So we check,
        // repeatedly if that candidate is usable, incrementing the generation until we find one which is.
        while (!newBuildIsUsablePredicate.test(candidate))
            candidate = new ComponentsBuildId(version, ++generation);

        return candidate;
    }

    public Version version()
    {
        return version;
    }

    public int generation()
    {
        return generation;
    }

    public String formatAsComponent(IndexComponentType indexComponentType, IndexContext indexContext)
    {
        return version.fileNameFormatter().format(indexComponentType, indexContext, generation);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof ComponentsBuildId))
            return false;
        ComponentsBuildId that = (ComponentsBuildId) obj;
        return this.version.equals(that.version) && this.generation == that.generation;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(version, generation);
    }

    @Override
    public int compareTo(ComponentsBuildId that)
    {
        if (this.version.equals(that.version))
            return Integer.compare(generation, that.generation);

        return this.version.onOrAfter(that.version) ? 1 : -1;
    }

    @Override
    public String toString()
    {
        return version + "@" + generation;
    }

}
