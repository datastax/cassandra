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

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v2.V2OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v4.V4OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v5.V5OnDiskFormat;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Format version of indexing component, denoted as [major][minor]. Same forward-compatibility rules apply as to
 * {@link org.apache.cassandra.io.sstable.format.Version}.
 */
public class Version
{
    // 6.8 formats
    public static final Version AA = new Version("aa", V1OnDiskFormat.instance, Version::aaFileNameFormat);
    // Stargazer
    public static final Version BA = new Version("ba", V2OnDiskFormat.instance, (c, i, g) -> stargazerFileNameFormat(c, i, g, "ba"));
    // Converged Cassandra with JVector
    public static final Version CA = new Version("ca", V3OnDiskFormat.instance, (c, i, g) -> stargazerFileNameFormat(c, i, g, "ca"));
    // NOTE: use DB to prevent collisions with upstream file formats
    // Encode trie entries using their AbstractType to ensure trie entries are sorted for range queries and are prefix free.
    public static final Version DB = new Version("db", V4OnDiskFormat.instance, (c, i, g) -> stargazerFileNameFormat(c, i, g,"db"));
    // revamps vector postings lists to cause fewer reads from disk
    public static final Version DC = new Version("dc", V5OnDiskFormat.instance, (c, i, g) -> stargazerFileNameFormat(c, i, g, "dc"));

    // These are in reverse-chronological order so that the latest version is first. Version matching tests
    // are more likely to match the latest version so we want to test that one first.
    public static final List<Version> ALL = Lists.newArrayList(DC, DB, CA, BA, AA);

    public static final Version EARLIEST = AA;
    public static final Version VECTOR_EARLIEST = BA;
    // The latest version can be configured to be an earlier version to support partial upgrades that don't
    // write newer versions of the on-disk formats. This is volatile rather than final so that tests may
    // use reflection to change it and safely publish across threads.
    private static volatile Version LATEST = parse(System.getProperty("cassandra.sai.latest.version", "ca"));

    private static final Pattern GENERATION_PATTERN = Pattern.compile("\\d+");

    private final String version;
    private final OnDiskFormat onDiskFormat;
    private final FileNameFormatter fileNameFormatter;

    private Version(String version, OnDiskFormat onDiskFormat, FileNameFormatter fileNameFormatter)
    {
        this.version = version;
        this.onDiskFormat = onDiskFormat;
        this.fileNameFormatter = fileNameFormatter;
    }

    public static Version parse(String input)
    {
        checkArgument(input != null);
        checkArgument(input.length() == 2);
        for (var v : ALL) {
            if (input.equals(v.version))
                return v;
        }
        throw new IllegalArgumentException("Unrecognized SAI version string " + input);
    }

    public static Version latest()
    {
        return LATEST;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(version);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Version other = (Version)o;
        return Objects.equal(version, other.version);
    }

    @Override
    public String toString()
    {
        return version;
    }

    public boolean onOrAfter(Version other)
    {
        return version.compareTo(other.version) >= 0;
    }

    public OnDiskFormat onDiskFormat()
    {
        return onDiskFormat;
    }

    public FileNameFormatter fileNameFormatter()
    {
        return fileNameFormatter;
    }

    public boolean useImmutableComponentFiles()
    {
        // We only enable "immutable" components (meaning that new build don't delete or replace old versions) if the
        // flag is set and even then, only starting at version CA. There is no reason to need it for older versions,
        // and if older versions are involved, it means we likely want backward compatible behaviour.
        return CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS.getBoolean() && onOrAfter(Version.CA);
    }


    public static interface FileNameFormatter
    {
        /**
         * Format filename for given index component, context and generation.  Only the "component" part of the
         * filename is returned (so the suffix of the full filename), not a full path.
         */
        public String format(IndexComponentType indexComponentType, IndexContext indexContext, int generation);
    }

    /**
     * Try to parse the provided file name as a SAI component file name.
     *
     * @param filename the file name to try to parse.
     * @return the information parsed from the provided file name if it can be sucessfully parsed, or an empty optional
     * if the file name is not recognized as a SAI component file name for a supported version.
     */
    public static Optional<ParsedFileName> tryParseFileName(String filename)
    {
        if (!filename.endsWith(".db"))
            return Optional.empty();

        // For flexibility, we handle both "full" filename, of the form "<descriptor>-SAI+....db", or just the component
        // part, that is "SAI+....db". In the former, the following `lastIndexOf` will match, and we'll set
        // `startOfComponent` at the begining of "SAI", and in the later it will not match and return -1, which, with
        // the +1 will also be set at the begining of "SAI".
        int startOfComponent = filename.lastIndexOf('-') + 1;

        String componentStr = filename.substring(startOfComponent);
        if (componentStr.startsWith("SAI_"))
            return tryParseAAFileName(componentStr);
        else if (componentStr.startsWith("SAI" + SAI_SEPARATOR))
            return tryParseStargazerFileName(componentStr);
        else
            return Optional.empty();

    }

    public static class ParsedFileName
    {
        public final Version version;
        public final IndexComponentType component;
        public final @Nullable String indexName;
        public final int generation;

        private ParsedFileName(Version version, IndexComponentType component, @Nullable String indexName, int generation)
        {
            this.version = version;
            this.component = component;
            this.indexName = indexName;
            this.generation = generation;
        }
    }

    //
    // Version.AA filename formatter. This is the old DSE 6.8 SAI on-disk filename format
    //
    // Format: <sstable descriptor>-SAI(_<index name>)_<component name>.db
    //
    private static final String VERSION_AA_PER_SSTABLE_FORMAT = "SAI_%s.db";
    private static final String VERSION_AA_PER_INDEX_FORMAT = "SAI_%s_%s.db";

    private static String aaFileNameFormat(IndexComponentType indexComponentType, IndexContext indexContext, int generation)
    {
        Preconditions.checkArgument(generation == 0, "Generation is not supported for AA version");
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(indexContext == null ? String.format(VERSION_AA_PER_SSTABLE_FORMAT, indexComponentType.representation)
                                                  : String.format(VERSION_AA_PER_INDEX_FORMAT, indexContext.getIndexName(), indexComponentType.representation));

        return stringBuilder.toString();
    }

    private static Optional<ParsedFileName> tryParseAAFileName(String componentStr)
    {
        int generation = 0;
        int lastSepIdx = componentStr.lastIndexOf('_');
        if (lastSepIdx == -1)
            return Optional.empty();

        String indexComponentStr = componentStr.substring(lastSepIdx + 1, componentStr.length() - 3);
        IndexComponentType indexComponentType = IndexComponentType.fromRepresentation(indexComponentStr);

        String indexName = null;
        int firstSepIdx = componentStr.indexOf('_');
        if (firstSepIdx != -1 && firstSepIdx != lastSepIdx)
            indexName = componentStr.substring(firstSepIdx + 1, lastSepIdx);

        return Optional.of(new ParsedFileName(AA, indexComponentType, indexName, generation));
    }

    //
    // Stargazer filename formatter. This is the current SAI on-disk filename format
    //
    // Format: <sstable descriptor>-SAI+<version>(+<generation>)(+<index name>)+<component name>.db
    //
    public static final String SAI_DESCRIPTOR = "SAI";
    private static final String SAI_SEPARATOR = "+";
    private static final String EXTENSION = ".db";

    private static String stargazerFileNameFormat(IndexComponentType indexComponentType, IndexContext indexContext, int generation, String version)
    {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(SAI_DESCRIPTOR);
        stringBuilder.append(SAI_SEPARATOR).append(version);
        if (generation > 0)
            stringBuilder.append(SAI_SEPARATOR).append(generation);
        if (indexContext != null)
            stringBuilder.append(SAI_SEPARATOR).append(indexContext.getIndexName());
        stringBuilder.append(SAI_SEPARATOR).append(indexComponentType.representation);
        stringBuilder.append(EXTENSION);

        return stringBuilder.toString();
    }

    public ByteComparable.Version byteComparableVersionFor(IndexComponentType component, org.apache.cassandra.io.sstable.format.Version sstableFormatVersion)
    {
        return this == AA && component == IndexComponentType.TERMS_DATA
               ? sstableFormatVersion.getByteComparableVersion()
               : ByteComparable.Version.OSS41;
    }

    private static Optional<ParsedFileName> tryParseStargazerFileName(String componentStr)
    {
        // We skip the beginning `SAI+` and ending `.db` parts.
        String[] splits = componentStr.substring(4, componentStr.length() - 3).split("\\+");
        if (splits.length < 2 || splits.length > 4)
            return Optional.empty();

        Version version = parse(splits[0]);
        IndexComponentType indexComponentType = IndexComponentType.fromRepresentation(splits[splits.length - 1]);

        int generation = 0;
        String indexName = null;
        if (splits.length > 2)
        {
            // If we have 4 parts, then we know we have both the generation and index name. If we have 3
            // however, it means we have either one, but we don't know which, so we chekc if the additional
            // part is a number or not to distinguish.
            boolean hasGeneration = splits.length == 4 || GENERATION_PATTERN.matcher(splits[1]).matches();
            boolean hasIndexName = splits.length == 4 || !hasGeneration;
            if (hasGeneration)
                generation = Integer.parseInt(splits[1]);
            if (hasIndexName)
                indexName = splits[splits.length - 2];
        }

        return Optional.of(new ParsedFileName(version, indexComponentType, indexName, generation));
    }
}
