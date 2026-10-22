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

package org.apache.cassandra.io.sstable.format.bti;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Streams;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.format.AbstractTestVersionSupportedFeatures;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.utils.Pair;
import org.assertj.core.api.SoftAssertions;

import static org.assertj.core.api.Assertions.assertThat;

public class VersionSupportedFeaturesTest extends AbstractTestVersionSupportedFeatures
{
    @Override
    protected Version getVersion(String v)
    {
        return DatabaseDescriptor.getSSTableFormats().get(BtiFormat.NAME).getVersion(v);
    }

    /**
     * 'ab' (pre-GA DSE 6.8 "LABS") was renamed to 'ba' and has its features: every feature of 'ba' is a feature of
     * 'ab' too, and no feature 'ba' lacks.
     */
    @Test
    public void testAbIsHandledAsBa()
    {
        Version ab = getVersion("ab");
        Version ba = getVersion("ba");
        assertThat(ab.version).isEqualTo("ab");
        assertThat(ab.isLatestVersion()).isFalse();
        assertThat(ab.isCompatible()).isTrue();

        List<Pair<String, Predicate<Version>>> features = List.of(Pair.create("indicesAreEncrypted", Version::indicesAreEncrypted),
                                                                  Pair.create("metadataIsEncrypted", Version::metadataIsEncrypted),
                                                                  Pair.create("hasImprovedMinMax", Version::hasImprovedMinMax),
                                                                  Pair.create("hasLegacyMinMax", Version::hasLegacyMinMax),
                                                                  Pair.create("hasAccurateMinMax", Version::hasAccurateMinMax),
                                                                  Pair.create("hasOldBfFormat", Version::hasOldBfFormat),
                                                                  Pair.create("hasOriginatingHostId", Version::hasOriginatingHostId),
                                                                  Pair.create("hasZeroCopyMetadata", Version::hasZeroCopyMetadata),
                                                                  Pair.create("hasIncrementalNodeSyncMetadata", Version::hasIncrementalNodeSyncMetadata),
                                                                  Pair.create("hasMaxColumnValueLengths", Version::hasMaxColumnValueLengths),
                                                                  Pair.create("hasMisplacedPartitionLevelDeletionsPresenceMarker", Version::hasMisplacedPartitionLevelDeletionsPresenceMarker),
                                                                  Pair.create("hasPartitionLevelDeletionsPresenceMarker", Version::hasPartitionLevelDeletionsPresenceMarker),
                                                                  Pair.create("hasIsTransient", Version::hasIsTransient),
                                                                  Pair.create("hasTokenSpaceCoverage", Version::hasTokenSpaceCoverage),
                                                                  Pair.create("hasKeyRange", Version::hasKeyRange),
                                                                  Pair.create("hasUIntDeletionTime", Version::hasUIntDeletionTime),
                                                                  Pair.create("hasImplicitlyFrozenTuples", Version::hasImplicitlyFrozenTuples));
        SoftAssertions assertions = new SoftAssertions();
        for (Pair<String, Predicate<Version>> feature : features)
            assertions.assertThat(feature.right.test(ab)).describedAs(feature.left).isEqualTo(feature.right.test(ba));
        assertions.assertThat(ab.getByteComparableVersion()).describedAs("byteComparableVersion").isEqualTo(ba.getByteComparableVersion());
        assertions.assertThat(ab.correspondingMessagingVersion()).describedAs("correspondingMessagingVersion").isEqualTo(ba.correspondingMessagingVersion());
        assertions.assertAll();

        // in particular, 'ab' sstables of encrypted tables have encrypted indexes and metadata
        assertThat(ab.indicesAreEncrypted()).isTrue();
        assertThat(ab.metadataIsEncrypted()).isTrue();
    }

    /**
     * @return the given versions, with 'ab' (which has the features of 'ba') added if they contain 'ba' and removed
     * otherwise
     */
    private static Stream<String> withAbAsBa(Stream<String> versions)
    {
        List<String> list = versions.filter(v -> !v.equals("ab")).collect(Collectors.toList());
        if (list.contains("ba"))
            list.add("ab");
        return ALL_VERSIONS.stream().filter(list::contains);
    }

    @Override
    protected Stream<String> getPendingRepairSupportedVersions()
    {
        return ALL_VERSIONS.stream();
    }

    @Override
    protected Stream<String> getPartitionLevelDeletionPresenceMarkerSupportedVersions()
    {
        return withAbAsBa(range("da", "zz"));
    }

    @Override
    protected Stream<String> getLegacyMinMaxSupportedVersions()
    {
        return withAbAsBa(range("aa", "az"));
    }

    @Override
    protected Stream<String> getImprovedMinMaxSupportedVersions()
    {
        return withAbAsBa(range("ba", "zz"));
    }

    @Override
    protected Stream<String> getKeyRangeSupportedVersions()
    {
        return withAbAsBa(range("da", "zz"));
    }

    @Override
    protected Stream<String> getOriginatingHostIdSupportedVersions()
    {
        return withAbAsBa(Streams.concat(range("ad", "az"), range("bb", "zz")));
    }

    @Override
    protected Stream<String> getAccurateMinMaxSupportedVersions()
    {
        return withAbAsBa(range("ac", "az"));
    }

    @Override
    protected Stream<String> getCommitLogLowerBoundSupportedVersions()
    {
        return ALL_VERSIONS.stream();
    }

    @Override
    protected Stream<String> getCommitLogIntervalsSupportedVersions()
    {
        return ALL_VERSIONS.stream();
    }

    @Override
    protected Stream<String> getZeroCopyMetadataSupportedVersions()
    {
        return withAbAsBa(range("ba", "bz"));
    }

    @Override
    protected Stream<String> getIncrementalNodeSyncMetadataSupportedVersions()
    {
        return withAbAsBa(range("ba", "bz"));
    }

    @Override
    protected Stream<String> getMaxColumnValueLengthsSupportedVersions()
    {
        return withAbAsBa(range("ba", "bz"));
    }

    @Override
    protected Stream<String> getIsTransientSupportedVersions()
    {
        return withAbAsBa(range("ca", "zz"));
    }

    @Override
    protected Stream<String> getMisplacedPartitionLevelDeletionsPresenceMarkerSupportedVersions()
    {
        return withAbAsBa(range("ba", "cz"));
    }

    @Override
    protected Stream<String> getTokenSpaceCoverageSupportedVersions()
    {
        return withAbAsBa(range("cb", "zz"));
    }

    @Override
    protected Stream<String> getOldBfFormatSupportedVersions()
    {
        return withAbAsBa(range("aa", "az"));
    }
}
