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

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.assertj.core.api.SoftAssertions;

public abstract class AbstractTestVersionSupportedFeatures
{
    public static final List<String> ALL_VERSIONS = IntStream.rangeClosed('a', 'z')
                                                             .mapToObj(i -> String.valueOf((char) i))
                                                             .flatMap(first -> IntStream.rangeClosed('a', 'z').mapToObj(second -> first + (char) second))
                                                             .collect(Collectors.toList());

    protected abstract Version getVersion(String v);

    protected abstract Stream<String> getPendingRepairSupportedVersions();

    protected abstract Stream<String> getPartitionLevelDeletionPresenceMarkerSupportedVersions();

    protected abstract Stream<String> getLegacyMinMaxSupportedVersions();

    protected abstract Stream<String> getImprovedMinMaxSupportedVersions();

    protected abstract Stream<String> getKeyRangeSupportedVersions();

    protected abstract Stream<String> getOriginatingHostIdSupportedVersions();

    protected abstract Stream<String> getAccurateMinMaxSupportedVersions();

    protected abstract Stream<String> getCommitLogLowerBoundSupportedVersions();

    protected abstract Stream<String> getCommitLogIntervalsSupportedVersions();

    protected abstract Stream<String> getZeroCopyMetadataSupportedVersions();

    protected abstract Stream<String> getIncrementalNodeSyncMetadataSupportedVersions();

    protected abstract Stream<String> getMaxColumnValueLengthsSupportedVersions();

    protected abstract Stream<String> getIsTransientSupportedVersions();

    protected abstract Stream<String> getMisplacedPartitionLevelDeletionsPresenceMarkerSupportedVersions();

    protected abstract Stream<String> getTokenSpaceCoverageSupportedVersions();

    protected abstract Stream<String> getOldBfFormatSupportedVersions();

    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testCompatibility()
    {
        SoftAssertions assertions = new SoftAssertions();
        checkPredicateAgainstVersions(Version::hasPendingRepair, getPendingRepairSupportedVersions(), "hasPendingRepair", assertions);
        checkPredicateAgainstVersions(Version::hasImprovedMinMax, getImprovedMinMaxSupportedVersions(), "hasImprovedMinMax", assertions);
        checkPredicateAgainstVersions(Version::hasLegacyMinMax, getLegacyMinMaxSupportedVersions(), "hasLegacyMinMax", assertions);
        checkPredicateAgainstVersions(Version::hasPartitionLevelDeletionsPresenceMarker, getPartitionLevelDeletionPresenceMarkerSupportedVersions(), "hasPartitionLevelDeletionsPresenceMarker", assertions);
        checkPredicateAgainstVersions(Version::hasKeyRange, getKeyRangeSupportedVersions(), "hasKeyRange", assertions);
        checkPredicateAgainstVersions(Version::hasOriginatingHostId, getOriginatingHostIdSupportedVersions(), "hasOriginatingHostId", assertions);
        checkPredicateAgainstVersions(Version::hasAccurateMinMax, getAccurateMinMaxSupportedVersions(), "hasAccurateMinMax", assertions);
        checkPredicateAgainstVersions(Version::hasCommitLogLowerBound, getCommitLogLowerBoundSupportedVersions(), "hasCommitLogLowerBound", assertions);
        checkPredicateAgainstVersions(Version::hasCommitLogIntervals, getCommitLogIntervalsSupportedVersions(), "hasCommitLogIntervals", assertions);
        checkPredicateAgainstVersions(Version::hasZeroCopyMetadata, getZeroCopyMetadataSupportedVersions(), "hasZeroCopyMetadata", assertions);
        checkPredicateAgainstVersions(Version::hasIncrementalNodeSyncMetadata, getIncrementalNodeSyncMetadataSupportedVersions(), "hasIncrementalNodeSyncMetadata", assertions);
        checkPredicateAgainstVersions(Version::hasMaxColumnValueLengths, getMaxColumnValueLengthsSupportedVersions(), "hasMaxColumnValueLengths", assertions);
        checkPredicateAgainstVersions(Version::hasIsTransient, getIsTransientSupportedVersions(), "hasIsTransient", assertions);
        checkPredicateAgainstVersions(Version::hasMisplacedPartitionLevelDeletionsPresenceMarker, getMisplacedPartitionLevelDeletionsPresenceMarkerSupportedVersions(), "hasMisplacedPartitionLevelDeletionsPresenceMarker", assertions);
        checkPredicateAgainstVersions(Version::hasTokenSpaceCoverage, getTokenSpaceCoverageSupportedVersions(), "hasTokenSpaceCoverage", assertions);
        checkPredicateAgainstVersions(Version::hasOldBfFormat, getOldBfFormatSupportedVersions(), "hasOldBfFormat", assertions);

        checkPredicateAgainstVersions(v -> !(v.hasPartitionLevelDeletionsPresenceMarker() && v.hasMisplacedPartitionLevelDeletionsPresenceMarker()), ALL_VERSIONS.stream(), "hasPartitionLevelDeletionsPresenceMarker and hasMisplacedPartitionLevelDeletionsPresenceMarker", assertions);

        assertions.assertAll();;
    }

    public static Stream<String> range(String fromIncl, String toIncl)
    {
        int fromIdx = ALL_VERSIONS.indexOf(fromIncl);
        int toIdx = ALL_VERSIONS.indexOf(toIncl);
        assert fromIdx >= 0 && toIdx >= 0;
        return ALL_VERSIONS.subList(fromIdx, toIdx + 1).stream();
    }

    /**
     * Check the version predicate against the provided versions.
     *
     * @param predicate     predicate to check against version
     * @param versionBounds a stream of versions for which the predicate should return true
     * @param assertions
     */
    private void checkPredicateAgainstVersions(Predicate<Version> predicate, Stream<String> versionBounds, String name, SoftAssertions assertions)
    {
        List<String> expected = versionBounds.collect(Collectors.toList());
        List<String> actual = ALL_VERSIONS.stream().filter(v -> predicate.test(getVersion(v))).collect(Collectors.toList());
        assertions.assertThat(actual).describedAs(name).isEqualTo(expected);
    }
}
