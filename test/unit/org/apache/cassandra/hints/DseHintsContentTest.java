/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.hints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The content of the hints files DataStax Enterprise 6 left in the fixtures: the hints catalog never loads them (see
 * {@link DseHintsFileTest}), and in {@code DSE_6} mode {@link DseHintsConverter} reads them (see
 * {@link DseHintsConversionTest}). Their hints are made of what this software can read. Past DSE's descriptor (version -1), the
 * records are framed as this software frames its own (see {@link DseFixtures#hintsRecords}), every hint starts with
 * its creation time and gc grace seconds, and every hint of a table this software knows deserializes with the mutation
 * layout of the DSE 6.8 messaging version, the one the commit log replay uses for DSE's segments; the others stop at
 * the unknown table id. Applied, the hints of the fixture table give back the data set the fixture workload wrote. How
 * many hints a file holds is not pinned: the 6.9.26 file holds those of two runs of the workload.
 */
public class DseHintsContentTest extends CQLTester
{
    private static final String FIXTURE_KEYSPACE = DseFixtures.FIXTURE_KEYSPACE;
    private static final TableId ROLE_PERMISSIONS = TableId.fromUUID(DseFixtures.ROLE_PERMISSIONS_TABLE_ID);
    /** DSE's {@code system_distributed.nodesync_status}, unknown to this software. */
    private static final TableId NODESYNC_STATUS = TableId.fromUUID(DseFixtures.NODESYNC_STATUS_TABLE_ID);
    /** The gc grace seconds each hint carries: the default of the tables, 90 days for {@code system_auth}. */
    private static final Map<TableId, Integer> GC_GRACE_SECONDS = ImmutableMap.of(ROLE_PERMISSIONS, 7776000,
                                                                                 NODESYNC_STATUS, 864000);
    private static final int FIXTURE_GC_GRACE_SECONDS = 864000;
    /**
     * The first and the last creation time of the hints of each file, in ms: hints are buffered before the file is
     * created, so the first ones are a few seconds older than its timestamp (1790335633208 and 1790587137402); the
     * last hints of 6.8.65 came 110 seconds after it, those of the second 6.9.26 run about 42 minutes after it.
     */
    private static final Map<DseVersion, long[]> CREATION_TIMES = ImmutableMap.of(DseVersion.DSE_6_8_65, new long[]{ 1790335623985L, 1790335742925L },
                                                                                 DseVersion.DSE_6_9_26, new long[]{ 1790587131628L, 1790589641757L });

    @After
    public void dropFixtureKeyspace() throws Throwable
    {
        execute("DROP KEYSPACE IF EXISTS " + FIXTURE_KEYSPACE);
    }

    @Test
    public void theHintsOfDse6865DecodeAndGiveBackTheFixtureRows() throws Throwable
    {
        assertThat(decodeApplyAndVerify(DseVersion.DSE_6_8_65)).containsExactlyInAnyOrder(ROLE_PERMISSIONS, NODESYNC_STATUS);
    }

    @Test
    public void theHintsOfDse6926DecodeAndGiveBackTheFixtureRows() throws Throwable
    {
        assertThat(decodeApplyAndVerify(DseVersion.DSE_6_9_26)).containsExactly(ROLE_PERMISSIONS);
    }

    /**
     * The DSE 6.8 layout is required, not merely accepted: DSE's partition updates carry a max timestamp after the table
     * id that this software's own layout does not have, so read at the current messaging version, not one hint of
     * the fixture table decodes, in either file: each fails somewhere in the rows (end of input, a vint out of range,
     * a sanity assertion of the partition update deserializer such as the one on the reversed flag of the header).
     * Where depends on the bytes of each hint, so the assertion accepts those three kinds of failure, and only rules
     * out an unknown table: the table id comes before the max timestamp, so it is read right in both layouts.
     */
    @Test
    public void theHintsDoNotDecodeAtTheCurrentMessagingVersion() throws Throwable
    {
        for (DseVersion version : DseVersion.values())
        {
            for (String statement : DseFixtures.fixtureSchema(version))
                execute(statement);
            UUID fixtureTable = DseFixtures.fixtureTableId(version);
            int fixtureHints = 0;
            for (byte[] record : DseFixtures.hintsRecords(version))
            {
                if (!DseFixtures.hintTableId(record).equals(fixtureTable))
                    continue;
                fixtureHints++;
                assertThatThrownBy(() -> {
                    try (DataInputBuffer in = new DataInputBuffer(record))
                    {
                        Hint.serializer.deserialize(in, MessagingService.current_version);
                    }
                }).as("a hint of the fixture table of " + version + " read at the current messaging version")
                  .isInstanceOfAny(IOException.class, RuntimeException.class, AssertionError.class)
                  .isNotInstanceOf(UnknownTableException.class);
            }
            assertThat(fixtureHints).as("hints of the fixture table in " + version).isPositive();
            execute("DROP KEYSPACE " + FIXTURE_KEYSPACE);
        }
    }

    /**
     * Decodes every hint of the file of a version, applies those of the fixture table and checks the data set of the
     * README of the fixtures; returns the other tables hinted, whether this test server knows them or not.
     */
    private Set<TableId> decodeApplyAndVerify(DseVersion version) throws Throwable
    {
        for (String statement : DseFixtures.fixtureSchema(version))
            execute(statement);
        TableId fixtureTable = TableId.fromUUID(DseFixtures.fixtureTableId(version));

        HintsDescriptor descriptor;
        try (FileInputStreamPlus in = new FileInputStreamPlus(DseFixtures.hintsFile(version)))
        {
            descriptor = HintsDescriptor.deserialize(in);
        }
        assertThat(descriptor.version).isEqualTo(-1);

        Set<TableId> others = new HashSet<>();
        List<Hint> fixtureHints = new ArrayList<>();
        long firstCreation = Long.MAX_VALUE;
        long lastCreation = Long.MIN_VALUE;
        for (byte[] record : DseFixtures.hintsRecords(version))
        {
            // read ahead of the mutation, for every hint: creation time, gc grace seconds and table id
            ByteBuffer raw = ByteBuffer.wrap(record);
            long creationTime = raw.getLong(0);
            int gcgs = (int) VIntCoding.getUnsignedVInt(raw, 8);
            TableId table = TableId.fromUUID(DseFixtures.hintTableId(record));
            firstCreation = Math.min(firstCreation, creationTime);
            lastCreation = Math.max(lastCreation, creationTime);
            assertThat(gcgs).as("gc grace seconds of a hint for " + table).isEqualTo(table.equals(fixtureTable) ? FIXTURE_GC_GRACE_SECONDS
                                                                                                             : GC_GRACE_SECONDS.get(table));

            try (DataInputBuffer in = new DataInputBuffer(record))
            {
                Hint hint = Hint.serializer.deserialize(in, MessagingService.VERSION_DSE_68);
                assertThat(in.available()).as("bytes left after a hint of " + version).isZero();
                assertThat(hint.creationTime).isEqualTo(creationTime);
                assertThat(hint.gcgs).isEqualTo(gcgs);
                assertThat(hint.mutation.getPartitionUpdates()).hasSize(1);
                PartitionUpdate update = hint.mutation.getPartitionUpdates().iterator().next();
                assertThat(update.metadata().id).isEqualTo(table);
                if (table.equals(fixtureTable))
                    fixtureHints.add(hint);
                else
                    others.add(table);
            }
            catch (UnknownTableException e)
            {
                assertThat(e.id).isEqualTo(table);
                others.add(table);
            }
        }
        assertThat(new long[]{ firstCreation, lastCreation }).as("first and last creation time of the hints of " + version)
                                                              .containsExactly(CREATION_TIMES.get(version));
        assertThat(fixtureHints).as("hints of the fixture table in " + version).isNotEmpty();
        // every run of the workload hinted its 200 inserts and three deletions
        assertThat(fixtureHints.size() % DseFixtures.FIXTURE_MUTATIONS_PER_RUN).as("hints of the fixture table in " + version + ", by run").isZero();

        // the timestamps of the mutations decide the outcome, not the order they are applied in
        for (Hint hint : fixtureHints)
            hint.mutation.apply();

        String t = FIXTURE_KEYSPACE + ".t";
        assertRows(execute("SELECT count(*) FROM " + t), row((long) DseFixtures.COLLECTION_TABLE_COUNT_STAR));
        assertRows(execute("SELECT id, ck FROM " + t + " WHERE id = 1"));
        assertRows(execute("SELECT id, ck, s FROM " + t + " WHERE id = 3"), row(3, null, 30));
        assertRows(execute("SELECT id, ck, v, n, s FROM " + t + " WHERE id = 2"),
                   row(2, "ck1", null, 300L, 20),
                   row(2, "ck2", "val4", 400L, 20));
        assertRows(execute("SELECT id, ck, fu, l, m, n, s, tp, u, v FROM " + t + " WHERE id = 25 AND ck = 'ck2'"),
                   row(25, "ck2",
                       userType("street", "Street25", "zip", 100025),
                       list(25, 26, 27),
                       map("m", 25),
                       5000L,
                       250,
                       tuple(25, "tuple25"),
                       userType("street", "Avenue25", "zip", 200025),
                       "val50"));
        assertRows(execute("SELECT id, ck, v, n, s FROM " + t + " WHERE id = 100"),
                   row(100, "ck1", "val199", 19900L, 1000),
                   row(100, "ck2", "val200", 20000L, 1000));
        return others;
    }
}
