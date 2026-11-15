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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.dsecompat.Dse51Fixtures;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tools.dsepreflight.Dse51HintsFileCheck;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The hints files of {@code storage_compatibility_mode: DSE_5_1}: every one at hints version 1, its mutations at
 * messaging version 11, as DSE 5.1.51 reads them. Written by the node's own path (the hints service, its buffers, the
 * write executor, drained as {@code nodetool drain} drains it), a file is named after its descriptor, has the
 * descriptor of the file DSE 5.1.51 wrote but for its host id and timestamp, holds its entries back to back with
 * nothing after the last one, and comes with the two companions DSE 5.1.51's writer leaves; its hints round-trip at 11,
 * and the hints of the file DSE 5.1.51 wrote decode and encode again at 11, and at 10, byte for byte. A hint the mode's
 * files cannot hold (a deletion or expiration time past 2038, a size above what the buffer takes) is refused before it
 * is buffered, counted and logged; the other hints of the buffer are written as they are. In the other storage modes
 * nothing changes, and after the flag drop the files the mode wrote are read at 10 and their hints delivered.
 */
public class Dse51HintsWriterTest extends CQLTester
{
    private static final long PAST_2038 = CellData.MAX_DELETION_TIME_2038_LEGACY_CAP + 1000L;

    private StorageCompatibilityMode storageModeBefore;
    private InternodeCompatibilityMode internodeModeBefore;
    private ParameterizedClass compressionBefore;
    private final List<HintsService> services = new ArrayList<>();
    private final List<UUID> hosts = new ArrayList<>();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void save()
    {
        storageModeBefore = DatabaseDescriptor.getStorageCompatibilityMode();
        internodeModeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        compressionBefore = DatabaseDescriptor.getHintsCompression();
        DseCompatRefusals.reset();
    }

    @After
    public void restore() throws Exception
    {
        for (HintsService service : services)
            if (!service.isShutDown())
                service.shutdownBlocking();
        DatabaseDescriptor.setStorageCompatibilityMode(storageModeBefore);
        DatabaseDescriptor.setInternodeCompatibilityMode(internodeModeBefore);
        DatabaseDescriptor.setHintsCompression(compressionBefore);
        for (UUID host : hosts)
            for (Path file : filesOf(host))
                Files.delete(file);
        // the node's own service again, once the files of the tests are gone, so that its catalog holds none of them
        if (!services.isEmpty())
            HintsService.instance = new HintsService(HintsEndpointProvider.instance::isAlive);
        DseCompatRefusals.reset();
    }

    private static void dse51Mode()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.DSE_5_1);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
    }

    private UUID newHost()
    {
        UUID host = UUID.randomUUID();
        hosts.add(host);
        return host;
    }

    /**
     * A hints service on the node's hints directory, as the node builds it, with its targets down, in place of the
     * node's own: one at a time, the services register the same beans.
     */
    private HintsService newService() throws Exception
    {
        if (!HintsService.instance.isShutDown())
            HintsService.instance.shutdownBlocking();
        HintsService service = new HintsService(host -> false);
        services.add(service);
        HintsService.instance = service;
        return service;
    }

    /** The files in the hints directory for {@code host}, sorted by name. */
    private static List<Path> filesOf(UUID host) throws IOException
    {
        try (Stream<Path> files = Files.list(DatabaseDescriptor.getHintsDirectory().toPath()))
        {
            return files.filter(f -> f.getFileName().toString().startsWith(host.toString()))
                        .sorted()
                        .collect(Collectors.toList());
        }
    }

    /** The hints file of {@code host}, the only one. */
    private static Path hintsFileOf(UUID host) throws IOException
    {
        List<Path> files = filesOf(host).stream()
                                        .filter(f -> f.getFileName().toString().endsWith(".hints") &&
                                                     !f.getFileName().toString().endsWith("-Statistics.hints"))
                                        .collect(Collectors.toList());
        assertThat(files).hasSize(1);
        return files.get(0);
    }

    private static byte[] serializedAt(Hint hint, int version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Hint.serializer.serialize(hint, out, version);
            return out.toByteArray();
        }
    }

    private static Hint deserializedAt(byte[] bytes, int version) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            Hint hint = Hint.serializer.deserialize(in, version);
            assertThat(in.available()).as("bytes left after a hint").isZero();
            return hint;
        }
    }

    /** The hints of a file, read as the dispatcher of the mode reads them, each serialized again at 11. */
    private static List<byte[]> readBack(Path file) throws IOException
    {
        List<byte[]> hints = new ArrayList<>();
        try (HintsReader reader = HintsReader.open(new File(file)))
        {
            assertThat(reader.descriptor().messagingVersion()).isEqualTo(MessagingService.VERSION_3014);
            for (HintsReader.Page page : reader)
                page.hintsIterator().forEachRemaining(hint -> {
                    try
                    {
                        hints.add(serializedAt(hint, MessagingService.VERSION_3014));
                    }
                    catch (IOException e)
                    {
                        throw new AssertionError(e);
                    }
                });
        }
        return hints;
    }

    /**
     * After the flag drop ({@code NONE}, {@code HCD_1}) a file of the mode is read at messaging version 10 and each of
     * its hints, decoded there, encoded again at the current version, as the dispatcher does for a target of another
     * version: the hints written, so the node delivers the files of version 1 left after the flag drop.
     */
    private static void assertDeliveredAfterTheFlagDrop(Path file, List<Hint> written) throws IOException
    {
        StorageCompatibilityMode storageMode = DatabaseDescriptor.getStorageCompatibilityMode();
        InternodeCompatibilityMode internodeMode = DatabaseDescriptor.getInternodeCompatibilityMode();
        List<byte[]> afterTheFlagDrop = new ArrayList<>();
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        try (HintsReader reader = HintsReader.open(new File(file)))
        {
            assertThat(reader.descriptor().messagingVersion()).isEqualTo(MessagingService.VERSION_30);
            for (HintsReader.Page page : reader)
            {
                for (Iterator<ByteBuffer> buffers = page.buffersIterator(); buffers.hasNext(); )
                {
                    try (DataInputBuffer in = new DataInputBuffer(buffers.next(), true))
                    {
                        Hint hint = Hint.serializer.deserialize(in, MessagingService.VERSION_30);
                        afterTheFlagDrop.add(serializedAt(hint, MessagingService.current_version));
                    }
                }
            }
            List<byte[]> current = new ArrayList<>();
            for (Hint hint : written)
                current.add(serializedAt(hint, MessagingService.current_version));
            assertThat(afterTheFlagDrop).containsExactlyElementsOf(current);
        }
        finally
        {
            DatabaseDescriptor.setStorageCompatibilityMode(storageMode);
            DatabaseDescriptor.setInternodeCompatibilityMode(internodeMode);
        }
    }

    /**
     * The rollback check of {@code dsepreflight} reads the file to its end as DSE 5.1.51 does, and finds no flaw;
     * returns the number of compressed blocks it read.
     */
    private static int assertAcceptedByTheRollbackCheck(Path file, int hints) throws IOException
    {
        Dse51HintsFileCheck.Outcome outcome = Dse51HintsFileCheck.check(file);
        assertThat(outcome.damaged).isNull();
        assertThat(outcome.unverified).isNull();
        assertThat(outcome.truncated).isNull();
        assertThat(outcome.hints).isEqualTo(hints);
        return outcome.blocks;
    }

    private Mutation simpleMutation(int k)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(currentTableMetadata(), k)
                                                               .timestamp(FBUtilities.timestampMicros());
        builder.row(1).add("v", "v" + k);
        return builder.buildAsMutation();
    }

    @Test
    public void aFileWrittenInTheModeIsTheFileDse51Reads() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        dse51Mode();
        UUID host = newHost();
        HintsService service = newService();
        long now = FBUtilities.now().toEpochMilli();
        List<Hint> written = new ArrayList<>();
        for (int k = 0; k < 3; k++)
        {
            Hint hint = Hint.create(simpleMutation(k), now + k);
            assertThat(service.tryWrite(List.of(host), hint)).isTrue();
            written.add(hint);
        }
        // the drain: the buffers flushed, the writers closed
        service.shutdownBlocking();

        Path file = hintsFileOf(host);
        HintsDescriptor descriptor = HintsDescriptor.readFromFileQuietly(file).orElseThrow();
        // named as DSE 5.1.51 opens it, from its descriptor
        assertThat(file.getFileName().toString()).isEqualTo(descriptor.fileName())
                                                 .isEqualTo(host + "-" + descriptor.timestamp + "-1.hints");
        assertThat(descriptor.version).isEqualTo(HintsDescriptor.VERSION_30);
        assertThat(descriptor.hostId).isEqualTo(host);
        assertThat(descriptor.parameters).isEmpty();
        assertThat(descriptor.messagingVersion()).isEqualTo(MessagingService.VERSION_3014);
        // the descriptor of the file DSE 5.1.51 wrote, field by field (the version, the host id and timestamp, the
        // parameters {} and the two checksums): the one DseFixtures writes as DSE 5.1.51 does, equal to the capture
        byte[] bytes = Files.readAllBytes(file);
        byte[] dse51Descriptor = Files.readAllBytes(DseFixtures.writeDse51HintsFile(folder.newFolder().toPath(), host,
                                                                                    descriptor.timestamp, List.of()));
        assertThat(Arrays.copyOf(bytes, dse51Descriptor.length)).isEqualTo(dse51Descriptor);

        // the entries back to back, each size and hint with its checksum, nothing after the last one (hintsRecords
        // reads to the end of the file and fails on any byte left), each hint at messaging version 11
        List<byte[]> records = DseFixtures.hintsRecords(file);
        assertThat(records).hasSize(written.size());
        for (int i = 0; i < records.size(); i++)
            assertThat(records.get(i)).isEqualTo(serializedAt(written.get(i), MessagingService.VERSION_3014));

        // the companions, as DSE 5.1.51's writer leaves them: the CRC32 of the whole file in lowercase hexadecimal,
        // with no leading zero nor line end, and the number of hints as one long
        CRC32 crc = new CRC32();
        crc.update(bytes);
        Path checksum = file.resolveSibling(descriptor.checksumFileName());
        assertThat(new String(Files.readAllBytes(checksum), StandardCharsets.US_ASCII))
        .isEqualTo(Integer.toHexString((int) crc.getValue()));
        Path statistics = file.resolveSibling(descriptor.statisticsFileName());
        assertThat(Files.readAllBytes(statistics)).isEqualTo(ByteBuffer.allocate(Long.BYTES)
                                                                       .putLong(written.size())
                                                                       .array());
        assertThat(filesOf(host)).containsExactlyInAnyOrder(file, checksum, statistics);
        assertThat(readBack(file)).containsExactlyElementsOf(records);
        assertThat(assertAcceptedByTheRollbackCheck(file, written.size())).isZero();

        assertDeliveredAfterTheFlagDrop(file, written);
    }

    /**
     * The hints of the file DSE 5.1.51 wrote decode at 11 and encode again at 11 byte for byte, and decode at 10 to
     * the same mutations, encoded at 10 to the same bytes: one layout for both versions, as in DSE 5.1.51.
     */
    @Test
    public void theHintsDse51WroteDecodeAndEncodeAgainAtElevenAndTenByteForByte() throws Throwable
    {
        Dse51Fixtures dse51 = DseFixtures.dse51();
        for (String statement : dse51.fixtureSchema())
            execute(statement);
        try
        {
            dse51Mode();
            List<byte[]> records = DseFixtures.hintsRecords(dse51.hintsFile());
            assertThat(records).hasSize(Dse51Fixtures.FIXTURE_HINTS);
            for (byte[] record : records)
            {
                assertThat(serializedAt(deserializedAt(record, MessagingService.VERSION_3014),
                                        MessagingService.VERSION_3014)).isEqualTo(record);
                assertThat(serializedAt(deserializedAt(record, MessagingService.VERSION_30),
                                        MessagingService.VERSION_30)).isEqualTo(record);
            }
        }
        finally
        {
            DatabaseDescriptor.setStorageCompatibilityMode(storageModeBefore);
            DatabaseDescriptor.setInternodeCompatibilityMode(internodeModeBefore);
            execute("DROP KEYSPACE IF EXISTS " + DseFixtures.FIXTURE_KEYSPACE);
        }
    }

    /**
     * Hints of collections (a whole-collection assignment, a complex deletion, beside an append, whose complex
     * deletion is LIVE), a static column, range tombstones, a row and a partition deletion, TTLs and a counter table
     * round-trip through a file written in the mode, at 11.
     */
    @Test
    public void hintsOfEveryKindRoundTripThroughAFileOfTheMode() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, s set<text>, l list<int>, " +
                                   "m map<text, int>, st text static, PRIMARY KEY (k, c))");
        TableMetadata metadata = currentTableMetadata();
        createTable("CREATE TABLE %s (k int PRIMARY KEY, n counter)");
        TableMetadata counterMetadata = currentTableMetadata();
        dse51Mode();
        long timestamp = FBUtilities.timestampMicros();
        long nowInSec = FBUtilities.nowInSeconds();
        List<Mutation> mutations = new ArrayList<>();
        PartitionUpdate.SimpleBuilder collections = PartitionUpdate.simpleBuilder(metadata, 1)
                                                                   .timestamp(timestamp)
                                                                   .nowInSec(nowInSec);
        collections.row(1).add("s", Collections.singleton("a")).appendAll("l", List.of(1, 2))
                   .add("m", Map.of("x", 1));
        mutations.add(collections.buildAsMutation());
        PartitionUpdate.SimpleBuilder statics = PartitionUpdate.simpleBuilder(metadata, 2).timestamp(timestamp);
        statics.row().add("st", "static");
        statics.row(1).ttl(3600).add("v", "expiring");
        mutations.add(statics.buildAsMutation());
        PartitionUpdate.SimpleBuilder rowDeletion = PartitionUpdate.simpleBuilder(metadata, 3)
                                                                   .timestamp(timestamp)
                                                                   .nowInSec(nowInSec);
        rowDeletion.row(1).delete();
        rowDeletion.addRangeTombstone().start(2).end(4).inclStart();
        mutations.add(rowDeletion.buildAsMutation());
        DecoratedKey key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(4));
        mutations.add(new Mutation(PartitionUpdate.fullPartitionDelete(metadata, key, timestamp, nowInSec)));
        ColumnMetadata n = counterMetadata.getColumn(ByteBuffer.wrap("n".getBytes(StandardCharsets.UTF_8)));
        DecoratedKey counterKey = counterMetadata.partitioner.decorateKey(Int32Type.instance.decompose(1));
        ByteBuffer context = CounterContext.instance().createGlobal(CounterId.fromInt(1), 1, 5);
        mutations.add(new Mutation(PartitionUpdate.singleRowUpdate(counterMetadata, counterKey,
                                                                   BTreeRow.singleCellRow(Clustering.EMPTY,
                                                                                          BufferCell.live(n, timestamp,
                                                                                                          context)))));

        UUID host = newHost();
        HintsService service = newService();
        long now = FBUtilities.now().toEpochMilli();
        List<byte[]> expected = new ArrayList<>();
        List<Hint> written = new ArrayList<>();
        for (Mutation mutation : mutations)
        {
            Hint hint = Hint.create(mutation, now);
            assertThat(service.tryWrite(List.of(host), hint)).isTrue();
            written.add(hint);
            expected.add(serializedAt(hint, MessagingService.VERSION_3014));
        }
        service.shutdownBlocking();
        Path file = hintsFileOf(host);
        assertThat(DseFixtures.hintsRecords(file)).containsExactlyElementsOf(expected);
        assertThat(readBack(file)).containsExactlyElementsOf(expected);
        assertDeliveredAfterTheFlagDrop(file, written);
    }

    /** A compressed file with each compressor and each option DSE 5.1.51 builds a hints compressor with. */
    @Test
    public void compressedFilesWithEachCompressionDse51Builds() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        dse51Mode();
        String level = "lz4_high_compressor_level";
        List<ParameterizedClass> compressions =
        List.of(new ParameterizedClass("LZ4Compressor", Map.of()),
                new ParameterizedClass("org.apache.cassandra.io.compress.LZ4Compressor",
                                       Map.of("lz4_compressor_type", "fast")),
                new ParameterizedClass("LZ4Compressor", Map.of("lz4_compressor_type", "high", level, "1")),
                new ParameterizedClass("LZ4Compressor", Map.of("lz4_compressor_type", "high", level, "17")),
                new ParameterizedClass("SnappyCompressor", Map.of()),
                new ParameterizedClass("org.apache.cassandra.io.compress.DeflateCompressor", Map.of()));
        for (ParameterizedClass compression : compressions)
        {
            String className = compression.class_name;
            Map<String, String> options = compression.parameters;
            assertThat(Dse51HintsCompression.refusal(className, options)).isNull();
            DatabaseDescriptor.setHintsCompression(compression);
            UUID host = newHost();
            HintsService service = newService();
            List<byte[]> expected = new ArrayList<>();
            for (int k = 0; k < 40; k++)
            {
                Hint hint = Hint.create(simpleMutation(k), FBUtilities.now().toEpochMilli());
                assertThat(service.tryWrite(List.of(host), hint)).isTrue();
                expected.add(serializedAt(hint, MessagingService.VERSION_3014));
                // flushed in two parts: the file holds at least two compressed blocks
                if (k == 19)
                    service.flushAndFsyncBlockingly(List.of(host));
            }
            service.shutdownBlocking();
            Path file = hintsFileOf(host);
            HintsDescriptor descriptor = HintsDescriptor.readFromFileQuietly(file).orElseThrow();
            assertThat(descriptor.version).isEqualTo(HintsDescriptor.VERSION_30);
            assertThat(descriptor.isCompressed()).as(className).isTrue();
            Map<?, ?> written = (Map<?, ?>) descriptor.parameters.get(HintsDescriptor.COMPRESSION);
            assertThat(written.get(ParameterizedClass.CLASS_NAME)).isEqualTo(className);
            if (!options.isEmpty())
                assertThat(written.get(ParameterizedClass.PARAMETERS)).isEqualTo(options);
            assertThat(readBack(file)).as(className + " " + options).containsExactlyElementsOf(expected);
            assertThat(assertAcceptedByTheRollbackCheck(file, expected.size())).as(className).isGreaterThanOrEqualTo(2);
        }
    }

    private static Mutation rowMutation(TableMetadata metadata, DecoratedKey key, Row row)
    {
        return new Mutation(PartitionUpdate.singleRowUpdate(metadata, key, row));
    }

    /** Mutations holding a local deletion or expiration time past 2038, by where it is. */
    private Map<String, Mutation> past2038Mutations()
    {
        TableMetadata metadata = currentTableMetadata();
        ColumnMetadata v = metadata.getColumn(ByteBuffer.wrap("v".getBytes(StandardCharsets.UTF_8)));
        DecoratedKey key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(1));
        Clustering<?> clustering = Clustering.make(Int32Type.instance.decompose(1));
        Map<String, Mutation> mutations = new LinkedHashMap<>();
        BufferCell tombstone = BufferCell.tombstone(v, 1000, PAST_2038);
        mutations.put("cell tombstone", rowMutation(metadata, key, BTreeRow.singleCellRow(clustering, tombstone)));
        BufferCell expiring = new BufferCell(v, 1000, 100, PAST_2038, UTF8Type.instance.decompose("x"), null);
        mutations.put("expiring cell", rowMutation(metadata, key, BTreeRow.singleCellRow(clustering, expiring)));
        LivenessInfo liveness = LivenessInfo.withExpirationTime(1000, 100, PAST_2038);
        mutations.put("row liveness", rowMutation(metadata, key, BTreeRow.noCellLiveRow(clustering, liveness)));
        PartitionUpdate.SimpleBuilder rowDeletion = PartitionUpdate.simpleBuilder(metadata, 1).timestamp(1000)
                                                                   .nowInSec(PAST_2038);
        rowDeletion.row(1).delete();
        mutations.put("row deletion", rowDeletion.buildAsMutation());
        PartitionUpdate.SimpleBuilder complexDeletion = PartitionUpdate.simpleBuilder(metadata, 1).timestamp(1000)
                                                                       .nowInSec(PAST_2038);
        complexDeletion.row(1).add("s", Collections.singleton("a"));
        mutations.put("complex deletion", complexDeletion.buildAsMutation());
        ColumnMetadata st = metadata.getColumn(ByteBuffer.wrap("st".getBytes(StandardCharsets.UTF_8)));
        BufferCell staticTombstone = BufferCell.tombstone(st, 1000, PAST_2038);
        mutations.put("static row", rowMutation(metadata, key, BTreeRow.singleCellRow(Clustering.STATIC_CLUSTERING,
                                                                                      staticTombstone)));
        mutations.put("partition deletion",
                      new Mutation(PartitionUpdate.fullPartitionDelete(metadata, key, 1000, PAST_2038)));
        Slice slice = Slice.make(BufferClusteringBound.inclusiveStartOf(Int32Type.instance.decompose(1)),
                                 BufferClusteringBound.inclusiveEndOf(Int32Type.instance.decompose(3)));
        mutations.put("range tombstone",
                      PartitionUpdate.simpleBuilder(metadata, 1)
                                     .addRangeTombstone(new RangeTombstone(slice, DeletionTime.build(1000, PAST_2038)))
                                     .buildAsMutation());
        return mutations;
    }

    /**
     * A hint holding a deletion or expiration time past 2038, wherever it is, is refused before it is buffered:
     * {@code write} says so, it is counted, and the hints buffered before and after it are written as they are.
     */
    @Test
    public void aHintWithATimePast2038IsRefusedAndCountedAndTheOthersAreWritten() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, s set<text>, st text static, PRIMARY KEY (k, c))");
        Map<String, Mutation> refused = past2038Mutations();
        dse51Mode();
        UUID host = newHost();
        HintsService service = newService();
        long now = FBUtilities.now().toEpochMilli();
        Hint before = Hint.create(simpleMutation(10), now);
        assertThat(service.tryWrite(List.of(host), before)).isTrue();
        long count = 0;
        for (Map.Entry<String, Mutation> mutation : refused.entrySet())
        {
            assertThat(service.tryWrite(List.of(host, newHost()), Hint.create(mutation.getValue(), now)))
            .as(mutation.getKey()).isFalse();
            assertThat(DseCompatRefusals.count(Dse51Hints.REFUSED_HINT)).as(mutation.getKey()).isEqualTo(++count);
        }
        Hint after = Hint.create(simpleMutation(11), now);
        assertThat(service.tryWrite(List.of(host), after)).isTrue();
        service.shutdownBlocking();
        assertThat(DseFixtures.hintsRecords(hintsFileOf(host)))
        .containsExactly(serializedAt(before, MessagingService.VERSION_3014),
                         serializedAt(after, MessagingService.VERSION_3014));
        // no store, no file, for the targets of refused hints alone
        for (UUID other : hosts.subList(1, hosts.size()))
            assertThat(filesOf(other)).isEmpty();

        // a boundary marker between two range tombstones: either deletion time past 2038 is refused by the walk, as
        // by the codec (in a mutation the bound of the range past 2038 is refused too)
        DeletionTime within = DeletionTime.build(1000, CellData.MAX_DELETION_TIME_2038_LEGACY_CAP);
        DeletionTime beyond = DeletionTime.build(2000, PAST_2038);
        ByteBuffer[] values = { Int32Type.instance.decompose(3) };
        ValueAccessor<ByteBuffer> accessor = ByteBufferAccessor.instance;
        int version = MessagingService.VERSION_3014;
        for (RangeTombstoneBoundaryMarker marker :
             List.of(RangeTombstoneBoundaryMarker.exclusiveCloseInclusiveOpen(false, values, accessor, beyond, within),
                     RangeTombstoneBoundaryMarker.exclusiveCloseInclusiveOpen(false, values, accessor, within, beyond)))
            assertThatThrownBy(() -> UnfilteredSerializer.checkPre40DeletionTimes(marker, version))
            .isInstanceOf(UnfilteredSerializer.Pre40DeletionTimeException.class)
            .hasFieldOrPropertyWithValue("localDeletionTime", PAST_2038);
        RangeTombstoneBoundaryMarker inRange =
        RangeTombstoneBoundaryMarker.exclusiveCloseInclusiveOpen(false, values, accessor, within,
                                                                 DeletionTime.build(900, 1000));
        UnfilteredSerializer.checkPre40DeletionTimes(inRange, version);
    }

    /**
     * Refused whatever its size: a mutation above 1 MiB, whose serialized bytes the mutation does not cache, with a
     * time past 2038 is refused as a small one is.
     */
    @Test
    public void aLargeHintWithATimePast2038IsRefused() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, s set<text>, st text static, PRIMARY KEY (k, c))");
        TableMetadata metadata = currentTableMetadata();
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, 1).timestamp(1000);
        char[] large = new char[2 * 1024 * 1024];
        Arrays.fill(large, 'x');
        builder.row(1).add("v", new String(large));
        PartitionUpdate.SimpleBuilder deletion = PartitionUpdate.simpleBuilder(metadata, 1).timestamp(1000)
                                                                .nowInSec(PAST_2038);
        deletion.row(2).delete();
        Mutation mutation = Mutation.merge(List.of(builder.buildAsMutation(), deletion.buildAsMutation()));
        dse51Mode();
        Hint hint = Hint.create(mutation, FBUtilities.now().toEpochMilli());
        assertThat(Hint.serializer.serializedSize(hint, MessagingService.VERSION_3014)).isGreaterThan(1024 * 1024);
        UUID host = newHost();
        HintsService service = newService();
        assertThat(service.tryWrite(List.of(host), hint)).isFalse();
        assertThat(DseCompatRefusals.count(Dse51Hints.REFUSED_HINT)).isEqualTo(1);
        service.shutdownBlocking();
        assertThat(filesOf(host)).isEmpty();
    }

    /** A hint larger at 11 than the largest the buffer takes is refused; one of that largest size is not. */
    @Test
    public void aHintAboveTheLargestTheBufferTakesIsRefused() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        dse51Mode();
        Hint hint = Hint.create(simpleMutation(1), FBUtilities.now().toEpochMilli());
        int size = (int) Hint.serializer.serializedSize(hint, MessagingService.VERSION_3014);
        assertThat(Dse51Hints.refusal(hint, size)).isNull();
        Dse51Hints.Refusal refusal = Dse51Hints.refusal(hint, size - 1);
        assertThat(refusal.counted).isEqualTo(Dse51Hints.REFUSED_LARGE_HINT);
        assertThat(refusal.reason)
        .isEqualTo("its size at messaging version 11, the version of the hints files DSE 5.1 reads, " + size +
                   " bytes, is above the largest hint the hints buffer takes, " + (size - 1) + " bytes");
        // the largest the buffers of a pool take: half of a buffer, less the size and the checksums of the entry
        int bufferSize = 1024;
        int largest = new HintsBufferPool(bufferSize, (buffer, pool) -> {}).maxHintSize();
        assertThat(largest).isEqualTo(bufferSize / 2 - HintsBuffer.ENTRY_OVERHEAD_SIZE);
        HintsBuffer buffer = HintsBuffer.create(bufferSize);
        try
        {
            buffer.allocate(largest).close();
            assertThatThrownBy(() -> buffer.allocate(largest + 1)).isInstanceOf(IllegalArgumentException.class);
        }
        finally
        {
            buffer.free();
        }
    }

    /**
     * A hint larger than the hints buffer takes is refused by the write, counted under its own reason, and stored for
     * no target: a mutation of 17 MiB, above the largest hint of the smallest buffer the node builds (32 MiB).
     */
    @Test
    public void aHintLargerThanTheBufferTakesIsRefusedByTheWrite() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(currentTableMetadata(), 1)
                                                               .timestamp(1000);
        char[] large = new char[17 * 1024 * 1024];
        Arrays.fill(large, 'x');
        builder.row(1).add("v", new String(large));
        Mutation mutation = builder.buildAsMutation();
        dse51Mode();
        UUID host = newHost();
        HintsService service = newService();
        assertThat(service.tryWrite(List.of(host), Hint.create(mutation, FBUtilities.now().toEpochMilli()))).isFalse();
        assertThat(DseCompatRefusals.count(Dse51Hints.REFUSED_LARGE_HINT)).isEqualTo(1);
        assertThat(DseCompatRefusals.count(Dse51Hints.REFUSED_HINT)).isZero();
        service.shutdownBlocking();
        assertThat(filesOf(host)).isEmpty();
    }

    /**
     * The refusal's WARN, at most once a minute: it names the targets, the table, the reason with the time refused,
     * and the repair; the counter counts every refusal.
     */
    @Test
    public void theRefusalIsLoggedAtMostOnceAMinute() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, s set<text>, st text static, PRIMARY KEY (k, c))");
        Mutation mutation = past2038Mutations().get("cell tombstone");
        String table = currentTableMetadata().toString();
        dse51Mode();
        UUID host = newHost();
        HintsService service = newService();
        Logger logger = (Logger) LoggerFactory.getLogger(Dse51Hints.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        Hint small = Hint.create(simpleMutation(1), FBUtilities.now().toEpochMilli());
        long[] clock = { Clock.Global.nanoTime() + 3_600_000_000_000L };
        NoSpamLogger.unsafeSetClock(() -> clock[0]);
        try
        {
            Hint hint = Hint.create(mutation, FBUtilities.now().toEpochMilli());
            assertThat(service.tryWrite(List.of(host), hint)).isFalse();
            assertThat(service.tryWrite(List.of(host), hint)).isFalse();
            // the other reason, within the same minute, is logged on its own
            assertThat(Dse51Hints.refuse(List.of(host), small, 1)).isTrue();
            clock[0] += 61_000_000_000L;
            assertThat(service.tryWrite(List.of(host), hint)).isFalse();
        }
        finally
        {
            NoSpamLogger.unsafeSetClock(Clock.Global::nanoTime);
            logger.detachAppender(appender);
        }
        String expected = "DSE 5.1 compatibility mode: a hint for [" + host + "] of " + table + " was not stored: " +
                          "a local deletion or expiration time, " + PAST_2038 + ", is past 2038, and the hints " +
                          "files DSE 5.1 reads hold 32-bit times (messaging version 11); the write reaches those " +
                          "nodes only by a repair: repair " + table + " once the cluster runs one release";
        long smallSize = Hint.serializer.serializedSize(small, MessagingService.VERSION_3014);
        String large = "DSE 5.1 compatibility mode: a hint for [" + host + "] of " + table + " was not stored: its " +
                       "size at messaging version 11, the version of the hints files DSE 5.1 reads, " + smallSize +
                       " bytes, is above the largest hint the hints buffer takes, 1 bytes; the write reaches those " +
                       "nodes only by a repair: repair " + table + " once the cluster runs one release";
        assertThat(appender.list).extracting(ILoggingEvent::getLevel).containsOnly(Level.WARN);
        assertThat(appender.list).extracting(ILoggingEvent::getFormattedMessage)
                                 .containsExactly(expected, large, expected);
        assertThat(DseCompatRefusals.count(Dse51Hints.REFUSED_HINT)).isEqualTo(3);
        assertThat(DseCompatRefusals.count(Dse51Hints.REFUSED_LARGE_HINT)).isEqualTo(1);
    }

    /** In the other storage modes nothing is checked: the same hint is stored, at the mode's own version. */
    @Test
    public void outsideTheModeTheSameHintIsStored() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, s set<text>, st text static, PRIMARY KEY (k, c))");
        Mutation mutation = past2038Mutations().get("cell tombstone");
        for (StorageCompatibilityMode mode : List.of(StorageCompatibilityMode.HCD_1, StorageCompatibilityMode.NONE))
        {
            DatabaseDescriptor.setStorageCompatibilityMode(mode);
            UUID host = newHost();
            HintsService service = newService();
            Hint hint = Hint.create(mutation, FBUtilities.now().toEpochMilli());
            assertThat(service.tryWrite(List.of(host), hint)).isTrue();
            service.shutdownBlocking();
            HintsDescriptor descriptor = HintsDescriptor.readFromFileQuietly(hintsFileOf(host)).orElseThrow();
            assertThat(descriptor.version)
            .isEqualTo(HintsDescriptor.hintsVersionFromMessagingVersion(mode.storageMessagingVersion()));
            assertThat(DseFixtures.hintsRecords(hintsFileOf(host))).hasSize(1);
        }
        assertThat(DseCompatRefusals.count(Dse51Hints.REFUSED_HINT)).isZero();
    }
}
