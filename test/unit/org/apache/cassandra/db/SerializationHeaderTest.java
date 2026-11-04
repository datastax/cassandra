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

package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


public class SerializationHeaderTest
{
    private static final String KEYSPACE = "SerializationHeaderTest";

    private final static Version versionWithExplicitFrozenTuples;
    private final static Version versionWithImplicitFrozenTuples;

    static
    {
        DatabaseDescriptor.daemonInitialization();

        versionWithImplicitFrozenTuples = BtiFormat.getInstance().getVersion("cb");
        assertThat(versionWithImplicitFrozenTuples.hasImplicitlyFrozenTuples()).isTrue();

        versionWithExplicitFrozenTuples = BtiFormat.getInstance().getVersion("cc");
        assertThat(versionWithExplicitFrozenTuples.hasImplicitlyFrozenTuples()).isFalse();
    }
    
    @Test
    public void testWrittenAsDifferentKind() throws Exception
    {
        SSTableFormat<?, ?> format = DatabaseDescriptor.getSelectedSSTableFormat();
        final String tableName = "testWrittenAsDifferentKind";
        ColumnIdentifier v = ColumnIdentifier.getInterned("v", false);
        TableMetadata schemaWithStatic = TableMetadata.builder(KEYSPACE, tableName)
                .addPartitionKeyColumn("k", Int32Type.instance)
                .addClusteringColumn("c", Int32Type.instance)
                .addStaticColumn("v", Int32Type.instance)
                .build();
        TableMetadata schemaWithRegular = TableMetadata.builder(KEYSPACE, tableName)
                .addPartitionKeyColumn("k", Int32Type.instance)
                .addClusteringColumn("c", Int32Type.instance)
                .addRegularColumn("v", Int32Type.instance)
                .build();
        ColumnMetadata columnStatic = schemaWithStatic.getColumn(v);
        ColumnMetadata columnRegular = schemaWithRegular.getColumn(v);
        schemaWithStatic = schemaWithStatic.unbuild().recordColumnDrop(columnRegular, 0L).build();
        schemaWithRegular = schemaWithRegular.unbuild().recordColumnDrop(columnStatic, 0L).build();

        SSTableReader readerWithStatic = null;
        SSTableReader readerWithRegular = null;
        Supplier<SequenceBasedSSTableId> id = Util.newSeqGen();
        File dir = new File(Files.createTempDir());
        try
        {
            BiFunction<TableMetadata, Function<ByteBuffer, Clustering<?>>, Callable<Descriptor>> writer = (schema, clusteringFunction) -> () -> {
                Descriptor descriptor = new Descriptor(format.getLatestVersion(), dir, schema.keyspace, schema.name, id.get());

                SerializationHeader header = SerializationHeader.makeWithoutStats(schema);
                try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE, TableMetadataRef.forOfflineTools(schema));
                     SSTableWriter sstableWriter = descriptor.getFormat().getWriterFactory()
                                                             .builder(descriptor)
                                                             .setTableMetadataRef(TableMetadataRef.forOfflineTools(schema))
                                                             .setKeyCount(1)
                                                             .setSerializationHeader(header)
                                                             .setMetadataCollector(new MetadataCollector(schema.comparator))
                                                             .addDefaultComponents(Collections.emptySet())
                                                             .build(txn, null))
                {
                    ColumnMetadata cd = schema.getColumn(v);
                    for (int i = 0 ; i < 5 ; ++i) {
                        final ByteBuffer value = Int32Type.instance.decompose(i);
                        Cell<?> cell = BufferCell.live(cd, 1L, value);
                        Clustering<?> clustering = clusteringFunction.apply(value);
                        Row row = BTreeRow.singleCellRow(clustering, cell);
                        sstableWriter.append(PartitionUpdate.singleRowUpdate(schema, schema.partitioner.decorateKey(value), row).unfilteredIterator());
                    }
                    sstableWriter.finish(false, null);
                    txn.finish();
                }
                return descriptor;
            };

            Descriptor sstableWithRegular = writer.apply(schemaWithRegular, BufferClustering::new).call();
            Descriptor sstableWithStatic = writer.apply(schemaWithStatic, value -> Clustering.STATIC_CLUSTERING).call();
            readerWithStatic = SSTableReader.openNoValidation(null, sstableWithStatic, TableMetadataRef.forOfflineTools(schemaWithRegular));
            readerWithRegular = SSTableReader.openNoValidation(null, sstableWithRegular, TableMetadataRef.forOfflineTools(schemaWithStatic));

            try (ISSTableScanner partitions = readerWithStatic.getScanner()) {
                for (int i = 0 ; i < 5 ; ++i)
                {
                    UnfilteredRowIterator partition = partitions.next();
                    Assert.assertFalse(partition.hasNext());
                    long value = Int32Type.instance.compose(partition.staticRow().getCell(columnStatic).buffer());
                    Assert.assertEquals(value, (long)i);
                }
                Assert.assertFalse(partitions.hasNext());
            }
            try (ISSTableScanner partitions = readerWithRegular.getScanner()) {
                for (int i = 0 ; i < 5 ; ++i)
                {
                    UnfilteredRowIterator partition = partitions.next();
                    long value = Int32Type.instance.compose(((Row)partition.next()).getCell(columnRegular).buffer());
                    Assert.assertEquals(value, i);
                    Assert.assertTrue(partition.staticRow().isEmpty());
                    Assert.assertFalse(partition.hasNext());
                }
                Assert.assertFalse(partitions.hasNext());
            }
        }
        finally
        {
            if (readerWithStatic != null)
                readerWithStatic.selfRef().close();
            if (readerWithRegular != null)
                readerWithRegular.selfRef().close();
            FileUtils.deleteRecursive(dir);
        }
    }

    @Test
    public void testDecodeFrozenTuplesEncodedAsNonFrozen() throws UnknownColumnException
    {
        TupleType multicellTupleType = new TupleType(Arrays.asList(Int32Type.instance, Int32Type.instance), true);
        AbstractType<?> frozenTupleType = multicellTupleType.freeze();

        TableMetadata metadata = TableMetadata.builder("ks", "tab")
                                              .addPartitionKeyColumn("k", frozenTupleType)
                                              .addStaticColumn("s", frozenTupleType)
                                              .addClusteringColumn("c", frozenTupleType)
                                              .addRegularColumn("v", frozenTupleType)
                                              .build();

        SerializationHeader.Component component = SerializationHeader.Component.buildComponentForTools(multicellTupleType,
                                                                                                       Arrays.asList(multicellTupleType),
                                                                                                       new LinkedHashMap<>(Map.of(ByteBufferUtil.bytes("s"), multicellTupleType)),
                                                                                                       new LinkedHashMap<>(Map.of(ByteBufferUtil.bytes("v"), multicellTupleType)),
                                                                                                       EncodingStats.NO_STATS);

        SerializationHeader header = component.toHeader("test-descriptor", metadata, versionWithImplicitFrozenTuples, false);
        assertThat(header.keyType().isMultiCell()).isFalse();
        assertThat(header.clusteringTypes().get(0).isMultiCell()).isFalse();
        assertThat(header.columns().statics.iterator().next().type.isMultiCell()).isFalse();
        assertThat(header.columns().regulars.iterator().next().type.isMultiCell()).isFalse();

        // Tuples are always fixed regardless of version (type.isTuple() triggers the fix path),
        // so the explicit frozen version should produce the same result as the implicit frozen version.
        SerializationHeader header2 = component.toHeader("test-descriptor", metadata, versionWithExplicitFrozenTuples, false);
        assertThat(header2.keyType().isMultiCell()).isFalse();
        assertThat(header2.clusteringTypes().get(0).isMultiCell()).isFalse();
        assertThat(header2.columns().statics.iterator().next().type.isMultiCell()).isFalse();
        assertThat(header2.columns().regulars.iterator().next().type.isMultiCell()).isFalse();
    }

    @Test
    public void testDecodeDroppedUTDsEncodedAsNonFrozenTuples() throws UnknownColumnException
    {
        TupleType multicellTupleType = new TupleType(Arrays.asList(Int32Type.instance, Int32Type.instance), true);

        TableMetadata metadata = TableMetadata.builder("ks", "tab")
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .addStaticColumn("s", Int32Type.instance)
                                              .addClusteringColumn("c", Int32Type.instance)
                                              .addRegularColumn("v", Int32Type.instance)
                                              .recordColumnDrop(ColumnMetadata.regularColumn("ks", "tab", "dv", multicellTupleType), 0L)
                                              .recordColumnDrop(ColumnMetadata.staticColumn("ks", "tab", "ds", multicellTupleType), 0L)
                                              .build();

        SerializationHeader.Component component = SerializationHeader.Component.buildComponentForTools(multicellTupleType,
                                                                                                       Arrays.asList(Int32Type.instance),
                                                                                                       new LinkedHashMap<>(Map.of(ByteBufferUtil.bytes("s"), Int32Type.instance,
                                                                                                                                  ByteBufferUtil.bytes("ds"), multicellTupleType)),
                                                                                                       new LinkedHashMap<>(Map.of(ByteBufferUtil.bytes("v"), Int32Type.instance,
                                                                                                                                  ByteBufferUtil.bytes("dv"), multicellTupleType)),
                                                                                                       EncodingStats.NO_STATS);

        SerializationHeader header = component.toHeader("tab", metadata, versionWithImplicitFrozenTuples, false);
        assertThat(Iterables.find(header.columns().regulars, md -> md.name.toString().equals("dv")).type.isMultiCell()).isTrue();
        assertThat(Iterables.find(header.columns().statics, md -> md.name.toString().equals("ds")).type.isMultiCell()).isTrue();

        // Tuples are always fixed regardless of version (type.isTuple() triggers the fix path),
        // so the explicit frozen version should produce the same result as the implicit frozen version.
        SerializationHeader header2 = component.toHeader("tab", metadata, versionWithExplicitFrozenTuples, false);
        assertThat(Iterables.find(header2.columns().regulars, md -> md.name.toString().equals("dv")).type.isMultiCell()).isTrue();
        assertThat(Iterables.find(header2.columns().statics, md -> md.name.toString().equals("ds")).type.isMultiCell()).isTrue();
    }

    /**
     * Verify that DeletionTime.LIVE round-trips correctly through writeDeletionTime/readDeletionTime
     * when the header has realistic (non-epoch) encoding stats. This exercises the fix for the 32-bit
     * overflow bug where Long.MAX_VALUE (LIVE's localDeletionTime) minus a realistic minLocalDeletionTime
     * doesn't fit in an int.
     */
    @Test
    public void testLiveDeletionTimeRoundTrip() throws IOException
    {
        // Use realistic encoding stats — a real compaction would have a recent minLocalDeletionTime,
        // which makes the delta (Long.MAX_VALUE - minLocalDeletionTime) overflow a 32-bit int.
        long realisticTimestamp = System.currentTimeMillis() * 1000;
        long realisticLocalDeletionTime = System.currentTimeMillis() / 1000;
        EncodingStats stats = new EncodingStats(realisticTimestamp, realisticLocalDeletionTime, 0);

        TableMetadata metadata = TableMetadata.builder("ks", "tab")
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .addRegularColumn("v", Int32Type.instance)
                                              .build();
        SerializationHeader header = new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(), stats);

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            header.writeDeletionTime(DeletionTime.LIVE, out);

            try (DataInputBuffer in = new DataInputBuffer(out.getData(), 0, out.getLength()))
            {
                DeletionTime result = header.readDeletionTime(in);
                assertThat(result.isLive()).isTrue();
                assertThat(result).isEqualTo(DeletionTime.LIVE);
            }
        }
    }

    /**
     * Verify that old (broken) sstables where LIVE DeletionTime was written with the overflow bug
     * are still read back as LIVE. In the broken encoding, markedForDeleteAt is Long.MIN_VALUE
     * (correctly delta-encoded) but localDeletionTime is garbage (-1 after 32-bit overflow).
     * The read side should recognize LIVE by markedForDeleteAt == Long.MIN_VALUE and ignore the
     * corrupt localDeletionTime.
     */
    @Test
    public void testLiveDeletionTimeReadBackwardCompatibility() throws IOException
    {
        long realisticTimestamp = System.currentTimeMillis() * 1000;
        long realisticLocalDeletionTime = System.currentTimeMillis() / 1000;
        EncodingStats stats = new EncodingStats(realisticTimestamp, realisticLocalDeletionTime, 0);

        TableMetadata metadata = TableMetadata.builder("ks", "tab")
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .addRegularColumn("v", Int32Type.instance)
                                              .build();
        SerializationHeader header = new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(), stats);

        // Simulate the old broken encoding: write the timestamp delta for Long.MIN_VALUE (correct),
        // then write -1 as the localDeletionTime delta (what the old code produced from overflow).
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            // Write timestamp: Long.MIN_VALUE - stats.minTimestamp (this is what the old code wrote correctly)
            header.writeTimestamp(Long.MIN_VALUE, out);
            // Write the overflowed localDeletionTime delta: the old broken code cast
            // (Long.MAX_VALUE - minLocalDeletionTime) to int, which wraps to -1 for realistic stats.
            // writeUnsignedVInt32 with -1 (0xFFFFFFFF) produces a specific byte sequence.
            out.writeUnsignedVInt32(-1);

            try (DataInputBuffer in = new DataInputBuffer(out.getData(), 0, out.getLength()))
            {
                DeletionTime result = header.readDeletionTime(in);
                assertThat(result.isLive()).isTrue();
            }
        }
    }

    /**
     * A LIVE deletion time sent to a pre-4.0 peer (a DSE 6 node, in DSE 6 compatibility mode) is written with the local
     * deletion time delta that a 32-bit reader, as DSE 6 and Cassandra 3.x have, decodes to {@code Integer.MAX_VALUE},
     * and is sized so. At the other versions, in sstables and without the mode, LIVE is written as without a version;
     * other deletion times are written as without a version at every version.
     */
    @Test
    public void testLiveDeletionTimeForPre40Peers() throws IOException
    {
        long minLocalDeletionTime = System.currentTimeMillis() / 1000;
        EncodingStats stats = new EncodingStats(System.currentTimeMillis() * 1000, minLocalDeletionTime, 0);
        TableMetadata metadata = TableMetadata.builder("ks", "tab")
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .addRegularColumn("v", Int32Type.instance)
                                              .build();
        RegularAndStaticColumns columns = metadata.regularAndStaticColumns();
        SerializationHeader forMessaging = new SerializationHeader(false, metadata, columns, stats);
        SerializationHeader forSSTable = new SerializationHeader(true, metadata, columns, stats);
        DeletionTime deletion = DeletionTime.build(stats.minTimestamp + 10, minLocalDeletionTime + 10);
        int[] pre40 = { MessagingService.VERSION_30, MessagingService.VERSION_3014 };

        InternodeCompatibilityMode previous = DatabaseDescriptor.getInternodeCompatibilityMode();
        try
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
            for (int version : pre40)
            {
                byte[] live = written(forMessaging, DeletionTime.LIVE, version);
                try (DataInputBuffer in = new DataInputBuffer(live))
                {
                    assertThat(forMessaging.readTimestamp(in)).isEqualTo(Long.MIN_VALUE);
                    assertThat((int) in.readUnsignedVInt() + (int) minLocalDeletionTime).isEqualTo(Integer.MAX_VALUE);
                    assertThat(in.available()).isZero();
                }
                // and this software reads it back as LIVE
                try (DataInputBuffer in = new DataInputBuffer(live))
                {
                    assertThat(forMessaging.readDeletionTime(in)).isEqualTo(DeletionTime.LIVE);
                }
                assertThat(live).isNotEqualTo(written(forMessaging, DeletionTime.LIVE));
            }
            for (MessagingService.Version version : MessagingService.Version.values())
            {
                for (SerializationHeader header : new SerializationHeader[]{ forMessaging, forSSTable })
                {
                    if (header == forMessaging && version.value < MessagingService.VERSION_40)
                        continue;
                    assertThat(written(header, DeletionTime.LIVE, version.value))
                        .isEqualTo(written(header, DeletionTime.LIVE));
                }
            }
            for (MessagingService.Version version : MessagingService.Version.values())
            {
                for (SerializationHeader header : new SerializationHeader[]{ forMessaging, forSSTable })
                    assertThat(written(header, deletion, version.value)).isEqualTo(written(header, deletion));
            }

            // a header minimum past 2038 cannot be encoded for a 32-bit reader: refused, as such a deletion time is
            long past2038 = Integer.MAX_VALUE + 1000L;
            EncodingStats pastCap = new EncodingStats(stats.minTimestamp, past2038, 0);
            SerializationHeader pastCapForMessaging = new SerializationHeader(false, metadata, columns, pastCap);
            SerializationHeader pastCapForSSTable = new SerializationHeader(true, metadata, columns, pastCap);
            String refusal = "DSE 6 compatibility mode: local deletion time " + past2038 +
                             " is past 2038 and cannot be sent at messaging version %d," +
                             " whose deletion times are 32-bit";
            for (int version : pre40)
            {
                try (DataOutputBuffer out = new DataOutputBuffer())
                {
                    assertThatThrownBy(() -> pastCapForMessaging.writeDeletionTime(DeletionTime.LIVE, out, version))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage(String.format(refusal, version));
                }
                assertThatThrownBy(() -> pastCapForMessaging.deletionTimeSerializedSize(DeletionTime.LIVE, version))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(String.format(refusal, version));
                assertThat(written(pastCapForSSTable, DeletionTime.LIVE, version))
                    .isEqualTo(written(pastCapForSSTable, DeletionTime.LIVE));
            }
            assertThat(written(pastCapForMessaging, DeletionTime.LIVE, MessagingService.VERSION_40))
                .isEqualTo(written(pastCapForMessaging, DeletionTime.LIVE));

            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            for (int version : pre40)
                assertThat(written(forMessaging, DeletionTime.LIVE, version))
                    .isEqualTo(written(forMessaging, DeletionTime.LIVE));
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(previous);
        }
    }

    private static byte[] written(SerializationHeader header, DeletionTime deletionTime, int version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            header.writeDeletionTime(deletionTime, out, version);
            assertThat(header.deletionTimeSerializedSize(deletionTime, version)).isEqualTo(out.getLength());
            return out.toByteArray();
        }
    }

    private static byte[] written(SerializationHeader header, DeletionTime deletionTime) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            header.writeDeletionTime(deletionTime, out);
            assertThat(header.deletionTimeSerializedSize(deletionTime)).isEqualTo(out.getLength());
            return out.toByteArray();
        }
    }
}
