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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.VectorType;
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
import org.apache.cassandra.io.sstable.format.big.BigFormat;
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
    private static final String MARSHAL = "org.apache.cassandra.db.marshal.";

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
     * The header of a big me sstable (DSE 5.1's version, written in the DSE 5.1 compatibility mode) writes a LIVE
     * deletion time as Cassandra 3.x writes it: a 32-bit reader, which adds the delta to the header's minimum in 32-bit
     * arithmetic and takes {@code Integer.MAX_VALUE} for LIVE, reads it as LIVE, and so does this software; a deletion
     * that is not LIVE, and the headers of the other versions and of messages, keep the default encoding, a zero delta.
     */
    @Test
    public void aLiveDeletionTimeIsWrittenForA32BitReaderInMe() throws IOException
    {
        long minTimestamp = 1793958609519670L;
        int minLocalDeletionTime = 1793958609;
        EncodingStats stats = new EncodingStats(minTimestamp, minLocalDeletionTime, 0);
        TableMetadata metadata = TableMetadata.builder("ks", "tab")
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .addRegularColumn("v", Int32Type.instance)
                                              .build();
        RegularAndStaticColumns columns = metadata.regularAndStaticColumns();
        SerializationHeader sstable = new SerializationHeader(true, metadata, columns, stats);
        Version me = BigFormat.getInstance().getVersion("me");
        assertThat(me.hasPre40Encoding()).isTrue();
        SerializationHeader forMe = sstable.forSSTableVersion(me);
        assertThat(forMe).isNotSameAs(sstable);
        assertThat(forMe.forSSTableVersion(me)).isSameAs(forMe);
        for (String version : new String[]{ "nb", "oa" })
        {
            assertThat(BigFormat.getInstance().getVersion(version).hasPre40Encoding()).as(version).isFalse();
            assertThat(sstable.forSSTableVersion(BigFormat.getInstance().getVersion(version))).isSameAs(sstable);
        }
        assertThat(sstable.forSSTableVersion(BtiFormat.getInstance().getVersion("bb"))).isSameAs(sstable);
        SerializationHeader messaging = new SerializationHeader(false, metadata, columns, stats);
        assertThat(messaging.forSSTableVersion(me)).isSameAs(messaging);

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            forMe.writeDeletionTime(DeletionTime.LIVE, out);
            assertThat((long) out.getLength()).isEqualTo(forMe.deletionTimeSerializedSize(DeletionTime.LIVE));
            try (DataInputBuffer in = new DataInputBuffer(out.getData(), 0, out.getLength()))
            {
                // a 32-bit reader: the timestamp delta, then the local deletion time delta added in 32 bits
                long markedForDeleteAt = in.readUnsignedVInt() + minTimestamp;
                int localDeletionTime = (int) in.readUnsignedVInt() + minLocalDeletionTime;
                assertThat(markedForDeleteAt).isEqualTo(Long.MIN_VALUE);
                assertThat(localDeletionTime).isEqualTo(Integer.MAX_VALUE);
            }
            try (DataInputBuffer in = new DataInputBuffer(out.getData(), 0, out.getLength()))
            {
                assertThat(forMe.readDeletionTime(in)).isEqualTo(DeletionTime.LIVE);
            }
        }
        // a deletion that is not LIVE, in every header, and LIVE in the other headers: the default encoding
        DeletionTime deleted = DeletionTime.build(minTimestamp + 10, minLocalDeletionTime + 5);
        for (SerializationHeader header : new SerializationHeader[]{ forMe, sstable, messaging })
        {
            try (DataOutputBuffer expected = new DataOutputBuffer(); DataOutputBuffer out = new DataOutputBuffer())
            {
                expected.writeUnsignedVInt(10L);
                expected.writeUnsignedVInt32(5);
                header.writeDeletionTime(deleted, out);
                assertThat(out.toByteArray()).isEqualTo(expected.toByteArray());
            }
        }
        // the header of a me sstable reused by the writer of another version (a scrub, a stream) writes it as nb does
        SerializationHeader backToNb = forMe.forSSTableVersion(BigFormat.getInstance().getVersion("nb"));
        assertThat(backToNb).isNotSameAs(forMe);
        for (SerializationHeader header : new SerializationHeader[]{ sstable, messaging, backToNb })
        {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                header.writeDeletionTime(DeletionTime.LIVE, out);
                try (DataInputBuffer in = new DataInputBuffer(out.getData(), 0, out.getLength()))
                {
                    in.readUnsignedVInt();
                    assertThat(in.readUnsignedVInt()).isZero();
                }
            }
        }
        // a minimum outside 32 bits, negative (CASSANDRA-14092 data) or past 2038: the delta wraps as the 32-bit
        // reader's sum does, and this software still reads LIVE
        for (long minimum : new long[]{ -5L, Integer.MIN_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE + 10L })
        {
            SerializationHeader header = new SerializationHeader(true, metadata, columns,
                                                                 new EncodingStats(minTimestamp, minimum, 0))
                                         .forSSTableVersion(me);
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                header.writeDeletionTime(DeletionTime.LIVE, out);
                assertThat((long) out.getLength()).as("%s", minimum)
                                                  .isEqualTo(header.deletionTimeSerializedSize(DeletionTime.LIVE));
                try (DataInputBuffer in = new DataInputBuffer(out.getData(), 0, out.getLength()))
                {
                    in.readUnsignedVInt();
                    int localDeletionTime = (int) in.readUnsignedVInt() + (int) minimum;
                    assertThat(localDeletionTime).as("%s", minimum).isEqualTo(Integer.MAX_VALUE);
                }
                try (DataInputBuffer in = new DataInputBuffer(out.getData(), 0, out.getLength()))
                {
                    assertThat(header.readDeletionTime(in)).as("%s", minimum).isEqualTo(DeletionTime.LIVE);
                }
            }
        }
    }

    /**
     * In the DSE 5.1 compatibility mode the header of a big me sstable names the types as Cassandra 3.x does: a frozen
     * tuple without {@code FrozenType}, at the top, reversed and inside a collection. Outside the mode, and for the
     * other versions, the names of this software are written.
     */
    @Test
    public void theTypeNamesOfMeAreThoseDse51Reads()
    {
        TupleType tuple = new TupleType(Arrays.asList(Int32Type.instance, UTF8Type.instance));
        TableMetadata metadata = TableMetadata.builder("ks", "tuples")
                                              .addPartitionKeyColumn("k", tuple)
                                              .addClusteringColumn("c", ReversedType.getInstance(tuple))
                                              .addRegularColumn("l", ListType.getInstance(tuple, true))
                                              .addStaticColumn("s", tuple)
                                              .build();
        SerializationHeader header = new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(),
                                                             EncodingStats.NO_STATS);
        Version me = BigFormat.getInstance().getVersion("me");
        String tupleName = MARSHAL + "TupleType(" + MARSHAL + "Int32Type," + MARSHAL + "UTF8Type)";
        String frozenTuple = MARSHAL + "FrozenType(" + tupleName + ')';
        assertThat(tuple.toString()).isEqualTo(frozenTuple);

        InternodeCompatibilityMode before = DatabaseDescriptor.getInternodeCompatibilityMode();
        try
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
            assertThat(typeNames(header, me)).contains(tupleName)
                                             .doesNotContain("FrozenType")
                                             .contains(MARSHAL + "ListType(" + tupleName + ')')
                                             .contains(MARSHAL + "ReversedType(" + tupleName + ')');
            // the other versions keep the names of this software
            assertThat(typeNames(header, BigFormat.getInstance().getVersion("nb"))).contains(frozenTuple);

            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            assertThat(typeNames(header, me)).contains(frozenTuple);
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(before);
        }
    }

    /** A vector, which DSE 5.1 cannot read, is refused in the header of a big me sstable written in the mode. */
    @Test
    public void aTypeDse51CannotReadIsRefusedInMe()
    {
        TableMetadata metadata = TableMetadata.builder("ks", "vectors")
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .addRegularColumn("v", VectorType.getInstance(FloatType.instance, 2))
                                              .build();
        SerializationHeader header = new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(),
                                                             EncodingStats.NO_STATS);
        Version me = BigFormat.getInstance().getVersion("me");
        InternodeCompatibilityMode before = DatabaseDescriptor.getInternodeCompatibilityMode();
        try
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
            assertThatThrownBy(() -> typeNames(header, me))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageStartingWith("A big me sstable for DSE 5.1 cannot hold a column of type " +
                                    "org.apache.cassandra.db.marshal.VectorType(")
            .hasMessageContaining(": DSE 5.1 cannot read org.apache.cassandra.db.marshal.VectorType(");
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            assertThat(typeNames(header, me)).contains("VectorType");
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(before);
        }
    }

    /** The header component of {@code header} serialized for {@code version}, as text, checked against its size. */
    private static String typeNames(SerializationHeader header, Version version)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            SerializationHeader.Component component = header.toComponent();
            SerializationHeader.serializer.serialize(version, component, out);
            assertThat(out.getLength()).isEqualTo(SerializationHeader.serializer.serializedSize(version, component));
            return new String(out.toByteArray(), StandardCharsets.ISO_8859_1);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * The header of an sstable of {@code system.peers} or {@code system.local} that names the five columns DSE 5.1
     * adds to those tables (a compacted one names all five) is read in every mode, the five as dropped regular columns
     * of their types; any other unknown column of those tables is refused. The same names are refused for another
     * table, which has no record of them, as before this software read them.
     */
    @Test
    public void theColumnsDse51AddsToTheLocalAndPeersTablesAreReadAsDropped() throws UnknownColumnException
    {
        Version version = BigFormat.getInstance().getLatestVersion();
        for (TableMetadata table : Arrays.asList(SystemKeyspace.LegacyPeers, SystemKeyspace.Local))
        {
            TableMetadata withDse51Columns = Dse51SystemTables.withDse51Columns(table);
            SerializationHeader.Component written = SerializationHeader.makeWithoutStats(withDse51Columns)
                                                                       .toComponent();
            SerializationHeader read = written.toHeader("dse51", table, version, false);
            for (Map.Entry<String, AbstractType<?>> column : Dse51SystemTables.COLUMNS.entrySet())
            {
                ColumnMetadata dropped = named(read.columns().regulars, column.getKey());
                assertThat(dropped).as("%s in %s", column.getKey(), table).isNotNull();
                assertThat(dropped.isDropped()).isTrue();
                assertThat(dropped.type).isEqualTo(column.getValue());
            }
            assertThat(read.columns().regulars.size()).isEqualTo(withDse51Columns.regularColumns().size());

            TableMetadata other = userTable(withDse51Columns);
            TableMetadata otherWithoutDseColumns = userTable(table);
            SerializationHeader.Component otherWritten = SerializationHeader.makeWithoutStats(other).toComponent();
            assertThatThrownBy(() -> otherWritten.toHeader("other", otherWithoutDseColumns, version, false))
            .isInstanceOf(UnknownColumnException.class)
            .hasMessageMatching("Unknown column (workload|workloads|graph|dse_version|server_id) " +
                                "during deserialization");

            TableMetadata withUnknown = withColumnFoo(table);
            SerializationHeader.Component unknownWritten = SerializationHeader.makeWithoutStats(withUnknown)
                                                                              .toComponent();
            assertThatThrownBy(() -> unknownWritten.toHeader("unknown", table, version, false))
            .isInstanceOf(UnknownColumnException.class)
            .hasMessage("Unknown column foo during deserialization");
        }
    }

    /**
     * The columns of an update of {@code system.peers} naming a column DSE 5.1 adds are read at messaging version
     * 10 in {@code DSE_5_1} only, the version of the commit log segments of DSE 5.1: in the other modes, and at
     * versions 11 and 12 in the mode, the name is refused as an unknown column, as before. Any other unknown column is
     * refused in every mode.
     */
    @Test
    public void anUpdateNamingAColumnDse51AddsIsReadInDse51ModeOnly() throws IOException
    {
        TableMetadata peers = SystemKeyspace.LegacyPeers;
        TableMetadata withDse51Columns = Dse51SystemTables.withDse51Columns(peers);
        ByteBuffer dataCenter = UTF8Type.instance.decompose("data_center");
        ByteBuffer dseVersion = UTF8Type.instance.decompose(Dse51SystemTables.DSE_VERSION);
        Columns written = Columns.from(Arrays.asList(withDse51Columns.getColumn(dataCenter),
                                                     withDse51Columns.getColumn(dseVersion)));
        TableMetadata withUnknown = withColumnFoo(peers);
        Columns unknown = Columns.of(withUnknown.getColumn(UTF8Type.instance.decompose("foo")));
        int[] versions = { MessagingService.VERSION_30, MessagingService.VERSION_3014, MessagingService.VERSION_40 };
        InternodeCompatibilityMode previous = DatabaseDescriptor.getInternodeCompatibilityMode();
        try
        {
            for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
            {
                DatabaseDescriptor.setInternodeCompatibilityMode(mode);
                for (int version : versions)
                {
                    boolean reads = mode == InternodeCompatibilityMode.DSE_5_1
                                    && version == MessagingService.VERSION_30;
                    assertThat(Dse51SystemTables.readsStoredDse51Columns(peers, version))
                    .as("%s at %s", mode, version)
                    .isEqualTo(reads);
                    if (reads)
                    {
                        Columns read = deserializeColumns(written, peers, version);
                        assertThat(named(read, "data_center")).isEqualTo(peers.getColumn(dataCenter));
                        assertThat(named(read, Dse51SystemTables.DSE_VERSION).isDropped()).isTrue();
                    }
                    else
                    {
                        assertThatThrownBy(() -> deserializeColumns(written, peers, version))
                        .hasMessage("Unknown column dse_version during deserialization of system.peers");
                    }
                    assertThatThrownBy(() -> deserializeColumns(unknown, peers, version))
                    .hasMessage("Unknown column foo during deserialization of system.peers");
                }
                assertThat(Dse51SystemTables.readsStoredDse51Columns(SystemKeyspace.Local, MessagingService.VERSION_30))
                .isEqualTo(mode == InternodeCompatibilityMode.DSE_5_1);
                assertThat(Dse51SystemTables.readsStoredDse51Columns(userTable(peers), MessagingService.VERSION_30))
                .isFalse();
            }
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(previous);
        }
    }

    /** The column named {@code name} in {@code columns}, null if none. */
    private static ColumnMetadata named(Columns columns, String name)
    {
        for (ColumnMetadata column : columns)
        {
            if (column.name.toString().equals(name))
                return column;
        }
        return null;
    }

    /** A table of the keyspace of this test with the name and the columns of {@code source}. */
    private static TableMetadata userTable(TableMetadata source)
    {
        TableMetadata.Builder builder = TableMetadata.builder(KEYSPACE, source.name);
        for (ColumnMetadata column : source.partitionKeyColumns())
            builder.addPartitionKeyColumn(column.name, column.type);
        for (ColumnMetadata column : source.clusteringColumns())
            builder.addClusteringColumn(column.name, column.type);
        for (ColumnMetadata column : source.regularColumns())
            builder.addRegularColumn(column.name, column.type);
        return builder.build();
    }

    /** {@code table} with a regular column {@code foo}, unknown to {@code table}, and no dropped column. */
    private static TableMetadata withColumnFoo(TableMetadata table)
    {
        return table.unbuild()
                    .droppedColumns(Collections.emptyMap())
                    .addRegularColumn("foo", UTF8Type.instance)
                    .build();
    }

    /** {@code columns} written as an update writes them, read back with {@code metadata} at {@code version}. */
    private static Columns deserializeColumns(Columns columns, TableMetadata metadata, int version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Columns.serializer.serialize(columns, out);
            try (DataInputBuffer in = new DataInputBuffer(out.getData(), 0, out.getLength()))
            {
                boolean storedByDse51 = Dse51SystemTables.readsStoredDse51Columns(metadata, version);
                return Columns.serializer.deserialize(in, metadata, false, false, storedByDse51);
            }
        }
    }
}
