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

package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.filter.DataLimits.NO_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

public class DataLimitsTest
{
    private final static Logger logger = LoggerFactory.getLogger(DataLimitsTest.class);

    ByteBuffer lastReturnedKey = ByteBufferUtil.bytes("lastReturnedKey");

    DataLimits cqlLimits = DataLimits.cqlLimits(19, 17);
    DataLimits cqlLimitsForPagingInRows = cqlLimits.forPaging(PageSize.inRows(13));
    DataLimits cqlLimitsForPagingInBytes = cqlLimits.forPaging(PageSize.inBytes(13));
    DataLimits cqlLimitsForPagingInRowsWithLastRow = cqlLimits.forPaging(PageSize.inRows(13), lastReturnedKey, 5);
    DataLimits cqlLimitsForPagingInBytesWithLastRow = cqlLimits.forPaging(PageSize.inBytes(13), lastReturnedKey, 5);
    DataLimits groupByLimits = DataLimits.groupByLimits(19, 17, NO_LIMIT, NO_LIMIT, AggregationSpecification.AGGREGATE_EVERYTHING);
    DataLimits groupByLimitsForPagingInRows = groupByLimits.forPaging(PageSize.inRows(13));
    DataLimits groupByLimitsForPagingInBytes = groupByLimits.forPaging(PageSize.inBytes(13));
    DataLimits groupByLimitsForPagingInRowsWithLastRow = groupByLimits.forPaging(PageSize.inRows(13), lastReturnedKey, 5);
    DataLimits groupByLimitsForPagingInBytesWithLastRow = groupByLimits.forPaging(PageSize.inBytes(13), lastReturnedKey, 5);

    @BeforeClass
    public static void initClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void serializationTest()
    {
        TableMetadata metadata = SchemaLoader.standardCFMD("ks", "cf", 1, IntegerType.instance, IntegerType.instance, IntegerType.instance).build();
        for (MessagingService.Version version : MessagingService.Version.values())
        {
            checkSerialization(version, cqlLimits, "cql limits", metadata);
            checkSerialization(version, cqlLimitsForPagingInRows, "cql limits for paging in rows", metadata);
            checkSerialization(version, cqlLimitsForPagingInBytes, "cql limits for paging in bytes", metadata);
            checkSerialization(version, cqlLimitsForPagingInRowsWithLastRow, "cql limits for paging in rows with last row", metadata);
            checkSerialization(version, cqlLimitsForPagingInBytesWithLastRow, "cql limits for paging in bytes with last row", metadata);
            checkSerialization(version, groupByLimits, "group by limits", metadata);
            checkSerialization(version, groupByLimitsForPagingInRows, "group by limits for paging in rows", metadata);
            checkSerialization(version, groupByLimitsForPagingInBytes, "group by limits for paging in bytes", metadata);
            checkSerialization(version, groupByLimitsForPagingInRowsWithLastRow, "group by limits for paging in rows with last row", metadata);
            checkSerialization(version, groupByLimitsForPagingInBytesWithLastRow, "group by limits for paging in bytes with last row", metadata);
        }
    }

    /**
     * At 10 (the older DSE migration dialect) and, in DSE 6 compatibility mode, at 11 (its dialect) the kind is written
     * with DSE's ordinal, which lacks THRIFT_LIMIT and SUPER_COLUMN_COUNTING_LIMIT: the GROUP BY kinds are 2
     * and 3 there, 4 and 5 from 4.0. Neither dialect has the bytes limit of the DS versions.
     */
    @Test
    public void kindOrdinalsBelow40AreDseOnes() throws IOException
    {
        InternodeCompatibilityMode before = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        try
        {
            checkKindOrdinals(true);
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(before);
        }
        // with the mode off only 10 uses the DSE ordinals, as before the mode existed
        checkKindOrdinals(false);
    }

    private void checkKindOrdinals(boolean dse6Mode) throws IOException
    {
        TableMetadata metadata = SchemaLoader.standardCFMD("ks", "cf", 1, IntegerType.instance, IntegerType.instance, IntegerType.instance).build();
        int[] versions = { MessagingService.VERSION_30, MessagingService.VERSION_3014, MessagingService.VERSION_40 };
        for (int version : versions)
        {
            boolean pre40 = version == MessagingService.VERSION_30 || (dse6Mode && version == MessagingService.VERSION_3014);
            // CQL_LIMIT: kind, row limit, per-partition limit, distinct
            assertThat(ByteBufferUtil.bytesToHex(ByteBuffer.wrap(serialize(cqlLimits, version, metadata)))).isEqualTo("00131100");
            // CQL_PAGING_LIMIT: then the last returned key (vint length) and the rows remaining in it
            assertThat(serialize(cqlLimitsForPagingInRowsWithLastRow, version, metadata)).isEqualTo(expected(1, out -> {
                out.writeUnsignedVInt32(13);
                out.writeUnsignedVInt32(17);
                out.writeBoolean(false);
                ByteBufferUtil.writeWithVIntLength(lastReturnedKey, out);
                out.writeUnsignedVInt32(5);
            }));
            // CQL_GROUP_BY_LIMIT: group limit, group per-partition limit, row limit, the aggregation (AGGREGATE_EVERYTHING,
            // ordinal 0) and an empty grouping state
            assertThat(serialize(groupByLimits, version, metadata)).isEqualTo(expected(pre40 ? 2 : 4, out -> {
                out.writeUnsignedVInt32(19);
                out.writeUnsignedVInt32(17);
                out.writeUnsignedVInt32(NO_LIMIT);
                out.writeByte(0);
                out.writeBoolean(false);
            }));
            // CQL_GROUP_BY_PAGING_LIMIT: then the last returned key and the rows remaining in it
            assertThat(serialize(groupByLimitsForPagingInRowsWithLastRow, version, metadata)).isEqualTo(expected(pre40 ? 3 : 5, out -> {
                out.writeUnsignedVInt32(19);
                out.writeUnsignedVInt32(17);
                out.writeUnsignedVInt32(13);
                out.writeByte(0);
                out.writeBoolean(false);
                ByteBufferUtil.writeWithVIntLength(lastReturnedKey, out);
                out.writeUnsignedVInt32(5);
            }));
        }
    }

    /**
     * In DSE 6 compatibility mode an ordinal no kind has, sent by a DSE 6 peer at 11, is refused with an explicit error;
     * with the mode off an unknown ordinal fails as it did before the mode existed.
     */
    @Test
    public void unknownKindOrdinalIsRefused()
    {
        TableMetadata metadata = SchemaLoader.standardCFMD("ks", "cf", 1, IntegerType.instance, IntegerType.instance, IntegerType.instance).build();
        InternodeCompatibilityMode before = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        try
        {
            // 4 is CQL_GROUP_BY_LIMIT from 4.0, but DSE has no fifth kind
            assertThatThrownBy(() -> deserialize(new byte[]{ 4 }, MessagingService.VERSION_3014, metadata))
            .isInstanceOf(IOException.class)
            .hasMessage("Unknown DataLimits.Kind ordinal 4 at messaging version " + MessagingService.VERSION_3014);
            assertThatThrownBy(() -> deserialize(new byte[]{ 9 }, MessagingService.VERSION_3014, metadata))
            .isInstanceOf(IOException.class)
            .hasMessage("Unknown DataLimits.Kind ordinal 9 at messaging version " + MessagingService.VERSION_3014);
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(before);
        }
        assertThatThrownBy(() -> deserialize(new byte[]{ 4 }, MessagingService.VERSION_30, metadata))
        .isInstanceOf(AssertionError.class)
        .hasMessage("Unknown DataLimits.Kind with ordinal 4 and version " + MessagingService.VERSION_30);
        assertThatThrownBy(() -> deserialize(new byte[]{ 9 }, MessagingService.VERSION_40, metadata))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class);
    }

    private interface Writer
    {
        void write(DataOutputBuffer out) throws IOException;
    }

    private static byte[] expected(int ordinal, Writer body) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeByte(ordinal);
            body.write(out);
            return out.toByteArray();
        }
    }

    private static byte[] serialize(DataLimits limits, int version, TableMetadata metadata) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            DataLimits.serializer.serialize(limits, out, version, metadata.comparator);
            byte[] bytes = out.toByteArray();
            assertThat(DataLimits.serializer.serializedSize(limits, version, metadata.comparator)).isEqualTo(bytes.length);
            try (DataInputBuffer in = new DataInputBuffer(bytes))
            {
                assertThat(DataLimits.serializer.deserialize(in, version, metadata).kind()).isEqualTo(limits.kind());
            }
            return bytes;
        }
    }

    private static DataLimits deserialize(byte[] bytes, int version, TableMetadata metadata) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            return DataLimits.serializer.deserialize(in, version, metadata);
        }
    }

    @Test
    public void toStringTest()
    {
        String lastRetKeyStr = String.format("lastReturnedKey=%s", ByteBufferUtil.bytesToHex(lastReturnedKey));
        String lastRetKeyRemainingStr = "lastReturnedKeyRemaining=5";

        assertThat(cqlLimits.toString()).contains("LIMIT 19").contains("PER PARTITION LIMIT 17").doesNotContain("BYTES LIMIT");
        assertThat(cqlLimitsForPagingInRows.toString()).contains("LIMIT 13").contains("PER PARTITION LIMIT 17").doesNotContain("BYTES LIMIT");
        assertThat(cqlLimitsForPagingInBytes.toString()).contains("BYTES LIMIT 13").contains("LIMIT 19").contains("PER PARTITION LIMIT 17");
        assertThat(cqlLimitsForPagingInRowsWithLastRow.toString()).contains("LIMIT 13").contains("PER PARTITION LIMIT 17").doesNotContain("BYTES LIMIT").contains(lastRetKeyStr).contains(lastRetKeyRemainingStr);
        assertThat(cqlLimitsForPagingInBytesWithLastRow.toString()).contains("BYTES LIMIT 13").contains("LIMIT 19").contains("PER PARTITION LIMIT 17").contains(lastRetKeyStr).contains(lastRetKeyRemainingStr);

        assertThat(groupByLimits.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").doesNotContain("BYTES LIMIT");
        assertThat(groupByLimitsForPagingInRows.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").contains("LIMIT 13").doesNotContain("BYTES LIMIT");
        assertThat(groupByLimitsForPagingInBytes.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").contains("BYTES LIMIT 13");
        assertThat(groupByLimitsForPagingInRowsWithLastRow.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").contains("LIMIT 13").doesNotContain("BYTES LIMIT").contains(lastRetKeyStr).contains(lastRetKeyRemainingStr);
        assertThat(groupByLimitsForPagingInBytesWithLastRow.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").contains("BYTES LIMIT 13").contains(lastRetKeyStr).contains(lastRetKeyRemainingStr);
    }

    private void checkSerialization(MessagingService.Version version, DataLimits limits, String name, TableMetadata metadata)
    {
        String msg = String.format("serialization of %s for version %s", name, version);
        int size = (int) DataLimits.serializer.serializedSize(limits, version.value, metadata.comparator);
        try (DataOutputBuffer out = new DataOutputBuffer(2 * size))
        {
            DataLimits.serializer.serialize(limits, out, version.value, metadata.comparator);
            out.flush();
            assertThat(out.getLength()).describedAs(msg).isEqualTo(size);
            try (DataInputBuffer in = new DataInputBuffer(out.getData()))
            {
                DataLimits deserializedLimits = DataLimits.serializer.deserialize(in, version.value, metadata);
                assertThat(deserializedLimits.count()).describedAs(msg).isEqualTo(limits.count());

                if (version.value >= MessagingService.VERSION_DS_10)
                    assertThat(deserializedLimits.bytes()).describedAs(msg).isEqualTo(limits.bytes());
                else
                    assertThat(deserializedLimits.bytes()).describedAs(msg).isEqualTo(NO_LIMIT);

                assertThat(deserializedLimits.rows()).describedAs(msg).isEqualTo(limits.rows());
                assertThat(deserializedLimits.perPartitionCount()).describedAs(msg).isEqualTo(limits.perPartitionCount());
                assertThat(deserializedLimits.isDistinct()).describedAs(msg).isEqualTo(limits.isDistinct());
                assertThat(deserializedLimits.isUnlimited()).describedAs(msg).isEqualTo(limits.isUnlimited());
                assertThat(deserializedLimits.kind()).describedAs(msg).isEqualTo(limits.kind());
                assertThat(deserializedLimits.isGroupByLimit()).describedAs(msg).isEqualTo(limits.isGroupByLimit());
            }
            catch (IOException | RuntimeException e)
            {
                logger.error("Failed to deserialize: " + msg, e);
                fail(msg);
            }
        }
        catch (IOException | RuntimeException e)
        {
            logger.error("Failed to serialize: " + msg, e);
            fail(msg);
        }
    }
}