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

package org.apache.cassandra.db.view;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * The materialized-view data exchanged with DSE 6 nodes at the pre-4.0 dialect (DSE 6 compatibility mode, messaging
 * version 11), translated between DSE's encoding of a view row and the encoding of this software.
 * <p>
 * DSE lists the hidden columns of its views ({@link DseViewHiddenColumns}) among the regular columns of the view, so
 * they appear wherever the view's regular columns do on the wire: in the column filters of its reads, in the
 * serialization headers of its mutations and read responses, and as cells in the rows. For a view with one regular
 * base column in its primary key ({@code PRIMARY KEY (v, pk, ck)} over a base table keyed by {@code (pk, ck)}), a DSE
 * view row is live if and only if its {@code .v} cell is, while this software keeps the same fact in the row's
 * primary key liveness. Which views DSE keeps such a column for is read from DSE's own
 * {@code system_schema.hidden_columns} ({@link DseViewHiddenColumnCatalog}): a view that table lists nothing for (taken
 * to be a view of DSE's old format, which has no hidden column, as the fixtures
 * {@code test/data/dse-compat/sstables/dse-<version>-mv-updates} show) is exchanged as it is. The translation, for the
 * views with that hidden column only:
 * <ul>
 *     <li>inbound column filters: {@code .v} is left out of the filter the local read runs with, and the filter DSE
 *     sent is kept to serialize the response ({@code ReadCommand.Serializer});</li>
 *     <li>inbound serialization headers (mutations, hints, batched mutations, read-repair writes, read responses):
 *     {@code .v} is accepted and every row gets the liveness of its {@code .v} cell, or none without one, as for the
 *     DSE view sstables ({@link DseViewHiddenColumns#livenessOf}, applied by
 *     {@link org.apache.cassandra.db.rows.UnfilteredSerializer}); the cell and the column are dropped;</li>
 *     <li>outbound column filters: {@code .v} is fetched and queried, as DSE's own filters fetch it and so that a DSE
 *     replica never skips the cell as the value of a column nobody asked for;</li>
 *     <li>outbound rows (mutations, hints, batched mutations, read-repair writes, read responses to a DSE
 *     coordinator): every row with a primary key liveness gets the {@code .v} cell DSE writes for it
 *     ({@link #hiddenCellOf}), and the column is added to the header; the expired liveness of an entry whose base cell
 *     was deleted or changed becomes a live one at the same timestamp next to the hidden tombstone, which is how DSE
 *     holds the old entry of a base row written by {@code INSERT};</li>
 *     <li>read digests at version 11: computed over the rows in DSE's encoding, with DSE's rule for hidden cells
 *     (their local deletion time is part of the digest, as DSE 6's digest code has it), so that they compare with the
 *     digests of DSE replicas.</li>
 * </ul>
 * A view with any other hidden column (a base column the view filters on or does not select, or several regular base
 * columns in its primary key) is refused in both directions with the remedy of {@link DseViewHiddenColumns}: such views
 * must be dropped before the upgrade.
 * <p>
 * Limit: the primary key liveness DSE gives a view row is the liveness of its base row, which may differ from the one
 * of the base cell of {@code v} (it is empty for a base row written by {@code UPDATE}, older for a base row inserted
 * again without {@code v}). This software keeps only the latter, so such a row has a different read digest on the two
 * sides: a read of it at a consistency level above ONE that involves both a DSE replica and a replica of this software
 * always falls back to a data read (a digest mismatch), whose data reconciles to the same rows and repairs nothing.
 * <p>
 * Not translated: the mutations DSE stored on its own disk before the upgrade, in its batchlog and its hints, which this
 * software decodes at DSE's own messaging version, not at 11. A DSE view update that writes the hidden cell (every
 * update of a live view entry) names {@code .v} there, so such a batch is kept in the batchlog as one that cannot be
 * decoded, its ERROR saying how to restore the view, and such a hint is dropped by the conversion of DSE's hints,
 * counted among the hints for an unknown column.
 * <p>
 * Nothing here runs outside of DSE 6 compatibility mode, nor at messaging versions other than 11, nor for tables that
 * are not views: {@link #hiddenColumn} and {@link #translates} return {@code null} and {@code false} there.
 */
public final class DseViewWireFormat
{
    /** The {@code compatstatus} refusal reason of a read of a view from a DSE 6 peer filtering on a DSE hidden column. */
    public static final String REFUSED_HIDDEN_COLUMN_FILTER = "read of a materialized view filtering on a DSE hidden column";

    private DseViewWireFormat()
    {
    }

    /**
     * Whether a message at {@code version} may name DSE hidden columns of {@code metadata}: the node is in DSE 6
     * compatibility mode, {@code version} is DSE's dialect (11) and {@code metadata} is a materialized view.
     */
    public static boolean translates(TableMetadata metadata, int version)
    {
        return metadata.isView() && version == DseCompat.DSE_DIALECT && DseCompat.enabled();
    }

    /**
     * The DSE hidden column the data of {@code metadata} carries at {@code version}, or {@code null} when the data is
     * exchanged as is: when {@link #translates} is false, or when DSE keeps no hidden column for the view
     * ({@link DseViewHiddenColumnCatalog#translatedColumn}).
     *
     * @throws IllegalStateException for a view with a DSE hidden column this software does not reproduce, or when DSE's
     * table cannot be read; a node in DSE 6 compatibility mode does not start with such a view
     * ({@link DseViewHiddenColumnCatalog#checkViewsAtStartup}), so this is only a fallback, thrown by the serializer (outbound)
     * or the deserializer (inbound) of the message
     */
    public static ColumnMetadata hiddenColumn(TableMetadata metadata, int version)
    {
        return translates(metadata, version) ? DseViewHiddenColumnCatalog.instance().translatedColumn(metadata) : null;
    }

    /**
     * Resolves a column name a DSE peer sent that is neither a column nor a dropped column of the view {@code metadata},
     * when {@link #translates} is true.
     *
     * @return the hidden column of {@link #hiddenColumn}, or {@code null} when {@code name} is not the name of a DSE
     * hidden column, or the view is no longer in the schema; the caller then fails as before
     * @throws UnknownColumnException for a DSE hidden column DSE's {@code system_schema.hidden_columns} does not list here
     * @throws IllegalStateException for a view with a DSE hidden column this software does not reproduce
     */
    public static ColumnMetadata resolveHiddenColumn(TableMetadata metadata, ByteBuffer name) throws UnknownColumnException
    {
        String columnName;
        try
        {
            columnName = ByteBufferUtil.string(name);
        }
        catch (CharacterCodingException e)
        {
            return null;
        }
        if (columnName.length() < 2 || !columnName.startsWith(DseViewHiddenColumns.HIDDEN_COLUMN_PREFIX))
            return null;
        // a view dropped meanwhile: an unknown column, as for any other table
        if (Schema.instance.getView(metadata.keyspace, metadata.name) == null)
            return null;

        ColumnMetadata hidden = DseViewHiddenColumnCatalog.instance().translatedColumn(metadata);
        if (hidden != null && hidden.name.bytes.equals(name))
            return hidden;
        DseCompatRefusals.refused(DseViewHiddenColumnCatalog.REFUSED_VIEW);
        throw new UnknownColumnException(String.format("A message from a DSE 6 peer names the column %s of materialized view %s, which DSE's system_schema.%s " +
                                                       "on this node does not list as a hidden column of that view; this software refuses the message " +
                                                       "rather than return or store wrong view rows. Check that the data directories of this node hold the " +
                                                       "sstables of system_schema.%s that DSE wrote",
                                                       columnName, metadata, DseViewHiddenColumnCatalog.HIDDEN_COLUMNS, DseViewHiddenColumnCatalog.HIDDEN_COLUMNS));
    }

    /**
     * The column filter {@code command} is serialized with at {@code version}: its own, with the hidden column of
     * {@link #hiddenColumn} fetched and queried when there is one. The data response of a DSE replica is read with the
     * same filter.
     */
    public static ColumnFilter columnFilterFor(ReadCommand command, int version)
    {
        ColumnMetadata hidden = hiddenColumn(command.metadata(), version);
        return hidden == null ? command.columnFilter() : command.columnFilter().withDseViewHiddenColumn(hidden);
    }

    /**
     * The column filter the data of a response to {@code command} is serialized with: the one a DSE 6 coordinator sent,
     * naming the hidden column of the view ({@link ColumnFilter#dseColumnFilter()}), or the command's own.
     */
    public static ColumnFilter responseColumnFilter(ReadCommand command)
    {
        ColumnFilter dseFilter = command.columnFilter().dseColumnFilter();
        return dseFilter == null ? command.columnFilter() : dseFilter;
    }

    /**
     * The hidden column the data of a response to {@code command} carries, when a DSE 6 coordinator sent it with a column
     * filter fetching that column; {@code null} otherwise. DSE fetches the hidden column as a regular column of the view,
     * so its filters of such a view are expected to fetch it; one that did not would get a data response without hidden
     * cells while {@link #digestHiddenColumn} still digests them, which costs a data read (a digest mismatch), not a
     * wrong row.
     */
    public static ColumnMetadata responseHiddenColumn(ReadCommand command)
    {
        ColumnFilter dseFilter = command.columnFilter().dseColumnFilter();
        if (dseFilter == null)
            return null;
        ColumnMetadata hidden = hiddenColumn(command.metadata(), DseCompat.DSE_DIALECT);
        return hidden != null && dseFilter.fetchedColumns().regulars.contains(hidden) ? hidden : null;
    }

    /**
     * The hidden column a digest of the data of {@code command} includes, following DSE: the one of {@link #hiddenColumn}
     * when the command's digest version is DSE's, 11. The coordinator sets that version as soon as one of the replicas of
     * the read is a DSE 6 node (it takes the lowest version of the replicas), and a DSE coordinator always sends it, so
     * every replica of such a read, whichever its version, digests in DSE's encoding. A command whose digest version was
     * never set (0) digests as before.
     */
    public static ColumnMetadata digestHiddenColumn(ReadCommand command)
    {
        int version = command.digestVersion();
        return version == DseCompat.DSE_DIALECT ? hiddenColumn(command.metadata(), version) : null;
    }

    /**
     * {@code data}, in the encoding of this software, in DSE's encoding: see {@link #withHiddenCells(UnfilteredRowIterator, ColumnMetadata)}.
     */
    public static UnfilteredPartitionIterator withHiddenCells(UnfilteredPartitionIterator data, ColumnMetadata hidden)
    {
        return Transformation.apply(data, new Transformation<UnfilteredRowIterator>()
        {
            @Override
            protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
            {
                return withHiddenCells(partition, hidden);
            }
        });
    }

    /**
     * {@code partition}, in the encoding of this software, in DSE's encoding: its columns include the hidden column and
     * every row gets the hidden cell of {@link #hiddenCellOf}.
     */
    public static UnfilteredRowIterator withHiddenCells(UnfilteredRowIterator partition, ColumnMetadata hidden)
    {
        return Transformation.apply(partition, new Transformation<UnfilteredRowIterator>()
        {
            @Override
            protected RegularAndStaticColumns applyToPartitionColumns(RegularAndStaticColumns columns)
            {
                return columns.mergeTo(RegularAndStaticColumns.of(hidden));
            }

            @Override
            protected Row applyToRow(Row row)
            {
                return withHiddenCell(row, hidden);
            }
        });
    }

    /**
     * The row DSE holds for {@code row}: the same row with the hidden cell of {@link #hiddenCellOf}, the expired liveness
     * this software writes for a view row whose base cell was deleted or changed replaced by a live one at the same
     * timestamp (DSE has no expired liveness: it keeps the liveness of the base row, which for a base row written by
     * {@code INSERT} is the one of its base cell, next to the hidden tombstone; either way DSE decides by the hidden cell).
     * A row without primary key liveness is returned as it is.
     */
    static Row withHiddenCell(Row row, ColumnMetadata hidden)
    {
        LivenessInfo liveness = row.primaryKeyLivenessInfo();
        if (liveness.isEmpty())
            return row;

        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(row.clustering());
        builder.addPrimaryKeyLivenessInfo(liveness.ttl() == LivenessInfo.EXPIRED_LIVENESS_TTL
                                          ? LivenessInfo.withExpirationTime(liveness.timestamp(), LivenessInfo.NO_TTL, LivenessInfo.NO_EXPIRATION_TIME)
                                          : liveness);
        builder.addRowDeletion(row.deletion());
        Cell<?> hiddenCell = hiddenCellOf(liveness, hidden);
        boolean hiddenAdded = false;
        for (ColumnData data : row)
        {
            // the columns of a row come in their order, the hidden cell goes in its place among them
            if (!hiddenAdded && hidden.compareTo(data.column()) < 0)
            {
                builder.addCell(hiddenCell);
                hiddenAdded = true;
            }
            if (data.column().isSimple())
            {
                builder.addCell((Cell<?>) data);
            }
            else
            {
                ComplexColumnData complex = (ComplexColumnData) data;
                if (!complex.complexDeletion().isLive())
                    builder.addComplexDeletion(complex.column(), complex.complexDeletion());
                for (Cell<?> cell : complex)
                    builder.addCell(cell);
            }
        }
        if (!hiddenAdded)
            builder.addCell(hiddenCell);
        return builder.build();
    }

    /**
     * The hidden cell DSE writes for a view row whose primary key liveness is {@code liveness} in this software (see
     * {@link DseViewHiddenColumns}): a live cell for a live liveness, an expiring one for an expiring liveness, and a
     * tombstone at the same timestamp and local deletion time for the expired liveness written when the base cell is
     * deleted or changes value. The inverse of {@link DseViewHiddenColumns#livenessOf}. The value is empty.
     */
    public static Cell<?> hiddenCellOf(LivenessInfo liveness, ColumnMetadata hidden)
    {
        assert !liveness.isEmpty();
        if (liveness.ttl() == LivenessInfo.EXPIRED_LIVENESS_TTL)
            return BufferCell.tombstone(hidden, liveness.timestamp(), liveness.localExpirationTime());
        if (liveness.isExpiring())
            return new BufferCell(hidden, liveness.timestamp(), liveness.ttl(), liveness.localExpirationTime(), ByteBufferUtil.EMPTY_BYTE_BUFFER, null);
        return BufferCell.live(hidden, liveness.timestamp(), ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    /**
     * Feeds {@code digest} with the digest DSE computes for {@code data} at version 11 as DSE 6's digest code has it: the
     * one of {@link org.apache.cassandra.db.partitions.UnfilteredPartitionIterators#digest}, except that the cells of the
     * hidden column {@code hidden} also contribute their local deletion time. A reading of DSE's message layouts, with
     * less confidence, finds DSE's digest at that version to be the one of Apache Cassandra 3.0.14 with no DSE-specific
     * part; neither is checked against the digests of real DSE nodes. Either way a wrong digest only costs a data read
     * (a digest mismatch), never a wrong row.
     * {@code data} must be in DSE's encoding ({@link #withHiddenCells(UnfilteredPartitionIterator, ColumnMetadata)}, or
     * read from a DSE peer without translation).
     */
    public static void digest(UnfilteredPartitionIterator data, Digest digest, ColumnMetadata hidden)
    {
        while (data.hasNext())
        {
            try (UnfilteredRowIterator partition = data.next())
            {
                digest(partition, digest, hidden);
            }
        }
    }

    /**
     * {@link org.apache.cassandra.db.rows.UnfilteredRowIterators#digest}, with the rows digested by {@link #digest(Row, Digest, ColumnMetadata)}.
     */
    static void digest(UnfilteredRowIterator partition, Digest digest, ColumnMetadata hidden)
    {
        digest.update(partition.partitionKey().getKey());
        partition.partitionLevelDeletion().digest(digest);
        partition.columns().regulars.digest(digest);
        if (partition.staticRow() != Rows.EMPTY_STATIC_ROW)
            partition.columns().statics.digest(digest);
        digest.updateWithBoolean(partition.isReverseOrder());
        digest(partition.staticRow(), digest, hidden);

        while (partition.hasNext())
        {
            Unfiltered unfiltered = partition.next();
            if (unfiltered.isRow())
                digest((Row) unfiltered, digest, hidden);
            else
                unfiltered.digest(digest);
        }
    }

    /**
     * {@link org.apache.cassandra.db.rows.AbstractRow#digest}, with the cell of the hidden column digested as DSE digests
     * the cells of hidden columns: after its TTL comes its local deletion time, as a 32-bit integer, {@code Integer.MAX_VALUE}
     * for a live cell (the "no deletion time" of the 32-bit encoding, in which the deletion times of the pre-4.0 dialect
     * always fit). The cells of the other columns, which are neither counters nor hidden, are digested as by this software.
     */
    static void digest(Row row, Digest digest, ColumnMetadata hidden)
    {
        digest.updateWithByte(row.kind().ordinal());
        row.clustering().digest(digest);
        row.deletion().digest(digest);
        row.primaryKeyLivenessInfo().digest(digest);
        for (ColumnData data : row)
        {
            if (data.column().equals(hidden))
                digestHiddenCell((Cell<?>) data, digest);
            else
                data.digest(digest);
        }
    }

    private static <V> void digestHiddenCell(Cell<V> cell, Digest digest)
    {
        digest.update(cell.value(), cell.accessor());
        digest.updateWithLong(cell.timestamp())
              .updateWithInt(cell.ttl())
              .updateWithInt(cell.localDeletionTime() == Cell.NO_DELETION_TIME ? Integer.MAX_VALUE : CellData.deletionTimeLongToUnsignedInteger(cell.localDeletionTime()))
              .updateWithBoolean(false);
    }

    /**
     * Feeds {@code digest} with the digest of the data response {@code data} of a DSE replica to {@code command}, serialized
     * at {@code version}: the rows are read without translating their hidden cells, as the DSE replica holds them, and
     * digested by {@link #digest(UnfilteredPartitionIterator, Digest, ColumnMetadata)}, which gives the digest that replica
     * computes for the same rows.
     */
    public static void digestDseData(ByteBuffer data, int version, ReadCommand command, ColumnMetadata hidden, Digest digest) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(data, true);
             UnfilteredPartitionIterator partitions = UnfilteredPartitionIterators.serializerForIntraNode().deserialize(in, version, command.metadata(),
                                                                                                                       columnFilterFor(command, version),
                                                                                                                       DeserializationHelper.Flag.FROM_REMOTE,
                                                                                                                       true))
        {
            digest(partitions, digest, hidden);
        }
    }
}
