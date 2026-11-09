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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * The hidden columns DataStax Enterprise 6.7+ keeps in the sstables of its materialized views.
 * <p>
 * A DSE view created in the "new" format stores, next to the columns the view shows, columns named
 * {@code .<base column>} that DSE lists in {@code system_schema.hidden_columns}, not in
 * {@code system_schema.columns}: this software does not load them, so they reach it only as names in the
 * serialization header of a view sstable written by DSE. Their cells have an empty value and carry the
 * timestamp, TTL and deletion of a base cell. There are three kinds:
 * <ul>
 *     <li>a regular base column that is part of the view primary key ({@code .v} of
 *     {@code PRIMARY KEY (v, pk, ck)} over a base table keyed by {@code (pk, ck)}). DSE marks it
 *     {@code required_for_liveness}: a view row is live if and only if its {@code .v} cell is present and
 *     live, whatever the row's primary key liveness. DSE copies the base row's liveness there (which may be
 *     empty), writes the {@code .v} cell in every update of a live view entry (see below), and turns it into a
 *     tombstone at the timestamp of the old base cell when the base cell is deleted or changes value;</li>
 *     <li>a base column the view does not select but filters on (also {@code required_for_liveness});</li>
 *     <li>a base column the view does not select, in a view without required columns: a "virtual cell"
 *     keeping the view row alive while only unselected base columns are live.</li>
 * </ul>
 * This software encodes the first kind in the view row's primary key liveness instead: {@link ViewUpdateGenerator}
 * gives a view entry the liveness of the base cell (timestamp, TTL, expiration) and, when the base cell is deleted
 * or changes value, an expired liveness at the timestamp of the old base cell, which is exactly what DSE writes in
 * the {@code .v} cell. So a DSE row reads correctly here once its {@code .v} cell becomes its primary key liveness:
 * {@link #livenessOf(Cell)}, applied by {@link org.apache.cassandra.db.rows.UnfilteredSerializer} to every row of
 * the sstable before the row meets any other source. A row without the cell is dead for DSE, whatever its primary
 * key liveness and its other cells, unless the merge with another source gives it a live hidden cell: DSE writes
 * such a row for an update that deletes the base row (a row deletion, possibly with the liveness of a base row
 * inserted again without the view key column), and its compaction may leave one with live cells once it purges the
 * hidden tombstone of an old view entry. As DSE writes the hidden cell in every update of a live view entry (see
 * below), no such row holds the newer cells of a live entry whose hidden cell is in another sstable. Such a row
 * gets {@link #LIVENESS_WITHOUT_HIDDEN_CELL}, an expired primary key liveness older than any write: it
 * supersedes no liveness of the row in another source, so the merged row is live exactly when one of its sources
 * has a live hidden cell, as for DSE, and otherwise the strict liveness of the view hides it. Its cells are kept
 * until a compaction purges that liveness (at once: its timestamp and local expiration time are older than any
 * purge point), then the strict liveness drops the row with its cells, as this software does with its own expired
 * view entries; cleanup and anticompaction, which compact one sstable at a time, purge it too. The cell itself is
 * dropped, and so is the column from the header of every sstable this software writes
 * ({@link SerializationHeader#withoutDseViewLivenessColumn()}, applied by {@link SerializationHeader#make} and by the
 * sstable writer builder): only sstables written by DSE name it, so the rule above never reaches a row this
 * software wrote. Entire-sstable streaming is the one exception, and a consistent one: it copies a DSE sstable byte
 * for byte, header and hidden cells together, and the copy is read by the same rule. The expired liveness of a
 * converted hidden tombstone shadows nothing when compaction looks for overlapping tombstones, like the expired
 * liveness this software writes itself: it only purges less, it never resurrects. A partial stream and scrub, which
 * write the rows they read as they are, write {@link #LIVENESS_WITHOUT_HIDDEN_CELL} too, and the sstable they write
 * has the smallest minimum timestamp there is until it is compacted, which purges the liveness. Until then,
 * compactions that overlap it purge no tombstone, none of the view's expired sstables is dropped whole, the
 * timestamps of a compaction that takes it as input are encoded in more bytes, and an incremental repair that
 * validates it without the sstable of the row's live hidden cell sees a row the other replicas may have purged: it
 * purges less, and may stream a dead row again, which reads as dead.
 * <p>
 * Three facts about DSE the rule relies on, which the sstables DSE 6.8.65 and 6.9.26 wrote for a view over a base
 * table with other regular columns show ({@code test/data/dse-compat/sstables/dse-6.8.65-mv-updates} and
 * {@code test/data/dse-compat/sstables/dse-6.9.26-mv-updates}: inserts, partial updates, deletions of cells, rows,
 * ranges and partitions, inserts without the view key column, TTLs, changes of the view key column, and a base row
 * deleted and inserted again without the view key column in one update, one flush each):
 * <ul>
 *     <li>DSE writes the hidden cell in every update of a live view entry, whichever base columns the update
 *     writes or deletes; the only view rows without it carry a row deletion. Otherwise a compaction without the
 *     sstable of the entry's live hidden cell would drop newer cells DSE shows;</li>
 *     <li>DSE writes the cells of a row in the order of the columns of this software: simple columns before complex
 *     ones, each kind by name, the hidden column among the simple ones by its name (after a column named
 *     {@code "-x"}, before {@code a}). Otherwise the cells of an sstable naming other regular columns besides the
 *     hidden one would be misread;</li>
 *     <li>the header of a DSE view sstable names every regular column of the view, the hidden one included,
 *     whatever its rows hold (even in an sstable holding only row deletions, without a cell). So every row DSE
 *     wrote without the hidden cell is read by the rule above.</li>
 * </ul>
 * The rule relies on every reader of these sstables deserializing their rows: compaction by cursors, which reads
 * cells and liveness directly, is never used for a view whose primary key holds a regular base column, a table
 * with strict liveness ({@code CompactionTask} compacts those by iterators).
 * <p>
 * The other two kinds have no counterpart in this software (the liveness of several cells cannot be folded
 * into one primary key liveness), so an sstable carrying them is refused with an error naming the remedy,
 * as is a view with more than one regular base column in its primary key. This applies in every
 * compatibility mode: DSE view sstables stay on disk until compaction rewrites them, and in
 * {@code internode_compatibility_mode: NONE} such a header was refused before this class existed, so no
 * sstable this software could read before is read differently.
 * <p>
 * DSE lists the hidden columns of each view in its {@code system_schema.hidden_columns}, which
 * {@link DseViewHiddenColumnCatalog} reads.
 * <p>
 * Limits: view rows this software writes to disk (its own view updates, and every sstable compaction or scrub
 * rewrites) carry no hidden cell, so DSE considers them dead: after a rollback to DSE, the views of the rolled-back
 * node must be rebuilt from their base tables. DSE's repair Merkle trees include the hidden cells, and
 * {@code sstableloader} builds its table metadata without the view kind, so it still refuses a DSE view sstable;
 * this class handles neither. A DSE view sstable whose header did not name the column, which none of the fixtures
 * has, would be read as any sstable: a row DSE wrote there with a primary key liveness and no hidden cell (the
 * liveness of a base row inserted again without the view key column, newer than the row deletion) would read as
 * live, where DSE does not show it.
 */
public final class DseViewHiddenColumns
{
    private static final Logger logger = LoggerFactory.getLogger(DseViewHiddenColumns.class);

    /** The first character of every DSE hidden column name. */
    public static final String HIDDEN_COLUMN_PREFIX = ".";

    /**
     * The primary key liveness of a row of a DSE view sstable without the hidden cell, which DSE considers dead: expired,
     * at the smallest timestamp there is and with the local deletion time of {@link EncodingStats#NO_STATS}, the same on
     * every replica and older than any purge point.
     */
    public static final LivenessInfo LIVENESS_WITHOUT_HIDDEN_CELL = LivenessInfo.withExpirationTime(LivenessInfo.NO_TIMESTAMP + 1,
                                                                                                      LivenessInfo.EXPIRED_LIVENESS_TTL,
                                                                                                      EncodingStats.NO_STATS.minLocalDeletionTime);

    public static final String REMEDY = "Drop the view on DSE before the upgrade and create it again once no node runs DSE; " +
                                        "a node already running this software must first be rolled back to DSE";

    private static final Set<TableId> loggedViews = ConcurrentHashMap.newKeySet();

    private DseViewHiddenColumns()
    {
    }

    /**
     * Resolves a column named in an sstable serialization header that is neither in the table's schema nor
     * a dropped column.
     *
     * @param source the sstable or stream the header comes from, for the messages
     * @return the metadata to deserialize the cells of a DSE hidden column of the first kind with, or
     * {@code null} when {@code name} is not a DSE hidden column (the table is not a view, the column is static,
     * or the name does not start with {@value #HIDDEN_COLUMN_PREFIX}); the caller then fails as before
     * @throws UnknownColumnException for a DSE hidden column this software cannot give the right meaning to
     */
    public static ColumnMetadata resolve(String source, TableMetadata metadata, ByteBuffer name, AbstractType<?> type, boolean isStatic)
    throws UnknownColumnException
    {
        if (!metadata.isView() || isStatic)
            return null;

        String columnName;
        try
        {
            columnName = ByteBufferUtil.string(name);
        }
        catch (CharacterCodingException e)
        {
            return null;
        }
        if (columnName.length() < 2 || !columnName.startsWith(HIDDEN_COLUMN_PREFIX))
            return null;

        String baseColumnName = columnName.substring(HIDDEN_COLUMN_PREFIX.length());
        ColumnMetadata viewColumn = metadata.getColumn(ByteBufferUtil.bytes(baseColumnName));
        if (viewColumn == null || !viewColumn.isPrimaryKeyColumn() || !isRegularBaseColumn(metadata, viewColumn))
            throw new UnknownColumnException(String.format("The sstable header of %s, for materialized view %s, holds the DSE hidden column %s for the base column %s, " +
                                                           "which is not a regular base column in the view primary key; this software cannot " +
                                                           "reproduce the liveness DSE gives such columns and refuses the sstable rather than " +
                                                           "return wrong view rows. %s",
                                                           source, metadata, columnName, baseColumnName, REMEDY));
        if (!type.unwrap().equals(viewColumn.type.unwrap()))
            throw new UnknownColumnException(String.format("The sstable header of %s, for materialized view %s, holds the DSE hidden column %s of type %s, but the view " +
                                                           "column %s has type %s: the sstable does not belong to this view",
                                                           source, metadata, columnName, type.asCQL3Type(), baseColumnName, viewColumn.type.unwrap().asCQL3Type()));

        if (loggedViews.add(metadata.id))
            logger.info("Materialized view {} has sstables written by DSE with the hidden column {} (first seen in {}): " +
                        "its cells give those view rows their liveness, as DSE reads them", metadata, columnName, source);
        return ColumnMetadata.regularColumn(metadata, name, type);
    }

    /**
     * The DSE hidden column of the regular base column in the primary key of the view {@code view} (the first kind
     * above), as DSE names it when it creates the view: {@code .<column>}, regular, of the type of the view column without
     * its clustering order (DSE lists the base type, {@code text} in the DSE 6.8.65 fixture). It is derived from the view
     * definition; whether DSE keeps it for the view (not for a view of DSE's old format, which DSE's table lists nothing
     * for, as the test fixtures show) is for {@link DseViewHiddenColumnCatalog} to say.
     *
     * @return the column, or {@code null} when {@code view} is not a materialized view in the schema, its base table
     * is not in the schema, or its primary key holds no regular base column or more than one (DSE's hidden columns of
     * such a view have no counterpart here)
     */
    public static ColumnMetadata requiredHiddenColumn(TableMetadata view)
    {
        if (!view.isView())
            return null;
        ViewMetadata viewMetadata = Schema.instance.getView(view.keyspace, view.name);
        if (viewMetadata == null)
            return null;
        TableMetadata baseTable = viewMetadata.baseTableMetadata();
        if (baseTable == null)
            return null;
        return requiredHiddenColumn(view, baseTable);
    }

    /**
     * {@link #requiredHiddenColumn(TableMetadata)} for a view and its base table given, which need not be in the schema.
     *
     * @return the column, or {@code null} when {@code view} is not a materialized view, or its primary key holds no
     * regular column of {@code baseTable} or more than one
     */
    public static ColumnMetadata requiredHiddenColumn(TableMetadata view, TableMetadata baseTable)
    {
        if (!view.isView())
            return null;
        ColumnMetadata regularBaseColumn = null;
        for (ColumnMetadata viewColumn : view.primaryKeyColumns())
        {
            ColumnMetadata baseColumn = baseTable.getColumn(viewColumn.name);
            if (baseColumn == null || !baseColumn.isRegular())
                continue;
            if (regularBaseColumn != null)
                return null;
            regularBaseColumn = viewColumn;
        }
        if (regularBaseColumn == null)
            return null;
        return ColumnMetadata.regularColumn(view, ByteBufferUtil.bytes(HIDDEN_COLUMN_PREFIX + regularBaseColumn.name), regularBaseColumn.type.unwrap());
    }

    /**
     * Whether the view primary key column {@code viewColumn} comes from a regular column of the base table. When the
     * view is not in the schema (offline tools), DSE's own rule decides: it creates a hidden column for a view
     * primary key column only when that column is regular in the base table. The same holds while the base table is
     * not in the schema. Offline, the hidden column of a view primary key column that is a primary key column of the
     * base table, which DSE never writes, is therefore read as the one of a regular base column.
     */
    private static boolean isRegularBaseColumn(TableMetadata view, ColumnMetadata viewColumn)
    {
        ViewMetadata viewMetadata = Schema.instance.getView(view.keyspace, view.name);
        if (viewMetadata == null)
            return true;
        TableMetadata baseTable = viewMetadata.baseTableMetadata();
        if (baseTable == null)
            return true;
        ColumnMetadata baseColumn = baseTable.getColumn(viewColumn.name);
        return baseColumn != null && baseColumn.isRegular();
    }

    /**
     * The error for a DSE view sstable whose header names more than one hidden column of the first kind, that
     * is a DSE view with several regular base columns in its primary key.
     */
    public static UnknownColumnException multipleRequiredColumns(String source, TableMetadata metadata, ColumnMetadata first, ColumnMetadata second)
    {
        return new UnknownColumnException(String.format("The sstable header of %s, for materialized view %s, holds the DSE hidden columns %s and %s: the view has more than " +
                                                        "one regular base column in its primary key, which this software does not support. %s",
                                                        source, metadata, first.name, second.name, REMEDY));
    }

    /**
     * The primary key liveness this software gives a view row whose DSE hidden cell (of a regular base column
     * in the view primary key) is {@code hiddenCell}: the cell's own liveness when it is live or expiring, an
     * expired liveness at its timestamp when it is a tombstone. That is what {@link ViewUpdateGenerator}
     * writes for the same base cell, and it is live exactly when DSE considers the row live.
     */
    public static LivenessInfo livenessOf(Cell<?> hiddenCell)
    {
        if (hiddenCell.isTombstone())
            return LivenessInfo.withExpirationTime(hiddenCell.timestamp(), LivenessInfo.EXPIRED_LIVENESS_TTL, hiddenCell.localDeletionTime());
        return LivenessInfo.withExpirationTime(hiddenCell.timestamp(), hiddenCell.ttl(), hiddenCell.localDeletionTime());
    }
}
