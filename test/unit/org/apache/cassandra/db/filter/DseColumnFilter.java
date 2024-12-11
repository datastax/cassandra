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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Represents which (non-PK) columns (and optionally which sub-part of a column for complex columns) are selected
 * by a query.
 * <p>
 * We distinguish 2 sets of columns in practice: the _fetched_ columns, which are the columns that we (may, see
 * below) need to fetch internally, and the _queried_ columns, which are the columns that the user has selected
 * in its request.
 * <p>
 * The reason for distinguishing those 2 sets is that due to the CQL semantic (see #6588 for more details), we
 * often need to internally fetch all regular columns for the queried table, but can still do some optimizations for
 * those columns that are not directly queried by the user (see #10657 for more details).
 * <p>
 * Note that in practice:
 * - the _queried_ columns set is always included in the _fetched_ one.
 * - whenever those sets are different, we know 1) the _fetched_ set contains all regular columns for the table
 * unless the schema has changed, see {@link FetchType}, and 2)
 * _fetched_ == _queried_ for static columns, so we don't have to record this set, we just keep a pointer to the
 * table metadata. The only set we concretely store is thus the _queried_ one.
 * - in the special case of a {@code SELECT *} query, we want to query all columns, and _fetched_ == _queried.
 * As this is a common case, we special case it by keeping the _queried_ set {@code null} (and we retrieve
 * the columns through the metadata pointer).
 * <p>
 * For complex columns, this class optionally allows to specify a subset of the cells to query for each column.
 * We can either select individual cells by path name, or a slice of them. Note that this is a sub-selection of
 * _queried_ cells, so if _fetched_ != _queried_, then the cell selected by this sub-selection are considered
 * queried and the other ones are considered fetched (and if a column has some sub-selection, it must be a queried
 * column, which is actually enforced by the Builder below).
 */
public class DseColumnFilter
{
    public static volatile CassandraVersion cassandraVersion = new CassandraVersion("3.0.0");

    public static final DseColumnFilter NONE = selection(RegularAndStaticColumns.NONE);

    private static final Logger logger = LoggerFactory.getLogger(DseColumnFilter.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    public static final Serializer serializer = new Serializer(MessagingService.VERSION_30);

    public static DseColumnFilter maybeApply(DseColumnFilter selection)
    {
        if (CassandraVersion.CASSANDRA_3_4.compareTo(cassandraVersion) > 0)
            return selection.fetchType != FetchType.COLUMNS_IN_QUERIED ? new DseColumnFilter(selection.fetchType, selection.fetched, null, selection.subSelections) : selection;
        else
            return selection;
    }

    /**
     * This enum describes which columns the column filter is fetching. It is conceptually equivalent to a boolean
     * that indicates if the filter is fetching all the table columns or just the columns selected by the user. This
     * boolean used to be called {@code fetchAllRegulars} before this enum was introduced. The reason for adding an
     * additional case is that even if the filter is asked to fetch all the table columns, if there is a schema
     * mismatch between the schema used to create the filter {@link #fetched} columns and the iterator's schema, it may
     * be that the filter needs to exclude columns that are pased by the iterator but that the filter does not know about.
     * <p>
     * This could be better achieved if we stared a schema id in the filter and we made the iterators pass in their
     * schema id, but such id mechanism doesn't exist at present.
     */
    enum FetchType
    {
        /**
         * Fetch all columns without ever checking {@link #fetched}. In this case {@link #fetched} will include
         * all regular columns (and any static in {@link #queried}). We can assume that this is true only when we know
         * that the schema used to create the column filter and the iterator (memtable or sstable) match, otherwise we
         * must use {@link #COLUMNS_IN_FETCHED}.
         */
        ALL_COLUMNS,

        /**
         * This is conceptually the same as {@link #ALL_COLUMNS} except that, due to a schema mismatch, the iterator
         * may pass a column that is not part of {@link #fetched}. In this case, this column should not be fetched.
         * The filter must therefore check that all columns it receives are contained in {@link #fetched}.
         */
        COLUMNS_IN_FETCHED,

        /**
         * Fetch only the columns in {@link #queried}, in this case {@link #fetched} will be equal to {@link #queried}.
         */
        COLUMNS_IN_QUERIED


        // TODO - eventually we could consider adding one more type to indicate a "SELECT *" so that we don't need to
        // look at queried == null
    }

    final FetchType fetchType;
    final RegularAndStaticColumns fetched;
    final RegularAndStaticColumns queried; // can be null if fetchType.fetchesAll(), to represent a wildcard query (all
    // static and regular columns are both _fetched_ and _queried_).
    private final SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections; // can be null

    private DseColumnFilter(FetchType fetchType,
                            TableMetadata metadata,
                            RegularAndStaticColumns queried,
                            SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections)
    {
        queried = includeColumnsRequiredForLiveness(metadata, queried);

        if (fetchType != FetchType.COLUMNS_IN_QUERIED)
        {
            assert metadata != null : "Metadata is required if fetch type is not a selection";
            RegularAndStaticColumns all = metadata.regularAndStaticColumns();

            this.fetched = (all.statics.isEmpty() || queried == null)
                           ? all
                           : new RegularAndStaticColumns(queried.statics, all.regulars);
        }
        else
        {
            assert queried != null : "Queried columns are required if fetch type is a selection";
            this.fetched = queried;
        }

        this.fetchType = fetchType;
        this.queried = queried;
        this.subSelections = subSelections;
    }

    private DseColumnFilter(FetchType fetchType,
                            RegularAndStaticColumns fetched,
                            RegularAndStaticColumns queried,
                            SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections)
    {
        this.fetchType = fetchType;
        this.fetched = fetched;
        this.queried = queried;
        this.subSelections = subSelections;

        switch (fetchType)
        {
            case ALL_COLUMNS:
            case COLUMNS_IN_FETCHED:
                assert fetched != null : "When fetching all columns, the _fetched_ set is required";
                break;
            case COLUMNS_IN_QUERIED:
                assert queried != null : "When fetching a COLUMNS_IN_QUERIED, the _queried_ set is required";
                assert queried == fetched : "When fetching a COLUMNS_IN_QUERIED both _fetched_ and _queried_ must point to the same set of columns";
                break;
        }
    }

    private RegularAndStaticColumns includeColumnsRequiredForLiveness(TableMetadata metadata, RegularAndStaticColumns queriedColumns)
    {
        // Hidden and required columns for liveness should always be queried (ie. cannot be skipped)
        if (metadata != null && queriedColumns != null && !queriedColumns.isEmpty())
        {
            Collection<ColumnMetadata> required = List.of(); // TODO changed, there is method for this in CC
            if (!required.isEmpty())
                queriedColumns = queriedColumns.mergeTo(RegularAndStaticColumns.builder().addAll(required).build());
        }
        return queriedColumns;
    }

    /**
     * A filter that includes all columns for the provided table.
     */
    public static DseColumnFilter all(TableMetadata metadata)
    {
        return maybeApply(new DseColumnFilter(FetchType.ALL_COLUMNS, metadata, null, null));
    }

    /**
     * A filter that only fetches/queries the provided columns.
     * <p>
     * Note that this shouldn't be used for CQL queries in general as all columns should be queried to
     * preserve CQL semantic (see class javadoc). This is ok for some internal queries however (and
     * for #6588 if/when we implement it).
     */
    public static DseColumnFilter selection(RegularAndStaticColumns columns)
    {
        return maybeApply(new DseColumnFilter(FetchType.COLUMNS_IN_QUERIED, (TableMetadata) null, columns, null));
    }

    /**
     * A filter that fetches all columns for the provided table, but returns
     * only the queried ones.
     */
    public static DseColumnFilter selection(TableMetadata metadata, RegularAndStaticColumns queried)
    {
        return maybeApply(new DseColumnFilter(FetchType.ALL_COLUMNS, metadata, queried, null));
    }

    /**
     * Return this column filter if all the partition columns are included in the fetched columns,
     * or a de-optimised column filter with {@code fetchType} set to {@link FetchType#COLUMNS_IN_FETCHED}.
     * <p>
     * This is required because occasionally we may receive column filters that are stale, for example
     * if a coordinator has sent a request bound to an older schema version, or if a prepared statement
     * hasn't been updated.
     *
     * @param partitionColumns - the columns of the partition that will use this filter
     * @return this filter or a de-optimised copy
     */
    public DseColumnFilter withPartitionColumnsVerified(RegularAndStaticColumns partitionColumns)
    {
        if (fetchType == FetchType.ALL_COLUMNS && !fetched.includes(partitionColumns))
        {
            noSpamLogger.info("Columns mismatch: `{}` does not include `{}`, falling back to the original set of columns.", fetched, partitionColumns);

            // if fetched doesn't contain all the columns that we may be asked to filter, then we cannot
            // optimize based on fetchType == ALL but we need to fall back to checking if the column
            // is a part of the fetched set
            return maybeApply(new DseColumnFilter(FetchType.COLUMNS_IN_FETCHED, fetched, queried, subSelections));
        }
        else
        {
            // optimize the most common case, which is that all columns are included
            return this;
        }
    }

    /**
     * The columns that needs to be fetched internally for this filter.
     *
     * @return the columns to fetch for this filter.
     */
    public RegularAndStaticColumns fetchedColumns()
    {
        return fetched;
    }

    /**
     * The columns actually queried by the user.
     * <p>
     * Note that this is in general not all the columns that are fetched internally (see {@link #fetchedColumns}).
     */
    public RegularAndStaticColumns queriedColumns()
    {
        return queried == null ? fetched : queried;
    }

    /**
     * Whether all the (regular or static) columns are fetched by this filter.
     * <p>
     * Note that this method is meant as an optimization but a negative return
     * shouldn't be relied upon strongly: this can return {@code false} but
     * still have all the columns fetches if those were manually selected by the
     * user. The goal here is to cheaply avoid filtering things on wildcard
     * queries, as those are common.
     *
     * @param isStatic whether to check for static columns or not. If {@code true},
     *                 the method returns if all static columns are fetched, otherwise it checks
     *                 regular columns.
     */
    public boolean fetchesAllColumns(boolean isStatic)
    {
        // this method is used by AbstractRow.filter() as an optimization to avoid checking if columns are contained in
        // _fetched_ or _queried_ so we cannot return true if fetchType != ALL_COLUMNS
        switch (fetchType)
        {
            case ALL_COLUMNS:
                return isStatic ? queried == null : true;
            case COLUMNS_IN_FETCHED:
            case COLUMNS_IN_QUERIED:
                return false;
            default:
                throw new IllegalStateException("Unrecognized fetch type: " + fetchType);
        }
    }

    /**
     * Whether _fetched_ == _queried_ for this filter, and so if the {@code isQueried()} methods
     * can return {@code false} for some column/cell.
     */
    public boolean allFetchedColumnsAreQueried()
    {
        // if fetchType is COLUMNS_IN_QUERIED then we know that only queried columns were fetched, otherwise
        // this could be true if queried == null
        return fetchType == FetchType.COLUMNS_IN_QUERIED || queried == null;
    }

    /**
     * Whether the provided column is fetched by this filter.
     */
    public boolean fetches(ColumnMetadata column)
    {
        switch (fetchType)
        {
            // For statics, it is included only if it's part of _queried_, or if _queried_ is null (wildcard query).
            // For regulars, depending on the fetch type we check either the fetched or queried set, or we assume that the column is fetched
            case ALL_COLUMNS:
                if (column.isStatic())
                    return queried == null || queried.contains(column);
                else
                    return true;
            case COLUMNS_IN_FETCHED:
                return fetched.contains(column);
            case COLUMNS_IN_QUERIED:
                return queried.contains(column);
            default:
                throw new IllegalStateException("Unrecognized fetch type: " + fetchType);
        }
    }

    /**
     * Whether the provided column, which is assumed to be _fetched_ by this filter (so the caller must guarantee
     * that {@code fetches(column) == true}, is also _queried_ by the user.
     * <p>
     * !WARNING! please be sure to understand the difference between _fetched_ and _queried_
     * columns that this class made before using this method. If unsure, you probably want
     * to use the {@link #fetches} method.
     */
    public boolean fetchedColumnIsQueried(ColumnMetadata column)
    {
        // if fetchType is COLUMNS_IN_QUERIED then we know that only queried columns were fetched and so we don't need
        // to check anything else. Otherwise, if queried == null that means a SELECT * or, if it isn't null, that
        // means we need to check that the column is included in _queried_
        return fetchType == FetchType.COLUMNS_IN_QUERIED || queried == null || queried.contains(column);
    }

    /**
     * Whether the provided complex cell (identified by its column and path), which is assumed to be _fetched_ by
     * this filter, is also _queried_ by the user.
     * <p>
     * !WARNING! please be sure to understand the difference between _fetched_ and _queried_
     * columns that this class made before using this method. If unsure, you probably want
     * to use the {@link #fetches} method.
     */
    public boolean fetchedCellIsQueried(ColumnMetadata column, CellPath path)
    {
        assert path != null;
        if (fetchType == FetchType.COLUMNS_IN_QUERIED || subSelections == null)
            return true;

        SortedSet<ColumnSubselection> s = subSelections.get(column.name);
        // No subsection for this column means everything is queried
        if (s.isEmpty())
            return true;

        for (ColumnSubselection subSel : s)
            if (subSel.compareInclusionOf(path) == 0)
                return true;

        return false;
    }

    /**
     * Creates a new {@code Tester} to efficiently test the inclusion of cells of complex column
     * {@code column}.
     *
     * @param column for complex column for which to create a tester.
     * @return the created tester or {@code null} if all the cells from the provided column
     * are queried.
     */
    public Tester newTester(ColumnMetadata column)
    {
        if (subSelections == null || !column.isComplex())
            return null;

        SortedSet<ColumnSubselection> s = subSelections.get(column.name);
        if (s.isEmpty())
            return null;

        return new Tester(!column.isStatic() && fetchType != FetchType.COLUMNS_IN_QUERIED, s.iterator());
    }

    /**
     * Given an iterator on the cell of a complex column, returns an iterator that only include the cells selected by
     * this filter.
     *
     * @param column the (complex) column for which the cells are.
     * @param cells  the cells to filter.
     * @return a filtered iterator that only include the cells from {@code cells} that are included by this filter.
     */
    public Iterator<Cell> filterComplexCells(ColumnMetadata column, Iterator<Cell> cells)
    {
        Tester tester = newTester(column);
        if (tester == null)
            return cells;

        return Iterators.filter(cells, cell -> tester.fetchedCellIsQueried(cell.path()));
    }

    /**
     * Returns a {@code DseColumnFilter}} builder that fetches all regular columns (and queries the columns
     * added to the builder, or everything if no column is added).
     */
    public static Builder allRegularColumnsBuilder(TableMetadata metadata)
    {
        return new Builder(metadata);
    }

    /**
     * Returns a {@code DseColumnFilter} builder that only fetches the columns/cells added to the builder.
     */
    public static Builder selectionBuilder()
    {
        return new Builder(null);
    }

    public static class Tester
    {
        private final boolean isFetched;
        private ColumnSubselection current;
        private final Iterator<ColumnSubselection> iterator;

        private Tester(boolean isFetched, Iterator<ColumnSubselection> iterator)
        {
            this.isFetched = isFetched;
            this.iterator = iterator;
        }

        public boolean fetches(CellPath path)
        {
            return isFetched || hasSubselection(path);
        }

        /**
         * Must only be called if {@code fetches(path) == true}.
         */
        public boolean fetchedCellIsQueried(CellPath path)
        {
            return !isFetched || hasSubselection(path);
        }

        private boolean hasSubselection(CellPath path)
        {
            while (current != null || iterator.hasNext())
            {
                if (current == null)
                    current = iterator.next();

                int cmp = current.compareInclusionOf(path);
                if (cmp == 0) // The path is included
                    return true;
                else if (cmp < 0) // The path is before this sub-selection, it's not included by any
                    return false;

                // the path is after this sub-selection, we need to check the next one.
                current = null;
            }
            return false;
        }
    }

    /**
     * A builder for a {@code DseColumnFilter} object.
     * <p>
     * Note that the columns added to this build are the _queried_ column. Whether or not all columns
     * are _fetched_ depends on which constructor you've used to obtained this builder, allColumnsBuilder (all
     * columns are fetched) or selectionBuilder (only the queried columns are fetched).
     * <p>
     * Note that for a allColumnsBuilder, if no queried columns are added, this is interpreted as querying
     * all columns, not querying none (but if you know you want to query all columns, prefer
     * {@link DseColumnFilter#all(TableMetadata)}. For selectionBuilder, adding no queried columns means no column will be
     * fetched (so the builder will return {@code PartitionColumns.NONE}).
     * <p>
     * Also, if only a subselection of a complex column should be queried, then only the corresponding
     * subselection method of the builder ({@link #slice} or {@link #select}) should be called for the
     * column, but {@link #add} shouldn't. if {@link #add} is also called, the whole column will be
     * queried and the subselection(s) will be ignored. This is done for correctness of CQL where
     * if you do "SELECT m, m[2..5]", you are really querying the whole collection.
     */
    public static class Builder
    {
        private final TableMetadata metadata; // null if we don't fetch all columns
        private RegularAndStaticColumns.Builder queriedBuilder;
        private List<ColumnSubselection> subSelections;

        private Set<ColumnMetadata> fullySelectedComplexColumns;

        private Builder(TableMetadata metadata)
        {
            this.metadata = metadata;
        }

        public Builder add(ColumnMetadata c)
        {
            if (c.isComplex() && c.type.isMultiCell())
            {
                if (fullySelectedComplexColumns == null)
                    fullySelectedComplexColumns = Sets.newHashSet();
                fullySelectedComplexColumns.add(c);
            }
            return addInternal(c);
        }

        public Builder addAll(Iterable<ColumnMetadata> columns)
        {
            for (ColumnMetadata column : columns)
                add(column);
            return this;
        }

        private Builder addInternal(ColumnMetadata c)
        {
            if (c.isPrimaryKeyColumn())
                return this;

            if (queriedBuilder == null)
                queriedBuilder = RegularAndStaticColumns.builder();
            queriedBuilder.add(c);
            return this;
        }

        private Builder addSubSelection(ColumnSubselection subSelection)
        {
            ColumnMetadata column = subSelection.column();
            assert column.isComplex() && column.type.isMultiCell();
            addInternal(column);
            if (subSelections == null)
                subSelections = new ArrayList<>();
            subSelections.add(subSelection);
            return this;
        }

        public Builder slice(ColumnMetadata c, CellPath from, CellPath to)
        {
            return addSubSelection(ColumnSubselection.slice(c, from, to));
        }

        public Builder select(ColumnMetadata c, CellPath elt)
        {
            return addSubSelection(ColumnSubselection.element(c, elt));
        }

        public DseColumnFilter build()
        {
            FetchType fetchType = metadata != null ? FetchType.ALL_COLUMNS : FetchType.COLUMNS_IN_QUERIED;

            RegularAndStaticColumns queried = queriedBuilder == null ? null : queriedBuilder.build();

            // It's only ok to have queried == null in DseColumnFilter if isFetchAll. So deal with the case of a selectionBuilder
            // with nothing selected (we can at least happen on some backward compatible queries - CASSANDRA-10471).
            if (fetchType == FetchType.COLUMNS_IN_QUERIED && queried == null)
                queried = RegularAndStaticColumns.NONE;

            SortedSetMultimap<ColumnIdentifier, ColumnSubselection> s = null;
            if (subSelections != null)
            {
                s = TreeMultimap.create(Comparator.<ColumnIdentifier>naturalOrder(), Comparator.<ColumnSubselection>naturalOrder());
                for (ColumnSubselection subSelection : subSelections)
                {
                    if (fullySelectedComplexColumns == null || !fullySelectedComplexColumns.contains(subSelection.column()))
                        s.put(subSelection.column().name, subSelection);
                }
            }

            return maybeApply(new DseColumnFilter(fetchType, metadata, queried, s));
        }
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this)
            return true;

        if (!(other instanceof DseColumnFilter))
            return false;

        DseColumnFilter otherCf = (DseColumnFilter) other;

        return otherCf.fetchType == this.fetchType &&
               Objects.equals(otherCf.fetched, this.fetched) &&
               Objects.equals(otherCf.queried, this.queried) &&
               Objects.equals(otherCf.subSelections, this.subSelections);
    }

    @Override
    public String toString()
    {
        if (fetchType != FetchType.COLUMNS_IN_QUERIED && queried == null)
            return "*";

        if (queried.isEmpty())
            return "";

        Iterator<ColumnMetadata> defs = queried.selectOrderIterator();
        if (!defs.hasNext())
            return "<none>";

        StringBuilder sb = new StringBuilder();
        while (defs.hasNext())
        {
            appendColumnDef(sb, defs.next());
            if (defs.hasNext())
                sb.append(", ");
        }
        return sb.toString();
    }

    private void appendColumnDef(StringBuilder sb, ColumnMetadata column)
    {
        if (subSelections == null)
        {
            sb.append(column.name);
            return;
        }

        SortedSet<ColumnSubselection> s = subSelections.get(column.name);
        if (s.isEmpty())
        {
            sb.append(column.name);
            return;
        }

        int i = 0;
        for (ColumnSubselection subSel : s)
            sb.append(i++ == 0 ? "" : ", ").append(column.name).append(subSel);
    }

    public static class Serializer
    {
        private static final int FETCH_ALL_MASK = 0x01;
        private static final int HAS_QUERIED_MASK = 0x02;
        private static final int HAS_SUB_SELECTIONS_MASK = 0x04;

        private final ColumnSubselection.Serializer columnSubselectionSerializer;

        private final int version;

        Serializer(int version)
        {
            this.version = version;
            this.columnSubselectionSerializer = ColumnSubselection.serializer;
        }

        private static int makeHeaderByte(DseColumnFilter selection)
        {
            return (selection.fetchType != FetchType.COLUMNS_IN_QUERIED ? FETCH_ALL_MASK : 0)
                   | (selection.queried != null ? HAS_QUERIED_MASK : 0)
                   | (selection.subSelections != null ? HAS_SUB_SELECTIONS_MASK : 0);
        }

        public DseColumnFilter maybeUpdatedForBackwardCompatibility(DseColumnFilter selection)
        {
            if (version > MessagingService.VERSION_3014 || selection.fetchType == FetchType.COLUMNS_IN_QUERIED || selection.queried == null)
                return selection;

            // The meaning of fetchType changed (at least when queried != null) due to CASSANDRA-12768: in
            // pre-4.0 it means that *all* columns are fetched, not just the regular ones, and so 3.0/3.X nodes
            // would send us more than we'd like. So instead recreating a filter that correspond to what we
            // actually want (it's a tiny bit less efficient as we include all columns manually and will mark as
            // queried some columns that are actually only fetched, but it's fine during upgrade).
            // More concretely, we replace our filter by a non-fetch-all one that queries every columns that our
            // current filter fetches.
            List<ColumnMetadata> queriedStatic = new ArrayList<>();
            Iterables.addAll(queriedStatic, Iterables.filter(selection.queried, ColumnMetadata::isStatic));
            return new DseColumnFilter(FetchType.COLUMNS_IN_QUERIED,
                                       (TableMetadata) null,
                                       new RegularAndStaticColumns(Columns.from(queriedStatic), selection.fetched.regulars),
                                       selection.subSelections);
        }

        public void serialize(DseColumnFilter selection, DataOutputPlus out) throws IOException
        {
            selection = maybeUpdatedForBackwardCompatibility(selection);

            out.writeByte(makeHeaderByte(selection));

            if (version >= MessagingService.VERSION_3014 && selection.fetchType != FetchType.COLUMNS_IN_QUERIED)
            {
                Columns.serializer.serialize(selection.fetched.statics, out);
                Columns.serializer.serialize(selection.fetched.regulars, out);
            }

            if (selection.queried != null)
            {
                Columns.serializer.serialize(selection.queried.statics, out);
                Columns.serializer.serialize(selection.queried.regulars, out);
            }

            if (selection.subSelections != null)
            {
                out.writeUnsignedVInt(selection.subSelections.size());
                for (ColumnSubselection subSel : selection.subSelections.values())
                    columnSubselectionSerializer.serialize(subSel, out, version);
            }
        }

        public DseColumnFilter deserialize(DataInputPlus in, TableMetadata metadata) throws IOException
        {
            int header = in.readUnsignedByte();
            boolean isFetchAll = (header & FETCH_ALL_MASK) != 0;
            boolean hasQueried = (header & HAS_QUERIED_MASK) != 0;
            boolean hasSubSelections = (header & HAS_SUB_SELECTIONS_MASK) != 0;

            RegularAndStaticColumns fetched = null;
            RegularAndStaticColumns queried = null;

            if (isFetchAll)
            {
                if (version >= MessagingService.VERSION_3014)
                {
                    Columns statics = Columns.serializer.deserialize(in, metadata);
                    Columns regulars = Columns.serializer.deserialize(in, metadata);
                    fetched = new RegularAndStaticColumns(statics, regulars);
                }
                else
                {
                    fetched = metadata.regularAndStaticColumns();
                }
            }

            if (hasQueried)
            {
                Columns statics = Columns.serializer.deserialize(in, metadata);
                Columns regulars = Columns.serializer.deserialize(in, metadata);
                queried = new RegularAndStaticColumns(statics, regulars);
            }

            SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections = null;
            if (hasSubSelections)
            {
                subSelections = TreeMultimap.create(Comparator.<ColumnIdentifier>naturalOrder(), Comparator.<ColumnSubselection>naturalOrder());
                int size = (int) in.readUnsignedVInt();
                for (int i = 0; i < size; i++)
                {
                    ColumnSubselection subSel = columnSubselectionSerializer.deserialize(in, version, metadata);
                    subSelections.put(subSel.column().name, subSel);
                }
            }

            // Same concern than in serialize/serializedSize: we should be wary of the change in meaning for isFetchAll.
            // If we get a filter with isFetchAll from 3.0/3.x, it actually expects all static columns to be fetched,
            // make sure we do that (note that if queried == null, that's already what we do).
            // Note that here again this will make us do a bit more work that necessary, namely we'll _query_ all
            // statics even though we only care about _fetching_ them all, but that's a minor inefficiency, so fine
            // during upgrade.
            if (version == MessagingService.VERSION_30 && isFetchAll && queried != null)
                queried = new RegularAndStaticColumns(metadata.staticColumns(), queried.regulars);

            FetchType fetchType = isFetchAll ? FetchType.ALL_COLUMNS : FetchType.COLUMNS_IN_QUERIED;
            return maybeApply(new DseColumnFilter(fetchType,
                                                  fetchType == FetchType.ALL_COLUMNS ? fetched : queried,
                                                  queried,
                                                  subSelections)
                              .withPartitionColumnsVerified(metadata.regularAndStaticColumns()));
        }

        public long serializedSize(DseColumnFilter selection)
        {
            selection = maybeUpdatedForBackwardCompatibility(selection);

            long size = 1; // header byte

            if (version >= MessagingService.VERSION_3014 && selection.fetchType != FetchType.COLUMNS_IN_QUERIED)
            {
                size += Columns.serializer.serializedSize(selection.fetched.statics);
                size += Columns.serializer.serializedSize(selection.fetched.regulars);
            }

            if (selection.queried != null)
            {
                size += Columns.serializer.serializedSize(selection.queried.statics);
                size += Columns.serializer.serializedSize(selection.queried.regulars);
            }

            if (selection.subSelections != null)
            {

                size += TypeSizes.sizeofUnsignedVInt(selection.subSelections.size());
                for (ColumnSubselection subSel : selection.subSelections.values())
                    size += columnSubselectionSerializer.serializedSize(subSel, version);
            }

            return size;
        }
    }
}
