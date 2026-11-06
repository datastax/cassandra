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

package org.apache.cassandra.index.sai;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.FieldPath;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.analyzer.LuceneAnalyzer;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.vector.VectorValidation;
import org.apache.cassandra.index.sai.iterators.KeyRangeAntiJoinIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeUnionIterator;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.MemtableKeyRangeIterator;
import org.apache.cassandra.index.sai.metrics.AbstractMetrics;
import org.apache.cassandra.index.sai.metrics.ColumnQueryMetrics;
import org.apache.cassandra.index.sai.metrics.IndexMetrics;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.index.sai.view.IndexViewManager;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_MAX_ANALYZED_SIZE;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_MAX_FROZEN_TERM_SIZE;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_MAX_STRING_TERM_SIZE;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_MAX_VECTOR_TERM_SIZE;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VALIDATE_MAX_TERM_SIZE_AT_COORDINATOR;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_INDEX_READS_DISABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_INDEX_METRICS_ENABLED;
import static org.apache.cassandra.index.sai.plan.QueryController.INDEX_VERSION_DOES_NOT_SUPPORT_BM25;

/**
 * Manage metadata for each column index.
 */
public class IndexContext
{
    private static final Logger logger = LoggerFactory.getLogger(IndexContext.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    public static final int MAX_STRING_TERM_SIZE = SAI_MAX_STRING_TERM_SIZE.getInt() * 1024;
    public static final int MAX_FROZEN_TERM_SIZE = SAI_MAX_FROZEN_TERM_SIZE.getInt() * 1024;
    public static final int MAX_VECTOR_TERM_SIZE = SAI_MAX_VECTOR_TERM_SIZE.getInt() * 1024;
    public static final int MAX_ANALYZED_SIZE = SAI_MAX_ANALYZED_SIZE.getInt() * 1024;
    private static final String TERM_OVERSIZE_LOG_MESSAGE =
    "Can't add term of column {} to index for key: {}, term size {} max allowed size {}.";
    private static final String TERM_OVERSIZE_ERROR_MESSAGE =
    "Term of column %s exceeds the byte limit for index. Term size %s. Max allowed size %s.";

    private static final String ANALYZED_TERM_OVERSIZE_LOG_MESSAGE =
    "Term's analyzed size for column {} exceeds the cumulative limit for index. Max allowed size {}.";
    private static final String ANALYZED_TERM_OVERSIZE_ERROR_MESSAGE =
    "Term's analyzed size for column %s exceeds the cumulative limit for index. Max allowed size %s.";

    public static final String MULTIPLE_HINTS_WITH_ORDER_BY_ERROR_MESSAGE =
    "Index hints including a SAI index used for ORDER BY cannot include any other index";

    // Besides EQ and IN, only RANGE (see supports(Expression.Op)) and, on the multi-valued field paths, NOT_EQ (the
    // anti-join, which only needs equality) are served on these types
    private static final Set<AbstractType<?>> EQ_ONLY_TYPES =
            ImmutableSet.of(UTF8Type.instance, AsciiType.instance, BooleanType.instance, UUIDType.instance);

    public static final String ENABLE_SEGMENT_COMPACTION_OPTION_NAME = "enable_segment_compaction";

    private final AbstractType<?> partitionKeyType;
    private final ClusteringComparator clusteringComparator;

    private final String keyspace;
    private final String table;
    private final TableId tableId;
    private final ColumnMetadata column;
    private final IndexTarget.Type indexType;
    // the path to the indexed field of the column, null if the whole column is indexed
    @Nullable
    private final FieldPath fieldPath;
    // whether the indexed field is a collection indexed by its elements (map values)
    private final boolean unpacksLeafCollection;
    // whether a row can have more than one indexed value, see isMultiValued()
    private final boolean isMultiValued;
    // the column name, followed by the field path for an index on a field
    private final String targetName;
    private final AbstractType<?> validator;
    private final ColumnFamilyStore cfs;

    // Config can be null if the column context is "fake" (i.e. created for a filtering expression).
    private final IndexMetadata config;
    private final VectorSimilarityFunction vectorSimilarityFunction;

    private final ConcurrentMap<Memtable, MemtableIndex> liveMemtables = new ConcurrentHashMap<>();

    private final IndexViewManager viewManager;
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<IndexMetrics> indexMetrics;
    private final ColumnQueryMetrics columnQueryMetrics;
    private final IndexWriterConfig indexWriterConfig;
    private final boolean isAnalyzed;
    private final boolean hasEuclideanSimilarityFunc;
    private final AbstractAnalyzer.AnalyzerFactory analyzerFactory;
    private final AbstractAnalyzer.AnalyzerFactory queryAnalyzerFactory;
    private final PrimaryKey.Factory primaryKeyFactory;

    private final Version version;
    private final int maxTermSize;

    private volatile boolean dropped = false;

    public IndexContext(@Nonnull String keyspace,
                        @Nonnull String table,
                        @Nonnull TableId tableId,
                        @Nonnull AbstractType<?> partitionKeyType,
                        @Nonnull ClusteringComparator clusteringComparator,
                        @Nonnull ColumnMetadata column,
                        @Nonnull IndexTarget.Type indexType,
                        IndexMetadata config,
                        @Nonnull ColumnFamilyStore cfs)
    {
        this(keyspace, table, tableId, partitionKeyType, clusteringComparator, column, indexType, null, config, cfs);
    }

    /**
     * @param fieldPath the path to the indexed field of the column (see {@link FieldPath}), or {@code null} if the
     * whole column is indexed
     */
    public IndexContext(@Nonnull String keyspace,
                        @Nonnull String table,
                        @Nonnull TableId tableId,
                        @Nonnull AbstractType<?> partitionKeyType,
                        @Nonnull ClusteringComparator clusteringComparator,
                        @Nonnull ColumnMetadata column,
                        @Nonnull IndexTarget.Type indexType,
                        @Nullable FieldPath fieldPath,
                        IndexMetadata config,
                        @Nonnull ColumnFamilyStore cfs)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.tableId = tableId;
        this.partitionKeyType = partitionKeyType;
        this.clusteringComparator = clusteringComparator;
        this.column = column;
        this.indexType = indexType;
        this.fieldPath = fieldPath;
        this.unpacksLeafCollection = fieldPath != null
                                     && indexType == IndexTarget.Type.VALUES
                                     && fieldPath.leafType(column.type).isCollection();
        this.isMultiValued = fieldPath == null
                             ? TypeUtil.isNonFrozenCollection(column.type)
                             : fieldPath.hasElements() || unpacksLeafCollection;
        this.targetName = fieldPath == null
                          ? column.name.toCQLString()
                          : column.name.toCQLString() + '.' + fieldPath.toCQLString(column.type);
        this.config = config;
        this.viewManager = new IndexViewManager(this);
        this.validator = TypeUtil.cellValueType(column, indexType, fieldPath);
        this.cfs = cfs;
        this.version = Version.current(keyspace);
        this.primaryKeyFactory = version.onDiskFormat().newPrimaryKeyFactory(clusteringComparator);

        String columnName = column.name.toString();

        if (config != null)
        {
            String fullIndexName = String.format("%s.%s.%s", this.keyspace, this.table, this.config.name);
            this.indexWriterConfig = IndexWriterConfig.fromOptions(fullIndexName, validator, config.options);
            this.isAnalyzed = AbstractAnalyzer.isAnalyzed(config.options);
            this.analyzerFactory = AbstractAnalyzer.fromOptions(columnName, validator, config.options);
            this.queryAnalyzerFactory = AbstractAnalyzer.hasQueryAnalyzer(config.options)
                                        ? AbstractAnalyzer.fromOptionsQueryAnalyzer(validator, config.options)
                                        : this.analyzerFactory;
            this.vectorSimilarityFunction = indexWriterConfig.getSimilarityFunction();
            this.hasEuclideanSimilarityFunc = vectorSimilarityFunction == VectorSimilarityFunction.EUCLIDEAN;

            this.indexMetrics = SAI_INDEX_METRICS_ENABLED.getBoolean() ? Optional.of(new IndexMetrics(this)) : Optional.empty();
            this.columnQueryMetrics = isVector() ? new ColumnQueryMetrics.VectorIndexMetrics(keyspace, table, getIndexName()) :
                                      isLiteral() ? new ColumnQueryMetrics.TrieIndexMetrics(keyspace, table, getIndexName())
                                                  : new ColumnQueryMetrics.BKDIndexMetrics(keyspace, table, getIndexName());

        }
        else
        {
            this.indexWriterConfig = IndexWriterConfig.emptyConfig();
            this.isAnalyzed = AbstractAnalyzer.isAnalyzed(Collections.emptyMap());
            this.analyzerFactory = AbstractAnalyzer.fromOptions(columnName, validator, Collections.EMPTY_MAP);
            this.queryAnalyzerFactory = this.analyzerFactory;
            this.vectorSimilarityFunction = null;
            this.hasEuclideanSimilarityFunc = false;

            // null config indicates a "fake" index context. As such, it won't actually be used for indexing/accessing
            // data, leaving these metrics unused. This also eliminates the overhead of creating these metrics on the
            // query path.
            this.indexMetrics = Optional.empty();
            this.columnQueryMetrics = null;
        }

        this.maxTermSize = isVector() ? MAX_VECTOR_TERM_SIZE
                                      : isAnalyzed ? MAX_ANALYZED_SIZE
                                                   : isFrozen() ? MAX_FROZEN_TERM_SIZE : MAX_STRING_TERM_SIZE;


        logger.debug(logMessage("Initialized index context with index writer config: {}"), indexWriterConfig);
    }

    public Version version()
    {
        return version;
    }

    public AbstractType<?> keyValidator()
    {
        return partitionKeyType;
    }

    public PrimaryKey.Factory keyFactory()
    {
        return primaryKeyFactory;
    }

    public ClusteringComparator comparator()
    {
        return clusteringComparator;
    }

    public Optional<IndexMetrics> getIndexMetrics()
    {
        return indexMetrics;
    }

    public ColumnQueryMetrics getColumnQueryMetrics()
    {
        return columnQueryMetrics;
    }

    public String getKeyspace()
    {
        return keyspace;
    }
    public String getTable()
    {
        return table;
    }

    public TableId getTableId()
    {
        return tableId;
    }

    public ColumnFamilyStore columnFamilyStore()
    {
        return cfs;
    }

    public IPartitioner getPartitioner()
    {
        return cfs.getPartitioner();
    }

    public MemtableIndex initializeMemtableIndex(Memtable memtable)
    {
        return liveMemtables.computeIfAbsent(memtable, mt -> MemtableIndex.createIndex(this, mt));
    }

    public void index(DecoratedKey key, Row row, Memtable mt, OpOrder.Group opGroup)
    {
        MemtableIndex target = initializeMemtableIndex(mt);

        long start = nanoTime();

        if (isMultiValued())
        {
            long now = FBUtilities.nowInSeconds();
            Iterator<ByteBuffer> bufferIterator = getValuesOf(key, row, now, expiringCellsObserver(target, now));
            if (bufferIterator != null)
            {
                while (bufferIterator.hasNext())
                {
                    ByteBuffer value = bufferIterator.next();
                    target.index(key, row.clustering(), value, mt, opGroup);
                }
            }
        }
        else
        {
            ByteBuffer value = getValueOf(key, row, FBUtilities.nowInSeconds());
            target.index(key, row.clustering(), value, mt, opGroup);
        }
        indexMetrics.flatMap(metrics -> metrics.memtableIndexWriteLatency).ifPresent(timer ->
                timer.update(nanoTime() - start, TimeUnit.NANOSECONDS));
    }

    /**
     * Validate maximum term size for given row. Throw an exception when invalid.
     */
    public void validateMaxTermSizeForRow(DecoratedKey key, Row row)
    {
        AbstractAnalyzer analyzer = getAnalyzerFactory().create();
        if (isMultiValued())
        {
            Iterator<ByteBuffer> bufferIterator = getValuesOf(key, row, FBUtilities.nowInSeconds());
            while (bufferIterator != null && bufferIterator.hasNext())
                validateMaxTermSizeForCell(analyzer, key, bufferIterator.next());
        }
        else
        {
            ByteBuffer value = getValueOf(key, row, FBUtilities.nowInSeconds());
            validateMaxTermSizeForCell(analyzer, key, value);
        }
    }

    private void validateMaxTermSizeForCell(AbstractAnalyzer analyzer, DecoratedKey key, @Nullable ByteBuffer cellBuffer)
    {
        if (cellBuffer == null || cellBuffer.remaining() == 0)
            return;

        analyzer.reset(cellBuffer);
        try
        {
            if (analyzer.transformValue())
            {
                if (!validateCumulativeAnalyzedTermLimit(key, analyzer))
                {
                    String error = String.format(ANALYZED_TERM_OVERSIZE_ERROR_MESSAGE,
                                                 getTargetName(), FBUtilities.prettyPrintMemory(maxTermSize));
                    throw new InvalidRequestException(error);
                }
            }
            else
            {
                while (analyzer.hasNext())
                {
                    int size = analyzer.next().remaining();
                    if (!validateMaxTermSize(key, size))
                    {
                        String error = String.format(TERM_OVERSIZE_ERROR_MESSAGE,
                                                     getTargetName(),
                                                     FBUtilities.prettyPrintMemory(size),
                                                     FBUtilities.prettyPrintMemory(maxTermSize));
                        throw new InvalidRequestException(error);
                    }
                }
            }
        }
        finally
        {
            analyzer.end();
        }
    }


    /**
     * Validate maximum term size for given term
     * @return true if given term is valid; otherwise false.
     */
    public boolean validateMaxTermSize(DecoratedKey key, ByteBuffer term)
    {
        return validateMaxTermSize(key, term.remaining());
    }

    private boolean validateMaxTermSize(DecoratedKey key, int termSize)
    {
        if (termSize > maxTermSize)
        {
            noSpamLogger.warn(logMessage(TERM_OVERSIZE_LOG_MESSAGE),
                              getTargetName(),
                              keyValidator().getString(key.getKey()),
                              FBUtilities.prettyPrintMemory(termSize),
                              FBUtilities.prettyPrintMemory(maxTermSize));
            return false;
        }

        return true;
    }

    private boolean validateCumulativeAnalyzedTermLimit(DecoratedKey key, AbstractAnalyzer analyzer)
    {
        int bytesCount = 0;
        // VSTODO anayzer.hasNext copies the byteBuffer, but we don't need that here.
        while (analyzer.hasNext())
        {
            final ByteBuffer token = analyzer.next();
            bytesCount += token.remaining();
            if (bytesCount > maxTermSize)
            {
                noSpamLogger.warn(logMessage(ANALYZED_TERM_OVERSIZE_LOG_MESSAGE),
                                  getTargetName(),
                                  keyValidator().getString(key.getKey()),
                                  FBUtilities.prettyPrintMemory(maxTermSize));
                return false;
            }
        }
        return true;
    }

    public void update(DecoratedKey key, Row oldRow, Row newRow, Memtable memtable, OpOrder.Group opGroup)
    {
        if (version.equals(Version.AA))
        {
            // AA cannot handle updates because it indexes partition keys instead of fully qualified primary keys.
            index(key, newRow, memtable, opGroup);
            return;
        }

        MemtableIndex target = liveMemtables.get(memtable);
        if (target == null)
            return;

        // Use 0 for nowInSecs to get the value(s) from the oldRow regardless of its liveness status. To get to this point,
        // C* has already determined this is the current represntation of the oldRow in the memtable, and that means
        // we need to add the newValue to the index and remove the oldValue from it, even if it has already expired via
        // TTL.
        if (isMultiValued())
        {
            long now = FBUtilities.nowInSeconds();
            Iterator<ByteBuffer> oldValues = getValuesOf(key, oldRow, 0);
            Iterator<ByteBuffer> newValues = getValuesOf(key, newRow, now, expiringCellsObserver(target, now));
            target.update(key, oldRow.clustering(), oldValues, newValues, memtable, opGroup);
        }
        else
        {
            ByteBuffer oldValue = getValueOf(key, oldRow, 0);
            ByteBuffer newValue = getValueOf(key, newRow, FBUtilities.nowInSeconds());
            target.update(key, oldRow.clustering(), oldValue, newValue, memtable, opGroup);
        }
    }

    /**
     * @return the consumer recording the expiration of the indexed expiring cells (with a TTL) in the memtable index,
     * which keeps the terms of the expired cells (see {@link #subtractsEqualKeys}), or {@code null} if the index may
     * have expired terms already (it can't become fresh again, as the time passes)
     */
    @Nullable
    private static LongConsumer expiringCellsObserver(MemtableIndex target, long nowInSec)
    {
        return target.mayHaveExpiredTerms(nowInSec) ? null : target::markExpiringTerm;
    }

    /**
     * Whether a partition deletion or a range tombstone in the given memtable or SSTable indexes may shadow the indexed
     * terms of any source, at the given time, so that a non-equality ({@code NOT CONTAINS}, {@code NOT CONTAINS KEY})
     * can't be searched as "all the keys minus the keys with an equal term" in any source (see
     * {@link #subtractsEqualKeys}).
     * <p>
     * Such a deletion doesn't remove the terms of the rows it shadows: from the memtable index (nor, depending on the
     * memtable implementation, the rows from the memtable, so a re-inserted row keeps the old cells in its reconciled
     * row, or gets new terms on top of the stale ones), nor from the SSTable index written by the flush, which has the
     * terms of the memtable index for the rows written. The source of the deletion has no row for the key, so it
     * doesn't include it, and it can shadow the terms of another source (e.g. a write older than a deletion flushed
     * before it). The other deletions (of cells, collections and rows) are in a row of their source, which includes the
     * key.
     * <p>
     * The memtables record their partition deletions and range tombstones
     * ({@link MemtableIndex#markShadowingDeletion()}); the SSTables record their partition deletions, but not their
     * range tombstones apart from the other tombstones: an SSTable of a table with clustering columns may have range
     * tombstones if its minimum local deletion time isn't after the query time.
     *
     * @param nowInSec the time of the query, in seconds
     * @return whether a deletion may shadow terms
     */
    public static boolean hasShadowingDeletions(Collection<MemtableIndex> memtables, Collection<SSTableIndex> sstables, long nowInSec)
    {
        for (MemtableIndex memtable : memtables)
            if (memtable.hasShadowingDeletions())
                return true;
        for (SSTableIndex index : sstables)
        {
            SSTableReader sstable = index.getSSTable();
            if (sstable.getSSTableMetadata().hasPartitionLevelDeletions)
                return true;
            if (!sstable.metadata().clusteringColumns().isEmpty() && sstable.getMinLocalDeletionTime() <= nowInSec)
                return true;
        }
        return false;
    }

    /**
     * @param nowInSec the time of the query, in seconds
     * @return whether the SSTable may have an expiring cell expired at the given time: its index has no expiration, so
     * it keeps the terms of the expired cells (its minimum local deletion time is that of its tombstones and expiring
     * cells)
     */
    public static boolean mayHaveExpiredTerms(SSTableReader sstable, long nowInSec)
    {
        return sstable.getMaxTTL() > 0 && sstable.getMinLocalDeletionTime() <= nowInSec;
    }

    /**
     * @return whether the current memtable and SSTable indexes of this index may have deletions shadowing indexed terms
     * (see {@link #hasShadowingDeletions(Collection, Collection, long)})
     */
    @VisibleForTesting
    public boolean hasShadowingDeletions(long nowInSec)
    {
        return hasShadowingDeletions(liveMemtables.values(), getView().getIndexes(), nowInSec);
    }

    /**
     * @return the live memtable indexes of this index
     */
    @VisibleForTesting
    public Collection<MemtableIndex> getLiveMemtableIndexes()
    {
        return liveMemtables.values();
    }

    public void renewMemtable(Memtable renewed)
    {
        // remove every index but the one that corresponds to the post-truncate Memtable
        liveMemtables.keySet().removeIf(m -> m != renewed);
    }

    public void discardMemtable(Memtable discarded)
    {
        liveMemtables.remove(discarded);
    }

    public MemtableIndex getPendingMemtableIndex(LifecycleNewTracker tracker)
    {
        return liveMemtables.keySet().stream()
                            .filter(m -> tracker.equals(m.getFlushTransaction()))
                            .findFirst()
                            .map(liveMemtables::get)
                            .orElse(null);
    }

    // Returns an iterator for NEQ, NOT_CONTAINS_KEY, NOT_CONTAINS_VALUE, which
    // 1. Either includes everything if the column type values can be truncated and
    // thus the keys cannot be matched precisely,
    // 2. or includes everything minus the keys matching the expression
    // if the column type values cannot be truncated, i.e., matching the keys is always precise.
    // (not matching precisely will lead to false negatives)
    //
    // keys k such that row(k) not contains v = (all keys) \ (keys k such that row(k) contains v)
    //
    // Note that rows in other indexes are not matched, so this can return false positives,
    // but they are not a problem as post-filtering would get rid of them.
    // The keys matched in other indexes cannot be safely subtracted
    // as indexes may contain false positives caused by deletes and updates.
    //
    // The subtraction is done memtable by memtable (the keys of each memtable minus the keys matching in that same
    // memtable): during a flush, the old and the new memtables are both live, and the new one may have a key without
    // the term that the old one has (e.g. an element removed). It's done only if the caller found no deletion that may
    // shadow terms (see hasShadowingDeletions), and only for the memtable indexes without expired terms (see
    // MemtableIndex#mayHaveExpiredTerms): otherwise the keys of the memtable are all included, and the post-filter
    // decides.
    private KeyRangeIterator getNonEqIterator(QueryContext context,
                                              Collection<MemtableIndex> memtables,
                                              Expression expression,
                                              AbstractBounds<PartitionPosition> keyRange,
                                              boolean subtractEqualKeys)
    {
        if (!subtractEqualKeys || TypeUtil.supportsRounding(expression.validator) || !version.onDiskFormat().indexFeatureSet().isRowAware())
            return scanMemtable(keyRange, memtables);

        if (memtables.isEmpty())
            return KeyRangeIterator.empty();

        // an expression with exclusions (a NOT IN on a multi-valued field) excludes the keys matching any of its
        // values, see Expression#negations
        List<Expression> negations = expression.negations();
        KeyRangeIterator.Builder builder = KeyRangeUnionIterator.builder(memtables.size());
        try
        {
            for (MemtableIndex memtableIndex : memtables)
            {
                Collection<MemtableIndex> memtable = Collections.singletonList(memtableIndex);
                KeyRangeIterator allKeys = scanMemtable(keyRange, memtable);
                if (memtableIndex.mayHaveExpiredTerms(context.nowInSec()))
                {
                    builder.add(allKeys);
                    continue;
                }
                KeyRangeUnionIterator.Builder matched = KeyRangeUnionIterator.builder(negations.size());
                try
                {
                    for (Expression negation : negations)
                        matched.add(searchMemtable(context, memtable, negation, keyRange));
                }
                catch (Throwable t)
                {
                    FileUtils.closeQuietly(matched.ranges());
                    FileUtils.closeQuietly(allKeys);
                    throw t;
                }
                builder.add(KeyRangeAntiJoinIterator.create(allKeys, matched.build()));
            }
            return builder.build();
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(builder.ranges());
            throw t;
        }
    }

    /**
     * As {@link #searchMemtable(QueryContext, Collection, Expression, AbstractBounds, boolean)}, never subtracting the
     * keys with an equal term for a non-equality: the SSTables of the query, whose deletions may shadow the terms of the
     * memtables, are not known.
     */
    public KeyRangeIterator searchMemtable(QueryContext context, Collection<MemtableIndex> memtables, Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        return searchMemtable(context, memtables, expression, keyRange, false);
    }

    /**
     * @param subtractEqualKeys for a non-equality, whether the keys with an equal term may be subtracted from all the
     * keys: no deletion of the query's memtables and SSTables may shadow terms (see
     * {@link #hasShadowingDeletions(Collection, Collection, long)}); the memtable indexes that may have expired terms
     * don't subtract them anyway
     */
    public KeyRangeIterator searchMemtable(QueryContext context,
                                           Collection<MemtableIndex> memtables,
                                           Expression expression,
                                           AbstractBounds<PartitionPosition> keyRange,
                                           boolean subtractEqualKeys)
    {
        if (expression.getOp().isNonEquality())
        {
            return getNonEqIterator(context, memtables, expression, keyRange, subtractEqualKeys);
        }

        if (memtables.isEmpty())
        {
            return KeyRangeIterator.empty();
        }

        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder();

        try
        {
            for (MemtableIndex index : memtables)
                builder.add(index.search(context, expression, keyRange));

            return builder.build();
        }
        catch (Exception ex)
        {
            FileUtils.closeQuietly(builder.ranges());
            throw ex;
        }
    }

    private KeyRangeIterator scanMemtable(AbstractBounds<PartitionPosition> keyRange, Collection<MemtableIndex> memtables)
    {
        if (memtables.isEmpty())
        {
            return KeyRangeIterator.empty();
        }

        KeyRangeIterator.Builder builder = KeyRangeUnionIterator.builder(memtables.size());

        try
        {
            for (MemtableIndex memtableIndex : memtables)
            {
                Memtable memtable = memtableIndex.getMemtable();
                KeyRangeIterator memtableIterator = new MemtableKeyRangeIterator(memtable, primaryKeyFactory, keyRange);
                builder.add(memtableIterator);
            }

            return builder.build();
        }
        catch (Exception ex)
        {
            FileUtils.closeQuietly(builder.ranges());
            throw ex;
        }
    }


    public long liveMemtableWriteCount()
    {
        return liveMemtables.values().stream().mapToLong(MemtableIndex::writeCount).sum();
    }

    public long estimatedOnHeapMemIndexMemoryUsed()
    {
        return liveMemtables.values().stream().mapToLong(MemtableIndex::estimatedOnHeapMemoryUsed).sum();
    }

    public long estimatedOffHeapMemIndexMemoryUsed()
    {
        return liveMemtables.values().stream().mapToLong(MemtableIndex::estimatedOffHeapMemoryUsed).sum();
    }

    /**
     * @return A set of SSTables which have attached to them invalid index components.
     */
    public Set<SSTableContext> onSSTableChanged(Collection<SSTableReader> oldSSTables,
                                                Collection<SSTableContext> newContexts,
                                                boolean validate)
    {
        return viewManager.update(oldSSTables, newContexts, validate);
    }

    public ColumnMetadata getDefinition()
    {
        return column;
    }

    public AbstractType<?> getValidator()
    {
        return validator;
    }

    /**
     * @return the path to the indexed field of the column, or {@code null} if the whole column is indexed
     */
    @Nullable
    public FieldPath getFieldPath()
    {
        return fieldPath;
    }

    /**
     * @return whether the index is on a field of the column, rather than on the whole column
     */
    public boolean hasFieldPath()
    {
        return fieldPath != null;
    }

    /**
     * @return the type of index target; for an index on a field, it applies to the field
     */
    public IndexTarget.Type getIndexType()
    {
        return indexType;
    }

    /**
     * @return whether the index is on the keys, values or entries of a non-frozen collection column. This is not the
     * case of the indexes on a field of the column, even if the field is inside the elements of the collection.
     */
    public boolean isNonFrozenCollection()
    {
        return fieldPath == null && TypeUtil.isNonFrozenCollection(column.type);
    }

    /**
     * @return whether a row can have more than one indexed value: the indexes on non-frozen collections, and the
     * indexes on a field that is inside collection elements or that is a collection indexed by its values
     */
    public boolean isMultiValued()
    {
        return isMultiValued;
    }

    public boolean isCollection()
    {
        return fieldPath == null ? column.type.isCollection() : isMultiValued();
    }

    public boolean isFrozen()
    {
        return TypeUtil.isFrozen(fieldPath == null ? column.type : validator);
    }

    public String getColumnName()
    {
        return column.name.toString();
    }

    /**
     * @return the names of the fields on the path to the indexed field, separated by dots and not quoted (as
     * {@link #getColumnName()}), without the column name: for example {@code city} or {@code loc.lat} for the indexes
     * on {@code addr.city} and {@code addr.loc.lat}; {@code null} for an index on a whole column. For the SAI system
     * views.
     */
    @Nullable
    public String getFieldPathName()
    {
        if (fieldPath == null)
            return null;
        StringJoiner joiner = new StringJoiner(".");
        for (FieldIdentifier name : fieldPath.names(column.type))
            joiner.add(name.toString());
        return joiner.toString();
    }

    /**
     * @return the target of the index as stored in the schema, in canonical CQL form (quoted when needed): for
     * example {@code v}, {@code values(m)}, {@code addr.city}, {@code addrs.city} (also for {@code values(addrs).city}),
     * {@code values(addr.tags)} or {@code full(addr.tags)}; {@code null} without index metadata. For the SAI system
     * views.
     */
    @Nullable
    public String getIndexTarget()
    {
        return config == null ? null : config.options.get(IndexTarget.TARGET_OPTION_NAME);
    }

    /**
     * @return the name of the indexed column, followed by the path to the indexed field for an index on a field
     * (for example {@code addr.city}), in CQL form (quoted when needed), for messages
     */
    public String getTargetName()
    {
        return targetName;
    }

    public String getIndexName()
    {
        return this.config == null ? null : config.name;
    }

    public int getIntOption(String name, int defaultValue)
    {
        String value = this.config.options.get(name);
        if (value == null)
            return defaultValue;

        try
        {
            return Integer.parseInt(value);
        }
        catch (NumberFormatException e)
        {
            logger.error("Failed to parse index configuration " + name + " = " + value + " as integer");
            return defaultValue;
        }
    }

    public AbstractAnalyzer.AnalyzerFactory getAnalyzerFactory()
    {
        return analyzerFactory;
    }

    public AbstractAnalyzer.AnalyzerFactory getQueryAnalyzerFactory()
    {
        return queryAnalyzerFactory;
    }

    public IndexWriterConfig getIndexWriterConfig()
    {
        return indexWriterConfig;
    }

    public View getView()
    {
        return viewManager.getView();
    }

    public View getReferencedView(long timeoutNanos)
    {
        long deadline = MonotonicClock.Global.approxTime.now() + timeoutNanos;
        do
        {
            View view = viewManager.getView();
            if (view.reference())
                return view;
        } while (!MonotonicClock.Global.approxTime.isAfter(deadline));

        return null;
    }

    /**
     * @return total number of per-index open files
     */
    public int openPerIndexFiles()
    {
        return viewManager.getView().size() * version.onDiskFormat().openFilesPerIndex(this);
    }

    public void prepareSSTablesForRebuild(Collection<SSTableReader> sstablesToRebuild)
    {
        viewManager.prepareSSTablesForRebuild(sstablesToRebuild);
    }

    public boolean isIndexed()
    {
        return config != null && !dropped;
    }

    public boolean isDropped()
    {
        return dropped;
    }

    /**
     * @return whether the column is analyzed, meaning it uses an analyzer that isn't no-op.
     */
    public boolean isAnalyzed()
    {
        return isAnalyzed;
    }

    /**
     * Called when index is dropped. Mark all {@link SSTableIndex} as released and per-column index files
     * will be removed when in-flight queries completed and {@code obsolete} is true.
     *
     * @param obsolete true if index files should be deleted after invalidate; false otherwise.
     */
    public void invalidate(boolean obsolete)
    {
        dropped = true;
        liveMemtables.clear();
        viewManager.invalidate(obsolete);
        indexMetrics.ifPresent(AbstractMetrics::release);
        if (columnQueryMetrics != null)
            columnQueryMetrics.release();

        analyzerFactory.close();
        if (queryAnalyzerFactory != analyzerFactory)
        {
            queryAnalyzerFactory.close();
        }
    }

    public ConcurrentMap<Memtable, MemtableIndex> getLiveMemtables()
    {
        return liveMemtables;
    }

    public SSTableContext getSSTableContext(SSTableReader sstable)
    {
        // This method is only used in tests, returning null for now
        return null;
    }

    /**
     * Returns whether this index supports the CQL {@code LIKE} restrictions: prefix ({@code LIKE '<term>%'}),
     * suffix, contains and generic patterns.
     * <p>
     * These are supported on non-tokenized indexes of plain {@code text}/{@code ascii} columns: the terms
     * dictionary then contains whole (though possibly normalized, e.g. lowercased) terms in sorted order, so
     * prefixes map to contiguous scans of it and the other patterns to automaton intersections with it.
     * Tokenizing (Lucene) analyzers break the 1:1 mapping between column values and indexed terms, making
     * pattern semantics over the indexed terms undefined.
     * <p>
     * Collections are not supported, frozen (the term is the whole serialized collection) or not: {@code LIKE}
     * is a restriction on the value of a column, and CQL defines no element-wise semantics for it, as it does for
     * {@code CONTAINS}/{@code CONTAINS KEY} (which {@code LIKE_KEY}/{@code LIKE_VALUE} operators and the
     * corresponding post-filtering would have to introduce). The terms of the keys/values/entries indexes of
     * non-frozen collections could serve such a feature (the entries index excepted: its terms are composites).
     * <p>
     * Reversed (DESC clustering) string columns are supported: their terms are the raw, un-reversed value bytes.
     */
    public boolean supportsLikeQueries()
    {
        // a string type: the validators of frozen collections and of map entries are not
        return TypeUtil.isUTF8OrAscii(validator)
               // not the keys or values of a non-frozen collection, whose validator is the element type
               && !isNonFrozenCollection()
               // nor the values of a collection field
               && !unpacksLeafCollection
               // no tokenizing analyzer, so that the terms are whole (possibly normalized) values
               && !hasCustomAnalyzer();
    }

    /**
     * @return whether this index uses a custom (Lucene) index or query analyzer, i.e. one that may tokenize
     * a value into multiple terms. Indexes with only the non-tokenizing options (case_sensitive, normalize,
     * ascii) are not considered custom-analyzed.
     */
    private boolean hasCustomAnalyzer()
    {
        return config != null && (config.options.containsKey(LuceneAnalyzer.INDEX_ANALYZER)
                                  || config.options.containsKey(LuceneAnalyzer.QUERY_ANALYZER));
    }

    public boolean supports(Operator op)
    {
        // IS NOT NULL on a field: an index on the field has a term when the field is set (null and unset values are
        // not indexed, empty values only for the literal types, as RowFilter.FieldExpression#isSet), so the scan of
        // all its terms serves it, whatever the order of the terms. The exception is a value whose term (the analyzed
        // value, for an index with an analyzer) exceeds the maximum term size of the index: it is skipped at index
        // time with a warning, so the rows with such a value are missing from the results, as from those of every
        // other operator the index serves. Not when it indexes the values of a leaf collection (an empty collection
        // is set but has no term) nor with a tokenizing analyzer (a value may have no token). Never on whole
        // columns, for which IS NOT NULL is only a materialized view restriction.
        if (op == Operator.IS_NOT)
            return fieldPath != null && !unpacksLeafCollection && !hasCustomAnalyzer();

        if (op.isLike())
            return supportsLikeQueries();
        // Analyzed columns store the indexed result, so we are unable to compute raw equality.
        // The only supported operators are ANALYZER_MATCHES and BM25.
        if (op == Operator.ANALYZER_MATCHES) return isAnalyzed;
        // BM25 frequency calculations only work on non-collection columns because it assumes a 1:1 mapping from PrK
        // to frequency, but collections have mulitple documents.
        if (op == Operator.BM25) return isAnalyzed && !isCollection() && fieldPath == null;

        // If the column is analyzed and the operator is EQ, we need to check if the analyzer supports it.
        if (op == Operator.EQ && isAnalyzed && !analyzerFactory.supportsEquals())
            return false;

        // ANN is only supported against vectors.
        // BOUNDED_ANN is only supported against vectors with a Euclidean similarity function.
        // Vector indexes only support ANN and BOUNDED_ANN
        if (fieldPath == null && column.type instanceof VectorType)
            return op == Operator.ANN || (op == Operator.BOUNDED_ANN && hasEuclideanSimilarityFunc);
        if (op == Operator.ANN || op == Operator.BOUNDED_ANN)
            return false;

        // Only regular columns can be sorted by SAI (at least for now), and not by their fields
        if (op == Operator.ORDER_BY_ASC || op == Operator.ORDER_BY_DESC)
            return fieldPath == null
                   && !isCollection()
                   && column.isRegular()
                   && !isAnalyzed
                   && !(column.type instanceof InetAddressType  // Possible, but need to add decoding logic based on
                                                                 // SAI's TypeUtil.encode method.
                         || column.type instanceof DecimalType   // Currently truncates to 24 bytes
                         || column.type instanceof IntegerType); // Currently truncates to 20 bytes

        Expression.Op operator = Expression.Op.valueOf(op);
        // the values of a collection field are indexed as those of a non-frozen collection column
        if (isNonFrozenCollection() || unpacksLeafCollection)
        {
            if (indexType == IndexTarget.Type.KEYS)
                return operator == Expression.Op.CONTAINS_KEY
                       || operator == Expression.Op.NOT_CONTAINS_KEY;
            if (indexType == IndexTarget.Type.VALUES)
                return operator == Expression.Op.CONTAINS_VALUE
                       || operator == Expression.Op.NOT_CONTAINS_VALUE;
            return indexType == IndexTarget.Type.KEYS_AND_VALUES &&
                   (operator == Expression.Op.EQ || operator == Expression.Op.NOT_EQ || operator == Expression.Op.RANGE);
        }
        if (indexType == IndexTarget.Type.FULL)
            return operator == Expression.Op.EQ;
        AbstractType<?> validator = getValidator();
        if (operator == Expression.Op.IN)
            return true;
        if (operator == Expression.Op.RANGE)
            return !TypeUtil.isLiteral(validator) || supportsLiteralRangeQueries();
        // A != on a field inside collection elements ("no element is equal") is served as NOT CONTAINS is on the
        // collection columns, as all the keys minus the keys with an equal term (see getNonEqIterator), so it only
        // needs equality, also on the EQ-only types; not with an analyzer, whose terms aren't the raw values: the
        // keys it subtracted would depend on the analyzer, while a != compares the raw values.
        if (operator == Expression.Op.NOT_EQ && hasFieldPath() && isMultiValued() && !unpacksLeafCollection)
            return !isAnalyzed;
        // unwrapped, so that the reversed (DESC clustering) columns support the same operators as the others
        if (operator != Expression.Op.EQ && EQ_ONLY_TYPES.contains(validator.unwrap())) return false;
        return operator != null;
    }

    /**
     * Whether this index serves range restrictions on a literal column, in the unsigned byte order of its terms.
     * That needs the raw values as terms: no analyzer, as the result of a query must not depend on the indexes on the
     * column, and no frozen collection, whose serialized bytes are not in the order of the collections.
     */
    public boolean supportsLiteralRangeQueries()
    {
        return !isAnalyzed && (TypeUtil.isBoolean(validator) || supportsLikeQueries());
    }

    public ByteBuffer getValueOf(DecoratedKey key, Row row, long nowInSecs)
    {
        if (row == null)
            return null;

        if (fieldPath != null)
            return getFieldValueOf(key, row, nowInSecs);

        switch (column.kind)
        {
            case PARTITION_KEY:
                if (key == null)
                    return null;
                return partitionKeyType instanceof CompositeType
                       ? CompositeType.extractComponent(key.getKey(), column.position())
                       : key.getKey();
            case CLUSTERING:
                // skip indexing of static clustering when regular column is indexed
                return row.isStatic() ? null : row.clustering().bufferAt(column.position());

            // treat static cell retrieval the same was as regular
            // only if row kind is STATIC otherwise return null
            case STATIC:
                if (!row.isStatic())
                    return null;
            case REGULAR:
                Cell cell = row.getCell(column);
                return cell == null || !cell.isLive(nowInSecs) ? null : cell.buffer();

            default:
                return null;
        }
    }

    public Iterator<ByteBuffer> getValuesOf(Row row, long nowInSecs)
    {
        return getValuesOf(null, row, nowInSecs);
    }

    /**
     * @param key the partition key of the row, needed for the indexes on the fields of partition key columns
     * @param row the row
     * @param nowInSecs the current time, the cells that are not live at that time are ignored
     * @return the indexed values of the row, for a {@link #isMultiValued() multi-valued} index
     */
    public Iterator<ByteBuffer> getValuesOf(@Nullable DecoratedKey key, Row row, long nowInSecs)
    {
        return getValuesOf(key, row, nowInSecs, null);
    }

    /**
     * As {@link #getValuesOf(DecoratedKey, Row, long)}, also giving the local expiration time of each live expiring
     * cell (with a TTL) of the indexed column to the given consumer, if not {@code null}, as the values are extracted.
     */
    private Iterator<ByteBuffer> getValuesOf(@Nullable DecoratedKey key, Row row, long nowInSecs, @Nullable LongConsumer expiringCells)
    {
        if (row == null)
            return null;

        if (fieldPath != null)
        {
            List<ByteBuffer> values = getFieldValuesOf(key, row, nowInSecs, expiringCells);
            return values == null ? null : values.iterator();
        }

        switch (column.kind)
        {
            // treat static cell retrieval the same was as regular
            // only if row kind is STATIC otherwise return null
            case STATIC:
                if (!row.isStatic())
                    return null;
            case REGULAR:
                return TypeUtil.collectionIterator(validator, row.getComplexColumnData(column), column, indexType, nowInSecs, expiringCells);

            default:
                return null;
        }
    }

    /**
     * @return the value of the indexed field in the row, for an index that isn't {@link #isMultiValued()}
     */
    @Nullable
    private ByteBuffer getFieldValueOf(@Nullable DecoratedKey key, Row row, long nowInSecs)
    {
        assert fieldPath != null && !isMultiValued : "Multi-valued index " + this;
        switch (column.kind)
        {
            case PARTITION_KEY:
                if (key == null)
                    return null;
                return fieldPath.singleValue(partitionKeyType instanceof CompositeType
                                             ? CompositeType.extractComponent(key.getKey(), column.position())
                                             : key.getKey());
            case CLUSTERING:
                // skip indexing of static clustering when regular column is indexed
                return row.isStatic() ? null : fieldPath.singleValue(row.clustering().bufferAt(column.position()));
            // treat static cell retrieval the same was as regular
            // only if row kind is STATIC otherwise return null
            case STATIC:
                if (!row.isStatic())
                    return null;
            case REGULAR:
                if (column.type.isMultiCell())
                    return fieldPath.singleValue(row.getComplexColumnData(column), nowInSecs);
                Cell<?> cell = row.getCell(column);
                return cell == null || !cell.isLive(nowInSecs) ? null : fieldPath.singleValue(cell.buffer());
            default:
                return null;
        }
    }

    /**
     * @return the values of the indexed field in the row, {@code null} if the column has no value in the row
     */
    @Nullable
    private List<ByteBuffer> getFieldValuesOf(@Nullable DecoratedKey key, Row row, long nowInSecs, @Nullable LongConsumer expiringCells)
    {
        assert fieldPath != null;
        List<ByteBuffer> values = new ArrayList<>(isMultiValued() ? 4 : 1);
        switch (column.kind)
        {
            case PARTITION_KEY:
                if (key == null)
                    return null;
                ByteBuffer keyValue = partitionKeyType instanceof CompositeType
                                      ? CompositeType.extractComponent(key.getKey(), column.position())
                                      : key.getKey();
                fieldPath.collectValues(column.type, keyValue, unpacksLeafCollection, values::add);
                break;
            case CLUSTERING:
                // skip indexing of static clustering when regular column is indexed
                if (row.isStatic())
                    return null;
                fieldPath.collectValues(column.type, row.clustering().bufferAt(column.position()), unpacksLeafCollection, values::add);
                break;
            // treat static cell retrieval the same was as regular
            // only if row kind is STATIC otherwise return null
            case STATIC:
                if (!row.isStatic())
                    return null;
            case REGULAR:
                if (column.type.isMultiCell())
                {
                    ComplexColumnData data = row.getComplexColumnData(column);
                    if (data == null)
                        return null;
                    fieldPath.collectValues(column.type, data, nowInSecs, unpacksLeafCollection, expiringCells, values::add);
                }
                else
                {
                    Cell<?> cell = row.getCell(column);
                    if (cell == null || !cell.isLive(nowInSecs))
                        return null;
                    if (expiringCells != null && cell.isExpiring())
                        expiringCells.accept(cell.localDeletionTime());
                    fieldPath.collectValues(column.type, cell.buffer(), unpacksLeafCollection, values::add);
                }
                break;
            default:
                return null;
        }

        // as for collection columns (see TypeUtil#collectionIterator)
        TypeUtil.sortCollectionValues(validator, values);
        return values;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("columnName", getColumnName())
                          .add("indexName", getIndexName())
                          .toString();
    }

    public boolean isLiteral()
    {
        return TypeUtil.isLiteral(getValidator());
    }

    public boolean isVector()
    {
        //VSTODO probably move this down to TypeUtils eventually
        return getValidator().isVector();
    }

    public void validate(DecoratedKey key, Row row)
    {
        // Validate the size of the inserted term.
        if (SAI_VALIDATE_MAX_TERM_SIZE_AT_COORDINATOR.getBoolean())
            validateMaxTermSizeForRow(key, row);

        // Verify vector is valid.
        if (isVector())
        {
            float[] value = TypeUtil.decomposeVector(getValidator(), getValueOf(key, row, FBUtilities.nowInSeconds()));
            if (value != null)
                VectorValidation.validateIndexable(value, vectorSimilarityFunction);
        }
    }

    public void validate(RowFilter rowFilter)
    {
        // Only iterate over the top level expressions because that is where the ANN and BM25 expressions are located.
        for (RowFilter.Expression expression : rowFilter.root.expressions())
        {
            // the expressions on this index's target only: its column or, for an index on a field, that field
            if (!expression.column().equals(column) || !Objects.equals(expression.fieldPath(), fieldPath))
                continue;

            Operator operator = expression.operator();

            // We cannot use indexes for both filtering and sorting, it's either sort-then-filter or filter-then-sort.
            // We should reject index hints asking to include indexes for both sorting and filtering.
            if (operator.isOrderBy()
                && rowFilter.indexHints.includes(config.name)
                && rowFilter.indexHints.included.size() > 1)
            {
                throw new InvalidRequestException(MULTIPLE_HINTS_WITH_ORDER_BY_ERROR_MESSAGE);
            }

            switch (operator)
            {
                case ANN:
                    float[] value = TypeUtil.decomposeVector(getValidator(), expression.getIndexValue());
                    VectorValidation.validateIndexable(value, vectorSimilarityFunction);
                    return;
                case BM25:
                    if (version().onOrAfter(Version.BM25_EARLIEST))
                        return;
                    throw new InvalidRequestException(String.format(INDEX_VERSION_DOES_NOT_SUPPORT_BM25, getIndexName()));
                default:
                    return;
            }
        }
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;

        if (!(obj instanceof IndexContext))
            return false;

        IndexContext other = (IndexContext) obj;

        return Objects.equals(column, other.column) &&
               Objects.equals(indexType, other.indexType) &&
               Objects.equals(fieldPath, other.fieldPath) &&
               Objects.equals(config, other.config) &&
               Objects.equals(partitionKeyType, other.partitionKeyType) &&
               Objects.equals(clusteringComparator, other.clusteringComparator);
    }

    public int hashCode()
    {
        return Objects.hash(column, indexType, fieldPath, config, partitionKeyType, clusteringComparator);
    }

    /**
     * A helper method for constructing consistent log messages for specific column indexes.
     *
     * Example: For the index "idx" in keyspace "ks" on table "tb", calling this method with the raw message
     * "Flushing new index segment..." will produce...
     *
     * "[ks.tb.idx] Flushing new index segment..."
     *
     * @param message The raw content of a logging message, without information identifying it with an index.
     *
     * @return A log message with the proper keyspace, table and index name prepended to it.
     */
    public String logMessage(String message)
    {
        // Index names are unique only within a keyspace.
        return String.format("[%s.%s.%s] %s", keyspace, table, config == null ? "?" : config.name, message);
    }

    /**
     * @return the indexes that are built on the given SSTables on the left and corrupted indexes'
     * corresponding contexts on the right
     */
    public Pair<Set<SSTableIndex>, Set<SSTableContext>> getBuiltIndexes(Collection<SSTableContext> sstableContexts, boolean validate)
    {
        Set<SSTableIndex> valid = ConcurrentHashMap.newKeySet();
        Set<SSTableContext> invalid = ConcurrentHashMap.newKeySet();

        sstableContexts.stream().parallel().forEach(context -> {
            if (context.sstable.isMarkedCompacted())
                return;

            var perSSTableComponents = context.usedPerSSTableComponents();
            var perIndexComponents = perSSTableComponents.indexDescriptor().perIndexComponents(this);
            if (!perSSTableComponents.isComplete() || !perIndexComponents.isComplete())
            {
                logger.debug(logMessage("An on-disk index build for SSTable {} has not completed (per-index components={})."), context.descriptor(), perIndexComponents.all());
                return;
            }

            try
            {
                if (validate)
                {
                    if (!perIndexComponents.validateComponents(context.sstable, cfs.getTracker(), false, false))
                    {
                        // Note that a precise warning is already logged by the validation if there is an issue.
                        invalid.add(context);
                        return;
                    }
                }

                SSTableIndex index = new SSTableIndex(context, perIndexComponents);
                if (SAI_INDEX_READS_DISABLED.getBoolean())
                {
                    logger.debug(logMessage("Skipped loading index for SSTable {} as it's disabled by {}"), context.descriptor(), SAI_INDEX_READS_DISABLED.getKey());
                }
                else
                {
                    long count = context.primaryKeyMapFactory().count();
                    if (count > 0)
                        logger.debug(logMessage("Successfully loaded index for SSTable {} with {} rows."), context.descriptor(), count);
                    else
                        logger.debug(logMessage("Skipped loading index for SSTable {} as it is empty."), context.descriptor());
                }

                // Try to add new index to the set, if set already has such index, we'll simply release and move on.
                // This covers situation when SSTable collection has the same SSTable multiple
                // times because we don't know what kind of collection it actually is.
                if (!valid.add(index))
                    index.release();
            }
            catch (Throwable e)
            {
                logger.error(logMessage("Failed to update per-column components for SSTable {}"), context.descriptor(), e);
                invalid.add(context);
            }
        });

        return Pair.create(valid, invalid);
    }

    /**
     * @return the number of indexed rows in this index (aka. pair of term and rowId)
     */
    public long getCellCount()
    {
        return getView().getIndexes()
                        .stream()
                        .mapToLong(SSTableIndex::getRowCount)
                        .sum();
    }

    /**
     * @return the total size (in bytes) of per-column index components
     */
    public long diskUsage()
    {
        return getView().getIndexes()
                        .stream()
                        .mapToLong(SSTableIndex::sizeOfPerColumnComponents)
                        .sum();
    }

    /**
     * @return the total memory usage (in bytes) of per-column index on-disk data structure
     */
    public long indexFileCacheSize()
    {
        return getView().getIndexes()
                        .stream()
                        .mapToLong(SSTableIndex::indexFileCacheSize)
                        .sum();
    }

    public IndexFeatureSet indexFeatureSet()
    {
        IndexFeatureSet.Accumulator accumulator = new IndexFeatureSet.Accumulator(version);
        getView().getIndexes().stream().map(SSTableIndex::indexFeatureSet).forEach(set -> accumulator.accumulate(set));
        return accumulator.complete();
    }
}
