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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.QualifiedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.metrics.CQLMetrics;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.metrics.ClientRequestsMetrics;
import org.apache.cassandra.metrics.ClientRequestsMetricsProvider;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.config.CassandraRelevantProperties.ENABLE_NODELOCAL_QUERIES;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

public class QueryProcessor implements QueryHandler
{
    public static final CassandraVersion CQL_VERSION = new CassandraVersion("3.4.5");

    // See comments on QueryProcessor #prepare
    public static final CassandraVersion NEW_PREPARED_STATEMENT_BEHAVIOUR_SINCE_30 = new CassandraVersion("3.0.26");
    public static final CassandraVersion NEW_PREPARED_STATEMENT_BEHAVIOUR_SINCE_3X = new CassandraVersion("3.11.12");
    public static final CassandraVersion NEW_PREPARED_STATEMENT_BEHAVIOUR_SINCE_40 = new CassandraVersion("4.0.2");

    public static final QueryProcessor instance = new QueryProcessor();

    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);

    // DSP-24330 heavy contention (almost deadlock) on Caffeine cache on insertions while measuring entry size. We
    // precompute sizes instead to reduce contention on Caffeine's underlying stipped locking structures. We also avoid
    // duplication of work as every statement was being measured 2 times: before instertion to check size limits and
    // upon insertion by Caffeine
    private static final Cache<MD5Digest, Pair<Prepared, Integer>> preparedStatements;

    // A map for prepared statements used internally (which we don't want to mix with user statement, in particular we don't
    // bother with expiration on those.
    private static final ConcurrentMap<String, Prepared> internalStatements = new ConcurrentHashMap<>();

    // Direct calls to processStatement do not increment the preparedStatementsExecuted/regularStatementsExecuted
    // counters. Callers of processStatement are responsible for correctly notifying metrics
    public static final CQLMetrics metrics = new CQLMetrics();

    private static final AtomicInteger lastMinuteEvictionsCount = new AtomicInteger(0);

    private final List<QueryInterceptor> interceptors = new ArrayList<>();

    static
    {
        preparedStatements = Caffeine.newBuilder()
                             .executor(MoreExecutors.directExecutor())
                             .maximumWeight(capacityToBytes(DatabaseDescriptor.getPreparedStatementsCacheSizeMB()))
                             .weigher(QueryProcessor::getPrecomputedSize)
                             .removalListener((key, prepared, cause) -> {
                                 MD5Digest md5Digest = (MD5Digest) key;
                                 if (cause.wasEvicted())
                                 {
                                     metrics.preparedStatementsEvicted.inc();
                                     lastMinuteEvictionsCount.incrementAndGet();
                                     SystemKeyspace.removePreparedStatement(md5Digest);
                                 }
                             }).build();

        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(() -> {
            long count = lastMinuteEvictionsCount.getAndSet(0);
            if (count > 0)
                logger.warn("{} prepared statements discarded in the last minute because cache limit reached ({} MB)",
                            count,
                            DatabaseDescriptor.getPreparedStatementsCacheSizeMB());
        }, 1, 1, TimeUnit.MINUTES);

        logger.debug("Initialized prepared statement caches with {} MB",
                     DatabaseDescriptor.getPreparedStatementsCacheSizeMB());
    }

    private static long capacityToBytes(long cacheSizeMB)
    {
        return cacheSizeMB * 1024 * 1024;
    }

    public static int preparedStatementsCount()
    {
        return preparedStatements.asMap().size();
    }

    // Work around initialization dependency
    private enum InternalStateInstance
    {
        INSTANCE;

        private final ClientState clientState;

        InternalStateInstance()
        {
            clientState = ClientState.forInternalCalls(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        }
    }

    public void preloadPreparedStatements()
    {
        int count = SystemKeyspace.loadPreparedStatements((id, query, keyspace) -> {
            try
            {
                ClientState clientState = ClientState.forInternalCalls();
                if (keyspace != null)
                    clientState.setKeyspace(keyspace);

                Prepared prepared = parseAndPrepare(query, clientState, false);
                int precomputedCacheEntrySize = measurePStatementCacheEntrySize(id, prepared);
                Pair<Prepared, Integer> cacheValue = Pair.create(prepared, precomputedCacheEntrySize);
                preparedStatements.put(id, cacheValue);

                // Preload `null` statement for non-fully qualified statements, since it can't be parsed if loaded from cache and will be dropped
                if (!prepared.fullyQualified)
                    preparedStatements.get(computeId(query, null), (ignored_) -> cacheValue);
                return true;
            }
            catch (RequestValidationException e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                logger.warn(String.format("Prepared statement recreation error, removing statement: %s %s %s", id, query, keyspace));
                SystemKeyspace.removePreparedStatement(id);
                return false;
            }
        });
        logger.debug("Preloaded {} prepared statements", count);
    }


    /**
     * Clears the prepared statement cache.
     * @param memoryOnly {@code true} if only the in memory caches must be cleared, {@code false} otherwise.
     */
    @VisibleForTesting
    public static void clearPreparedStatements(boolean memoryOnly)
    {
        preparedStatements.invalidateAll();
        if (!memoryOnly)
            SystemKeyspace.resetPreparedStatements();
    }

    @VisibleForTesting
    public static ConcurrentMap<String, Prepared> getInternalStatements()
    {
        return internalStatements;
    }

    @VisibleForTesting
    public static QueryState internalQueryState()
    {
        return new QueryState(InternalStateInstance.INSTANCE.clientState);
    }

    private QueryProcessor()
    {
        Schema.instance.registerListener(new StatementInvalidatingListener());
    }

    @VisibleForTesting
    public void evictPrepared(MD5Digest id)
    {
        preparedStatements.invalidate(id);
        SystemKeyspace.removePreparedStatement(id);
    }

    public HashMap<MD5Digest, Prepared> getPreparedStatements()
    {
        HashMap<MD5Digest, Prepared> res = new HashMap<>(preparedStatements.asMap().size());
        for(Map.Entry<MD5Digest, Pair<Prepared, Integer>> entry: preparedStatements.asMap().entrySet())
            res.put(entry.getKey(), entry.getValue().left());

        return res;
    }

    public Prepared getPrepared(MD5Digest id)
    {
        return extractPreparedFromPair(preparedStatements.getIfPresent(id));
    }

    public static void validateKey(ByteBuffer key) throws InvalidRequestException
    {
        if (key == null || key.remaining() == 0)
        {
            throw new InvalidRequestException("Key may not be empty");
        }
        if (key == ByteBufferUtil.UNSET_BYTE_BUFFER)
            throw new InvalidRequestException("Key may not be unset");

        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new InvalidRequestException("Key length of " + key.remaining() +
                                              " is longer than maximum of " + FBUtilities.MAX_UNSIGNED_SHORT);
        }
    }

    public ResultMessage processStatement(CQLStatement statement, QueryState queryState, QueryOptions options, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        return processStatement(statement, queryState, options, Collections.emptyMap(), queryStartNanoTime);
    }

    public ResultMessage processStatement(CQLStatement statement, QueryState queryState, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        logger.trace("Process {} @CL.{}", statement, options.getConsistency());
        ClientState clientState = queryState.getClientState();

        Tracing.trace("Authorizing against client state");
        statement.authorize(clientState);
        statement.validate(queryState);

        Tracing.setupTracedKeyspace(statement);

        for (QueryInterceptor interceptor: interceptors)
        {
            ResultMessage result = interceptor.interceptStatement(statement, queryState, options, customPayload, queryStartNanoTime);

            if (result != null)
                return result;
        }

        Tracing.trace("Executing prepared statement");
        ResultMessage result = options.getConsistency() == ConsistencyLevel.NODE_LOCAL
                               ? processNodeLocalStatement(statement, queryState, options, queryStartNanoTime)
                               : statement.execute(queryState, options, queryStartNanoTime);

        return result == null ? new ResultMessage.Void() : result;
    }

    /**
     * Register a new {@link QueryInterceptor}
     * @param interceptor the {@link QueryInterceptor} to register
     */
    public void registerInterceptor(QueryInterceptor interceptor)
    {
        interceptors.add(Objects.requireNonNull(interceptor));
    }

    @VisibleForTesting
    public void clearInterceptors()
    {
        interceptors.clear();
    }

    private ResultMessage processNodeLocalStatement(CQLStatement statement, QueryState queryState, QueryOptions options, long queryStartNanoTime)
    {
        if (!ENABLE_NODELOCAL_QUERIES.getBoolean())
            throw new InvalidRequestException("NODE_LOCAL consistency level is highly dangerous and should be used only for debugging purposes");

        if (statement instanceof BatchStatement || statement instanceof ModificationStatement)
            return processNodeLocalWrite(statement, queryState, options, queryStartNanoTime);
        else if (statement instanceof SelectStatement)
            return processNodeLocalSelect((SelectStatement) statement, queryState, options, queryStartNanoTime);
        else
            throw new InvalidRequestException("NODE_LOCAL consistency level can only be used with BATCH, UPDATE, INSERT, DELETE, and SELECT statements");
    }

    private ResultMessage processNodeLocalWrite(CQLStatement statement, QueryState queryState, QueryOptions options, long queryStartNanoTime)
    {
        ClientRequestsMetrics metrics = ClientRequestsMetricsProvider.instance.metrics((statement instanceof QualifiedStatement) ? ((QualifiedStatement) statement).keyspace() : null);
        ClientRequestMetrics  levelMetrics = metrics.writeMetricsForLevel(ConsistencyLevel.NODE_LOCAL);
        ClientRequestMetrics globalMetrics = metrics.writeMetrics;

        long startTime = System.nanoTime();
        try
        {
            return statement.executeLocally(queryState, options);
        }
        finally
        {
            long endTime = System.nanoTime();
            long latency = endTime - startTime;
            levelMetrics.executionTimeMetrics.addNano(latency);
            levelMetrics.serviceTimeMetrics.addNano(endTime - queryStartNanoTime);
            globalMetrics.executionTimeMetrics.addNano(latency);
            globalMetrics.serviceTimeMetrics.addNano(endTime - queryStartNanoTime);
        }
    }

    private ResultMessage processNodeLocalSelect(SelectStatement statement, QueryState queryState, QueryOptions options, long queryStartNanoTime)
    {
        ClientRequestsMetrics metrics = ClientRequestsMetricsProvider.instance.metrics(statement.keyspace());
        ClientRequestMetrics levelMetrics = metrics.readMetricsForLevel(ConsistencyLevel.NODE_LOCAL);
        ClientRequestMetrics globalMetrics = metrics.readMetrics;

        if (StorageService.instance.isBootstrapMode() && !SchemaConstants.isLocalSystemKeyspace(statement.keyspace()))
        {
            levelMetrics.unavailables.mark();
            globalMetrics.unavailables.mark();
            throw new IsBootstrappingException();
        }

        long startTime = System.nanoTime();
        try
        {
            return statement.executeLocally(queryState, options);
        }
        finally
        {
            long endTime = System.nanoTime();
            long latency = endTime - startTime;
            levelMetrics.executionTimeMetrics.addNano(latency);
            levelMetrics.serviceTimeMetrics.addNano(endTime - queryStartNanoTime);
            globalMetrics.executionTimeMetrics.addNano(latency);
            globalMetrics.serviceTimeMetrics.addNano(endTime - queryStartNanoTime);
        }
    }

    public static ResultMessage process(String queryString, ConsistencyLevel cl, QueryState queryState, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        QueryOptions options = QueryOptions.forInternalCalls(cl, Collections.<ByteBuffer>emptyList());
        CQLStatement statement = instance.parse(queryString, queryState, options);
        return instance.process(statement, queryState, options, queryStartNanoTime);
    }

    public CQLStatement parse(String queryString, QueryState queryState, QueryOptions options)
    {
        return getStatement(queryString, queryState.getClientState().cloneWithKeyspaceIfSet(options.getKeyspace()));
    }

    public ResultMessage process(CQLStatement prepared,
                                 QueryState queryState,
                                 QueryOptions options,
                                 Map<String, ByteBuffer> customPayload,
                                 long queryStartNanoTime) throws RequestExecutionException, RequestValidationException
    {
        options.prepare(prepared.getBindVariables());
        if (prepared.getBindVariables().size() != options.getValues().size())
            throw new InvalidRequestException("Invalid amount of bind variables");

        if (!queryState.getClientState().isInternal)
            metrics.regularStatementsExecuted.inc();

        return processStatement(prepared, queryState, options, customPayload, queryStartNanoTime);
    }

    public ResultMessage process(CQLStatement prepared, QueryState queryState, QueryOptions options, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        return process(prepared, queryState, options, Collections.emptyMap(), queryStartNanoTime);
    }

    public static CQLStatement parseStatement(String queryStr, ClientState clientState) throws RequestValidationException
    {
        return getStatement(queryStr, clientState);
    }

    public static UntypedResultSet process(String query, ConsistencyLevel cl) throws RequestExecutionException
    {
        return process(query, cl, Collections.<ByteBuffer>emptyList());
    }

    public static UntypedResultSet process(String query, ConsistencyLevel cl, List<ByteBuffer> values) throws RequestExecutionException
    {
        QueryState queryState = QueryState.forInternalCalls();
        QueryOptions options = QueryOptions.forInternalCalls(cl, values);
        CQLStatement statement = instance.parse(query, queryState, options);
        ResultMessage result = instance.process(statement, queryState, options, System.nanoTime());
        if (result instanceof ResultMessage.Rows)
            return UntypedResultSet.create(((ResultMessage.Rows)result).result);
        else
            return null;
    }

    @VisibleForTesting
    public static QueryOptions makeInternalOptions(CQLStatement prepared, Object[] values)
    {
        return makeInternalOptions(prepared, values, ConsistencyLevel.ONE);
    }

    private static QueryOptions makeInternalOptions(CQLStatement prepared, Object[] values, ConsistencyLevel cl)
    {
        if (prepared.getBindVariables().size() != values.length)
            throw new IllegalArgumentException(String.format("Invalid number of values. Expecting %d but got %d", prepared.getBindVariables().size(), values.length));

        List<ByteBuffer> boundValues = new ArrayList<>(values.length);
        for (int i = 0; i < values.length; i++)
        {
            Object value = values[i];
            AbstractType type = prepared.getBindVariables().get(i).type;
            boundValues.add(value instanceof ByteBuffer || value == null ? (ByteBuffer)value : type.decompose(value));
        }
        return QueryOptions.forInternalCalls(cl, boundValues);
    }

    public static Prepared prepareInternal(String query) throws RequestValidationException
    {
        Prepared prepared = internalStatements.get(query);
        if (prepared != null)
            return prepared;

        prepared = parseAndPrepare(query, internalQueryState().getClientState(), true);
        internalStatements.put(query, prepared);
        return prepared;
    }

    public static Prepared parseAndPrepare(String query, ClientState clientState, boolean isInternal) throws RequestValidationException
    {
        CQLStatement.Raw raw = parseStatement(query);

        boolean fullyQualified = false;
        String keyspace = null;

        // Set keyspace for statement that require login
        if (raw instanceof QualifiedStatement)
        {
            QualifiedStatement qualifiedStatement = ((QualifiedStatement) raw);
            fullyQualified = qualifiedStatement.isFullyQualified();
            qualifiedStatement.setKeyspace(clientState);
            keyspace = qualifiedStatement.keyspace();
        }

        // Note: if 2 threads prepare the same query, we'll live so don't bother synchronizing
        CQLStatement statement = raw.prepare(clientState);
        statement.validate(new QueryState(clientState));

        return new Prepared(statement, fullyQualified, keyspace);
    }

    public static UntypedResultSet executeInternal(String query, Object... values)
    {
        Prepared prepared = prepareInternal(query);
        ResultMessage result = prepared.statement.executeLocally(internalQueryState(), makeInternalOptions(prepared.statement, values));
        if (result instanceof ResultMessage.Rows)
            return UntypedResultSet.create(((ResultMessage.Rows)result).result);
        else
            return null;
    }

    public static UntypedResultSet execute(String query, ConsistencyLevel cl, Object... values)
    throws RequestExecutionException
    {
        return execute(query, cl, internalQueryState(), values);
    }

    public static UntypedResultSet execute(String query, ConsistencyLevel cl, QueryState state, Object... values)
    throws RequestExecutionException
    {
        Prepared prepared = prepareInternal(query);
        ResultMessage<?> result = prepared.statement.execute(state, makeInternalOptions(prepared.statement, values, cl), System.nanoTime());
        if (result instanceof ResultMessage.Rows)
            return UntypedResultSet.create(((ResultMessage.Rows)result).result);
        else
            return null;
    }

    /**
     * Same than {@link #execute(String, ConsistencyLevel, Object...)}, but to use for queries we know are only executed
     * once so that the created statement object is not cached.
     */
    @VisibleForTesting
    static UntypedResultSet executeOnce(String query, ConsistencyLevel cl, Object... values)
    {
        QueryState queryState = internalQueryState();
        CQLStatement statement = parseStatement(query, queryState.getClientState());
        statement.validate(queryState);
        ResultMessage<?> result = statement.execute(queryState, makeInternalOptions(statement, values, cl), System.nanoTime());
        if (result instanceof ResultMessage.Rows)
            return UntypedResultSet.create(((ResultMessage.Rows)result).result);
        else
            return null;
    }

    public static UntypedResultSet executeInternalWithPaging(String query, PageSize pageSize, Object... values)
    {
        Prepared prepared = prepareInternal(query);
        if (!(prepared.statement instanceof SelectStatement))
            throw new IllegalArgumentException("Only SELECTs can be paged");

        SelectStatement select = (SelectStatement)prepared.statement;
        QueryPager pager = select.getQuery(QueryState.forInternalCalls(), makeInternalOptions(prepared.statement, values), FBUtilities.nowInSeconds()).getPager(null, ProtocolVersion.CURRENT);
        return UntypedResultSet.create(select, pager, pageSize);
    }

    /**
     * Same than executeLocally, but to use for queries we know are only executed once so that the
     * created statement object is not cached.
     */
    public static UntypedResultSet executeOnceInternal(String query, Object... values)
    {
        return executeOnceInternal(internalQueryState(), query, values);
    }

    /**
     * Execute an internal query with the provided {@code nowInSec} and {@code timestamp} for the {@code QueryState}.
     * <p>This method ensure that the statement will not be cached in the prepared statement cache.</p>
     */
    @VisibleForTesting
    public static UntypedResultSet executeOnceInternalWithNowAndTimestamp(int nowInSec, long timestamp, String query, Object... values)
    {
        QueryState queryState = new QueryState(InternalStateInstance.INSTANCE.clientState, timestamp, nowInSec);
        return executeOnceInternal(queryState, query, values);
    }

    private static UntypedResultSet executeOnceInternal(QueryState queryState, String query, Object... values)
    {
        CQLStatement statement = parseStatement(query, queryState.getClientState());
        statement.validate(queryState);
        ResultMessage result = statement.executeLocally(queryState, makeInternalOptions(statement, values));
        if (result instanceof ResultMessage.Rows)
            return UntypedResultSet.create(((ResultMessage.Rows)result).result);
        else
            return null;
    }

    /**
     * A special version of executeLocally that takes the time used as "now" for the query in argument.
     * Note that this only make sense for Selects so this only accept SELECT statements and is only useful in rare
     * cases.
     */
    public static UntypedResultSet executeInternalWithNow(int nowInSec, long queryStartNanoTime, String query, Object... values)
    {
        Prepared prepared = prepareInternal(query);
        assert prepared.statement instanceof SelectStatement;
        SelectStatement select = (SelectStatement)prepared.statement;
        ResultMessage result = select.executeInternal(internalQueryState(), makeInternalOptions(prepared.statement, values), nowInSec, queryStartNanoTime);
        assert result instanceof ResultMessage.Rows;
        return UntypedResultSet.create(((ResultMessage.Rows)result).result);
    }

    public static UntypedResultSet resultify(String query, RowIterator partition)
    {
        return resultify(query, PartitionIterators.singletonIterator(partition));
    }

    public static UntypedResultSet resultify(String query, PartitionIterator partitions)
    {
        try (PartitionIterator iter = partitions)
        {
            SelectStatement ss = (SelectStatement) getStatement(query, null);
            ResultSet cqlRows = ss.process(iter, FBUtilities.nowInSeconds());
            return UntypedResultSet.create(cqlRows);
        }
    }

    public ResultMessage.Prepared prepare(String query,
                                          ClientState clientState,
                                          Map<String, ByteBuffer> customPayload) throws RequestValidationException
    {
        return prepare(query, clientState);
    }

    private volatile boolean newPreparedStatementBehaviour = false;
    public boolean useNewPreparedStatementBehaviour()
    {
        if (newPreparedStatementBehaviour || DatabaseDescriptor.getForceNewPreparedStatementBehaviour())
            return true;

        synchronized (this)
        {
            CassandraVersion minVersion = Gossiper.instance.getMinVersion();
            if ((minVersion.major == 3 && minVersion.minor == 0 && minVersion.compareTo(NEW_PREPARED_STATEMENT_BEHAVIOUR_SINCE_30) >= 0) ||
                (minVersion.major == 3 && minVersion.minor > 0 && minVersion.compareTo(NEW_PREPARED_STATEMENT_BEHAVIOUR_SINCE_3X) >= 0) ||
                (minVersion.compareTo(NEW_PREPARED_STATEMENT_BEHAVIOUR_SINCE_40, true) >= 0))
            {
                logger.info("Fully upgraded to at least {}", minVersion);
                newPreparedStatementBehaviour = true;
            }

            return newPreparedStatementBehaviour;
        }
    }

    /**
     * This method got slightly out of hand, but this is with best intentions: to allow users to be upgraded from any
     * prior version, and help implementers avoid previous mistakes by clearly separating fully qualified and non-fully
     * qualified statement behaviour.
     *
     * Basically we need to handle 4 different hashes here;
     * 1. fully qualified query with keyspace
     * 2. fully qualified query without keyspace
     * 3. unqualified query with keyspace
     * 4. unqualified query without keyspace
     *
     * The correct combination to return is 2/3 - the problem is during upgrades (assuming upgrading from < 3.0.26)
     * - Existing clients have hash 1 or 3
     * - Query prepared on a 3.0.25/3.11.12/4.0.2 instance needs to return hash 1/3 to be able to execute it on a 3.0.25 instance
     * - This is handled by the useNewPreparedStatementBehaviour flag - while there still are 3.0.25 instances in
     *   the cluster we always return hash 1/3
     * - Once fully upgraded we start returning hash 2/3, this will cause a prepared statement id mismatch for existing
     *   clients, but they will be able to continue using the old prepared statement id after that exception since we
     *   store the query both with and without keyspace.
     */
    public ResultMessage.Prepared prepare(String queryString, ClientState clientState)
    {
        boolean useNewPreparedStatementBehaviour = useNewPreparedStatementBehaviour();
        MD5Digest hashWithoutKeyspace = computeId(queryString, null);
        MD5Digest hashWithKeyspace = computeId(queryString, clientState.getRawKeyspace());
        Prepared cachedWithoutKeyspace = extractPreparedFromPair(preparedStatements.getIfPresent(hashWithoutKeyspace));
        Prepared cachedWithKeyspace = extractPreparedFromPair(preparedStatements.getIfPresent(hashWithKeyspace));
        // We assume it is only safe to return cached prepare if we have both instances
        boolean safeToReturnCached = cachedWithoutKeyspace != null && cachedWithKeyspace != null;

        if (safeToReturnCached)
        {
            if (useNewPreparedStatementBehaviour)
            {
                if (cachedWithoutKeyspace.fullyQualified) // For fully qualified statements, we always skip keyspace to avoid digest switching
                    return createResultMessage(hashWithoutKeyspace, cachedWithoutKeyspace);

                if (clientState.getRawKeyspace() != null && !cachedWithKeyspace.fullyQualified) // For non-fully qualified statements, we always include keyspace to avoid ambiguity
                    return createResultMessage(hashWithKeyspace, cachedWithKeyspace);

            }
            else // legacy caches, pre-CASSANDRA-15252 behaviour
            {
                return createResultMessage(hashWithKeyspace, cachedWithKeyspace);
            }
        }
        else
        {
            // Make sure the missing one is going to be eventually re-prepared
            evictPrepared(hashWithKeyspace);
            evictPrepared(hashWithoutKeyspace);
        }

        Prepared prepared = parseAndPrepare(queryString, clientState, false);
        CQLStatement statement = prepared.statement;

        int boundTerms = statement.getBindVariables().size();
        if (boundTerms > FBUtilities.MAX_UNSIGNED_SHORT)
            throw new InvalidRequestException(String.format("Too many markers(?). %d markers exceed the allowed maximum of %d", boundTerms, FBUtilities.MAX_UNSIGNED_SHORT));

        if (prepared.fullyQualified)
        {
            ResultMessage.Prepared qualifiedWithoutKeyspace = storePreparedStatement(queryString, null, prepared);
            ResultMessage.Prepared qualifiedWithKeyspace = null;
            if (clientState.getRawKeyspace() != null)
                qualifiedWithKeyspace = storePreparedStatement(queryString, clientState.getRawKeyspace(), prepared);

            if (!useNewPreparedStatementBehaviour && qualifiedWithKeyspace != null)
                return qualifiedWithKeyspace;

            return qualifiedWithoutKeyspace;
        }
        else
        {
            clientState.warnAboutUseWithPreparedStatements(hashWithKeyspace, clientState.getRawKeyspace());

            ResultMessage.Prepared nonQualifiedWithKeyspace = storePreparedStatement(queryString, clientState.getRawKeyspace(), prepared);
            ResultMessage.Prepared nonQualifiedWithNullKeyspace = storePreparedStatement(queryString, null, prepared);
            if (!useNewPreparedStatementBehaviour)
                return nonQualifiedWithNullKeyspace;

            return nonQualifiedWithKeyspace;
        }
    }

    private static MD5Digest computeId(String queryString, String keyspace)
    {
        String toHash = keyspace == null ? queryString : keyspace + queryString;
        return MD5Digest.compute(toHash);
    }

    @VisibleForTesting
    public static ResultMessage.Prepared getStoredPreparedStatement(String queryString, String clientKeyspace)
    throws InvalidRequestException
    {
        MD5Digest statementId = computeId(queryString, clientKeyspace);
        Prepared existing = extractPreparedFromPair(preparedStatements.getIfPresent(statementId));
        if (existing == null)
            return null;

        checkTrue(queryString.equals(existing.statement.getRawCQLStatement()),
                String.format("MD5 hash collision: query with the same MD5 hash was already prepared. \n Existing: '%s'", existing.statement.getRawCQLStatement()));

        return createResultMessage(statementId, existing);
    }

    @VisibleForTesting
    private static ResultMessage.Prepared createResultMessage(MD5Digest statementId, Prepared existing)
    throws InvalidRequestException
    {
        ResultSet.PreparedMetadata preparedMetadata = ResultSet.PreparedMetadata.fromPrepared(existing.statement);
        ResultSet.ResultMetadata resultMetadata = ResultSet.ResultMetadata.fromPrepared(existing.statement);
        return new ResultMessage.Prepared(statementId, resultMetadata.getResultMetadataId(), preparedMetadata, resultMetadata);
    }

    @VisibleForTesting
    public static ResultMessage.Prepared storePreparedStatement(String queryString, String keyspace, Prepared prepared)
    throws InvalidRequestException
    {
        // Concatenate the current keyspace so we don't mix prepared statements between keyspace (#5352).
        // (if the keyspace is null, queryString has to have a fully-qualified keyspace so it's fine.

        MD5Digest statementId = computeId(queryString, keyspace);
        int precomputedCacheEntrySize = measurePStatementCacheEntrySize(statementId, prepared);
        // don't execute the statement if it's bigger than the allowed threshold
        if (precomputedCacheEntrySize > capacityToBytes(DatabaseDescriptor.getPreparedStatementsCacheSizeMB()))
            throw new InvalidRequestException(String.format("Prepared statement of size %d bytes is larger than allowed maximum of %d MB: %s...",
                                                            precomputedCacheEntrySize,
                                                            DatabaseDescriptor.getPreparedStatementsCacheSizeMB(),
                                                            queryString.substring(0, 200)));

        Pair<Prepared, Integer> cacheValue = Pair.create(prepared, precomputedCacheEntrySize);
        Pair<Prepared, Integer> previous = preparedStatements.get(statementId, (ignored_) -> cacheValue);
        if (previous != null && previous.left() == prepared)
            SystemKeyspace.writePreparedStatement(keyspace, statementId, queryString);

        ResultSet.PreparedMetadata preparedMetadata = ResultSet.PreparedMetadata.fromPrepared(prepared.statement);
        ResultSet.ResultMetadata resultMetadata = ResultSet.ResultMetadata.fromPrepared(prepared.statement);
        return new ResultMessage.Prepared(statementId, resultMetadata.getResultMetadataId(), preparedMetadata, resultMetadata);
    }

    public ResultMessage processPrepared(CQLStatement statement,
                                         QueryState state,
                                         QueryOptions options,
                                         Map<String, ByteBuffer> customPayload,
                                         long queryStartNanoTime)
                                                 throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> variables = options.getValues();
        // Check to see if there are any bound variables to verify
        if (!(variables.isEmpty() && statement.getBindVariables().isEmpty()))
        {
            if (variables.size() != statement.getBindVariables().size())
                throw new InvalidRequestException(String.format("there were %d markers(?) in CQL but %d bound variables",
                                                                statement.getBindVariables().size(),
                                                                variables.size()));

            // at this point there is a match in count between markers and variables that is non-zero
            if (logger.isTraceEnabled())
                for (int i = 0; i < variables.size(); i++)
                    logger.trace("[{}] '{}'", i+1, variables.get(i));
        }

        metrics.preparedStatementsExecuted.inc();
        return processStatement(statement, state, options, customPayload, queryStartNanoTime);
    }

    public ResultMessage processPrepared(CQLStatement statement, QueryState queryState, QueryOptions options, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        return processPrepared(statement, queryState, options, Collections.emptyMap(), queryStartNanoTime);
    }

    public ResultMessage processBatch(BatchStatement batch,
                                      QueryState queryState,
                                      BatchQueryOptions options,
                                      Map<String, ByteBuffer> customPayload,
                                      long queryStartNanoTime)
                                              throws RequestExecutionException, RequestValidationException
    {
        ClientState clientState = queryState.getClientState().cloneWithKeyspaceIfSet(options.getKeyspace());
        Tracing.setupTracedKeyspace(batch);

        for (QueryInterceptor interceptor: interceptors)
        {
            ResultMessage result = interceptor.interceptBatchStatement(batch, queryState, options, customPayload, queryStartNanoTime);

            if (result != null)
                return result;
        }

        Tracing.trace("Authorizing batch");
        batch.authorize(clientState);
        batch.validate();
        batch.validate(queryState);
        Tracing.trace("Executing batch");
        return batch.execute(queryState, options, queryStartNanoTime);
    }

    public ResultMessage processBatch(BatchStatement batch, QueryState queryState, BatchQueryOptions options, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        return processBatch(batch, queryState, options, Collections.emptyMap(), queryStartNanoTime);
    }

    public static CQLStatement getStatement(String queryStr, ClientState clientState)
    throws RequestValidationException
    {
        Tracing.trace("Parsing {}", queryStr);
        CQLStatement.Raw statement = parseStatement(queryStr);

        Tracing.trace("Preparing statement");
        return statement.prepare(clientState);
    }

    public static <T extends CQLStatement.Raw> T parseStatement(String queryStr, Class<T> klass, String type) throws SyntaxException
    {
        try
        {
            CQLStatement.Raw stmt = parseStatement(queryStr);

            if (!klass.isAssignableFrom(stmt.getClass()))
                throw new IllegalArgumentException("Invalid query, must be a " + type + " statement but was: " + stmt.getClass());

            return klass.cast(stmt);
        }
        catch (RequestValidationException e)
        {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }
    public static CQLStatement.Raw parseStatement(String queryStr) throws SyntaxException
    {
        try
        {
            CQLStatement.Raw stmt = CQLFragmentParser.parseAnyUnhandled(CqlParser::query, queryStr);
            stmt.setRawCQLStatement(queryStr);
            return stmt;
        }
        catch (CassandraException ce)
        {
            throw ce;
        }
        catch (RuntimeException re)
        {
            logger.error(String.format("The statement: [%s] could not be parsed.", queryStr), re);
            throw new SyntaxException(String.format("Failed parsing statement: [%s] reason: %s %s",
                                                    queryStr,
                                                    re.getClass().getSimpleName(),
                                                    re.getMessage()));
        }
        catch (RecognitionException e)
        {
            throw new SyntaxException("Invalid or malformed CQL query string: " + e.getMessage());
        }
    }

    private static int measurePStatementCacheEntrySize(Object key, Prepared value)
    {
        Pair<Prepared, Integer> valuePair = Pair.create(value, 0);
        return Ints.checkedCast(ObjectSizes.measureDeep(key) + ObjectSizes.measureDeep(valuePair));
    }

    private static int getPrecomputedSize(Object key, Pair<Prepared, Integer> value)
    {
        return value.right;
    }

    /**
     * Clear our internal statmeent cache for test purposes.
     */
    @VisibleForTesting
    public static void clearInternalStatementsCache()
    {
        internalStatements.clear();
    }

    @VisibleForTesting
    public static void clearPreparedStatementsCache()
    {
        preparedStatements.asMap().clear();
    }

    private static Prepared extractPreparedFromPair(Pair<Prepared, Integer> pair)
    {
        return pair != null ? pair.left : null;
    }

    private static class StatementInvalidatingListener implements SchemaChangeListener
    {
        private static void removeInvalidPreparedStatements(String ksName, String cfName)
        {
            removeInvalidPreparedStatements(internalStatements.values().iterator(), ksName, cfName);
            removeInvalidPersistentPreparedStatements(preparedStatements.asMap().entrySet().iterator(), ksName, cfName);
        }

        private static void removeInvalidPreparedStatementsForFunction(String ksName, String functionName)
        {
            Predicate<Function> matchesFunction = f -> ksName.equals(f.name().keyspace) && functionName.equals(f.name().name);

            for (Iterator<Map.Entry<MD5Digest, Pair<Prepared, Integer>>> iter = preparedStatements.asMap().entrySet().iterator();
                 iter.hasNext();)
            {
                Map.Entry<MD5Digest, Pair<Prepared, Integer>> pstmt = iter.next();
                if (Iterables.any(pstmt.getValue().left().statement.getFunctions(), matchesFunction))
                {
                    SystemKeyspace.removePreparedStatement(pstmt.getKey());
                    iter.remove();
                }
            }


            Iterators.removeIf(internalStatements.values().iterator(),
                               statement -> Iterables.any(statement.statement.getFunctions(), matchesFunction));
        }

        private static void removeInvalidPersistentPreparedStatements(Iterator<Map.Entry<MD5Digest, Pair<Prepared, Integer>>> iterator,
                                                                      String ksName, String cfName)
        {
            while (iterator.hasNext())
            {
                Map.Entry<MD5Digest, Pair<Prepared, Integer>> entry = iterator.next();
                if (shouldInvalidate(ksName, cfName, entry.getValue().left().statement))
                {
                    SystemKeyspace.removePreparedStatement(entry.getKey());
                    iterator.remove();
                }
            }
        }

        private static void removeInvalidPreparedStatements(Iterator<Prepared> iterator, String ksName, String cfName)
        {
            while (iterator.hasNext())
            {
                if (shouldInvalidate(ksName, cfName, iterator.next().statement))
                    iterator.remove();
            }
        }

        private static boolean shouldInvalidate(String ksName, String cfName, CQLStatement statement)
        {
            String statementKsName;
            String statementCfName;

            if (statement instanceof ModificationStatement)
            {
                ModificationStatement modificationStatement = ((ModificationStatement) statement);
                statementKsName = modificationStatement.keyspace();
                statementCfName = modificationStatement.columnFamily();
            }
            else if (statement instanceof SelectStatement)
            {
                SelectStatement selectStatement = ((SelectStatement) statement);
                statementKsName = selectStatement.keyspace();
                statementCfName = selectStatement.columnFamily();
            }
            else if (statement instanceof BatchStatement)
            {
                BatchStatement batchStatement = ((BatchStatement) statement);
                for (ModificationStatement stmt : batchStatement.getStatements())
                {
                    if (shouldInvalidate(ksName, cfName, stmt))
                        return true;
                }
                return false;
            }
            else
            {
                return false;
            }

            return ksName.equals(statementKsName) && (cfName == null || cfName.equals(statementCfName));
        }

        @Override
        public void onCreateFunction(UDFunction function)
        {
            onCreateFunctionInternal(function.name().keyspace, function.name().name, function.argTypes());
        }

        @Override
        public void onCreateAggregate(UDAggregate aggregate)
        {
            onCreateFunctionInternal(aggregate.name().keyspace, aggregate.name().name, aggregate.argTypes());
        }

        private static void onCreateFunctionInternal(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
            // in case there are other overloads, we have to remove all overloads since argument type
            // matching may change (due to type casting)
            if (Schema.instance.getKeyspaceMetadata(ksName).userFunctions.get(new FunctionName(ksName, functionName)).size() > 1)
                removeInvalidPreparedStatementsForFunction(ksName, functionName);
        }

        @Override
        public void onAlterTable(TableMetadata before, TableMetadata after, boolean affectsStatements)
        {
            logger.trace("Column definitions for {}.{} changed, invalidating related prepared statements", before.keyspace, before.name);
            if (affectsStatements)
                removeInvalidPreparedStatements(before.keyspace, before.name);
        }

        @Override
        public void onAlterFunction(UDFunction before, UDFunction after)
        {
            // Updating a function may imply we've changed the body of the function, so we need to invalid statements so that
            // the new definition is picked (the function is resolved at preparation time).
            // TODO: if the function has multiple overload, we could invalidate only the statement refering to the overload
            // that was updated. This requires a few changes however and probably doesn't matter much in practice.
            removeInvalidPreparedStatementsForFunction(before.name().keyspace, before.name().name);
        }

        @Override
        public void onAlterAggregate(UDAggregate before, UDAggregate after)
        {
            // Updating a function may imply we've changed the body of the function, so we need to invalid statements so that
            // the new definition is picked (the function is resolved at preparation time).
            // TODO: if the function has multiple overload, we could invalidate only the statement refering to the overload
            // that was updated. This requires a few changes however and probably doesn't matter much in practice.
            removeInvalidPreparedStatementsForFunction(before.name().keyspace, before.name().name);
        }

        @Override
        public void onDropKeyspace(KeyspaceMetadata keyspace, boolean dropData)
        {
            logger.trace("Keyspace {} was dropped, invalidating related prepared statements", keyspace.name);
            removeInvalidPreparedStatements(keyspace.name, null);
        }

        @Override
        public void onDropTable(TableMetadata table, boolean dropData)
        {
            logger.trace("Table {}.{} was dropped, invalidating related prepared statements", table.keyspace, table.name);
            removeInvalidPreparedStatements(table.keyspace, table.name);
        }

        @Override
        public void onDropFunction(UDFunction function)
        {
            removeInvalidPreparedStatementsForFunction(function.name().keyspace, function.name().name);
        }

        @Override
        public void onDropAggregate(UDAggregate aggregate)
        {
            removeInvalidPreparedStatementsForFunction(aggregate.name().keyspace, aggregate.name().name);
        }
    }
}
