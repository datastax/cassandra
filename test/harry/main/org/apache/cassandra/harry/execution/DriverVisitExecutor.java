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

package org.apache.cassandra.harry.execution;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.BootstrappingException;
import com.datastax.driver.core.exceptions.BusyConnectionException;
import com.datastax.driver.core.exceptions.BusyPoolException;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.exceptions.CrcMismatchException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.FrameTooLongException;
import com.datastax.driver.core.exceptions.FunctionExecutionException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.core.exceptions.ProtocolError;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.exceptions.ReadFailureException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.ServerError;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.exceptions.WriteFailureException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.op.Visit;

import static org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor.PageSizeSelector;

/**
 * Executes visits against a real cluster over the native protocol, through the Java driver.
 * <p>
 * Every write carries an explicit timestamp, so it is idempotent, and every statement is retried on errors that
 * leave the cluster unchanged or change it only in a way that the retry repeats: timeouts, unavailability,
 * connection errors, and replica failures whose reasons only say that a replica or an index was not there to
 * answer. The retries share a time budget per statement, and each one is logged with the visit and the error.
 * <p>
 * Nothing else is retried. A statement that the server failed or rejected throws {@link ServerErrorException};
 * a result that does not match the model throws {@link ValidationMismatchException}; a write still not
 * acknowledged when the budget runs out throws {@link IndeterminateWriteException}, as the model can no longer
 * know whether it was applied; and a read still failing then throws {@link RetriesExhaustedException}.
 */
public class DriverVisitExecutor extends CQLVisitExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(DriverVisitExecutor.class);

    /**
     * Replica failure reasons that only mean that a replica, or an index on it, could not answer yet: the same
     * statement can succeed once it is back, as happens when nodes restart underneath a run.
     */
    private static final Set<Integer> TRANSIENT_FAILURE_REASONS = Set.of(RequestFailureReason.TIMEOUT.code,
                                                                 RequestFailureReason.NODE_DOWN.code,
                                                                 RequestFailureReason.INDEX_NOT_AVAILABLE.code,
                                                                 RequestFailureReason.INDEX_BUILD_IN_PROGRESS.code);

    public static final long NO_VISIT = -1;

    private final Session session;
    private final ConsistencyLevel writeConsistencyLevel;
    private final ConsistencyLevel readConsistencyLevel;
    private final PageSizeSelector pageSizeSelector;
    private final RetryBudget retryBudget;
    private final Stats stats;

    protected DriverVisitExecutor(SchemaSpec schema,
                                  DataTracker dataTracker,
                                  Model model,
                                  Session session,
                                  ConsistencyLevel writeConsistencyLevel,
                                  ConsistencyLevel readConsistencyLevel,
                                  PageSizeSelector pageSizeSelector,
                                  RetryBudget retryBudget,
                                  Stats stats,
                                  QueryBuildingVisitExecutor.WrapQueries wrapQueries)
    {
        super(schema, dataTracker, model, new QueryBuildingVisitExecutor(schema, wrapQueries));
        this.session = session;
        this.writeConsistencyLevel = writeConsistencyLevel;
        this.readConsistencyLevel = readConsistencyLevel;
        this.pageSizeSelector = pageSizeSelector;
        this.retryBudget = retryBudget;
        this.stats = stats;
    }

    @Override
    protected void executeWithoutResult(Visit visit, CompiledStatement statement)
    {
        Statement driverStatement = toDriverStatement(statement, writeConsistencyLevel);
        executeWithRetries(visit.lts, statement, true, retryBudget, stats, () -> session.execute(driverStatement));
        stats.writes.incrementAndGet();
    }

    @Override
    protected List<ResultSetRow> executeWithResult(Visit visit, CompiledStatement statement)
    {
        Invariants.require(visit.operations.length == 1);
        return InJvmDTestVisitExecutor.rowsToResultSet(schema, (Operations.SelectStatement) visit.operations[0], read(visit, statement));
    }

    @Override
    protected void executeValidatingVisit(Visit visit, List<Operations.SelectStatement> selects, CompiledStatement statement)
    {
        Invariants.require(visit.operations.length == 1);
        Object[][] rows = read(visit, statement);
        try
        {
            model.validate(selects.get(0), InJvmDTestVisitExecutor.rowsToResultSet(schema, selects.get(0), rows));
        }
        catch (Throwable t)
        {
            throw new ValidationMismatchException(visit.lts, statement, t);
        }
        stats.readsValidated.incrementAndGet();
    }

    private Object[][] read(Visit visit, CompiledStatement statement)
    {
        int pageSize = pageSizeSelector.pages(visit);
        Statement driverStatement = toDriverStatement(statement, readConsistencyLevel)
                                    .setFetchSize(pageSize == PageSizeSelector.NO_PAGING ? Integer.MAX_VALUE : pageSize);
        // A page fetch can fail halfway through the result, so a retry restarts the read from its first page.
        return executeWithRetries(visit.lts, statement, false, retryBudget, stats, () -> {
            ResultSet resultSet = session.execute(driverStatement);
            List<Object[]> rows = new ArrayList<>();
            for (Row row : resultSet)
                rows.add(toObjectArray(row));
            return rows.toArray(new Object[rows.size()][]);
        });
    }

    private static Statement toDriverStatement(CompiledStatement statement, ConsistencyLevel consistencyLevel)
    {
        return new SimpleStatement(statement.cql(), statement.bindings())
               .setConsistencyLevel(consistencyLevel)
               .setIdempotent(true);
    }

    /**
     * Returns the row's values in selection order, as the driver's default codecs decode them. For the types
     * that Harry generates these are the Java types of the server's own types, which the value generators use:
     * tinyint is a Byte, smallint a Short, timestamp a Date, and so on.
     */
    private static Object[] toObjectArray(Row row)
    {
        Object[] values = new Object[row.getColumnDefinitions().size()];
        for (int i = 0; i < values.length; i++)
            values[i] = row.getObject(i);
        return values;
    }

    /**
     * Executes a query, retrying it on the errors that {@link #classify} says can be retried, until the budget runs
     * out. {@code lts} is that of the visit that the query is part of, or {@link #NO_VISIT}.
     */
    public static <T> T executeWithRetries(long lts, CompiledStatement statement, boolean isWrite, RetryBudget budget, Stats stats, Supplier<T> query)
    {
        long startNanos = System.nanoTime();
        long backoffMillis = budget.initialBackoffMillis;
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return query.get();
            }
            catch (DriverException e)
            {
                switch (classify(e))
                {
                    case SERVER_ERROR:
                        throw new ServerErrorException(lts, statement, e);
                    case NOT_RETRIED:
                        throw e;
                }

                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                if (elapsedMillis + backoffMillis > budget.budgetMillis)
                {
                    if (isWrite)
                        throw new IndeterminateWriteException(lts, statement, attempt, elapsedMillis, e);
                    throw new RetriesExhaustedException(lts, statement, attempt, elapsedMillis, e);
                }

                stats.retries.incrementAndGet();
                logger.warn("Retrying {} after {} (attempt {}, {}ms of the {}ms retry budget used): {}",
                            lts == NO_VISIT ? statement.cql() : "visit " + lts,
                            e.getClass().getName(), attempt, elapsedMillis, budget.budgetMillis, describe(e));
                Uninterruptibles.sleepUninterruptibly(backoffMillis, TimeUnit.MILLISECONDS);
                backoffMillis = Math.min(backoffMillis * 2, budget.maxBackoffMillis);
            }
        }
    }

    public enum ErrorKind
    {
        /** The statement can be repeated: either it did nothing, or what it did the repetition does again. */
        RETRY,
        /** A server failed to execute the statement, or rejected it: a finding, never retried. */
        SERVER_ERROR,
        /** An error on the client's side, which no retry can help. */
        NOT_RETRIED
    }

    public static ErrorKind classify(DriverException e)
    {
        // A corrupt frame or segment is a finding about the native protocol, even when the driver reports it as
        // the cause of the connection error that it closed the connection with.
        if (isCorruptFrame(e))
            return ErrorKind.SERVER_ERROR;

        if (e instanceof ReadTimeoutException
            || e instanceof WriteTimeoutException
            || e instanceof UnavailableException
            || e instanceof OverloadedException
            || e instanceof BootstrappingException
            || e instanceof NoHostAvailableException
            || e instanceof ConnectionException
            || e instanceof BusyConnectionException
            || e instanceof BusyPoolException)
            return ErrorKind.RETRY;

        if (e instanceof ReadFailureException)
            return onlyTransientReasons(((ReadFailureException) e).getFailuresMap()) ? ErrorKind.RETRY : ErrorKind.SERVER_ERROR;

        if (e instanceof WriteFailureException)
            return onlyTransientReasons(((WriteFailureException) e).getFailuresMap()) ? ErrorKind.RETRY : ErrorKind.SERVER_ERROR;

        if (e instanceof ServerError
            || e instanceof ProtocolError
            || e instanceof FunctionExecutionException
            || e instanceof QueryValidationException)
            return ErrorKind.SERVER_ERROR;

        return ErrorKind.NOT_RETRIED;
    }

    private static boolean isCorruptFrame(Throwable e)
    {
        for (Throwable t = e; t != null; t = t.getCause())
        {
            if (t instanceof CrcMismatchException || t instanceof FrameTooLongException)
                return true;
            if (t instanceof NoHostAvailableException)
            {
                for (Throwable hostError : ((NoHostAvailableException) t).getErrors().values())
                {
                    if (isCorruptFrame(hostError))
                        return true;
                }
            }
        }
        return false;
    }

    /**
     * Protocol v5 reports a reason for each failed replica; without reasons, as in v4, nothing tells a replica
     * that was not there from one that threw, so the failure is taken to be the latter.
     */
    private static boolean onlyTransientReasons(Map<InetAddress, Integer> reasons)
    {
        return !reasons.isEmpty() && TRANSIENT_FAILURE_REASONS.containsAll(reasons.values());
    }

    public static String describe(Throwable t)
    {
        if (t instanceof ReadFailureException)
            return t.getMessage() + " (failure reasons: " + reasonsToString(((ReadFailureException) t).getFailuresMap()) + ')';
        if (t instanceof WriteFailureException)
            return t.getMessage() + " (failure reasons: " + reasonsToString(((WriteFailureException) t).getFailuresMap()) + ')';
        if (t instanceof NoHostAvailableException)
            return ((NoHostAvailableException) t).getCustomMessage(10, true, false);
        return String.valueOf(t.getMessage());
    }

    private static String reasonsToString(Map<InetAddress, Integer> reasons)
    {
        if (reasons.isEmpty())
            return "none reported";

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<InetAddress, Integer> e : reasons.entrySet())
        {
            if (sb.length() > 0)
                sb.append(", ");
            sb.append(e.getKey().getHostAddress()).append('=')
              .append(RequestFailureReason.fromCode(e.getValue())).append('(').append(e.getValue()).append(')');
        }
        return sb.toString();
    }

    public static class RetryBudget
    {
        public final long budgetMillis;
        public final long initialBackoffMillis;
        public final long maxBackoffMillis;

        public RetryBudget(long budgetMillis, long initialBackoffMillis, long maxBackoffMillis)
        {
            Invariants.requireArgument(budgetMillis >= 0 && initialBackoffMillis > 0 && maxBackoffMillis >= initialBackoffMillis);
            this.budgetMillis = budgetMillis;
            this.initialBackoffMillis = initialBackoffMillis;
            this.maxBackoffMillis = maxBackoffMillis;
        }
    }

    public static class Stats
    {
        public final AtomicLong writes = new AtomicLong();
        public final AtomicLong readsValidated = new AtomicLong();
        public final AtomicLong retries = new AtomicLong();
    }

    /**
     * A failure of a visit's statement, carrying the visit's LTS and the statement with its bindings.
     */
    public static abstract class VisitFailure extends RuntimeException
    {
        public final long lts;
        public final CompiledStatement statement;

        protected VisitFailure(String message, long lts, CompiledStatement statement, Throwable cause)
        {
            super(lts == NO_VISIT ? String.format("%s: %s", message, statement)
                                  : String.format("%s at visit %d: %s", message, lts, statement),
                  cause);
            this.lts = lts;
            this.statement = statement;
        }
    }

    public static class ValidationMismatchException extends VisitFailure
    {
        public ValidationMismatchException(long lts, CompiledStatement statement, Throwable cause)
        {
            super("Result does not match the model", lts, statement, cause);
        }
    }

    public static class ServerErrorException extends VisitFailure
    {
        public ServerErrorException(long lts, CompiledStatement statement, DriverException cause)
        {
            super("Server error " + cause.getClass().getName() + ": " + describe(cause), lts, statement, cause);
        }
    }

    public static class IndeterminateWriteException extends VisitFailure
    {
        public IndeterminateWriteException(long lts, CompiledStatement statement, int attempts, long elapsedMillis, DriverException cause)
        {
            super(String.format("Write not acknowledged after %d attempts in %dms, last error %s: %s",
                                attempts, elapsedMillis, cause.getClass().getName(), describe(cause)),
                  lts, statement, cause);
        }
    }

    public static class RetriesExhaustedException extends VisitFailure
    {
        public RetriesExhaustedException(long lts, CompiledStatement statement, int attempts, long elapsedMillis, DriverException cause)
        {
            super(String.format("Read failed %d times in %dms, last error %s: %s",
                                attempts, elapsedMillis, cause.getClass().getName(), describe(cause)),
                  lts, statement, cause);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        protected ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.QUORUM;
        protected ConsistencyLevel readConsistencyLevel = ConsistencyLevel.QUORUM;
        protected PageSizeSelector pageSizeSelector = visit -> PageSizeSelector.NO_PAGING;
        protected RetryBudget retryBudget = new RetryBudget(TimeUnit.MINUTES.toMillis(5), 100, TimeUnit.SECONDS.toMillis(5));
        protected Stats stats = new Stats();
        protected QueryBuildingVisitExecutor.WrapQueries wrapQueries = QueryBuildingVisitExecutor.WrapQueries.UNLOGGED_BATCH;

        public Builder writeConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            this.writeConsistencyLevel = consistencyLevel;
            return this;
        }

        public Builder readConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            this.readConsistencyLevel = consistencyLevel;
            return this;
        }

        public Builder pageSizeSelector(PageSizeSelector pageSizeSelector)
        {
            this.pageSizeSelector = pageSizeSelector;
            return this;
        }

        public Builder retryBudget(RetryBudget retryBudget)
        {
            this.retryBudget = retryBudget;
            return this;
        }

        public Builder stats(Stats stats)
        {
            this.stats = stats;
            return this;
        }

        public Builder wrapQueries(QueryBuildingVisitExecutor.WrapQueries wrapQueries)
        {
            this.wrapQueries = wrapQueries;
            return this;
        }

        public DriverVisitExecutor build(SchemaSpec schema, Model.Replay replay, Session session)
        {
            DataTracker tracker = new DataTracker.SequentialDataTracker();
            Model model = new QuiescentChecker(schema.valueGenerators, tracker, replay);
            return new DriverVisitExecutor(schema, tracker, model, session,
                                           writeConsistencyLevel, readConsistencyLevel, pageSizeSelector,
                                           retryBudget, stats, wrapQueries);
        }
    }
}
