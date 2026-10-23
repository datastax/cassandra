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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Test;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.BusyPoolException;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.exceptions.CrcMismatchException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.core.exceptions.ProtocolError;
import com.datastax.driver.core.exceptions.ReadFailureException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.ServerError;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.exceptions.TransportException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.exceptions.WriteFailureException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.op.Operations;

import static org.apache.cassandra.harry.execution.DriverVisitExecutor.ErrorKind.NOT_RETRIED;
import static org.apache.cassandra.harry.execution.DriverVisitExecutor.ErrorKind.RETRY;
import static org.apache.cassandra.harry.execution.DriverVisitExecutor.ErrorKind.SERVER_ERROR;
import static org.apache.cassandra.harry.execution.DriverVisitExecutor.classify;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class DriverVisitExecutorTest
{
    private static final EndPoint ENDPOINT = () -> new InetSocketAddress(InetAddress.getLoopbackAddress(), 9042);
    private static final InetAddress REPLICA_1 = InetAddress.getLoopbackAddress();
    private static final InetAddress REPLICA_2;

    static
    {
        try
        {
            REPLICA_2 = InetAddress.getByAddress(new byte[]{ 127, 0, 0, 2 });
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    private static final Map<String, DataType> DRIVER_TYPES = Map.of("tinyint", DataType.tinyint(),
                                                                     "smallint", DataType.smallint(),
                                                                     "int", DataType.cint(),
                                                                     "bigint", DataType.bigint(),
                                                                     "float", DataType.cfloat(),
                                                                     "double", DataType.cdouble(),
                                                                     "ascii", DataType.ascii(),
                                                                     "text", DataType.text(),
                                                                     "uuid", DataType.uuid(),
                                                                     "timestamp", DataType.timestamp());

    private static final CompiledStatement STATEMENT = CompiledStatement.create("INSERT INTO ks.tbl (pk, v) VALUES (?, ?) USING TIMESTAMP 7;", 1L, "x");

    /**
     * Every Harry type survives being bound by the driver, written in the server's encoding, and read back through
     * the driver's codecs into the result row that the model deflates.
     */
    @Test
    public void testRowConversionForEveryType()
    {
        for (ColumnSpec.DataType<?> type : ColumnSpec.TYPES)
        {
            DataType driverType = DRIVER_TYPES.get(type.toString());
            assertThat(driverType).as("driver type of %s", type).isNotNull();
            for (boolean reversed : new boolean[]{ false, true })
                checkRowConversion(type, reversed, driverType);
        }
    }

    @SuppressWarnings("unchecked")
    private static void checkRowConversion(ColumnSpec.DataType<?> type, boolean reversed, DataType driverType)
    {
        SchemaSpec schema = new SchemaSpec(1, 100, "ks", "tbl",
                                           List.of(ColumnSpec.pk("pk0", type)),
                                           List.of(ColumnSpec.ck("ck0", type, reversed)),
                                           List.of(ColumnSpec.regularColumn("regular0", type)),
                                           List.of(ColumnSpec.staticColumn("static0", type)));
        HistoryBuilder.IndexedValueGenerators generators = (HistoryBuilder.IndexedValueGenerators) schema.valueGenerators;

        for (int i = 0; i < Math.min(10, generators.pkPopulation()); i++)
        {
            long pd = generators.pkGen().descriptorAt(i);
            long cd = generators.ckGen().descriptorAt(i % generators.ckPopulation());
            long vd = generators.regularColumnGen(0).descriptorAt(i % generators.regularPopulation(0));
            long sd = generators.staticColumnGen(0).descriptorAt(i % generators.staticPopulation(0));

            // SELECT * returns the partition key, the clustering, then the static and regular columns
            Object[] row = { throughDriver(schema.partitionKeys.get(0), generators.pkGen().inflate(pd)[0], driverType),
                             throughDriver(schema.clusteringKeys.get(0), generators.ckGen().inflate(cd)[0], driverType),
                             throughDriver(schema.staticColumns.get(0), generators.staticColumnGen(0).inflate(sd), driverType),
                             throughDriver(schema.regularColumns.get(0), generators.regularColumnGen(0).inflate(vd), driverType) };

            List<ResultSetRow> rows = InJvmDTestVisitExecutor.rowsToResultSet(schema, new Operations.SelectPartition(0, pd), new Object[][]{ row });
            assertEquals(1, rows.size());
            ResultSetRow converted = rows.get(0);
            String what = String.format("%s%s, index %d", reversed ? "reversed " : "", type, i);
            assertEquals(what, pd, converted.pd);
            assertEquals(what, cd, converted.cd);
            assertEquals(what, vd, converted.vds[0]);
            assertEquals(what, sd, converted.sds[0]);
        }
    }

    /**
     * Binds the value as the driver does a {@link com.datastax.driver.core.SimpleStatement}'s, checks that it is
     * what the server type would write, and decodes it as {@link com.datastax.driver.core.Row#getObject} does.
     */
    @SuppressWarnings("unchecked")
    private static Object throughDriver(ColumnSpec<?> column, Object value, DataType driverType)
    {
        AbstractType<Object> serverType = (AbstractType<Object>) column.type.asServerType().unwrap();
        ByteBuffer bound = CodecRegistry.DEFAULT_INSTANCE.codecFor(value).serialize(value, ProtocolVersion.V5);
        assertEquals(column.name + " = " + value, serverType.decompose(value), bound);

        Object decoded = CodecRegistry.DEFAULT_INSTANCE.codecFor(driverType).deserialize(bound, ProtocolVersion.V5);
        assertSame(column.name + " = " + value, value.getClass(), decoded.getClass());
        // Not equals(): a generated text value can hold lone surrogates, which UTF-8 replaces, on every path to the
        // server. The model compares text by its encoding, as the server does, so to it the value is unchanged.
        assertEquals(column.name + " = " + value, 0, ((Comparator<Object>) column.type.comparator()).compare(value, decoded));
        return decoded;
    }

    @Test
    public void testClassify()
    {
        assertEquals(RETRY, classify(new ReadTimeoutException(ConsistencyLevel.QUORUM, 1, 2, true)));
        assertEquals(RETRY, classify(new WriteTimeoutException(ConsistencyLevel.QUORUM, WriteType.UNLOGGED_BATCH, 1, 2)));
        assertEquals(RETRY, classify(new UnavailableException(ConsistencyLevel.QUORUM, 2, 1)));
        assertEquals(RETRY, classify(new OverloadedException(ENDPOINT, "overloaded")));
        assertEquals(RETRY, classify(new OperationTimedOutException(ENDPOINT)));
        assertEquals(RETRY, classify(new TransportException(ENDPOINT, "connection closed")));
        assertEquals(RETRY, classify(new BusyPoolException(ENDPOINT, 1)));
        assertEquals(RETRY, classify(new NoHostAvailableException(Map.of(ENDPOINT, new TransportException(ENDPOINT, "connection refused")))));

        assertEquals(RETRY, classify(readFailure(Map.of(REPLICA_1, RequestFailureReason.TIMEOUT.code))));
        assertEquals(RETRY, classify(readFailure(Map.of(REPLICA_1, RequestFailureReason.INDEX_NOT_AVAILABLE.code,
                                                        REPLICA_2, RequestFailureReason.INDEX_BUILD_IN_PROGRESS.code))));
        assertEquals(RETRY, classify(writeFailure(Map.of(REPLICA_1, RequestFailureReason.NODE_DOWN.code))));

        assertEquals(SERVER_ERROR, classify(readFailure(Map.of(REPLICA_1, RequestFailureReason.UNKNOWN.code))));
        assertEquals(SERVER_ERROR, classify(readFailure(Map.of(REPLICA_1, RequestFailureReason.TIMEOUT.code,
                                                               REPLICA_2, RequestFailureReason.READ_TOO_MANY_TOMBSTONES.code))));
        assertEquals(SERVER_ERROR, classify(readFailure(Map.of())));
        assertEquals(SERVER_ERROR, classify(writeFailure(Map.of(REPLICA_1, RequestFailureReason.UNKNOWN.code))));
        assertEquals(SERVER_ERROR, classify(new ServerError(ENDPOINT, "java.lang.AssertionError")));
        assertEquals(SERVER_ERROR, classify(new ProtocolError(ENDPOINT, "bad frame")));
        assertEquals(SERVER_ERROR, classify(new InvalidQueryException("Invalid order of range boundaries")));
        assertEquals(SERVER_ERROR, classify(new SyntaxError(ENDPOINT, "line 1:0")));
        assertEquals(SERVER_ERROR, classify(new CrcMismatchException("CRC mismatch")));
        assertEquals(SERVER_ERROR, classify(new TransportException(ENDPOINT, "unexpected exception", new CrcMismatchException("CRC mismatch"))));
        assertEquals(SERVER_ERROR, classify(new NoHostAvailableException(Map.of(ENDPOINT, new TransportException(ENDPOINT, "unexpected exception", new CrcMismatchException("CRC mismatch"))))));

        assertEquals(NOT_RETRIED, classify(new CodecNotFoundException("no codec", DataType.cint(), null)));
    }

    @Test
    public void testRetriesUntilSuccess()
    {
        DriverVisitExecutor.Stats stats = new DriverVisitExecutor.Stats();
        AtomicInteger attempts = new AtomicInteger();
        String result = DriverVisitExecutor.executeWithRetries(3, STATEMENT, true, budget(10_000), stats, failing(attempts, 3, () -> new WriteTimeoutException(ConsistencyLevel.QUORUM, WriteType.SIMPLE, 1, 2)));
        assertEquals("ok", result);
        assertEquals(4, attempts.get());
        assertEquals(3, stats.retries.get());

        attempts.set(0);
        result = DriverVisitExecutor.executeWithRetries(4, STATEMENT, false, budget(10_000), stats, failing(attempts, 2, () -> readFailure(Map.of(REPLICA_1, RequestFailureReason.INDEX_NOT_AVAILABLE.code))));
        assertEquals("ok", result);
        assertEquals(3, attempts.get());
        assertEquals(5, stats.retries.get());
    }

    @Test
    public void testWriteNotAcknowledgedWithinBudgetIsIndeterminate()
    {
        DriverVisitExecutor.Stats stats = new DriverVisitExecutor.Stats();
        AtomicInteger attempts = new AtomicInteger();
        assertThatThrownBy(() -> DriverVisitExecutor.executeWithRetries(7, STATEMENT, true, budget(50), stats, failing(attempts, Integer.MAX_VALUE, () -> new WriteTimeoutException(ConsistencyLevel.QUORUM, WriteType.SIMPLE, 1, 2))))
        .isInstanceOf(DriverVisitExecutor.IndeterminateWriteException.class)
        .hasCauseInstanceOf(WriteTimeoutException.class)
        .hasMessageContaining("at visit 7")
        .hasMessageContaining(STATEMENT.cql())
        .satisfies(e -> assertEquals(7, ((DriverVisitExecutor.VisitFailure) e).lts));
        assertThat(attempts.get()).isGreaterThan(1);
        assertEquals(attempts.get() - 1, stats.retries.get());
    }

    @Test
    public void testReadFailingForTheWholeBudgetIsNotIndeterminate()
    {
        DriverVisitExecutor.Stats stats = new DriverVisitExecutor.Stats();
        AtomicInteger attempts = new AtomicInteger();
        assertThatThrownBy(() -> DriverVisitExecutor.executeWithRetries(8, STATEMENT, false, budget(50), stats, failing(attempts, Integer.MAX_VALUE, () -> new ReadTimeoutException(ConsistencyLevel.QUORUM, 1, 2, false))))
        .isInstanceOf(DriverVisitExecutor.RetriesExhaustedException.class)
        .hasCauseInstanceOf(ReadTimeoutException.class);
        assertThat(attempts.get()).isGreaterThan(1);
    }

    @Test
    public void testServerErrorIsNeverRetried()
    {
        for (boolean isWrite : new boolean[]{ true, false })
        {
            DriverVisitExecutor.Stats stats = new DriverVisitExecutor.Stats();
            AtomicInteger attempts = new AtomicInteger();
            assertThatThrownBy(() -> DriverVisitExecutor.executeWithRetries(9, STATEMENT, isWrite, budget(10_000), stats, failing(attempts, Integer.MAX_VALUE, () -> readFailure(Map.of(REPLICA_1, RequestFailureReason.UNKNOWN.code)))))
            .isInstanceOf(DriverVisitExecutor.ServerErrorException.class)
            .hasCauseInstanceOf(ReadFailureException.class)
            .hasMessageContaining("UNKNOWN");
            assertEquals(1, attempts.get());
            assertEquals(0, stats.retries.get());
        }
    }

    @Test
    public void testClientErrorIsRethrown()
    {
        DriverVisitExecutor.Stats stats = new DriverVisitExecutor.Stats();
        AtomicInteger attempts = new AtomicInteger();
        CodecNotFoundException error = new CodecNotFoundException("no codec", DataType.cint(), null);
        assertThatThrownBy(() -> DriverVisitExecutor.executeWithRetries(10, STATEMENT, true, budget(10_000), stats, failing(attempts, Integer.MAX_VALUE, () -> error)))
        .isSameAs(error);
        assertEquals(1, attempts.get());
    }

    private static DriverVisitExecutor.RetryBudget budget(long millis)
    {
        return new DriverVisitExecutor.RetryBudget(millis, 1, 5);
    }

    private static Supplier<String> failing(AtomicInteger attempts, int failures, Supplier<DriverException> error)
    {
        return () -> {
            if (attempts.incrementAndGet() <= failures)
                throw error.get();
            return "ok";
        };
    }

    private static ReadFailureException readFailure(Map<InetAddress, Integer> reasons)
    {
        return new ReadFailureException(ConsistencyLevel.QUORUM, 1, 2, reasons.size(), reasons, false);
    }

    private static WriteFailureException writeFailure(Map<InetAddress, Integer> reasons)
    {
        return new WriteFailureException(ConsistencyLevel.QUORUM, WriteType.SIMPLE, 1, 2, reasons.size(), reasons);
    }
}
