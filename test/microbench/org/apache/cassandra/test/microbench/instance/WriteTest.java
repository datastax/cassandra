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

package org.apache.cassandra.test.microbench.instance;


import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import com.google.common.base.Throwables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class WriteTest extends CQLTester
{
    static String keyspace;
    String table;
    ColumnFamilyStore cfs;
    Random rand;

    @Param({"1000"})
    int BATCH = 1_000;

    public enum EndOp
    {
        INMEM, TRUNCATE, FLUSH
    }

    @Param({"1000000"})
    int count = 1_000_000;

    @Param({"INMEM", "TRUNCATE", "FLUSH"})
    EndOp flush = EndOp.INMEM;

    @Param({""})
    String memtableClass = "";

    public enum Execution
    {
        SERIAL,
        SERIAL_NET,
        PARALLEL,
        PARALLEL_NET,
    }

    @Param({"PARALLEL"})
    Execution async = Execution.PARALLEL;

    String writeStatement;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        rand = new Random(1);
        CQLTester.setUpClass();
        CQLTester.prepareServer();
        DatabaseDescriptor.setAutoSnapshot(false);
        System.err.println("setupClass done.");
        String memtableSetup = "";
        if (!memtableClass.isEmpty())
            memtableSetup = String.format(" AND memtable = { 'class': '%s' }", memtableClass);
        keyspace = createKeyspace(
        "CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace,
                            "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid)) with compression = {'enabled': false}" +
                            memtableSetup);
        execute("use " + keyspace + ";");
        switch (async)
        {
        case SERIAL_NET:
        case PARALLEL_NET:
            CQLTester.requireNetwork();
            executeNet(getDefaultVersion(), "use " + keyspace + ";");
        }
        writeStatement = "INSERT INTO " + table + "(userid,picid,commentid)VALUES(?,?,?)";
        System.err.println("Prepared, batch " + BATCH + " flush " + flush);
        System.err.println("Disk access mode " + DatabaseDescriptor.getDiskAccessMode() + " index " +
                           DatabaseDescriptor.getIndexAccessMode());

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
    }

    @Benchmark
    public void writeTable() throws Throwable
    {
        long i;
        for (i = 0; i <= count - BATCH; i += BATCH)
            performWrite(i, BATCH);
        if (i < count)
            performWrite(i, count - i);

        switch (flush)
        {
        case FLUSH:
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
            // if we flush we also must truncate to avoid accummulating sstables
        case TRUNCATE:
            execute("TRUNCATE TABLE " + table);
            // note: we turn snapshotting and durable writes (which would have caused a flush) off for this benchmark
            break;
        case INMEM:
            if (!cfs.getLiveSSTables().isEmpty())
                throw new AssertionError("SSTables created for INMEM test.");
            // leave unflushed, i.e. next iteration will overwrite data
        default:
        }
    }

    public Object[] writeArguments(long i)
    {
        return new Object[] { i, i, i };
    }
//    abstract Object[] writeArguments(long i);

    public void performWrite(long ofs, long count) throws Throwable
    {
        switch (async)
        {
        case SERIAL:
            performWriteSerial(ofs, count);
            break;
        case SERIAL_NET:
            performWriteSerialNet(ofs, count);
            break;
        case PARALLEL:
            performWriteThreads(ofs, count);
            break;
        case PARALLEL_NET:
            performWriteThreadsNet(ofs, count);
            break;
        }
    }

    public void performWriteSerial(long ofs, long count) throws Throwable
    {
        for (long i = ofs; i < ofs + count; ++i)
            execute(writeStatement, writeArguments(i));
    }

    public void performWriteThreads(long ofs, long count) throws Throwable
    {
        long done = LongStream.range(ofs, ofs + count)
                              .parallel()
                              .map(i ->
                                   {
                                       try
                                       {
                                           execute(writeStatement, writeArguments(i));
                                           return 1;
                                       }
                                       catch (Throwable throwable)
                                       {
                                           throw Throwables.propagate(throwable);
                                       }
                                   })
                              .sum();
        assert done == count;
    }

    public void performWriteSerialNet(long ofs, long count) throws Throwable
    {
        for (long i = ofs; i < ofs + count; ++i)
            sessionNet().execute(writeStatement, writeArguments(i));
    }

    public void performWriteThreadsNet(long ofs, long count) throws Throwable
    {
        long done = LongStream.range(ofs, ofs + count)
                              .parallel()
                              .map(i ->
                                   {
                                       try
                                       {
                                           sessionNet().execute(writeStatement, writeArguments(i));
                                           return 1;
                                       }
                                       catch (Throwable throwable)
                                       {
                                           throw Throwables.propagate(throwable);
                                       }
                                   })
                              .sum();
        assert done == count;
    }

    @TearDown(Level.Trial)
    public void teardown() throws InterruptedException
    {
        if (flush == EndOp.INMEM && !cfs.getLiveSSTables().isEmpty())
            throw new AssertionError("SSTables created for INMEM test.");

        // do a flush to print sizes
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        CommitLog.instance.shutdownBlocking();
        CQLTester.tearDownClass();
        CQLTester.cleanup();
    }

    public Object performReadSerial(String readStatement, Supplier<Object[]> supplier) throws Throwable
    {
        long sum = 0;
        for (int i = 0; i < BATCH; ++i)
            sum += execute(readStatement, supplier.get()).size();
        return sum;
    }

    public Object performReadThreads(String readStatement, Supplier<Object[]> supplier) throws Throwable
    {
        return IntStream.range(0, BATCH)
                        .parallel()
                        .mapToLong(i ->
                                   {
                                       try
                                       {
                                           return execute(readStatement, supplier.get()).size();
                                       }
                                       catch (Throwable throwable)
                                       {
                                           throw Throwables.propagate(throwable);
                                       }
                                   })
                        .sum();
    }

    public Object performReadSerialNet(String readStatement, Supplier<Object[]> supplier) throws Throwable
    {
        long sum = 0;
        for (int i = 0; i < BATCH; ++i)
            sum += executeNet(getDefaultVersion(), readStatement, supplier.get())
                           .getAvailableWithoutFetching();
        return sum;
    }

    public long performReadThreadsNet(String readStatement, Supplier<Object[]> supplier) throws Throwable
    {
        return IntStream.range(0, BATCH)
                        .parallel()
                        .mapToLong(i ->
                                   {
                                       try
                                       {
                                           return executeNet(getDefaultVersion(), readStatement, supplier.get())
                                                          .getAvailableWithoutFetching();
                                       }
                                       catch (Throwable throwable)
                                       {
                                           throw Throwables.propagate(throwable);
                                       }
                                   })
                        .sum();
    }


    public Object performRead(String readStatement, Supplier<Object[]> supplier) throws Throwable
    {
        switch (async)
        {
            case SERIAL:
                return performReadSerial(readStatement, supplier);
            case SERIAL_NET:
                return performReadSerialNet(readStatement, supplier);
            case PARALLEL:
                return performReadThreads(readStatement, supplier);
            case PARALLEL_NET:
                return performReadThreadsNet(readStatement, supplier);
        }
        return null;
    }
}
