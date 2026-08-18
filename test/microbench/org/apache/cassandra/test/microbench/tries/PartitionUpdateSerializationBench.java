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

package org.apache.cassandra.test.microbench.tries;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(1)
@State(Scope.Benchmark)
public class PartitionUpdateSerializationBench
{
    static
    {
        DatabaseDescriptor.toolInitialization();
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    static String keyspace = "keyspace_pu_bench";

    private PartitionUpdate update;
    private ByteBuffer buffer;
    private DataOutputBuffer outputBuffer;
    private DataInputBuffer inputBuffer;

    @Param({"legacy", "trie"})
    String format = "trie";

    @Param({"1", "10", "100"})
    int rows = 10;

    @Param({"VERSION_DS_20", "VERSION_DS_10"})
    String version = "VERSION_DS_20";

    private int messagingVersion;

    @State(Scope.Thread)
    public static class ThreadState
    {
        PartitionUpdate in;
        int counter = 0;
    }

    @Setup
    public void setup() throws IOException
    {
        if (version.equalsIgnoreCase("VERSION_DS_20") || version.equals("110"))
            messagingVersion = MessagingService.VERSION_DS_20;
        else if (version.equalsIgnoreCase("VERSION_DS_10") || version.equals("100"))
            messagingVersion = MessagingService.VERSION_DS_10;
        else if (version.equalsIgnoreCase("VERSION_DS_12") || version.equals("102"))
            messagingVersion = MessagingService.VERSION_DS_12;
        else if (version.equalsIgnoreCase("VERSION_DS_11") || version.equals("101"))
            messagingVersion = MessagingService.VERSION_DS_11;
        else if (version.equalsIgnoreCase("VERSION_40") || version.equals("12"))
            messagingVersion = MessagingService.VERSION_40;
        else if (version.equalsIgnoreCase("VERSION_30") || version.equals("10"))
            messagingVersion = MessagingService.VERSION_30;
        else
        {
            try {
                messagingVersion = Integer.parseInt(version);
            } catch (NumberFormatException e) {
                messagingVersion = MessagingService.current_version;
            }
        }
        SchemaTestUtil.addOrUpdateKeyspace(KeyspaceMetadata.create(keyspace, KeyspaceParams.simple(1)), false);
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
        TableMetadata metadata =
            CreateTableStatement.parse("CREATE TABLE userpics " +
                                       "( userid bigint," +
                                       "picid bigint," +
                                       "commentid bigint, " +
                                       "PRIMARY KEY(userid, picid));", keyspace)
                                .build();

        SchemaTestUtil.addOrUpdateKeyspace(ksm.withSwapped(ksm.tables.with(metadata)), false);

        UpdateBuilder builder = UpdateBuilder.create(metadata, 1L);
        for (int i = 0; i < rows; i++)
        {
            builder.newRow((long) i).add("commentid", (long) i * 10L);
        }
        PartitionUpdate builtUpdate = builder.build();
        if ("trie".equalsIgnoreCase(format))
        {
            update = TriePartitionUpdate.asTrieUpdate(builtUpdate);
        }
        else
        {
            update = builtUpdate;
        }

        int size = (int) PartitionUpdate.serializer.serializedSize(update, messagingVersion);
        buffer = ByteBuffer.allocate(size);
        outputBuffer = new DataOutputBufferFixed(buffer);

        PartitionUpdate.serializer.serialize(update, outputBuffer, messagingVersion);
        buffer.flip();
        inputBuffer = new DataInputBuffer(buffer, false);
    }

    @Benchmark
    public void serialize(ThreadState state, Blackhole bh) throws IOException
    {
        buffer.rewind();
        PartitionUpdate.serializer.serialize(update, outputBuffer, messagingVersion);
        bh.consume(outputBuffer.buffer(true));
        state.counter++;
    }

    @Benchmark
    public void deserialize(ThreadState state, Blackhole bh) throws IOException
    {
        buffer.rewind();
        state.in = PartitionUpdate.serializer.deserialize(inputBuffer, messagingVersion, DeserializationHelper.Flag.LOCAL);
        state.counter++;
        bh.consume(state.in);
    }

    public static void main(String... args) throws Exception {
        Options opts = new OptionsBuilder()
                       .include(".*"+PartitionUpdateSerializationBench.class.getSimpleName()+".*")
                       .jvmArgs("-server")
                       .forks(1)
                       .mode(Mode.AverageTime)
                       .addProfiler(StackProfiler.class)
                       .build();

        Collection<RunResult> records = new Runner(opts).run();
        for ( RunResult result : records) {
            Result r = result.getPrimaryResult();
            System.out.println("API replied benchmark score: "
                               + r.getScore() + " "
                               + r.getScoreUnit() + " over "
                               + r.getStatistics().getN() + " iterations");
        }
    }
}
