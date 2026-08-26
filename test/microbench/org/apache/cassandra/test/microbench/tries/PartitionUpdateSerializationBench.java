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
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.partitions.BTreePartitionUpdate;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.PartitionUpdateSizeCache;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Measures the cost of the commit-log / internode partition update codec.
 *
 * The axes are the two things that actually select a code path:
 *
 * <ul>
 * <li>{@code format} — whether the update is a {@link TriePartitionUpdate} or a
 *     {@link BTreePartitionUpdate}. {@code PartitionUpdate.serializer} dispatches on the instance
 *     type, so this is what decides whether the trie codec is used at all.</li>
 * <li>{@code version} — the messaging version. The trie codec is gated on
 *     {@code >= VERSION_DS_21}; at {@code VERSION_DS_20} a trie update is written through
 *     {@code UnfilteredRowIteratorSerializer} like any other, which is the pre-DS_21 baseline.</li>
 * </ul>
 *
 * The interesting cell is {@code trie}/{@code VERSION_DS_21} against {@code btree}/{@code
 * VERSION_DS_20}; {@code trie}/{@code VERSION_DS_20} additionally prices the iterator walk out of a
 * trie-backed update.
 *
 * {@code rows} straddles the writer's node-type boundaries — 10 rows fits the on-disk writer's
 * SPARSE node (max 25 children), 100 forces BITMAP.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(1)
@State(Scope.Thread)
public class PartitionUpdateSerializationBench
{
    static
    {
        DatabaseDescriptor.toolInitialization();
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static final String KEYSPACE = "keyspace_pu_bench";

    @Param({ "btree", "trie" })
    String format;

    @Param({ "1", "10", "100" })
    int rows;

    @Param({ "VERSION_DS_20", "VERSION_DS_21" })
    String version;

    private int messagingVersion;

    /** The update under test. Reused across invocations; nothing below mutates it. */
    private PartitionUpdate update;

    /** Reused serialization target, sized exactly once in setup. */
    private ByteBuffer outBytes;
    private DataOutputBufferFixed out;

    /** Immutable source for {@link #deserialize}, filled once in setup. */
    private ByteBuffer inBytes;

    @Setup(Level.Trial)
    public void setup() throws IOException
    {
        messagingVersion = messagingVersion(version);

        SchemaTestUtil.addOrUpdateKeyspace(KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1)), false);
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(KEYSPACE);
        TableMetadata metadata =
            CreateTableStatement.parse("CREATE TABLE userpics " +
                                       "( userid bigint," +
                                       "picid bigint," +
                                       "commentid bigint, " +
                                       "PRIMARY KEY(userid, picid));", KEYSPACE)
                                .build();
        SchemaTestUtil.addOrUpdateKeyspace(ksm.withSwapped(ksm.tables.with(metadata)), false);

        UpdateBuilder builder = UpdateBuilder.create(metadata, 1L);
        for (int i = 0; i < rows; i++)
            builder.newRow((long) i).add("commentid", (long) i * 10L);

        // Which concrete implementation the builder returns depends on configuration, so convert
        // explicitly rather than assuming. Without this the "btree" arm silently measures tries.
        PartitionUpdate built = builder.build();
        update = "trie".equals(format) ? TriePartitionUpdate.asTrieUpdate(built)
                                       : BTreePartitionUpdate.asBTreeUpdate(built);
        if (!expectedType().isInstance(update))
            throw new IllegalStateException("format=" + format + " produced " + update.getClass().getName());

        PartitionUpdateSizeCache.invalidate(update);
        int size = Math.toIntExact(PartitionUpdate.serializer.serializedSize(update, messagingVersion));

        outBytes = ByteBuffer.allocate(size);
        out = new DataOutputBufferFixed(outBytes);

        inBytes = ByteBuffer.allocate(size);
        try (DataOutputBufferFixed sink = new DataOutputBufferFixed(inBytes))
        {
            PartitionUpdate.serializer.serialize(update, sink, messagingVersion);
        }
        inBytes.flip();

        if (inBytes.remaining() != size)
            throw new IllegalStateException("serializedSize said " + size + " but serialize wrote " + inBytes.remaining());
    }

    private Class<? extends PartitionUpdate> expectedType()
    {
        return "trie".equals(format) ? TriePartitionUpdate.class : BTreePartitionUpdate.class;
    }

    /**
     * Cost of sizing an update that has not been sized before — what the commit-log path pays for
     * every mutation. On the on-disk trie format this is a full trie write, not a structure walk,
     * so it is not the cheap call the name suggests.
     */
    @Benchmark
    public long serializedSize()
    {
        PartitionUpdateSizeCache.invalidate(update);
        return PartitionUpdate.serializer.serializedSize(update, messagingVersion);
    }

    /** Cost of the write alone, into a buffer that is already the right size. */
    @Benchmark
    public int serialize() throws IOException
    {
        out.clear();
        PartitionUpdate.serializer.serialize(update, out, messagingVersion);
        return out.getLength();
    }

    /**
     * The sequence the commit log actually performs per mutation: size it, allocate nothing, write
     * it. On the on-disk format this walks the trie twice unless {@code Mutation} has cached the
     * bytes, which it only does below {@code CACHEABLE_MUTATION_SIZE_LIMIT}.
     */
    @Benchmark
    public int sizeThenSerialize() throws IOException
    {
        PartitionUpdateSizeCache.invalidate(update);
        long size = PartitionUpdate.serializer.serializedSize(update, messagingVersion);
        out.clear();
        PartitionUpdate.serializer.serialize(update, out, messagingVersion);
        if (out.getLength() != size)
            throw new IllegalStateException("size " + size + " != written " + out.getLength());
        return out.getLength();
    }

    /** Commit-log replay / internode receive. */
    @Benchmark
    public PartitionUpdate deserialize() throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(inBytes.duplicate(), false))
        {
            return PartitionUpdate.serializer.deserialize(in, messagingVersion, DeserializationHelper.Flag.LOCAL);
        }
    }

    private static int messagingVersion(String name)
    {
        switch (name)
        {
            case "VERSION_DS_21": return MessagingService.VERSION_DS_21;
            case "VERSION_DS_20": return MessagingService.VERSION_DS_20;
            case "VERSION_DS_12": return MessagingService.VERSION_DS_12;
            case "VERSION_DS_11": return MessagingService.VERSION_DS_11;
            case "VERSION_DS_10": return MessagingService.VERSION_DS_10;
            case "VERSION_50":    return MessagingService.VERSION_50;
            case "VERSION_40":    return MessagingService.VERSION_40;
            default: throw new IllegalArgumentException("Unknown messaging version: " + name);
        }
    }

    public static void main(String... args) throws RunnerException
    {
        new Runner(new OptionsBuilder().include(PartitionUpdateSerializationBench.class.getSimpleName())
                                       .build()).run();
    }
}
