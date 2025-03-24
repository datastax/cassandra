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

package org.apache.cassandra.db.tries;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.generateKeys;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Preencoded;

public abstract class ConsistencyTestBase<C, T extends BaseTrie<C, ?, T>, R extends BaseTrie<C, ?, ?>>
{
    // Note: This should not be run by default with verification to have the higher concurrency of faster writes and reads.

    private static final int COUNT = 30000;
    private static final int PROGRESS_UPDATE = COUNT / 15;
    private static final int READERS = 8;
    private static final int WALKERS = 2;
    private static final Random rand = new Random();

    /**
     * Force copy every modified cell below the partition/enumeration level. Provides atomicity of mutations within the
     * partition level as well as consistency.
     */
    public final Predicate<InMemoryTrie.NodeFeatures<C>> FORCE_COPY_PARTITION = features -> isPartition(features.content());
    /**
     * Force copy every modified cell below the earliest branching point. Provides atomicity of mutations at any level,
     * but readers/walkers may see inconsistent views of the data, in the sense that older mutations may be missed
     * while newer ones are returned.
     */
    public final Predicate<InMemoryTrie.NodeFeatures<C>> FORCE_ATOMIC = features -> features.isBranching();
    /**
     * Do not do any additional copying beyond what is required to build the tries safely for concurrent readers.
     * Mutations may be partially seen by readers, and older mutations may be missed while newer ones are returned.
     */
    public final Predicate<InMemoryTrie.NodeFeatures<C>> NO_ATOMICITY = features -> false;

    abstract R makeTrie(OpOrder readOrder);

    abstract C value(ByteComparable b, ByteComparable cprefix, ByteComparable c, int add, int seqId);
    abstract C metadata(ByteComparable b);
    abstract String pk(C c);
    abstract String ck(C c);
    abstract int seq(C c);
    abstract int value(C c);
    abstract int updateCount(C c);

    abstract T makeSingleton(ByteComparable b, C content);
    abstract T withRootMetadata(T wrapped, C metadata);

    abstract T merge(Collection<T> tries, Trie.CollectionMergeResolver<C> mergeResolver);
    abstract void apply(R trie,
                        T mutation,
                        InMemoryBaseTrie.UpsertTransformer<C, C> mergeResolver,
                        Predicate<InMemoryTrie.NodeFeatures<C>> forcedCopyChecker) throws TrieSpaceExhaustedException;

    abstract boolean isPartition(C c);
    abstract C mergeMetadata(C c1, C c2);

    abstract void printStats(R trie, Predicate<InMemoryBaseTrie.NodeFeatures<C>> forcedCopyChecker);

    @Test
    public void testConsistentUpdates() throws Exception
    {
        // Check that multi-path updates with below-partition-level copying are safe for concurrent readers,
        // and that content is atomically applied, i.e. that reader see either nothing from the update or all of it,
        // and consistent, i.e. that it is not possible to receive some newer updates while missing
        // older ones. (For example, if the sequence of additions is 3, 1, 5, without this requirement a reader
        // could see an enumeration which lists 3 and 5 but not 1.)
        testAtomicUpdates(3, FORCE_COPY_PARTITION, true, true);
        // Note: using 3 per mutation, so that the first and second update fit in a sparse in-memory trie block.
    }

    @Test
    public void testAtomicUpdates() throws Exception
    {
        // Check that multi-path updates with below-branching-point copying are safe for concurrent readers,
        // and that content is atomically applied, i.e. that reader see either nothing from the update or all of it.
        testAtomicUpdates(3, FORCE_ATOMIC, true, false);
    }

    @Test
    public void testSafeUpdates() throws Exception
    {
        // Check that multi path updates without additional copying are safe for concurrent readers.
        testAtomicUpdates(3, NO_ATOMICITY, false, false);
    }

    @Test
    public void testConsistentSinglePathUpdates() throws Exception
    {
        // Check that single path updates with below-partition-level copying are safe for concurrent readers,
        // and that content is consistent, i.e. that it is not possible to receive some newer updates while missing
        // older ones. (For example, if the sequence of additions is 3, 1, 5, without this requirement a reader
        // could see an enumeration which lists 3 and 5 but not 1.)
        testAtomicUpdates(1, FORCE_COPY_PARTITION, true, true);
    }


    @Test
    public void testAtomicSinglePathUpdates() throws Exception
    {
        // When doing single path updates atomicity comes for free. This only checks that the branching checker is
        // not doing anything funny.
        testAtomicUpdates(1, FORCE_ATOMIC, true, false);
    }

    @Test
    public void testSafeSinglePathUpdates() throws Exception
    {
        // Check that single path updates without additional copying are safe for concurrent readers.
        testAtomicUpdates(1, NO_ATOMICITY, true, false);
    }

    // The generated keys all start with NEXT_COMPONENT, which makes it impossible to test the precise behavior of the
    // partition-level force copying. Strip that byte.
    private static ByteComparable[] skipFirst(ByteComparable[] keys)
    {
        ByteComparable[] result = new ByteComparable[keys.length];
        for (int i = 0; i < keys.length; ++i)
            result[i] = skipFirst(keys[i]);
        return result;
    }

    private static ByteComparable skipFirst(ByteComparable key)
    {
        return v -> {
            var bs = key.asComparableBytes(v);
            int n = bs.next();
            assert n != ByteSource.END_OF_STREAM;
            return bs;
        };
    }

    public void testAtomicUpdates(int PER_MUTATION,
                                  Predicate<InMemoryTrie.NodeFeatures<C>> forcedCopyChecker,
                                  boolean checkAtomicity,
                                  boolean checkSequence)
    throws Exception
    {
        ByteComparable[] ckeys = skipFirst(generateKeys(rand, COUNT));
        ByteComparable[] pkeys = skipFirst(generateKeys(rand, Math.min(100, COUNT / 10)));  // to guarantee repetition

        /*
         * Adds COUNT partitions each with perPartition separate clusterings, where the sum of the values
         * of all clusterings is 0.
         * If the sum for any walk covering whole partitions is non-zero, we have had non-atomic updates.
         */

        OpOrder readOrder = new OpOrder();
        R trie = makeTrie(readOrder);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        List<Thread> threads = new ArrayList<Thread>();
        AtomicBoolean writeCompleted = new AtomicBoolean(false);
        AtomicInteger writeProgress = new AtomicInteger(0);

        for (int i = 0; i < WALKERS; ++i)
            threads.add(new Thread()
            {
                public void run()
                {
                    try
                    {
                        Random r = ThreadLocalRandom.current();
                        while (!writeCompleted.get())
                        {
                            int min = writeProgress.get();
                            try (OpOrder.Group group = readOrder.start())
                            {
                                Iterable<Map.Entry<Preencoded, C>> entries = trie.entrySet();
                                checkEntries("", min, true, checkAtomicity, false, PER_MUTATION, entries);
                            }
                        }
                    }
                    catch (Throwable t)
                    {
                        t.printStackTrace();
                        errors.add(t);
                    }
                }
            });

        for (int i = 0; i < READERS; ++i)
        {
            ByteComparable[] srcLocal = pkeys;
            threads.add(new Thread()
            {
                public void run()
                {
                    try
                    {
                        // await at least one ready partition
                        while (writeProgress.get() == 0) {}

                        Random r = ThreadLocalRandom.current();
                        while (!writeCompleted.get())
                        {
                            ByteComparable key = srcLocal[r.nextInt(srcLocal.length)];
                            int min = writeProgress.get() / (pkeys.length * PER_MUTATION) * PER_MUTATION;
                            Iterable<Map.Entry<Preencoded, C>> entries;

                            try (OpOrder.Group group = readOrder.start())
                            {
                                entries = trie.tailTrie(key).entrySet();
                                checkEntries(" in tail " + key.byteComparableAsString(VERSION), min, false, checkAtomicity, checkSequence, PER_MUTATION, entries);
                            }

                            try (OpOrder.Group group = readOrder.start())
                            {
                                entries = trie.subtrie(key, nextBranch(key)).entrySet();
                                checkEntries(" in branch " + key.byteComparableAsString(VERSION), min, true, checkAtomicity, checkSequence, PER_MUTATION, entries);
                            }
                        }
                    }
                    catch (Throwable t)
                    {
                        t.printStackTrace();
                        errors.add(t);
                    }
                }
            });
        }

        threads.add(new Thread()
        {
            public void run()
            {
                ThreadLocalRandom r = ThreadLocalRandom.current();
                final Trie.CollectionMergeResolver<C> mergeResolver = new Trie.CollectionMergeResolver<C>()
                {
                    @Override
                    public C resolve(C c1, C c2)
                    {
                        if (isPartition(c1) && isPartition(c2))
                            return mergeMetadata(c1, c2);
                        throw new AssertionError("Test error, keys should be distinct.");
                    }

                    public C resolve(Collection<C> contents)
                    {
                        return contents.stream().reduce(this::resolve).get();
                    }
                };

                try
                {
                    int lastUpdate = 0;
                    for (int i = 0; i < COUNT; i += PER_MUTATION)
                    {
                        ByteComparable b = pkeys[(i / PER_MUTATION) % pkeys.length];
                        C partitionMarker = metadata(b);
                        ByteComparable cprefix = null;
                        if (r.nextBoolean())
                            cprefix = ckeys[i]; // Also test branching point below the partition level

                        List<T> sources = new ArrayList<>();
                        for (int j = 0; j < PER_MUTATION; ++j)
                        {

                            ByteComparable k = ckeys[i + j];
                            T row = makeSingleton(k,
                                                  value(b, cprefix, k,
                                                        j == 0 ? -PER_MUTATION + 1 : 1,
                                                        (i / PER_MUTATION / pkeys.length) * PER_MUTATION + j));

                            if (cprefix != null)
                                row = row.prefixedBy(cprefix);

                            row = withRootMetadata(row, partitionMarker);
                            row = row.prefixedBy(b);
                            sources.add(row);
                        }

                        final T mutation = merge(sources, mergeResolver);

                        apply(trie, mutation,
                                   (existing, update) -> existing == null ? update : mergeResolver.resolve(existing, update),
                                   forcedCopyChecker);

                        if (i >= pkeys.length * PER_MUTATION && i - lastUpdate >= PROGRESS_UPDATE)
                        {
                            writeProgress.set(i);
                            lastUpdate = i;
                        }
                    }
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                    errors.add(t);
                }
                finally
                {
                    writeCompleted.set(true);
                }
            }
        });

        for (Thread t : threads)
            t.start();

        for (Thread t : threads)
            t.join();

        printStats(trie, forcedCopyChecker);

        if (!errors.isEmpty())
            Assert.fail("Got errors:\n" + errors);
    }

    static ByteComparable nextBranch(ByteComparable key)
    {
        return version -> {
            byte[] bytes = key.asByteComparableArray(version);
            int last = bytes.length - 1;
            while (last >= 0 && bytes[last] == ((byte) 0xFF))
                --last;
            if (last < 0)
                return null;
            ++bytes[last];
            return ByteSource.preencoded(bytes, 0, last + 1);
        };
    }

    public void checkEntries(String location,
                             int min,
                             boolean usePk,
                             boolean checkAtomicity,
                             boolean checkConsecutiveIds,
                             int PER_MUTATION,
                             Iterable<Map.Entry<Preencoded, C>> entries)
    {
        long sum = 0;
        int count = 0;
        long idSum = 0;
        long idMax = 0;
        int updateCount = 0;
        for (var en : entries)
        {
            String path = en.getKey().byteComparableAsString(VERSION);
            final C v = en.getValue();
            if (isPartition(v))
            {
                Assert.assertEquals("Partition metadata" + location, (usePk ? pk(v) : ""), path);
                updateCount += updateCount(v);
                continue;
            }
            String valueKey = (usePk ? pk(v) : "") + ck(v);
            Assert.assertEquals(location, valueKey, path);
            ++count;
            sum += value(v);
            int seq = seq(v);
            idSum += seq;
            if (seq > idMax)
                idMax = seq;
        }

        Assert.assertTrue("Values" + location + " should be at least " + min + ", got " + count, min <= count);

        if (checkAtomicity)
        {
            // If mutations apply atomically, the row count is always a multiple of the mutation size...
            Assert.assertTrue("Values" + location + " should be a multiple of " + PER_MUTATION + ", got " + count, count % PER_MUTATION == 0);
            // ... and the sum of the values is 0 (as the sum for each individual mutation is 0).
            Assert.assertEquals("Value sum" + location, 0, sum);
        }

        if (checkConsecutiveIds)
        {
            // The update count reflected in the partition metadata must match the row count.
            Assert.assertEquals("Update count" + location, count, updateCount);
            // If mutations apply consistently for the partition, for any row we see we have to have seen all rows that
            // were applied before that. In other words, the id sum should be the sum of the integers from 1 to the
            // highest id seen in the partition.
            Assert.assertEquals("Id sum" + location, idMax * (idMax + 1) / 2, idSum);
        }
    }
}
