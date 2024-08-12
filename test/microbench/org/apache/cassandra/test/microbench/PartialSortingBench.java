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

package org.apache.cassandra.test.microbench;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicates;
import com.google.common.collect.Comparators;
import com.google.common.collect.Iterators;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;

import org.apache.cassandra.utils.SortingIterator;
import org.apache.cassandra.utils.TopKSelector;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Fork(1)
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 10, time = 3)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@State(Scope.Thread)
public class PartialSortingBench
{

    @Param({"100", "1000", "10000"})
    public int size;

    @Param({"0.1"})
    double nullChance = 0.1;

    @Param({"0", "0.1", "1"})
    double consumeRatio = 0.1;

    @Param({"0", "100"})
    int comparatorSlowDown = 0;

    public List<Integer> data;

    @Setup(Level.Trial)
    public void setUp() {
        Random random = new Random();
        data = new ArrayList<>(size * 100);
        for (int i = 0; i < size * 100; i++) {
            data.add(random.nextDouble() < nullChance ? null : random.nextInt());
        }
        comparator = comparatorSlowDown <= 0 ? Comparator.naturalOrder()
                                             : (x, y) ->
                                               {
                                                   Blackhole.consumeCPU(comparatorSlowDown);
                                                   return Integer.compare(x, y);
                                               };
        comparatorNulls = (a, b) -> {
            if (a == null || b == null)
                return b != null ? 1 : a != null ? -1 : 0;
            return comparator.compare(a, b);
        };
    }

    Comparator<Integer> comparator;
    Comparator<Integer> comparatorNulls;

    @Benchmark
    public void testSortingIterator(Blackhole bh) {
        int startIndex = ThreadLocalRandom.current().nextInt(data.size() - size);
        List<Integer> integers = data.subList(startIndex, startIndex + size);
        Iterator<Integer> iterator = SortingIterator.create(comparator, integers);
        int i = (int) Math.ceil(consumeRatio * size + 0.01);
        while (iterator.hasNext() && i-- > 0) {
            bh.consume(iterator.next());
        }
    }

    @Benchmark
    public void testArrayListSortIterator(Blackhole bh) {
        int startIndex = ThreadLocalRandom.current().nextInt(data.size() - size);
        List<Integer> integers = data.subList(startIndex, startIndex + size);
        var al = new ArrayList<Integer>(data.size());
        for (Integer item : integers)
            if (item != null)
                al.add(item);
        al.sort(comparator);
        var iterator = al.iterator();
        int i = (int) Math.ceil(consumeRatio * size + 0.01);
        while (iterator.hasNext() && i-- > 0) {
            bh.consume(iterator.next());
        }
    }

    @Benchmark
    public void testArraySortIterator(Blackhole bh) {
        int startIndex = ThreadLocalRandom.current().nextInt(data.size() - size);
        List<Integer> integers = data.subList(startIndex, startIndex + size);
        Integer[] al = new Integer[data.size()];
        int sz = 0;
        for (Integer item : integers)
            if (item != null)
                al[sz++] = item;
        Arrays.sort(al, 0, sz, comparator);
        var iterator = Iterators.forArray(al);

        int i = (int) Math.ceil(consumeRatio * size + 0.01);
        while (iterator.hasNext() && i-- > 0) {
            bh.consume(iterator.next());
        }
    }

    @Benchmark
    public void testStreamIterator(Blackhole bh) {
        int startIndex = ThreadLocalRandom.current().nextInt(data.size() - size);
        List<Integer> integers = data.subList(startIndex, startIndex + size);
        int limit = (int) Math.ceil(consumeRatio * size + 0.01);
        var iterator = integers.stream().filter(Predicates.notNull()).sorted(comparator).limit(limit).iterator();
        while (iterator.hasNext()) {
            bh.consume(iterator.next());
        }
    }

    @Benchmark
    public void testPriorityQueuePreLimit(Blackhole bh) {
        int startIndex = ThreadLocalRandom.current().nextInt(data.size() - size);
        List<Integer> integers = data.subList(startIndex, startIndex + size);
        int limit = (int) Math.ceil(consumeRatio * size + 0.01);
        var pq = new PriorityQueue<Integer>(limit + 1, comparator.reversed());
        for (Integer i : integers)
        {
            if (i == null)
                continue;
            pq.add(i);
            if (pq.size() > limit)
                pq.poll();
        }
        limit = pq.size(); // less if close to size with nulls
        Integer[] values = new Integer[limit];
        for (int i = limit - 1; i >= 0; --i)
            values[i] = pq.poll();
        var iterator = Iterators.forArray(values);
        while (iterator.hasNext()) {
            bh.consume(iterator.next());
        }
    }

    @Benchmark
    public void testLucenePriorityQueuePreLimit(Blackhole bh) {
        int startIndex = ThreadLocalRandom.current().nextInt(data.size() - size);
        List<Integer> integers = data.subList(startIndex, startIndex + size);
        int limit = (int) Math.ceil(consumeRatio * size + 0.01);
        var pq = new org.apache.lucene.util.PriorityQueue<Integer>(limit)
        {
            @Override
            protected boolean lessThan(Integer t, Integer t1)
            {
                return t > t1;
            }
        };
        for (Integer i : integers)
        {
            if (i == null)
                continue;
            pq.insertWithOverflow(i);
        }
        limit = pq.size(); // less if close to size with nulls
        Integer[] values = new Integer[limit];
        for (int i = limit - 1; i >= 0; --i)
            values[i] = pq.pop();
        var iterator = Iterators.forArray(values);
        while (iterator.hasNext()) {
            bh.consume(iterator.next());
        }
    }

    @Benchmark
    public void testMinMaxPQPreLimit(Blackhole bh) {
        int startIndex = ThreadLocalRandom.current().nextInt(data.size() - size);
        List<Integer> integers = data.subList(startIndex, startIndex + size);
        int limit = (int) Math.ceil(consumeRatio * size + 0.01);
        var pq = MinMaxPriorityQueue.orderedBy(comparatorNulls).maximumSize(limit).create();
        for (Integer i : integers)
        {
            if (i == null)
                continue;
            pq.offer(i);
        }
        while (!pq.isEmpty()) {
            bh.consume(pq.poll());
        }
    }

    @Benchmark
    public void testOrderingLeastOf(Blackhole bh) {
        int startIndex = ThreadLocalRandom.current().nextInt(data.size() - size);
        List<Integer> integers = data.subList(startIndex, startIndex + size);
        int limit = (int) Math.ceil(consumeRatio * size + 0.01);
        var iterator = Ordering.from(comparatorNulls).leastOf(integers, limit).iterator();
        while (iterator.hasNext()) {
            bh.consume(iterator.next());
        }
    }

    @Benchmark
    public void testCollectLeast(Blackhole bh) {
        int startIndex = ThreadLocalRandom.current().nextInt(data.size() - size);
        List<Integer> integers = data.subList(startIndex, startIndex + size);
        int limit = (int) Math.ceil(consumeRatio * size + 0.01);
        var iterator = integers.stream().collect(Comparators.least(limit, comparatorNulls)).iterator();
        while (iterator.hasNext()) {
            bh.consume(iterator.next());
        }
    }

    @Benchmark
    public void testTopKSelector(Blackhole bh) {
        int startIndex = ThreadLocalRandom.current().nextInt(data.size() - size);
        List<Integer> integers = data.subList(startIndex, startIndex + size);
        int limit = (int) Math.ceil(consumeRatio * size + 0.01);
        var selector = new TopKSelector<>(comparator, limit);
        selector.addAll(integers);
        var iterator = selector.getShared().iterator();
        while (iterator.hasNext()) {
            bh.consume(iterator.next());
        }
    }
}
