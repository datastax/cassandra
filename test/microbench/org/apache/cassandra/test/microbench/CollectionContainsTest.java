/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.test.microbench;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.utils.AbstractTypeGenerators;
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

import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.quicktheories.QuickTheory.qt;

/**
 * Benchmarks {@link org.apache.cassandra.cql3.Operator#CONTAINS} and {@link org.apache.cassandra.cql3.Operator#CONTAINS_KEY}
 * comparing calls to {@link CollectionType#contains(ByteBuffer, ByteBuffer)} to the full collection deserialization
 * followed by a call to {@link java.util.Collection#contains(Object)} that was done before CNDB-11760.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 1) // seconds
@Measurement(iterations = 3, time = 1) // seconds
@Fork(value = 4)
@Threads(4)
@State(Scope.Benchmark)
public class CollectionContainsTest
{
    @Param({ "INT", "TEXT" })
    public String type;

    @Param({ "1", "10", "100", "1000" })
    public int collectionSize;

    private ListType<?> listType;
    private SetType<?> setType;
    private MapType<?, ?> mapType;

    private ByteBuffer list;
    private ByteBuffer set;
    private ByteBuffer map;

    private final List<ByteBuffer> values = new ArrayList<>();

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        AbstractType<?> elementsType = CQL3Type.Native.valueOf(type).getType();
        setup(elementsType);
    }

    private <T> void setup(AbstractType<T> elementsType)
    {
        ListType<T> listType = ListType.getInstance(elementsType, false);
        SetType<T> setType = SetType.getInstance(elementsType, false);
        MapType<T, T> mapType = MapType.getInstance(elementsType, elementsType, false);

        List<T> listValues = new ArrayList<>();
        Set<T> setValues = new HashSet<>();
        Map<T, T> mapValues = new HashMap<>();

        AbstractTypeGenerators.TypeSupport<T> support = getTypeSupport(elementsType);
        qt().withExamples(collectionSize).forAll(support.valueGen).checkAssert(value -> {
            listValues.add(value);
            setValues.add(value);
            mapValues.put(value, value);
        });

        list = listType.decompose(listValues);
        set = setType.decompose(setValues);
        map = mapType.decompose(mapValues);

        this.listType = listType;
        this.setType = setType;
        this.mapType = mapType;

        qt().withExamples(100).forAll(support.bytesGen()).checkAssert(values::add);
    }

    @Benchmark
    public Object listContainsNonDeserializing()
    {
        return test(v -> listType.contains(list, v));
    }

    @Benchmark
    public Object listContainsDeserializing()
    {
        return test(v -> listType.compose(list).contains(listType.getElementsType().compose(v)));
    }

    @Benchmark
    public Object setContainsNonDeserializing()
    {
        return test(v -> setType.contains(set, v));
    }

    @Benchmark
    public Object setContainsDeserializing()
    {
        return test(v -> setType.compose(set).contains(setType.getElementsType().compose(v)));
    }

    @Benchmark
    public Object mapContainsNonDeserializing()
    {
        return test(v -> mapType.contains(map, v));
    }

    @Benchmark
    public Object mapContainsDeserializing()
    {
        return test(v -> mapType.compose(map).containsValue(mapType.getValuesType().compose(v)));
    }

    @Benchmark
    public Object mapContainsKeyNonDeserializing()
    {
        return test(v -> mapType.containsKey(map, v));
    }

    @Benchmark
    public Object mapContainsKeyDeserializing()
    {
        return test(v -> mapType.compose(map).containsKey(mapType.getKeysType().compose(v)));
    }

    private int test(Function<ByteBuffer, Boolean> containsFunction)
    {
        int contained = 0;
        for (ByteBuffer v : values)
        {
            if (containsFunction.apply(v))
                contained++;
        }
        return contained;
    }
}
