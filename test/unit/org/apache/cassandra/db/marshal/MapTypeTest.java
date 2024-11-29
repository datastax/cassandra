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

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.assertj.core.api.Assertions;
import org.quicktheories.core.Gen;

import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.quicktheories.QuickTheory.qt;

public class MapTypeTest
{
    @Test
    public void testContains()
    {
        Gen<AbstractType<?>> primitiveTypeGen = AbstractTypeGenerators.primitiveTypeGen();

        qt().forAll(primitiveTypeGen, primitiveTypeGen)
            .checkAssert(MapTypeTest::testContains);
    }

    private static <K, V> void testContains(AbstractType<K> keyType, AbstractType<V> valType)
    {
        MapType<K, V> mapType = MapType.getInstance(keyType, valType, false);

        // generate a map of random key-value pairs
        Map<K, V> entries = new HashMap<>();
        Map<ByteBuffer, ByteBuffer> bytes = new HashMap<>();
        Gen<ByteBuffer> keyGen = getTypeSupport(keyType).bytesGen();
        Gen<ByteBuffer> valGen = getTypeSupport(valType).bytesGen();
        qt().withExamples(100).forAll(keyGen, valGen).checkAssert((k, v) -> {
            entries.put(keyType.compose(k), valType.compose(v));
            bytes.put(k, v);
        });
        ByteBuffer map = mapType.decompose(entries);

        // verify that the map contains its own keys and values
        bytes.values().forEach(v -> assertContains(mapType, map, v, true));
        bytes.keySet().forEach(k -> assertContainsKey(mapType, map, k, true));

        // verify some random keys and values, contained or not
        qt().withExamples(100)
            .forAll(keyGen, valGen)
            .checkAssert((k, v) -> {
                assertContains(mapType, map, v, contains(valType, bytes.values(), v));
                assertContainsKey(mapType, map, k, contains(keyType, bytes.keySet(), k));
            });
    }

    private static void assertContains(MapType<?, ?> type, ByteBuffer map, ByteBuffer value, boolean expected)
    {
        Assertions.assertThat(type.contains(map, value))
                  .isEqualTo(expected);
    }

    private static void assertContainsKey(MapType<?, ?> type, ByteBuffer map, ByteBuffer key, boolean expected)
    {
        Assertions.assertThat(type.containsKey(map, key))
                  .isEqualTo(expected);
    }

    private static boolean contains(AbstractType<?> type, Iterable<ByteBuffer> values, ByteBuffer value)
    {
        for (ByteBuffer v : values)
        {
            if (type.compare(v, value) == 0)
                return true;
        }
        return false;
    }
}
