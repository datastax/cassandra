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
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.assertj.core.api.Assertions;
import org.quicktheories.core.Gen;

import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.quicktheories.QuickTheory.qt;

public class ListTypeTest
{
    @Test
    public void testContains()
    {
        qt().forAll(AbstractTypeGenerators.primitiveTypeGen())
            .checkAssert(ListTypeTest::testContains);
    }

    private static <V> void testContains(AbstractType<V> type)
    {
        ListType<V> listType = ListType.getInstance(type, false);

        // generate a list of random values
        List<V> values = new ArrayList<>();
        List<ByteBuffer> bytes = new ArrayList<>();
        Gen<ByteBuffer> gen = getTypeSupport(type).bytesGen();
        qt().withExamples(100).forAll(gen).checkAssert(v -> {
            values.add(type.compose(v));
            bytes.add(v);
        });
        ByteBuffer list = listType.decompose(values);

        // verify that the list contains its own elements
        bytes.forEach(v -> assertContains(listType, list, v, true));

        // verify some random values, contained or not
        qt().withExamples(100)
            .forAll(gen)
            .checkAssert(v -> assertContains(listType, list, v, contains(type, bytes, v)));
    }

    private static void assertContains(ListType<?> type, ByteBuffer list, ByteBuffer value, boolean expected)
    {
        Assertions.assertThat(type.contains(list, value))
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
