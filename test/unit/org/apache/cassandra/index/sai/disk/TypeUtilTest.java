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
package org.apache.cassandra.index.sai.disk;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public class TypeUtilTest extends SaiRandomizedTest
{
    private final Version version;
    @ParametersFactory()
    public static Collection<Object[]> data()
    {
        // Required because it configures SEGMENT_BUILD_MEMORY_LIMIT, which is needed for Version.AA
        if (DatabaseDescriptor.getRawConfig() == null)
            DatabaseDescriptor.setConfig(DatabaseDescriptor.loadConfig());
        return Version.ALL.stream().map(v -> new Object[]{ v}).collect(Collectors.toList());
    }

    public TypeUtilTest(Version version)
    {
        this.version = version;
    }

    @Test
    public void testSimpleType()
    {
        for (CQL3Type cql3Type : StorageAttachedIndex.SUPPORTED_TYPES)
        {
            AbstractType<?> type = cql3Type.getType();
            AbstractType<?> reversedType = ReversedType.getInstance(type);

            boolean isUTF8OrAscii = cql3Type == CQL3Type.Native.ASCII || cql3Type == CQL3Type.Native.TEXT || cql3Type == CQL3Type.Native.VARCHAR;
            boolean isLiteral = cql3Type == CQL3Type.Native.ASCII || cql3Type == CQL3Type.Native.TEXT || cql3Type == CQL3Type.Native.VARCHAR || cql3Type == CQL3Type.Native.BOOLEAN;
            assertEquals(isLiteral, TypeUtil.isLiteral(type));
            assertEquals(TypeUtil.isLiteral(type), TypeUtil.isLiteral(reversedType));
            assertEquals(isUTF8OrAscii, TypeUtil.isUTF8OrAscii(type));
            assertEquals(TypeUtil.isUTF8OrAscii(type), TypeUtil.isUTF8OrAscii(reversedType));
            assertEquals(TypeUtil.isIn(type, AbstractAnalyzer.ANALYZABLE_TYPES),
                         TypeUtil.isIn(reversedType, AbstractAnalyzer.ANALYZABLE_TYPES));
        }
    }

    @Test
    public void testMapType()
    {
        for(CQL3Type keyCql3Type : StorageAttachedIndex.SUPPORTED_TYPES)
        {
            AbstractType<?> keyType = keyCql3Type.getType();

            testCollectionType((valueType, multiCell) -> MapType.getInstance(keyType, valueType, multiCell),
                               (valueType, nonFrozenMap) -> {
                assertEquals(keyType, cellValueType(nonFrozenMap, IndexTarget.Type.KEYS));
                assertEquals(valueType, cellValueType(nonFrozenMap, IndexTarget.Type.VALUES));
                AbstractType<?> entryType = cellValueType(nonFrozenMap, IndexTarget.Type.KEYS_AND_VALUES);
                assertEquals(CompositeType.getInstance(keyType, valueType), entryType);
                assertTrue(TypeUtil.isLiteral(entryType));
            });
        }
    }

    @Test
    public void testSetType()
    {
        testCollectionType(SetType::getInstance, (a, b) -> {});
    }

    @Test
    public void testListType()
    {
        testCollectionType(ListType::getInstance, (a, b) -> {});
    }

    @Test
    public void testTuple()
    {
        for(CQL3Type elementType : StorageAttachedIndex.SUPPORTED_TYPES)
        {
            TupleType type = new TupleType(Arrays.asList(elementType.getType(), elementType.getType()), true);
            assertFalse(TypeUtil.isFrozenCollection(type));
            assertFalse(TypeUtil.isFrozen(type));
            assertFalse(TypeUtil.isLiteral(type));

            type = new TupleType(Arrays.asList(elementType.getType(), elementType.getType()), false);
            assertFalse(TypeUtil.isFrozenCollection(type));
            assertTrue(TypeUtil.isFrozen(type));
            assertTrue(TypeUtil.isLiteral(type));
        }
    }

    @Test
    public void testUDT()
    {
        for(CQL3Type elementType : StorageAttachedIndex.SUPPORTED_TYPES)
        {
            UserType type = new UserType("ks", ByteBufferUtil.bytes("myType"),
                                         Arrays.asList(FieldIdentifier.forQuoted("f1"), FieldIdentifier.forQuoted("f2")),
                                         Arrays.asList(elementType.getType(), elementType.getType()),
                                         true);

            assertFalse(TypeUtil.isFrozenCollection(type));
            assertFalse(TypeUtil.isFrozen(type));
            assertFalse(TypeUtil.isLiteral(type));

            type = new UserType("ks", ByteBufferUtil.bytes("myType"),
                                Arrays.asList(FieldIdentifier.forQuoted("f1"), FieldIdentifier.forQuoted("f2")),
                                Arrays.asList(elementType.getType(), elementType.getType()),
                                false);
            assertFalse(TypeUtil.isFrozenCollection(type));
            assertTrue(TypeUtil.isFrozen(type));
            assertTrue(TypeUtil.isLiteral(type));
        }
    }

    private static void testCollectionType(BiFunction<AbstractType<?>, Boolean, AbstractType<?>> init,
                                           BiConsumer<AbstractType<?>, AbstractType<?>> nonFrozenCollectionTester)
    {
        for(CQL3Type elementType : StorageAttachedIndex.SUPPORTED_TYPES)
        {
            AbstractType<?> frozenCollection = init.apply(elementType.getType(), false);
            AbstractType<?> reversedFrozenCollection = ReversedType.getInstance(frozenCollection);

            AbstractType<?> type = TypeUtil.cellValueType(column(frozenCollection), IndexTarget.Type.FULL);
            assertTrue(TypeUtil.isFrozenCollection(type));
            assertTrue(TypeUtil.isLiteral(type));
            assertFalse(type.isReversed());

            type = TypeUtil.cellValueType(column(reversedFrozenCollection), IndexTarget.Type.FULL);
            assertTrue(TypeUtil.isFrozenCollection(type));
            assertTrue(TypeUtil.isLiteral(type));
            assertTrue(type.isReversed());

            AbstractType<?> nonFrozenCollection = init.apply(elementType.getType(), true);
            assertEquals(elementType.getType(), cellValueType(nonFrozenCollection, IndexTarget.Type.VALUES));
            nonFrozenCollectionTester.accept(elementType.getType(), nonFrozenCollection);
        }
    }

    private static AbstractType<?> cellValueType(AbstractType<?> type, IndexTarget.Type indexType)
    {
        return TypeUtil.cellValueType(column(type), indexType);
    }

    private static ColumnMetadata column(AbstractType<?> type)
    {
        return ColumnMetadata.regularColumn("ks", "cf", "col", type);
    }

    @Test
    public void shouldCompareByteBuffers()
    {
        final ByteBuffer a = Int32Type.instance.decompose(1);
        final ByteBuffer b = Int32Type.instance.decompose(2);

        assertEquals(a, TypeUtil.min(a, b, Int32Type.instance, version));
        assertEquals(a, TypeUtil.min(b, a, Int32Type.instance, version));
        assertEquals(a, TypeUtil.min(a, a, Int32Type.instance, version));
        assertEquals(b, TypeUtil.min(b, b, Int32Type.instance, version));
        assertEquals(b, TypeUtil.min(null, b, Int32Type.instance, version));
        assertEquals(a, TypeUtil.min(a, null, Int32Type.instance, version));

        assertEquals(b, TypeUtil.max(b, a, Int32Type.instance, version));
        assertEquals(b, TypeUtil.max(a, b, Int32Type.instance, version));
        assertEquals(a, TypeUtil.max(a, a, Int32Type.instance, version));
        assertEquals(b, TypeUtil.max(b, b, Int32Type.instance, version));
        assertEquals(b, TypeUtil.max(null, b, Int32Type.instance, version));
        assertEquals(a, TypeUtil.max(a, null, Int32Type.instance, version));
    }

    @Test
    public void testBigIntegerEncoding()
    {
        Random rng = new Random(-9078270684023566599L);

        BigInteger[] data = new BigInteger[10000];
        for (int i = 0; i < data.length; i++)
        {
            BigInteger randomNumber = new BigInteger(rng.nextInt(1000), rng);
            if (rng.nextBoolean())
                randomNumber = randomNumber.negate();

            data[i] = randomNumber;
        }

        Arrays.sort(data, BigInteger::compareTo);

        for (int i = 1; i < data.length; i++)
        {
            BigInteger i0 = data[i - 1];
            BigInteger i1 = data[i];
            assertTrue("#" + i, i0.compareTo(i1) <= 0);

            ByteBuffer b0 = TypeUtil.encode(ByteBuffer.wrap(i0.toByteArray()), IntegerType.instance);
            ByteBuffer b1 = TypeUtil.encode(ByteBuffer.wrap(i1.toByteArray()), IntegerType.instance);
            assertTrue("#" + i, TypeUtil.compare(b0, b1, IntegerType.instance, version) <= 0);
        }
    }

    @Test
    public void testMapEntryEncoding()
    {
        Random rng = new Random(-9078270684023566599L);
        CompositeType type = CompositeType.getInstance(UTF8Type.instance, Int32Type.instance);

        // simulate: index memtable insertion
        ByteBuffer[] data = new ByteBuffer[10000];
        byte[] temp = new byte[100];
        for (int i = 0; i < data.length; i++)
        {
            rng.nextBytes(temp);
            String v1 = new String(temp);
            int v2 = rng.nextInt();

            data[i] = type.decompose(v1, v2);
        }

        // Starting with DB, we sorted using the abstract type.
        if (version.onOrAfter(Version.DB))
            Arrays.sort(data, type);
        else
            Arrays.sort(data, FastByteOperations::compareUnsigned);

        for (int i = 1; i < data.length; i++)
        {
            // simulate: index memtable flush
            ByteBuffer b0 = data[i - 1];
            ByteBuffer b1 = data[i];
            assertTrue("#" + i, TypeUtil.compare(b0, b1, type, version) <= 0);

            // Before version DB, we didn't write terms in their ByteComparable order, so we skip
            // that check here.
            if (!version.onOrAfter(Version.DB))
                continue;

            // simulate: saving into on-disk trie
            ByteComparable t0 = v -> type.asComparableBytes(b0, v);
            ByteComparable t1 = v -> type.asComparableBytes(b1, v);
            assertTrue("#" + i, ByteComparable.compare(t0, t1, TypeUtil.BYTE_COMPARABLE_VERSION) <= 0);
        }
    }

    @Test
    public void testEncodeBigInteger()
    {
        testBigInteger(BigInteger.valueOf(0));
        testBigInteger(BigInteger.valueOf(1));
        testBigInteger(BigInteger.valueOf(-1));
        testBigInteger(BigInteger.valueOf(2));
        testBigInteger(BigInteger.valueOf(-2));
        testBigInteger(BigInteger.valueOf(17));
        testBigInteger(BigInteger.valueOf(-17));
        testBigInteger(BigInteger.valueOf(123456789));
        testBigInteger(BigInteger.valueOf(-123456789));
        testBigInteger(BigInteger.valueOf(1000000000000L));
        testBigInteger(BigInteger.valueOf(-1000000000000L));
        testBigInteger(BigInteger.valueOf(13).pow(1000));
        testBigInteger(BigInteger.valueOf(13).pow(1000).negate());

        testBigInteger(new BigInteger("123456789012345678901234567890123456789012345678901234567890"));
        testBigInteger(new BigInteger("-123456789012345678901234567890123456789012345678901234567890"));
    }

    private static void testBigInteger(BigInteger value)
    {
        var raw = IntegerType.instance.decompose(value);
        var raw2 = TypeUtil.decodeBigInteger(TypeUtil.encodeBigInteger(raw));
        var value2 = IntegerType.instance.compose(raw2);
        // this cannot be exact comparison, because `encode` truncates value and loses some precision
        assertEquals(value.doubleValue(), value2.doubleValue(), value.doubleValue() * 1.0e-15);
    }

    @Test
    public void testEncodeDecimal()
    {
        testDecimal(BigDecimal.valueOf(0));
        testDecimal(BigDecimal.valueOf(1));
        testDecimal(BigDecimal.valueOf(-1));
        testDecimal(BigDecimal.valueOf(12345678.9));
        testDecimal(BigDecimal.valueOf(-12345678.9));
        testDecimal(BigDecimal.valueOf(0.000005));
        testDecimal(BigDecimal.valueOf(-0.000005));
        testDecimal(BigDecimal.valueOf(0.1111111111111111));
        testDecimal(BigDecimal.valueOf(-0.1111111111111111));

        // test very large and very small values
        testDecimal(BigDecimal.valueOf(123456789, -10000));
        testDecimal(BigDecimal.valueOf(-123456789, -10000));
        testDecimal(BigDecimal.valueOf(123456789, 10000));
        testDecimal(BigDecimal.valueOf(-123456789, 10000));

        // test truncated values
        testDecimal(new BigDecimal("1234567890.1234567890123456789012345678901234567890"));
        testDecimal(new BigDecimal("-1234567890.1234567890123456789012345678901234567890"));
    }

    private static void testDecimal(BigDecimal value)
    {
        var raw = DecimalType.instance.decompose(value);
        var raw2 = TypeUtil.decodeDecimal(TypeUtil.encodeDecimal(raw));
        var value2 = DecimalType.instance.compose(raw2);
        // this cannot be exact comparison, because `encode` truncates value and loses some precision
        assertEquals(value.doubleValue(), value2.doubleValue(), value.doubleValue() * 1.0e-15);
    }
}
