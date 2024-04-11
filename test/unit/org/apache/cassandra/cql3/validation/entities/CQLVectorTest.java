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

package org.apache.cassandra.cql3.validation.entities;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.functions.NativeFunctions;
import org.apache.cassandra.cql3.functions.NativeScalarFunction;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;

public class CQLVectorTest extends CQLTester
{
    @BeforeClass
    public static void setupClass()
    {
        System.setProperty("cassandra.float_only_vectors", "false");
    }

    @Test
    public void select()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk vector<int, 2> primary key)");

        execute("INSERT INTO %s (pk) VALUES ([1, 2])");

        Vector<Integer> vector = vector(1, 2);
        Object[] row = row(vector);

        assertRows(execute("SELECT * FROM %s WHERE pk = [1, 2]"), row);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", vector), row);
        assertRows(execute("SELECT * FROM %s WHERE pk = [1, 1 + 1]"), row);
        assertRows(execute("SELECT * FROM %s WHERE pk = [1, ?]", 2), row);
        assertRows(execute("SELECT * FROM %s WHERE pk = [1, (int) ?]", 2), row);
        assertRows(execute("SELECT * FROM %s WHERE pk = [1, 1 + (int) ?]", 1), row);

        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, 2])"), row);
        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, 2], [1, 2])"), row);
        assertRows(execute("SELECT * FROM %s WHERE pk IN (?)", vector), row);
        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, 1 + 1])"), row);
        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, ?])", 2), row);
        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, (int) ?])", 2), row);
        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, 1 + (int) ?])", 1), row);

        assertRows(execute("SELECT * FROM %s WHERE pk > [0, 0] AND pk < [1, 3] ALLOW FILTERING"), row);
        assertRows(execute("SELECT * FROM %s WHERE token(pk) = token([1, 2])"), row);

        assertRows(execute("SELECT * FROM %s"), row);
        Assertions.assertThat(execute("SELECT * FROM %s").one().getVector("pk", Int32Type.instance, 2))
                  .isEqualTo(vector);
    }

    @Test
    public void insert()
    {
        Runnable test = () -> {
            assertRows(execute("SELECT * FROM %s"), row(list(1, 2)));
            execute("TRUNCATE %s");
            assertRows(execute("SELECT * FROM %s"));
        };

        createTable(KEYSPACE, "CREATE TABLE %s (pk vector<int, 2> primary key)");

        execute("INSERT INTO %s (pk) VALUES ([1, 2])");
        test.run();

        execute("INSERT INTO %s (pk) VALUES (?)", vector(1, 2));
        test.run();

        execute("INSERT INTO %s (pk) VALUES ([1, 1 + 1])");
        test.run();

        execute("INSERT INTO %s (pk) VALUES ([1, ?])", 2);
        test.run();

        execute("INSERT INTO %s (pk) VALUES ([1, (int) ?])", 2);
        test.run();

        execute("INSERT INTO %s (pk) VALUES ([1, 1 + (int) ?])", 1);
        test.run();
    }

    @Test
    public void insertNonPK()
    {
        Runnable test = () -> {
            assertRows(execute("SELECT * FROM %s"), row(0, list(1, 2)));
            execute("TRUNCATE %s");
            assertRows(execute("SELECT * FROM %s"));
        };

        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<int, 2>)");

        execute("INSERT INTO %s (pk, value) VALUES (0, [1, 2])");
        test.run();

        execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1, 2));
        test.run();

        execute("INSERT INTO %s (pk, value) VALUES (0, [1, 1 + 1])");
        test.run();

        execute("INSERT INTO %s (pk, value) VALUES (0, [1, ?])", 2);
        test.run();

        execute("INSERT INTO %s (pk, value) VALUES (0, [1, (int) ?])", 2);
        test.run();

        execute("INSERT INTO %s (pk, value) VALUES (0, [1, 1 + (int) ?])", 1);
        test.run();
    }

    @Test
    public void invalidNumberOfDimensionsFixedWidth() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value vector<int, 2>)");

        // fewer values than expected, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value of type vector<int, 2>; expected 2 elements, but given 1",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, [1])");
        assertInvalidThrowMessage("Not enough bytes to read a vector<int, 2>",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1));

        // more values than expected, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value of type vector<int, 2>; expected 2 elements, but given 3",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, [1, 2, 3])");
        assertInvalidThrowMessage("Unexpected 4 extraneous bytes after vector<int, 2> value",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1, 2, 3));
    }

    @Test
    public void invalidNumberOfDimensionsVariableWidth() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value vector<text, 2>)");

        // fewer values than expected, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value of type vector<text, 2>; expected 2 elements, but given 1",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ['a'])");
        assertInvalidThrowMessage("Not enough bytes to read a vector<text, 2>",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector("a"));

        // more values than expected, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value of type vector<text, 2>; expected 2 elements, but given 3",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ['a', 'b', 'c'])");
        assertInvalidThrowMessage("Unexpected 2 extraneous bytes after vector<text, 2> value",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector("a", "b", "c"));
    }

    @Test
    public void sandwichBetweenUDTs()
    {
        createType("CREATE TYPE cql_test_keyspace.b (y int);");
        createType("CREATE TYPE cql_test_keyspace.a (z vector<frozen<b>, 2>);");

        createTable("CREATE TABLE %s (pk int primary key, value a)");

        execute("INSERT INTO %s (pk, value) VALUES (0, {z: [{y:1}, {y:2}]})");
        assertRows(execute("SELECT * FROM %s"),
                   row(0, userType("z", vector(userType("y", 1), userType("y", 2)))));
    }

    @Test
    public void invalidElementTypeFixedWidth() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value vector<int, 2>)");

        // fixed-length bigint instead of int, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value: value (bigint)1 is not of type int",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, [(bigint) 1, (bigint) 2])");
        assertInvalidThrowMessage("Unexpected 8 extraneous bytes after vector<int, 2> value",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1L, Long.MAX_VALUE));

        // variable-length text instead of int, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value: value 'a' is not of type int",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ['a', 'b'])");
        assertInvalidThrowMessage("Not enough bytes to read a vector<int, 2>",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector("a", "b"));
    }

    @Test
    public void invalidElementTypeVariableWidth() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value vector<text, 2>)");

        // fixed-length int instead of text, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value: value 1 is not of type text",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, [1, 2])");
        assertInvalidThrowMessage("Unexpected 6 extraneous bytes after vector<text, 2> value",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1, 2));

        // variable-length varint instead of text, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value: value (varint)1 is not of type text",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, [(varint) 1, (varint) 2])");
        assertInvalidThrowMessage("String didn't validate.",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)",
                                  vector(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), BigInteger.ONE));
    }

    @Test
    public void update()
    {
        Runnable test = () -> {
            assertRows(execute("SELECT * FROM %s"), row(0, list(1, 2)));
            execute("TRUNCATE %s");
            assertRows(execute("SELECT * FROM %s"));
        };

        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<int, 2>)");

        execute("UPDATE %s set VALUE = [1, 2] WHERE pk = 0");
        test.run();

        execute("UPDATE %s set VALUE = ? WHERE pk = 0", vector(1, 2));
        test.run();

        execute("UPDATE %s set VALUE = [1, 1 + 1] WHERE pk = 0");
        test.run();

        execute("UPDATE %s set VALUE = [1, ?] WHERE pk = 0", 2);
        test.run();

        execute("UPDATE %s set VALUE = [1, (int) ?] WHERE pk = 0", 2);
        test.run();

        execute("UPDATE %s set VALUE = [1, 1 + (int) ?] WHERE pk = 0", 1);
        test.run();
    }

    @Test
    public void nullValues()
    {
        assertAcceptsNullValues("int"); // fixed length
        assertAcceptsNullValues("float"); // fixed length with special/optimized treatment
        assertAcceptsNullValues("text"); // variable length
    }

    private void assertAcceptsNullValues(String type)
    {
        createTable(format("CREATE TABLE %%s (k int primary key, v vector<%s, 2>)", type));

        execute("INSERT INTO %s (k, v) VALUES (0, null)");
        assertRows(execute("SELECT * FROM %s"), row(0, null));

        execute("INSERT INTO %s (k, v) VALUES (0, ?)", (List<Integer>) null);
        assertRows(execute("SELECT * FROM %s"), row(0, null));
    }

    @Test
    public void emptyValues() throws Throwable
    {
        assertRejectsEmptyValues("int"); // fixed length
        assertRejectsEmptyValues("float"); // fixed length with special/optimized treatment
        assertRejectsEmptyValues("text"); // variable length
    }

    private void assertRejectsEmptyValues(String type) throws Throwable
    {
        createTable(format("CREATE TABLE %%s (k int primary key, v vector<%s, 2>)", type));

        assertInvalidThrowMessage(format("Invalid HEX constant (0x) for \"v\" of type vector<%s, 2>", type),
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (k, v) VALUES (0, 0x)");

        assertInvalidThrowMessage("Invalid empty vector value",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (k, v) VALUES (0, ?)",
                                  ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    @Test
    public void functions()
    {
        VectorType<Integer> type = VectorType.getInstance(Int32Type.instance, 2);
        Vector<Integer> vector = vector(1, 2);

        NativeFunctions.instance.add(new NativeScalarFunction("f", type, type)
        {
            @Override
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters) throws InvalidRequestException
            {
                return parameters.get(0);
            }
        });

        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<int, 2>)");
        execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector);

        assertRows(execute("SELECT f(value) FROM %s WHERE pk=0"), row(vector));
        assertRows(execute("SELECT f([1, 2]) FROM %s WHERE pk=0"), row(vector));
    }

    @Test
    public void specializedFunctions()
    {
        VectorType<Float> type = VectorType.getInstance(FloatType.instance, 2);
        Vector<Float> vector = vector(1.0f, 2.0f);

        NativeFunctions.instance.add(new NativeScalarFunction("f", type, type, type)
        {
            @Override
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters) throws InvalidRequestException
            {
                float[] left = type.composeAsFloat(parameters.get(0));
                float[] right = type.composeAsFloat(parameters.get(1));
                int size = Math.min(left.length, right.length);
                float[] sum = new float[size];
                for (int i = 0; i < size; i++)
                    sum[i] = left[i] + right[i];
                return type.getSerializer().serializeFloatArray(sum);
            }
        });

        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<float, 2>)");
        execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector);
        execute("INSERT INTO %s (pk, value) VALUES (1, ?)", vector);

        Object[][] expected = { row(vector(2f, 4f)), row(vector(2f, 4f)) };
        assertRows(execute("SELECT f(value, [1.0, 2.0]) FROM %s"), expected);
        assertRows(execute("SELECT f([1.0, 2.0], value) FROM %s"), expected);
    }

    @Test
    public void token()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk vector<int, 2> primary key)");
        execute("INSERT INTO %s (pk) VALUES (?)", vector(1, 2));
        long tokenColumn = execute("SELECT token(pk) as t FROM %s").one().getLong("t");
        long tokenTerminal = execute("SELECT token([1, 2]) as t FROM %s").one().getLong("t");
        Assert.assertEquals(tokenColumn, tokenTerminal);
    }

    @Test
    public void udf() throws Throwable
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<int, 2>)");
        Vector<Integer> vector = vector(1, 2);
        execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector);

        // identity function
        String f = createFunction(KEYSPACE,
                                  "",
                                  "CREATE FUNCTION %s (x vector<int, 2>) " +
                                  "CALLED ON NULL INPUT " +
                                  "RETURNS vector<int, 2> " +
                                  "LANGUAGE java " +
                                  "AS 'return x;'");
        assertRows(execute(format("SELECT %s(value) FROM %%s", f)), row(vector));
        assertRows(execute(format("SELECT %s([2, 3]) FROM %%s", f)), row(vector(2, 3)));
        assertRows(execute(format("SELECT %s(null) FROM %%s", f)), row((Vector<Integer>) null));

        // identitiy function with nested type
        f = createFunction(KEYSPACE,
                           "",
                           "CREATE FUNCTION %s (x list<vector<int, 2>>) " +
                           "CALLED ON NULL INPUT " +
                           "RETURNS list<vector<int, 2>> " +
                           "LANGUAGE java " +
                           "AS 'return x;'");
        assertRows(execute(format("SELECT %s([value]) FROM %%s", f)), row(list(vector)));
        assertRows(execute(format("SELECT %s([[2, 3]]) FROM %%s", f)), row(list(vector(2, 3))));
        assertRows(execute(format("SELECT %s(null) FROM %%s", f)), row((Vector<Integer>) null));

        // identitiy function with elements of variable length
        f = createFunction(KEYSPACE,
                           "",
                           "CREATE FUNCTION %s (x vector<text, 2>) " +
                           "CALLED ON NULL INPUT " +
                           "RETURNS vector<text, 2> " +
                           "LANGUAGE java " +
                           "AS 'return x;'");
        assertRows(execute(format("SELECT %s(['abc', 'defghij']) FROM %%s", f)), row(vector("abc", "defghij")));
        assertRows(execute(format("SELECT %s(null) FROM %%s", f)), row((Vector<Integer>) null));

        // Test wrong types on function creation
        assertInvalidThrowMessage("vectors may only have positive dimensions; given 0",
                                  InvalidRequestException.class,
                                  "CREATE FUNCTION %s (x vector<int, 0>) " +
                                  "CALLED ON NULL INPUT " +
                                  "RETURNS vector<int, 2> " +
                                  "LANGUAGE java " +
                                  "AS 'return x;'");
        assertInvalidThrowMessage("vectors may only have positive dimensions; given 0",
                                  InvalidRequestException.class,
                                  "CREATE FUNCTION %s (x vector<int, 2>) " +
                                  "CALLED ON NULL INPUT " +
                                  "RETURNS vector<int, 0> " +
                                  "LANGUAGE java " +
                                  "AS 'return x;'");

        // make sure the function referencing the UDT is dropped before dropping the UDT at cleanup
        execute("DROP FUNCTION " + f);
    }

    @SafeVarargs
    protected final <T> Vector<T> vector(T... values)
    {
        return new Vector<>(values);
    }
}
