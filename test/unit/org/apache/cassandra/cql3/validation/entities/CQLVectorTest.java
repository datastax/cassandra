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

import com.google.common.primitives.Floats;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.assertj.core.api.Assertions;

public class CQLVectorTest extends CQLTester
{
    @BeforeClass
    public static void setupClass()
    {
        System.setProperty("cassandra.float_only_vectors", "false");
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
    public void randomVectorFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value vector<float, 2>)");

        // correct usage
        execute("INSERT INTO %s (pk, value) VALUES (0, random_float_vector(2, -1, 1))");
        Assert.assertEquals(1, execute("SELECT value FROM %s WHERE pk = 0").size());

        // wrong number of arguments
        assertInvalidThrowMessage("Invalid number of arguments for function system.random_float_vector(literal_int, float, float)",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, random_float_vector())");
        assertInvalidThrowMessage("Invalid number of arguments for function system.random_float_vector(literal_int, float, float)",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, random_float_vector(2, -1))");
        assertInvalidThrowMessage("Invalid number of arguments for function system.random_float_vector(literal_int, float, float)",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, random_float_vector(2, -1, 1, 0))");

        // mandatory arguments
        assertInvalidThrowMessage("Function system.random_float_vector(literal_int, float, float) requires a literal_int argument, " +
                                  "but found NULL",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, random_float_vector(null, null, null))");
        assertInvalidThrowMessage("Function system.random_float_vector(literal_int, float, float) requires a literal_int argument, " +
                                  "but found NULL",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, random_float_vector(null, -1, 1))");
        assertInvalidThrowMessage("Min argument of function system.random_float_vector(literal_int, float, float) must not be null",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, random_float_vector(2, null, null))");
        assertInvalidThrowMessage("Max argument of function system.random_float_vector(literal_int, float, float) must not be null",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, random_float_vector(2, -1, null))");
        assertInvalidThrowMessage("Min argument of function system.random_float_vector(literal_int, float, float) must not be null",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, random_float_vector(2, null, 1))");

        // wrong argument types
        assertInvalidThrowMessage("Function system.random_float_vector(literal_int, float, float) requires a literal_int argument, " +
                                  "but found 'a'",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, random_float_vector('a', -1, 1))");
        assertInvalidThrowMessage("Function system.random_float_vector(literal_int, float, float) requires a literal_int argument, " +
                                  "but found system.\"_add\"(1, 1)",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, random_float_vector(1 + 1, -1, 1))");
        assertInvalidThrowMessage("Function system.random_float_vector(literal_int, float, float) requires a literal_int argument, " +
                                  "but found value",
                                  InvalidRequestException.class,
                                  "SELECT random_float_vector(value, -1, 1) FROM %s");
        assertInvalidThrowMessage("Function system.random_float_vector(literal_int, float, float) requires a literal_int argument, " +
                                  "but found 1 + 1",
                                  InvalidRequestException.class,
                                  "SELECT random_float_vector(1 + 1, -1, 1) FROM %s");

        // wrong argument values
        assertInvalidThrowMessage("Max value must be greater than min value",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, random_float_vector(2, 1, -1))");

        // correct function with wrong receiver type
        assertInvalidThrowMessage("Type error: cannot assign result of function system.random_float_vector " +
                                  "(type vector<float, 1>) to value (type vector<float, 2>)",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, random_float_vector(1, -1, 1))");

        // test select
        for (int dimension : new int[]{ 1, 2, 3, 10, 1000 })
        {
            assertSelectRandomVectorFunction(dimension, -1, 1);
            assertSelectRandomVectorFunction(dimension, 0, 1);
            assertSelectRandomVectorFunction(dimension, -1.5f, 1.5f);
            assertSelectRandomVectorFunction(dimension, 0.999999f, 1);
            assertSelectRandomVectorFunction(dimension, 0, 0.000001f);
            assertSelectRandomVectorFunction(dimension, Float.MIN_VALUE, Float.MAX_VALUE);
        }
    }

    private void assertSelectRandomVectorFunction(int dimension, float min, float max)
    {
        String functionCall = String.format("random_float_vector(%d, %f, %f)", dimension, min, max);
        String select = "SELECT " + functionCall + " FROM %s";

        for (int i = 0; i < 100; i++)
        {
            UntypedResultSet rs = execute(select);
            Assertions.assertThat(rs).isNotEmpty();
            Assertions.assertThat(rs.one().getVector("system." + functionCall, FloatType.instance, dimension))
                      .hasSize(dimension)
                      .allSatisfy(v -> Assertions.assertThat(v).isBetween(min, max));
        }
    }

    @Test
    public void normalizeL2Function() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 2>)");

        float[] components = new float[]{3.0f, 4.0f};
        execute("INSERT INTO %s (k, v) VALUES (0, ?)", floatVector(components));

        assertRows(execute("SELECT normalize_l2(v) FROM %s"), row(floatVector(0.6f, 0.8f)));
        assertRows(execute("SELECT k, normalize_l2((vector<float, 2>) null) FROM %s"), row(0, null));

        assertInvalidThrowMessage("Invalid number of arguments for function system.normalize_l2(vector<float, n>)",
                                  InvalidRequestException.class,
                                  "SELECT normalize_l2() FROM %s");
        assertInvalidThrowMessage("Invalid number of arguments for function system.normalize_l2(vector<float, n>)",
                                  InvalidRequestException.class,
                                  "SELECT normalize_l2(v, 1) FROM %s");
        assertInvalidThrowMessage("Function system.normalize_l2(vector<float, n>) requires a float vector argument, " +
                                  "but found argument 123 of type int",
                                  InvalidRequestException.class,
                                  "SELECT normalize_l2(123) FROM %s");
    }

    protected final Vector<Float> floatVector(float... values)
    {
        return new Vector<>(Floats.asList(values).toArray(new Float[values.length]));
    }

    @SafeVarargs
    protected final <T> Vector<T> vector(T... values)
    {
        return new Vector<>(values);
    }
}
