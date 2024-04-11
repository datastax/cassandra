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

package org.apache.cassandra.cql3.functions;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.assertj.core.api.Assertions;

public class VectorFctsTest extends CQLTester
{
    @BeforeClass
    public static void setupClass()
    {
        System.setProperty("cassandra.float_only_vectors", "false");
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

        execute("INSERT INTO %s (k, v) VALUES (0, ?)", vector(3.0f, 4.0f));

        assertRows(execute("SELECT normalize_l2(v) FROM %s"), row(vector(0.6f, 0.8f)));
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

    @SafeVarargs
    protected final <T> Vector<T> vector(T... values)
    {
        return new Vector<>(values);
    }

}
