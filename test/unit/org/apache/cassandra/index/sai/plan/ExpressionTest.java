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

package org.apache.cassandra.index.sai.plan;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.assertj.core.api.Assertions.assertThat;

public class ExpressionTest
{

    @Test
    public void testBoundHashCode()
    {
        ByteBuffer buf1 = UTF8Type.instance.decompose("blah");
        Expression.Bound b1 = new Expression.Bound(buf1, UTF8Type.instance, true);
        ByteBuffer buf2 = UTF8Type.instance.decompose("blah");
        Expression.Bound b2 = new Expression.Bound(buf2, UTF8Type.instance, true);
        assertEquals(b1, b2);
        assertEquals(b1.hashCode(), b2.hashCode());
    }

    @Test
    public void testNotMatchingBoundHashCode()
    {
        ByteBuffer buf1 = UTF8Type.instance.decompose("blah");
        Expression.Bound b1 = new Expression.Bound(buf1, UTF8Type.instance, true);
        ByteBuffer buf2 = UTF8Type.instance.decompose("blah2");
        Expression.Bound b2 = new Expression.Bound(buf2, UTF8Type.instance, true);
        assertNotEquals(b1, b2);
        assertNotEquals(b1.hashCode(), b2.hashCode());
    }

    @Test
    public void toStringRedactionTest()
    {
        // test a few operators, just enough to see different variants to the string representations
        Expression expression = makeExpression(Operator.EQ);
        assertThat(expression.toString(true))
                .isEqualTo("Expression{name: col, op: EQ, lower: (?, true), upper: (?, true), exclusions: []}");
        assertThat(expression.toString(false))
                .isEqualTo("Expression{name: col, op: EQ, lower: (sensitive, true), upper: (sensitive, true), exclusions: []}");

        expression = makeExpression(Operator.GT);
        assertThat(expression.toString(true))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (?, false), upper: (null, false), exclusions: []}");
        assertThat(expression.toString(false))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (sensitive, false), upper: (null, false), exclusions: []}");

        expression = makeExpression(Operator.GTE);
        assertThat(expression.toString(true))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (?, true), upper: (null, false), exclusions: []}");
        assertThat(expression.toString(false))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (sensitive, true), upper: (null, false), exclusions: []}");

        expression = makeExpression(Operator.LT);
        assertThat(expression.toString(true))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (null, false), upper: (?, false), exclusions: []}");
        assertThat(expression.toString(false))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (null, false), upper: (sensitive, false), exclusions: []}");

        expression = makeExpression(Operator.LTE);
        assertThat(expression.toString(true))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (null, false), upper: (?, true), exclusions: []}");
        assertThat(expression.toString(false))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (null, false), upper: (sensitive, true), exclusions: []}");

        // test exclusions, which also need redaction
        expression = new Expression(SAITester.createIndexContext("col", UTF8Type.instance));
        expression.add(Operator.GT, UTF8Type.instance.decompose("sensitive_1"));
        expression.add(Operator.NEQ, UTF8Type.instance.decompose("sensitive_2"));
        assertThat(expression.toString(true))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (?, false), upper: (null, false), exclusions: [?]}");
        assertThat(expression.toString(false))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (sensitive_1, false), upper: (null, false), exclusions: [sensitive_2]}");
    }

    private static Expression makeExpression(Operator op)
    {
        Expression expression = new Expression(SAITester.createIndexContext("col", UTF8Type.instance));
        expression.add(op, UTF8Type.instance.decompose("sensitive"));
        return expression;
    }
}
