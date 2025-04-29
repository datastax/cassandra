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

package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.Index;
import org.assertj.core.api.Assertions;

public class OperatorTest
{
    @Test
    public void testAnalyzer()
    {
        // test with a text-based case-insensitive analyzer
        UTF8Type utf8Type = UTF8Type.instance;
        Function<ByteBuffer, List<ByteBuffer>> analyzer = value -> Collections.singletonList(utf8Type.decompose(utf8Type.compose(value).toUpperCase()));
        testAnalyzer(utf8Type, utf8Type.decompose("FOO"), utf8Type.decompose("FOO"), analyzer, true);
        testAnalyzer(utf8Type, utf8Type.decompose("FOO"), utf8Type.decompose("foo"), analyzer, true);
        testAnalyzer(utf8Type, utf8Type.decompose("foo"), utf8Type.decompose("foo"), analyzer, true);
        testAnalyzer(utf8Type, utf8Type.decompose("foo"), utf8Type.decompose("FOO"), analyzer, true);
        testAnalyzer(utf8Type, utf8Type.decompose("foo"), utf8Type.decompose("abc"), analyzer, false);

        // test with an int-based analyzer that decomposes an integer into its digits
        Int32Type intType = Int32Type.instance;

        analyzer = value -> intType.compose(value)
                                   .toString()
                                   .chars()
                                   .boxed()
                                   .map(intType::decompose)
                                   .collect(Collectors.toList());
        testAnalyzer(intType, intType.decompose(123), intType.decompose(123), analyzer, true);
        testAnalyzer(intType, intType.decompose(123), intType.decompose(1), analyzer, true);
        testAnalyzer(intType, intType.decompose(123), intType.decompose(2), analyzer, true);
        testAnalyzer(intType, intType.decompose(123), intType.decompose(3), analyzer, true);
        testAnalyzer(utf8Type, intType.decompose(123), intType.decompose(4), analyzer, false);
        testAnalyzer(utf8Type, intType.decompose(123), intType.decompose(12), analyzer, true);
        testAnalyzer(utf8Type, intType.decompose(123), intType.decompose(23), analyzer, true);
        testAnalyzer(utf8Type, intType.decompose(123), intType.decompose(13), analyzer, true);
        testAnalyzer(utf8Type, intType.decompose(123), intType.decompose(321), analyzer, true);
        testAnalyzer(utf8Type, intType.decompose(123), intType.decompose(1234), analyzer, false);
    }

    private static Index.Analyzer analyzer(Function<ByteBuffer, List<ByteBuffer>> analyzer, ByteBuffer queriedValue)
    {
        return new Index.Analyzer()
        {
            @Override
            public List<ByteBuffer> indexedTokens(ByteBuffer value)
            {
                return analyzer.apply(value);
            }

            @Override
            public List<ByteBuffer> queriedTokens()
            {
                return analyzer.apply(queriedValue);
            }
        };
    }

    private static void testAnalyzer(AbstractType<?> type,
                                     ByteBuffer leftOperand,
                                     ByteBuffer rightOperand,
                                     Function<ByteBuffer, List<ByteBuffer>> analyzingFunction,
                                     boolean shouldBeSatisfied)
    {
        // analyze the operands
        Index.Analyzer analyzer = analyzer(analyzingFunction, rightOperand);
        List<ByteBuffer> indexedTokens = analyzer.indexedTokens(leftOperand);
        List<ByteBuffer> queriedTokens = analyzer.queriedTokens();

        // test that EQ and ANALYZER_MATCHES are satisfied by the same value with an analyzer
        for (Operator operator : Arrays.asList(Operator.EQ, Operator.ANALYZER_MATCHES))
            Assertions.assertThat(operator.isSatisfiedByAnalyzed(type, indexedTokens, queriedTokens)).isEqualTo(shouldBeSatisfied);

        // test that EQ without an analyzer behaves as type-based identity
        Assertions.assertThat(Operator.EQ.isSatisfiedBy(type, leftOperand, rightOperand))
                  .isEqualTo(type.compareForCQL(leftOperand, rightOperand) == 0);

        // test that ANALYZER_MATCHES throws an exception when no analyzer is provided
        Assertions.assertThatThrownBy(() -> Operator.ANALYZER_MATCHES.isSatisfiedBy(type, leftOperand, rightOperand))
                  .isInstanceOf(UnsupportedOperationException.class)
                  .hasMessageContaining(": operation can only be computed by an indexed column with a configured analyzer");

        // test that all other operators don't support the analyzer
        for (Operator operator : Operator.values())
        {
            if (operator == Operator.EQ || operator == Operator.ANALYZER_MATCHES)
                continue;

            try
            {
                operator.isSatisfiedBy(type, leftOperand, rightOperand);
            }
            catch (Exception e)
            {
                Assertions.assertThatThrownBy(() -> operator.isSatisfiedByAnalyzed(type, indexedTokens, queriedTokens))
                          .isInstanceOf(UnsupportedOperationException.class)
                          .hasMessageContaining(operator + " operation does not support analyzers");
            }
        }
    }
}
