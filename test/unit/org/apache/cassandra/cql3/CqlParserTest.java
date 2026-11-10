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
package org.apache.cassandra.cql3;

import java.util.List;

import org.junit.Test;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.BaseRecognizer;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.apache.cassandra.cql3.statements.PropertyDefinitions;

import static org.junit.Assert.*;

public class CqlParserTest
{
    @Test
    public void testAddErrorListener() throws Exception
    {
        SyntaxErrorCounter firstCounter = new SyntaxErrorCounter();
        SyntaxErrorCounter secondCounter = new SyntaxErrorCounter();

        CharStream stream = new ANTLRStringStream("SELECT * FORM FROM test");
        CqlLexer lexer = new CqlLexer(stream);

        TokenStream tokenStream = new CommonTokenStream(lexer);
        CqlParser parser = new CqlParser(tokenStream);
        parser.addErrorListener(firstCounter);
        parser.addErrorListener(secondCounter);

        // By default CqlParser should recover from the syntax error by removing FORM
        // but as recoverFromMismatchedToken and recover have been overloaded, it will not
        // and the returned ParsedStatement will be null.
        assertNull(parser.query());

        // Only one error must be reported (mismatched: FORM).
        assertEquals(1, firstCounter.count);
        assertEquals(1, secondCounter.count);
    }

    @Test
    public void testRemoveErrorListener() throws Exception
    {
        SyntaxErrorCounter firstCounter = new SyntaxErrorCounter();
        SyntaxErrorCounter secondCounter = new SyntaxErrorCounter();

        CharStream stream = new ANTLRStringStream("SELECT * FORM test;");
        CqlLexer lexer = new CqlLexer(stream);

        TokenStream tokenStream = new CommonTokenStream(lexer);
        CqlParser parser = new CqlParser(tokenStream);
        parser.addErrorListener(firstCounter);
        parser.addErrorListener(secondCounter);
        parser.removeErrorListener(secondCounter);

        parser.query();

        assertEquals(1, firstCounter.count);
        assertEquals(0, secondCounter.count);
    }

    /**
     * {@code CONTAINS ELEMENT (...)} is a group of relations on the elements of a collection, but {@code ELEMENT} is
     * an unreserved keyword: {@code CONTAINS element(...)} stays a call to a function named {@code element}, and
     * {@code element} a valid column and field name.
     */
    @Test
    public void testContainsElement() throws Exception
    {
        assertElementMatch("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)", false, "addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)");
        assertElementMatch("addrs NOT CONTAINS ELEMENT (city = 'Rome')", true, "addrs NOT CONTAINS ELEMENT (city = 'Rome')");
        assertElementMatch("addrs contains element ((city = 'Rome' OR zip >= 1) AND loc.lat < 2)", false,
                           "addrs CONTAINS ELEMENT ((city = 'Rome' OR zip >= 1) AND loc.lat < 2)");
        assertElementMatch("p.home.names CONTAINS ELEMENT (first = ? AND last IN ('a', 'b'))", false,
                           "p.home.names CONTAINS ELEMENT (first = ? AND last IN ('a', 'b'))");
        assertElementMatch("element CONTAINS ELEMENT (element = 1)", false, "element CONTAINS ELEMENT (element = 1)");
        assertElementMatch("addrs CONTAINS ELEMENT (city LIKE 'R%' AND tags NOT CONTAINS 'x' AND zip NOT IN (1, 2))", false,
                           "addrs CONTAINS ELEMENT (city LIKE 'R%' AND tags NOT CONTAINS 'x' AND zip NOT IN (1, 2))");

        // calls to a function named element
        for (String cql : new String[]{ "c CONTAINS element(1)", "c CONTAINS element(?)", "c NOT CONTAINS element(:x)",
                                        "c CONTAINS element(foo(1))", "c CONTAINS element((int) 1)", "c CONTAINS element('a', 1)",
                                        "c CONTAINS KEY element(1)", "c CONTAINS ELEMENT ()" })
        {
            WhereClause clause = WhereClause.parse(cql);
            List<Relation> relations = clause.root().relations();
            assertEquals(cql, 1, relations.size());
            assertTrue(cql, relations.get(0) instanceof SingleColumnRelation);
        }

        // a group in a WHERE clause with other relations
        WhereClause clause = WhereClause.parse("id = 1 AND (addrs CONTAINS ELEMENT (city = 'a') OR v = 2) AND addrs CONTAINS ELEMENT (zip = 1)");
        assertEquals("id = 1 AND (addrs CONTAINS ELEMENT (city = 'a') OR v = 2) AND addrs CONTAINS ELEMENT (zip = 1)", clause.toCQLString());

        parseAndCountErrors("addrs CONTAINS ELEMENT (city = 1", 1, CqlParser::whereClause);
        parseAndCountErrors("addrs CONTAINS ELEMENT city = 1", 1, CqlParser::whereClause);
    }

    private static void assertElementMatch(String cql, boolean negated, String expected) throws Exception
    {
        WhereClause clause = WhereClause.parse(cql);
        List<Relation> relations = clause.root().relations();
        assertEquals(cql, 1, relations.size());
        assertTrue(cql, relations.get(0) instanceof ElementMatchRelation);
        ElementMatchRelation relation = (ElementMatchRelation) relations.get(0);
        assertEquals(negated, relation.isNegated());
        assertEquals(expected, relation.toCQLString());
    }

    @Test
    public void testDuplicateProperties() throws Exception
    {
        parseAndCountErrors("properties = { 'foo' : 'value1', 'bar': 'value2' };", 0, (p) -> p.properties(new PropertyDefinitions()));
        parseAndCountErrors("properties = { 'foo' : 'value1', 'foo': 'value2' };", 1, (p) -> p.properties(new PropertyDefinitions()));
        parseAndCountErrors("foo = 'value1' AND bar = 'value2' };", 0, (p) -> p.properties(new PropertyDefinitions()));
        parseAndCountErrors("foo = 'value1' AND foo = 'value2' };", 1, (p) -> p.properties(new PropertyDefinitions()));
    }

    private void parseAndCountErrors(String cql, int expectedErrors, ParserOperation operation) throws RecognitionException
    {
        SyntaxErrorCounter counter = new SyntaxErrorCounter();
        CharStream stream = new ANTLRStringStream(cql);
        CqlLexer lexer = new CqlLexer(stream);
        TokenStream tokenStream = new CommonTokenStream(lexer);
        CqlParser parser = new CqlParser(tokenStream);
        parser.addErrorListener(counter);

        operation.perform(parser);

        assertEquals(expectedErrors, counter.count);
    }

    @FunctionalInterface
    private interface ParserOperation
    {
        void perform(CqlParser cqlParser) throws RecognitionException;
    }

    private static final class SyntaxErrorCounter implements ErrorListener
    {
        private int count;

        @Override
        public void syntaxError(BaseRecognizer recognizer, String[] tokenNames, RecognitionException e)
        {
            count++;
        }

        @Override
        public void syntaxError(BaseRecognizer recognizer, String errorMsg)
        {
            count++;
        }
    }
}
