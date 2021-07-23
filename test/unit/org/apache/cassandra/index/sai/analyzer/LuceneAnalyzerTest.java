
/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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

package org.apache.cassandra.index.sai.analyzer;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.common.base.Charsets;
import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.analysis.Analyzer;

import static org.junit.Assert.assertArrayEquals;

public class LuceneAnalyzerTest
{
    @Test
    public void testNgram() throws Exception
    {
        String json = "[\n" +
                      "\t{\"tokenizer\":\"ngram\", \"minGramSize\":\"2\", \"maxGramSize\":\"3\"},\n" +
                      "\t{\"filter\":\"lowercase\"}\n" +
                      "]";
        String testString = "DoG";
        String[] expected = new String[]{ "do", "dog", "og" };
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    @Test
    // TODO: doesn't work
    public void testPattern() throws Exception
    {
        String json = "{\n" +
                      "  \"tokenizer\":{\n" +
                      "    \"pattern\":{\n" +
                      "      \"pattern\":\"\\W|_\"\n" +
                      "    }\n" +
                      "  }\n" +
                      "}";
        String testString = "Günther Günther is here";
        String[] expected = new String[]{ "Günther", "Günther", "is", "here" };
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    @Test
    public void testStopWords() throws Exception
    {
        String json = "[\n" +
                      "\t{\"tokenizer\":\"standard\"},\n" +
                      "\t{\"filter\":\"lowercase\"},\n" +
                      "\t{\"filter\":\"stop\", \"stop_words\":[\"the\",\"and\",\"in\"]}\t\n" +
                      "]";
        String testString = "Dogs withEring in the winDy";
        String[] expected = new String[]{ "dogs", "withering", "windy" };
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    @Test
    public void testDefaultStopWords() throws Exception
    {
        String json = "[\n" +
                      "\t{\"tokenizer\":\"standard\"},\n" +
                      "\t{\"filter\":\"lowercase\"},\n" +
                      "\t{\"filter\":\"stop\", \"default_stop_words\":\"spanish\"}\n" +
                      "]";
        String testString = "Dogs withEring es de winDy";
        String[] expected = new String[]{ "dogs", "withering", "windy" };
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    @Test
    public void testDefaultStopWordsClass() throws Exception
    {
        String json = "[\n" +
                      "\t{\"tokenizer\":\"standard\"},\n" +
                      "\t{\"filter\":\"lowercase\"},\n" +
                      "\t{\"filter\":\"stop\",\"default_stop_words\":\"org.apache.lucene.analysis.es.SpanishAnalyzer\"}\n" +
                      "]\n";
        String testString = "Dogs withEring es de winDy";
        String[] expected = new String[]{ "dogs", "withering", "windy" };
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    @Test
    public void testEnglishAnalyzer() throws Exception
    {
        String json = "[\n" +
                      "\t{\"tokenizer\":\"whitespace\"},\n" +
                      "\t{\"filter\":\"porterstem\"}\n" +
                      "]";
        String testString = "dogs withering in the windy";
        String[] expected = new String[]{ "dog", "wither", "in", "the", "windi" };
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    public static List<String> tokenize(String testString, String json) throws Exception
    {
        Analyzer luceneAnalyzer = JSONAnalyzerParser.parse(json);
        LuceneAnalyzer analyzer = new LuceneAnalyzer(UTF8Type.instance, luceneAnalyzer, new HashMap<String, String>());

        ByteBuffer toAnalyze = ByteBuffer.wrap(testString.getBytes(Charsets.UTF_8));
        analyzer.reset(toAnalyze);
        ByteBuffer analyzed = null;

        List<String> list = new ArrayList();

        while (analyzer.hasNext())
        {
            analyzed = analyzer.next();
            list.add(ByteBufferUtil.string(analyzed, Charsets.UTF_8));
        }

        analyzer.end();

        return list;
    }
}