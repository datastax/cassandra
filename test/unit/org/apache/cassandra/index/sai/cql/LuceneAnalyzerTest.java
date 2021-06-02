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

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import org.apache.cassandra.index.sai.SAITester;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class LuceneAnalyzerTest extends SAITester
{
    @Test
    public void testQueryAnalyzer() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = { \n" +
                    "'json_analyzer': '{\n" +
                    "  \"tokenizer\":{\n" +
                    "     \"ngram\":{\n" +
                    "       \"minGramSize\":\"2\",\n" +
                    "       \"maxGramSize\":\"3\"\n" +
                    "     }\n" +
                    "  }\n" +
                    "}',\n" +
                    "'json_query_analyzer': '{\n" +
                    "  \"tokenizer\":\"standard\"\n" +
                    "}\n'};");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'queries')");

        flush();

        assertEquals(0, execute("SELECT * FROM %s WHERE val = 'query'").size());
    }

    @Test
    public void testBogusOption() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'json_analyzer':'{\n" +
                                            "  \"analyzerlalalala\":\"org.apache.lucene.analysis.en.EnglishAnalyzer\"\n" +
                                            "}'}")).isInstanceOf(InvalidConfigurationInQueryException.class);
    }

    @Test
    public void testAnalyzer() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'json_analyzer':'{\n" +
                    "  \"analyzer\":\"org.apache.lucene.analysis.en.EnglishAnalyzer\"\n" +
                    "}'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'queries')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'query'").size());
    }

    @Test
    public void testBogusAnalyzer() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'json_analyzer':'{\n" +
                                            "  \"analyzer\":\"org.apache.lucene.analysis.en.English888Analyzer\"\n" +
                                            "}'}")).isInstanceOf(InvalidConfigurationInQueryException.class);
    }

    @Test
    public void testCharfilter() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'json_analyzer':'{\n" +
                    "  \"tokenizer\":\"keyword\",\n" +
                    "  \"charfilter\":\"htmlstrip\"\n" +
                    "}'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', '<b>hello</b>')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'hello'").size());
    }

    @Test
    public void testTokenizer() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'json_analyzer':'{\n" +
                    "  \"tokenizer\":\"whitespace\",\n" +
                    "  \"filter\":\"porterstem\"\n" +
                    "}'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'queries')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'query'").size());
    }
}
