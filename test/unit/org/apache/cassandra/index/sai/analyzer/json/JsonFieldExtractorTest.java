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

package org.apache.cassandra.index.sai.analyzer.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonFieldExtractorTest {
    private final ObjectMapper MAPPER = new ObjectMapper();

    private final JsonFieldExtractorFactory EXTRACTOR_FACTORY
            = JsonFieldExtractorFactory.construct(MAPPER);
    /*
    /**********************************************************
    /* Basic Object tests
    /**********************************************************
     */

    @Test
    public void testSimpleObjectInclusion() throws Exception {
        verifyInclusion("{'a':1,'b':2,'c':3}", "a",
                        "{'a':1}",
                        "1");
        verifyInclusion("{'a':{'b':1,'c':true,'x':false},'d':'xyz'}", "a.c",
                        "{'a':{'c':true}}",
                        "true");
        verifyInclusion("{'a':{'b':1,'c':true,'x':false},'d':'xyz'}", "a.b, a.x",
                        "{'a':{'b':1,'x':false}}",
                        "1 false");
        verifyInclusion("{'a':{'b':1,'c':true,'x':false},'d':'xyz'}", "d, a.b",
                        "{'a':{'b':1},'d':'xyz'}",
                        "1 xyz");
    }

    /*
    /**********************************************************
    /* Object, with missing paths
    /**********************************************************
     */

    @Test
    public void testMissingInclusion() throws Exception {
        verifyInclusion("{'a':1,'b':2,'c':3}", " ",
                "",
                "");

        verifyInclusion("{'a':1,'b':2,'c':3}", ", ",
                "",
                "");
    }

    @Test
    public void testEmptyInclusion() throws Exception {
        verifyInclusion("{'a':1,'b':2,'c':3}", " x, y",
                "",
                "");
    }

    // Test to verify that longer path won't match shorter one
    @Test
    public void testEmptyViaLongerShouldNotMatch() throws Exception {
        verifyInclusion("{'a':1,'b':2,'c':3}", "a.x",
                "",
                "");
    }

    /*
    /**********************************************************
    /* Object, overlapping paths
    /**********************************************************
     */

    @Test
    public void testLongerPathLast() throws Exception {
        verifyInclusion("{'a':1,'b':{'x':1,'y':2},'c':true}", "b, b.y",
                        "{'b':{'x':1,'y':2}}",
                        "1 2");
    }

    @Test
    public void testLongerPathFirst() throws Exception {
        verifyInclusion("{'a':1,'b':{'x':1,'y':2},'c':true}", "b.x, b",
                        "{'b':{'x':1,'y':2}}",
                        "1 2");
    }

    @Test
    public void testMultiLevelMatching() throws Exception {
        verifyInclusion("{'first':123,'a':{'b':{'x':{'z':'value'}},'c':true},'y':2}",
                        "a, a.b, a.b.x",
                        "{'a':{'b':{'x':{'z':'value'}},'c':true}}",
                        "value true");
    }

    /*
    /**********************************************************
    /* Array/nested Array tests
    /**********************************************************
     */

    @Test
    public void testSimpleArrayInclusion() throws Exception {
        verifyInclusion("{'a':'123','arr': ['abc', 'def']}", "arr",
                        "{'arr':['abc','def']}",
                        "abc def");
        verifyInclusion("{'a':'123','arr': ['abc', 'def']}", "a, ",
                        "{'a':'123'}",
                        "123");
        verifyInclusion("{'a':{'b':{'arr': ['abc', 'def']}}, 'z':3 }", "a.b ",
                        "{'a':{'b':{'arr':['abc','def']}}}",
                        "abc def");
    }

    @Test
    public void testNestedInArrayInclusion() throws Exception {
        verifyInclusion("{'arr': [{'name':'Bob','age':20},{'name':'Jack','age':30}]}",
                        "arr.name",
                        "{'arr':[{'name':'Bob'},{'name':'Jack'}]}",
                        "Bob Jack");
        verifyInclusion("{'arr': [{'name':'Bob','age':20},{'name':'Jack','age':30}]}",
                        "arr.age",
                        "{'arr':[{'age':20},{'age':30}]}",
                        "20 30");
    }

    /*
    /**********************************************************
    /* Bigger document test
    /**********************************************************
     */

    @Test
    public void testDocsApiExampleDoc() throws Exception
    {
        JsonFieldExtractor extr = EXTRACTOR_FACTORY.buildExtractor("products.food.Pear");
        try (InputStream in = getClass().getResourceAsStream("/jmh/docsapi-example.json")) {
            try (JsonParser p = extr.extractingParser(in)) {
                String str = extr._extractAsString(p, 1000);
                assertThat(str).isEqualTo("pear 0.89");
            }
        }

        // Use the buildExtractor that takes pre-split Paths
        extr = EXTRACTOR_FACTORY.buildExtractor(Arrays.asList("products.food.Apple",
                                                              "products.food.Orange "));
        try (InputStream in = getClass().getResourceAsStream("/jmh/docsapi-example.json")) {
            try (JsonParser p = extr.extractingParser(in)) {
                String str = extr._extractAsString(p, 1000);
                assertThat(str).isEqualTo("apple 0.99 100100010101001 orange 600.01");
            }
        }
    }

    /*
    /**********************************************************
    /* Non-JSON validation
    /**********************************************************
     */

    @Test
    public void testNonJSONHandling() throws Exception {
        JsonFieldExtractor extr = EXTRACTOR_FACTORY.buildExtractor("a,b");

        assertThat(extr.extractAsString("not json")).isEqualTo(Optional.empty());
        final byte[] docBytes = "Some text".getBytes("UTF-8");
        assertThat(extr.extractAsString(docBytes)).isEqualTo(Optional.empty());
        assertThat(extr.extractAsString(ByteBuffer.wrap(docBytes))).isEqualTo(Optional.empty());
    }

    /*
    /**********************************************************
    /* Checking for "isEmpty()" aspects
    /**********************************************************
     */

    @Test
    public void testWhetherExtractorEmpty() throws Exception {
        // First cases where not empty
        assertThat(EXTRACTOR_FACTORY.buildExtractor("field").isEmpty()).isFalse();
        assertThat(EXTRACTOR_FACTORY.buildExtractor("a,b").isEmpty()).isFalse();
        assertThat(EXTRACTOR_FACTORY.buildExtractor(",b").isEmpty()).isFalse();
        assertThat(EXTRACTOR_FACTORY.buildExtractor("path.from.root, ").isEmpty()).isFalse();

        // And then empty cases
        assertThat(EXTRACTOR_FACTORY.buildExtractor("").isEmpty()).isTrue();
        assertThat(EXTRACTOR_FACTORY.buildExtractor("    ").isEmpty()).isTrue();
        assertThat(EXTRACTOR_FACTORY.buildExtractor(",").isEmpty()).isTrue();
        assertThat(EXTRACTOR_FACTORY.buildExtractor(",,,").isEmpty()).isTrue();
        assertThat(EXTRACTOR_FACTORY.buildExtractor(" , ,  ").isEmpty()).isTrue();
        assertThat(EXTRACTOR_FACTORY.buildExtractor(" , ,\t").isEmpty()).isTrue();
    }

    /*
    /**********************************************************
    /* Helper methods
    /**********************************************************
     */

    private void verifyInclusion(String json, String paths, String expJson, String expText) throws Exception{
        assertThat(filterAsJson(json, paths)).isEqualTo(a2q(expJson));
        assertThat(filterAsText(json, paths)).isEqualTo(expText);
    }

    private String filterAsJson(String json, String paths) throws IOException {
        json = a2q(json);
        JsonFieldExtractor extr = EXTRACTOR_FACTORY.buildExtractor(paths);
        StringWriter sw = new StringWriter();
        try (JsonParser p = extr.extractingParser(json).get()) {
            try (JsonGenerator g = MAPPER.createGenerator(sw)) {
                while (p.nextToken() != null) {
                    g.copyCurrentStructure(p);
                }
            }
        }
        return sw.toString();
    }

    private String filterAsText(String json, String paths) throws IOException {
        json = a2q(json); // just so tests can use single quotes
        JsonFieldExtractor extr = EXTRACTOR_FACTORY.buildExtractor(paths);
        return extr.extractAsString(json).get();
    }

    protected static String a2q(String json) {
        return json.replace("'", "\"");
    }
}