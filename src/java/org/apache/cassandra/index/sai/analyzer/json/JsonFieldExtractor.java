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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.core.filter.FilteringParserDelegate;
import com.fasterxml.jackson.core.filter.TokenFilter;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class JsonFieldExtractor {
    private final JsonFactory jsonFactory;
    private final TokenFilter filter;

    /*
    /**********************************************************
    /* Construction
    /**********************************************************
     */

    private JsonFieldExtractor(JsonFactory jsonFactory, TokenFilter filter) {
        this.jsonFactory = jsonFactory;
        this.filter = filter;
    }

    public static JsonFieldExtractor construct(JsonFactory jsonFactory,
                                               String commaSeparatedPaths) {
        return new JsonFieldExtractor(jsonFactory,
                PathBasedFilterFactory.filterForPaths(commaSeparatedPaths));
    }

    /*
    /**********************************************************
    /* Public API
    /**********************************************************
     */

    public String extractAsString(String json) throws IOException {
        try (JsonParser p = jsonFactory.createParser(json)) {
            return _extractAsString(p, json.length());
        }
    }

    public String extractAsString(byte[] json) throws IOException {
        try (JsonParser p = jsonFactory.createParser(json)) {
            return _extractAsString(p, json.length);
        }
    }

    public String extractAsString(ByteBuffer json) throws IOException {
        try (InputStream in = new ByteBufferBackedInputStream(json)) {
            try (JsonParser p = jsonFactory.createParser(in)) {
                return _extractAsString(p, json.remaining());
            }
        }
    }

    public byte[] extractAsBytes(String json) throws IOException {
        try (JsonParser p = jsonFactory.createParser(json)) {
            return _extractAsBytes(p, json.length());
        }
    }

    public byte[] extractAsBytes(byte[] json) throws IOException {
        try (JsonParser p = jsonFactory.createParser(json)) {
            return _extractAsBytes(p, json.length);
        }
    }

    public byte[] extractAsBytes(ByteBuffer json) throws IOException {
        try (InputStream in = new ByteBufferBackedInputStream(json)) {
            try (JsonParser p = jsonFactory.createParser(in)) {
                return _extractAsBytes(p, json.remaining());
            }
        }
    }

    // Method mostly useful for testing purposes
    public JsonParser extractingParser(String json) throws IOException {
        JsonParser p = jsonFactory.createParser(json);
        JsonParser fp = new FilteringParserDelegate(p, filter,
                TokenFilter.Inclusion.INCLUDE_ALL_AND_PATH, true);
        return fp;
    }

    /*
    /**********************************************************
    /* Internal helper methods
    /**********************************************************
     */

    private String _extractAsString(JsonParser p, int jsonLength) throws IOException {
        StringWriter sw = new StringWriter(estimateResultLength(jsonLength));
        try (JsonParser fp = new FilteringParserDelegate(p, filter,
                TokenFilter.Inclusion.ONLY_INCLUDE_ALL, true)) {
            while (fp.nextToken() != null) {
                if (includeToken(fp.currentTokenId())) {
                    sw.append(fp.getText()).append(' ');
                }
            }
        }
        return sw.toString();
    }

    private byte[] _extractAsBytes(JsonParser p, int jsonLength) throws IOException {
        // !!! TODO: implement more efficiently
        return _extractAsString(p, jsonLength).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Helper method for estimating rough size of output buffer we need, to reduce
     * need for resizing, but ideally avoiding overallocation.
     */
    private int estimateResultLength(int jsonLength) {
        // Estimate that we'll need output buffer that's 1/4 the size of the input
        int estimate = jsonLength >> 2;
        // But avoid tiny buffers
        if (estimate < 100) {
            return 100;
        }
        if (estimate > 50_000) {
            return 50_000;
        }
        return estimate;
    }

    private boolean includeToken(int id) {
        switch (id) {
        case JsonTokenId.ID_STRING:
        case JsonTokenId.ID_NUMBER_FLOAT:
        case JsonTokenId.ID_NUMBER_INT:
        case JsonTokenId.ID_FALSE:
        case JsonTokenId.ID_TRUE:
            return true;
        default:
            return false;
        }
    }
}