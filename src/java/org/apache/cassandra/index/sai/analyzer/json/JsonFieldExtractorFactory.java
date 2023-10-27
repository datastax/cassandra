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
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Factory for constructing reusable {@link JsonFieldExtractor} instances.
 */
public class JsonFieldExtractorFactory {
    private final JsonFactory jsonFactory;

    private JsonFieldExtractorFactory(JsonFactory jf) {
        jsonFactory = jf;
    }

    public static JsonFieldExtractorFactory construct(JsonFactory jf) {
        return new JsonFieldExtractorFactory(jf);
    }

    public static JsonFieldExtractorFactory construct(ObjectMapper mapper) {
        return JsonFieldExtractorFactory.construct(mapper.getFactory());
    }

    /**
     * Factory method for constructing {@link JsonFieldExtractor} for paths specified
     * by a comma-separated list of dotted-notation paths.
     *
     * @param commaSeparatedInclusionPaths String containing zero or more paths, separated by commas.
     *   Each path will be trimmed of leading and trailing whitespace; empty paths ignored.
     *   Non-empty paths are further split by commas to separate individual path segments.
     *   Actual filter is built by a union of paths used for inclusion
     *
     * @return Extractor based on path definition
     */
    public JsonFieldExtractor buildExtractor(String commaSeparatedInclusionPaths) {
        return JsonFieldExtractor.construct(jsonFactory, commaSeparatedInclusionPaths);
    }

    /**
     * Factory method for constructing {@link JsonFieldExtractor} for a List of
     * dotted-notation paths.
     *
     * @param inclusionPaths List of zero or more dotted-notation inclusion paths.
     *   Each path will be trimmed of leading and trailing whitespace; empty paths ignored.
     *   Non-empty paths are further split by commas to separate individual path segments.
     *   Actual filter is built by a union of paths used for inclusion
     *
     * @return Extractor based on path definition
     */
    public JsonFieldExtractor buildExtractor(List<String> inclusionPaths) {
        return JsonFieldExtractor.construct(jsonFactory, inclusionPaths);
    }
}