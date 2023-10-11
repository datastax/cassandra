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

    public JsonFieldExtractor buildExtractor(String commaSeparatedPaths) {
        return JsonFieldExtractor.construct(jsonFactory, commaSeparatedPaths);
    }
}