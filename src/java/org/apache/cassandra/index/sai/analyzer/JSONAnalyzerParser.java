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

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Sets;

import org.apache.cassandra.cql3.Json;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;

public class JSONAnalyzerParser
{
    // unsupported because these filters open external files such as stop words
    public static final HashSet<String> unsupportedFilters =
    Sets.newHashSet("synonymgraph", // same as synonym
                    "synonym", // replaces words, loads external file, could be implemented
                    "commongrams", // loads external file terms, search feature
                    "stop", // stop words remove terms which doens't make sense for a database index
                    "snowballporter"); // bug in reflection instantiation

    public static Analyzer parse(String json) throws Exception
    {
        List list = (List) Json.decodeJson(json);
        CustomAnalyzer.Builder builder = CustomAnalyzer.builder();
        for (int x = 0; x < list.size(); x++)
        {
            Map map = (Map) list.get(x);

            // remove from the map to avoid passing as parameters to the TokenFilterFactory
            String tokenizer = (String) map.remove("tokenizer");
            if (tokenizer != null)
            {
                builder.withTokenizer(tokenizer, map);
            }
            String filter = (String) map.remove("filter");
            if (filter != null)
            {
                if (unsupportedFilters.contains(filter))
                {
                    throw new Exception("filter=" + filter + " is unsupported.");
                }
                builder.addTokenFilter(filter, map);
            }

            String charfilter = (String) map.remove("charfilter");
            if (charfilter != null)
            {
                builder.addCharFilter(charfilter, map);
            }
        }
        return builder.build();
    }
}
