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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.json.simple.parser.JSONParser;

public class JSONAnalyzerParser
{
    public static class NameOptions
    {
        final String type;
        final Map<String,String> options;
        final String name;

        public NameOptions(String type, Map<String, String> options, String name)
        {
            this.type = type;
            this.options = options;
            this.name = name;
        }
    }

    public static NameOptions parseNameOptions(Map.Entry entry)
    {
        String name = (String) entry.getKey();
        Object tokenizerObject = entry.getValue();
        if (tokenizerObject instanceof String)
        {
            return new NameOptions(name, new HashMap(), (String)tokenizerObject);
        }
        else
        {
            Map tokenizer = (Map) tokenizerObject;
            for (Object tokEntryObj : tokenizer.entrySet())
            {
                Map.Entry e = (Map.Entry) tokEntryObj;
                String tokenizerName = (String) e.getKey();
                Object tokObj = e.getValue();

                Map tokOptions = (Map) tokObj;
                HashMap<String, String> stringMap = new HashMap();
                for (Object optionObj : tokOptions.entrySet())
                {
                    Map.Entry option = (Map.Entry) optionObj;
                    String optionName = (String) option.getKey();
                    String optionValue = (String) option.getValue();
                    stringMap.put(optionName, optionValue);
                }
                return new NameOptions(name, stringMap, tokenizerName);
            }
        }
        throw new IllegalStateException();
    }

    public static Analyzer parse(String json) throws Exception
    {
        JSONParser parser = new JSONParser();
        Map map = (Map) parser.parse(json);
        String analyzer = (String)map.get("analyzer");
        if (analyzer != null)
        {
            return (Analyzer)Class.forName(analyzer).newInstance();
        }

        CustomAnalyzer.Builder builder = CustomAnalyzer.builder();

        for (Object entryObj : map.entrySet())
        {
            Map.Entry entry = (Map.Entry) entryObj;

            NameOptions nameOptions = parseNameOptions(entry);

            if (nameOptions.type.equals("tokenizer"))
            {
                builder.withTokenizer(nameOptions.name, nameOptions.options);
            }
            else if (nameOptions.type.equals("filter"))
            {
                builder.addTokenFilter(nameOptions.name, nameOptions.options);
            }
            else if (nameOptions.type.equals("charfilter"))
            {
                builder.addCharFilter(nameOptions.name, nameOptions.options);
            }
            else
            {
                throw new Exception("Unknown type: "+nameOptions.type);
            }
        }
        return builder.build();
    }
}
