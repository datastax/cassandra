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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.cql3.Json;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.ckb.SoraniAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lt.LithuanianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.analysis.util.TokenFilterFactory;

import static java.util.Collections.unmodifiableMap;

public class JSONAnalyzerParser
{
    public static final Map<String, Set<?>> NAMED_STOP_WORDS;

    static
    {
        Map<String, Set<?>> namedStopWords = new HashMap<>();
        namedStopWords.put("arabic", ArabicAnalyzer.getDefaultStopSet());
        namedStopWords.put("armenian", ArmenianAnalyzer.getDefaultStopSet());
        namedStopWords.put("basque", BasqueAnalyzer.getDefaultStopSet());
        namedStopWords.put("brazilian", BrazilianAnalyzer.getDefaultStopSet());
        namedStopWords.put("bulgarian", BulgarianAnalyzer.getDefaultStopSet());
        namedStopWords.put("catalan", CatalanAnalyzer.getDefaultStopSet());
        namedStopWords.put("czech", CzechAnalyzer.getDefaultStopSet());
        namedStopWords.put("danish", DanishAnalyzer.getDefaultStopSet());
        namedStopWords.put("dutch", DutchAnalyzer.getDefaultStopSet());
        namedStopWords.put("english", EnglishAnalyzer.getDefaultStopSet());
        namedStopWords.put("finnish", FinnishAnalyzer.getDefaultStopSet());
        namedStopWords.put("french", FrenchAnalyzer.getDefaultStopSet());
        namedStopWords.put("galician", GalicianAnalyzer.getDefaultStopSet());
        namedStopWords.put("german", GermanAnalyzer.getDefaultStopSet());
        namedStopWords.put("greek", GreekAnalyzer.getDefaultStopSet());
        namedStopWords.put("hindi", HindiAnalyzer.getDefaultStopSet());
        namedStopWords.put("hungarian", HungarianAnalyzer.getDefaultStopSet());
        namedStopWords.put("indonesian", IndonesianAnalyzer.getDefaultStopSet());
        namedStopWords.put("irish", IrishAnalyzer.getDefaultStopSet());
        namedStopWords.put("italian", ItalianAnalyzer.getDefaultStopSet());
        namedStopWords.put("latvian", LatvianAnalyzer.getDefaultStopSet());
        namedStopWords.put("lithuanian", LithuanianAnalyzer.getDefaultStopSet());
        namedStopWords.put("norwegian", NorwegianAnalyzer.getDefaultStopSet());
        namedStopWords.put("persian", PersianAnalyzer.getDefaultStopSet());
        namedStopWords.put("portuguese", PortugueseAnalyzer.getDefaultStopSet());
        namedStopWords.put("romanian", RomanianAnalyzer.getDefaultStopSet());
        namedStopWords.put("russian", RussianAnalyzer.getDefaultStopSet());
        namedStopWords.put("sorani", SoraniAnalyzer.getDefaultStopSet());
        namedStopWords.put("spanish", SpanishAnalyzer.getDefaultStopSet());
        namedStopWords.put("swedish", SwedishAnalyzer.getDefaultStopSet());
        namedStopWords.put("thai", ThaiAnalyzer.getDefaultStopSet());
        namedStopWords.put("turkish", TurkishAnalyzer.getDefaultStopSet());
        NAMED_STOP_WORDS = unmodifiableMap(namedStopWords);
    }

    public static Analyzer parse(String json) throws Exception
    {
        List list = (List) Json.decodeJson(json);
        CustomAnalyzer.Builder builder = CustomAnalyzer.builder();
        for (int x = 0; x < list.size(); x++)
        {
            Map map = (Map) list.get(x);
            String analyzer = (String) map.remove("analyzer");
            if (analyzer != null)
            {
                return (Analyzer) Class.forName(analyzer).newInstance();
            }

            // remove from the map to avoid passing as parameters to the TokenFilterFactory
            String tokenizer = (String) map.remove("tokenizer");
            if (tokenizer != null)
            {
                builder.withTokenizer(tokenizer, map);
            }
            String filter = (String) map.remove("filter");
            // the following filters are unsupported because they load local files
            // see StopWordFilterFactory for a conversion from lucene's way to encoding everything in json
            if (filter != null && filter.equals("synonymgraph"))
            {
                throw new Exception("filter=synonymgraph is unsupported.");
            }
            else if (filter != null && filter.equals("synonym"))
            {
                throw new Exception("filter=synonym is unsupported.");
            }
            else if (filter != null && filter.equals("commongrams"))
            {
                throw new Exception("filter=commongrams is unsupported.");
            }
            else if (filter != null && filter.equals("stop"))
            {
                String defaultStopWords = (String) map.get("default_stop_words");
                CharArraySet stopWordsChars = null;
                if (defaultStopWords != null)
                {
                    // first lookup in NAMED_STOP_WORDS
                    stopWordsChars = (CharArraySet) NAMED_STOP_WORDS.get(defaultStopWords);
                    if (stopWordsChars == null)
                    {
                        try
                        {
                            Class clazz = Class.forName(defaultStopWords);
                            Method method = clazz.getDeclaredMethod("getDefaultStopSet");
                            stopWordsChars = (CharArraySet) method.invoke(null, null);
                        }
                        catch (ClassNotFoundException ex)
                        {
                            throw new Exception("default_stop_words=" + defaultStopWords + " cannot be found.", ex);
                        }
                    }
                }

                List stopWords = (List) map.remove("stop_words");
                if (stopWords != null)
                {
                    stopWordsChars = new CharArraySet(10, false);

                    for (int y=0; y < stopWords.size(); y++)
                    {
                        stopWordsChars.add((String)stopWords.get(y));
                    }
                }
                TokenFilterFactory factory = new StopWordFilterFactory(map, stopWordsChars);
                builder.addTokenFilter(factory);
            }
            else if (filter != null)
            {
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
