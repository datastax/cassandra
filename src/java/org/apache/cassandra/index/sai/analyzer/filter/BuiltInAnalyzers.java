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

package org.apache.cassandra.index.sai.analyzer.filter;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.classic.ClassicAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

/**
 * Built-in {@link Analyzer} implementations. These are provided to allow users to easily configure analyzers with
 * a single word.
 */
public enum BuiltInAnalyzers
{
    STANDARD
    {
        public Analyzer getNewAnalyzer()
        {
            return new StandardAnalyzer();
        }
    },
    SIMPLE
    {
        public Analyzer getNewAnalyzer()
        {
            return new SimpleAnalyzer();
        }
    },
    WHITESPACE
    {
        public Analyzer getNewAnalyzer()
        {
            return new WhitespaceAnalyzer();
        }
    },
    CLASSIC
    {
        public Analyzer getNewAnalyzer()
        {
            return new ClassicAnalyzer();
        }
    },
    LOWERCASE
    {
        public Analyzer getNewAnalyzer()
        {
            try
            {
                CustomAnalyzer.Builder builder = CustomAnalyzer.builder();
                builder.withTokenizer("keyword");
                builder.addTokenFilter("lowercase");
                return builder.build();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    },
    ;

    public abstract Analyzer getNewAnalyzer();
}
