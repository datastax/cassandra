/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.common.base.Charsets;

import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.analyzer.JSONAnalyzerParser;
import org.apache.cassandra.index.sai.analyzer.LuceneAnalyzer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.analysis.Analyzer;

public abstract class IndexFcts
{
    public static void addFunctionsTo(NativeFunctions functions)
    {
        functions.add(new SAIAnalyzeFunction());
    }

    /**
     * CQL native function to get the tokens produced for given text value and the analyzer defined by the given JSON options.
     */
    private static class SAIAnalyzeFunction extends NativeScalarFunction
    {
        private static final String NAME = "sai_analyze";
        private static final ListType<String> returnType = ListType.getInstance(UTF8Type.instance, false);

        private SAIAnalyzeFunction()
        {
            super(NAME, returnType, UTF8Type.instance, UTF8Type.instance);
        }

        @Override
        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters) throws InvalidRequestException
        {
            if (parameters.get(0) == null)
                return null;
            String text = UTF8Type.instance.compose(parameters.get(0));

            if (parameters.get(1) == null)
                throw new InvalidRequestException("Function " + name + " requires a non-null json_analyzer parameter (2nd argument)");
            String json = UTF8Type.instance.compose(parameters.get(1));

            LuceneAnalyzer luceneAnalyzer = null;
            List<String> tokens = new ArrayList<>();
            try (Analyzer analyzer = JSONAnalyzerParser.parse(json))
            {
                luceneAnalyzer = new LuceneAnalyzer(UTF8Type.instance, analyzer, new HashMap<>());

                ByteBuffer toAnalyze = ByteBuffer.wrap(text.getBytes(Charsets.UTF_8));
                luceneAnalyzer.reset(toAnalyze);
                ByteBuffer analyzed;

                while (luceneAnalyzer.hasNext())
                {
                    analyzed = luceneAnalyzer.next();
                    tokens.add(ByteBufferUtil.string(analyzed, Charsets.UTF_8));
                }
            }
            catch (Exception ex)
            {
                throw new InvalidRequestException("Function " + name + " unable to analyze text=" + text + " json_analyzer=" + json, ex);
            }
            finally
            {
                if (luceneAnalyzer != null)
                {
                    luceneAnalyzer.end();
                }
            }

            return returnType.decompose(tokens);
        }
    }
}
