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

import org.junit.Test;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;

public class IndexFctsTest extends SAITester
{
    @Test
    public void testAnalyzeFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        execute("INSERT INTO %s (k, v) VALUES (1, 'johnny apples seedlings')");
        execute("INSERT INTO %s (k, v) VALUES (2, null)");

        assertRows(execute("SELECT k, sai_analyze(v, ?) FROM %s",
                           "{\n" +
                           "\t\"tokenizer\":{\"name\":\"whitespace\"},\n" +
                           "\t\"tokenizer\":{\"name\":\"whitespace\"},\n" +
                           "\t\"filters\":[{\"name\":\"porterstem\"}]\n" +
                           '}'),
                   row(1, list("johnni", "appl", "seedl")),
                   row(2, null));

        assertInvalidThrowMessage("Function system.sai_analyze requires a non-null json_analyzer parameter (2nd argument)",
                                  InvalidRequestException.class,
                                  "SELECT sai_analyze(v, null) FROM %s");

        assertInvalidThrowMessage("Function system.sai_analyze unable to analyze text=abc json_analyzer=def",
                                  InvalidRequestException.class,
                                  "SELECT sai_analyze('abc', 'def') FROM %s");
    }
}
