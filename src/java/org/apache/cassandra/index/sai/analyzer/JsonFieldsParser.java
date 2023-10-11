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
import java.nio.ByteBuffer;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.cassandra.index.sai.analyzer.json.JsonFieldExtractor;
import org.apache.cassandra.index.sai.analyzer.json.JsonFieldExtractorFactory;
import org.apache.cassandra.utils.ByteBufferUtil;

public class JsonFieldsParser implements DataParser
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final JsonFieldExtractor extr;
    public JsonFieldsParser(String parseFields){
        extr = JsonFieldExtractorFactory.construct(MAPPER)
                                        .buildExtractor(parseFields);
    }
    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    public ByteBuffer parse(ByteBuffer input) throws IOException
    {
        final byte firstByte = input.duplicate().get(0);
        if(firstByte == '{'){
            return ByteBuffer.wrap(extr.extractAsBytes(input));
        }else {
            return input;
        }
    }

    public int readShortLength(ByteBuffer bb)
    {
        int length = (bb.get() & 0xFF) << 8;
        return length | (bb.get() & 0xFF);
    }
}
