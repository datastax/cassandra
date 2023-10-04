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
import org.apache.cassandra.utils.ByteBufferUtil;

public class JsonFieldsParser implements DataParser
{
    private final List<String> parseFields;
    public JsonFieldsParser(List<String> parseFields){
        this.parseFields = parseFields;
    }
    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    public ByteBuffer parse(ByteBuffer input) throws IOException
    {
        String json = ByteBufferUtil.string(input.duplicate());
        if(json.charAt(0) == '{'){
            final ObjectNode jsonValue = (ObjectNode) mapper.readTree(json);
            StringBuilder sb = new StringBuilder();
            for (String field : parseFields)
            {
                JsonNode node = jsonValue.get(field);
                if(node != null){
                    sb.append(node.asText());
                    sb.append(" ");
                }
            }
            return ByteBufferUtil.bytes(sb.toString());
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
