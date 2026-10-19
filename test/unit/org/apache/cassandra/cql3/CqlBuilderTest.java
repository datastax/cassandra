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
package org.apache.cassandra.cql3;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Collections;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CqlBuilderTest
{
    @Test
    public void testAppendOptionsWithSingleOption()
    {
        CqlBuilder builder = new CqlBuilder();
        Map<String, String> options = new LinkedHashMap<>();
        options.put("class", "SimpleStrategy");
        options.put("replication_factor", "3");
        
        builder.appendOptions(optionsBuilder -> 
            optionsBuilder.append("replication", options)
        );
        
        assertEquals(" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}", 
                     builder.toString());
    }
    
    @Test
    public void testAppendOptionsWithMultipleOptions()
    {
        CqlBuilder builder = new CqlBuilder();
        Map<String, String> option1 = new LinkedHashMap<>();
        option1.put("class", "NetworkTopologyStrategy");
        option1.put("dc1", "3");
        option1.put("dc2", "2");
        
        Map<String, String> option2 = new LinkedHashMap<>();
        option2.put("enabled", "true");
        option2.put("chunk_length_in_kb", "64");
        
        builder.appendOptions(optionsBuilder -> 
            optionsBuilder.append("replication", option1)
                          .append("compression", option2)
        );
        
        assertEquals(" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '2'}" +
                     " AND compression = {'chunk_length_in_kb': '64', 'enabled': 'true'}", 
                     builder.toString());
    }
    
    @Test
    public void testAppendOptionsWithEmptyOptions()
    {
        CqlBuilder builder = new CqlBuilder();
        Map<String, String> emptyOptions = Collections.emptyMap();
        Map<String, String> validOptions = new LinkedHashMap<>();
        validOptions.put("class", "SimpleStrategy");
        validOptions.put("replication_factor", "1");
        
        builder.appendOptions(optionsBuilder -> 
            optionsBuilder.append("empty", emptyOptions)
                          .append("replication", validOptions)
        );
        
        assertEquals(" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", 
                     builder.toString());
    }
    
    @Test
    public void testAppendOptionsAllEmpty()
    {
        CqlBuilder builder = new CqlBuilder();
        Map<String, String> emptyOptions1 = Collections.emptyMap();
        Map<String, String> emptyOptions2 = Collections.emptyMap();
        
        builder.appendOptions(optionsBuilder -> 
            optionsBuilder.append("option1", emptyOptions1)
                          .append("option2", emptyOptions2)
        );
        
        assertEquals("", builder.toString());
    }
    
    @Test
    public void testAppendOptionsWithSpecialCharacters()
    {
        CqlBuilder builder = new CqlBuilder();
        Map<String, String> options = new LinkedHashMap<>();
        options.put("test'key", "test'value");
        options.put("another\"key", "another\"value");
        
        builder.appendOptions(optionsBuilder -> 
            optionsBuilder.append("special", options)
        );
        
        assertEquals(" WITH special = {'another\"key': 'another\"value', 'test''key': 'test''value'}", 
                     builder.toString());
    }
    
    @Test
    public void testAppendOptionsWithExistingContent()
    {
        CqlBuilder builder = new CqlBuilder();
        builder.append("CREATE KEYSPACE test");
        
        Map<String, String> options = new LinkedHashMap<>();
        options.put("class", "SimpleStrategy");
        options.put("replication_factor", "1");
        
        builder.appendOptions(optionsBuilder -> 
            optionsBuilder.append("replication", options)
        );
        
        assertEquals("CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", 
                     builder.toString());
    }
    
    @Test
    public void testAppendOptionsChaining()
    {
        CqlBuilder builder = new CqlBuilder();
        Map<String, String> option1 = new LinkedHashMap<>();
        option1.put("key1", "value1");
        
        Map<String, String> option2 = new LinkedHashMap<>();
        option2.put("key2", "value2");
        
        Map<String, String> option3 = new LinkedHashMap<>();
        option3.put("key3", "value3");
        
        builder.append("CREATE TABLE test")
               .appendOptions(optionsBuilder -> 
                   optionsBuilder.append("option1", option1)
                                 .append("option2", option2)
                                 .append("option3", option3)
               )
               .append(";");
        
        assertEquals("CREATE TABLE test WITH option1 = {'key1': 'value1'}" +
                     " AND option2 = {'key2': 'value2'}" +
                     " AND option3 = {'key3': 'value3'};", 
                     builder.toString());
    }
    
    @Test
    public void testOptionsBuilderWithMixedEmptyAndNonEmpty()
    {
        CqlBuilder builder = new CqlBuilder();
        Map<String, String> emptyMap = Collections.emptyMap();
        Map<String, String> option1 = new LinkedHashMap<>();
        option1.put("key1", "value1");
        
        Map<String, String> option2 = new LinkedHashMap<>();
        option2.put("key2", "value2");
        
        builder.appendOptions(optionsBuilder -> 
            optionsBuilder.append("empty1", emptyMap)
                          .append("option1", option1)
                          .append("empty2", emptyMap)
                          .append("option2", option2)
                          .append("empty3", emptyMap)
        );
        
        assertEquals(" WITH option1 = {'key1': 'value1'} AND option2 = {'key2': 'value2'}", 
                     builder.toString());
    }
    
    @Test
    public void testAppendOptionsSortsMapKeys()
    {
        CqlBuilder builder = new CqlBuilder();
        Map<String, String> options = new LinkedHashMap<>();
        options.put("zulu", "26");
        options.put("alpha", "1");
        options.put("bravo", "2");
        options.put("charlie", "3");
        
        builder.appendOptions(optionsBuilder -> 
            optionsBuilder.append("ordered", options)
        );
        
        // CqlBuilder sorts map keys alphabetically
        assertEquals(" WITH ordered = {'alpha': '1', 'bravo': '2', 'charlie': '3', 'zulu': '26'}", 
                     builder.toString());
    }
}