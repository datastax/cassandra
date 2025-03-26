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
package org.apache.cassandra.index.sai.metrics;

import org.junit.BeforeClass;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;


@RunWith(Enclosed.class)
public class TinySegmentFlushingFailureTest extends CQLTester
{
    /**
     * Set the necessary configuration before any tests run.
     */
    @BeforeClass
    public static void setUpClass()
    {
        try
        {
            Field confField = DatabaseDescriptor.class.getDeclaredField("conf");
            confField.setAccessible(true);
            Config conf = (Config) confField.get(null);
            conf.sai_options.segment_write_buffer_space_mb = 0;

            System.out.println("Configuration set: segment_write_buffer_space_mb = " + 0);
        }
        catch (NoSuchFieldException | IllegalAccessException e)
        {
            throw new RuntimeException("Failed to set configuration segment_write_buffer_space_mb = 0", e);
        }
        
        CQLTester.setUpClass();
    }

    /**
     * These tests will run only after the outer class has completed its setup. Otherwise, SAITester assigns default
     * value to segment_write_buffer_space_mb, and we cannot override it without reflection or using Unsafe.
     */
    public static class TinySegmentFlushingFailureInnerClassTest extends SegmentFlushingFailureTest
    {

        @Override
        protected long expectedBytesLimit()
        {
            return 0;
        }
    }
}