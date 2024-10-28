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

package org.apache.cassandra.dht.tokenallocator;

import java.util.Random;
import java.util.TreeMap;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NoReplicationTokenAllocatorFastTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void testOverridenSeedToken()
    {
        var random = new Random();
        var initialToken = Murmur3Partitioner.instance.getToken(UTF8Type.instance.fromString("initial"));
        // Confirm that up to num_tokens 32, we get the same first token.
        for (int i = 0; i < 32; i++)
        {
            var allocator = new NoReplicationTokenAllocator<>(new TreeMap<>(), new BasicReplicationStrategy(),
                                                              Murmur3Partitioner.instance, () -> initialToken);
            var tokens = allocator.addUnit(random.nextInt(), i);
            var first = tokens.stream().findFirst();
            assertTrue(first.isPresent());
            assertEquals(first.get().getToken(), initialToken);
        }
    }

    private static class BasicReplicationStrategy implements ReplicationStrategy<Integer>
    {
        @Override
        public int replicas()
        {
            return 1;
        }

        @Override
        public Object getGroup(Integer unit)
        {
            return unit;
        }
    }
}
