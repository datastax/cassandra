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

package org.apache.cassandra.service;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Provides an instance of {@link Mutator} that facilitates mutation writes for standard mutations, unlogged batches,
 * counters and paxos commits (LWT)s.
 * <br/>
 * An implementation may choose to fallback to the default implementation ({@link StorageProxy.DefaultMutator})
 * obtained via {@link #getDefaultMutator()}.
 */
public abstract class MutatorProvider
{
    static Mutator instance = getCustomOrDefault();

    private static Mutator getCustomOrDefault()
    {
        if (CassandraRelevantProperties.CUSTOM_MUTATOR_CLASS.isPresent())
        {
            String providerClassName = CassandraRelevantProperties.CUSTOM_MUTATOR_CLASS.getString();
            Class<Mutator> providerClass = FBUtilities.classForName(providerClassName,
                                                                    "Custom implementation of a mutation performer");
            try
            {
                return providerClass.newInstance();
            }
            catch (InstantiationException | IllegalAccessException ex)
            {
                throw new IllegalArgumentException(String.format("Failed to initialize mutator instance of %s " +
                                                                 "defined in %s system property.", providerClassName,
                                                                 CassandraRelevantProperties.CUSTOM_MUTATOR_CLASS), ex);
            }
        }
        else
        {
            return getDefaultMutator();
        }
    }

    public static Mutator getDefaultMutator()
    {
        return new StorageProxy.DefaultMutator();
    }
}
