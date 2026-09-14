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

package org.apache.cassandra.service.reads.range;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_REPLICA_PLAN_ITERATOR_PROVIDER_PROPERTY;

public interface ReplicaPlanIteratorProvider
{
    ReplicaPlanIteratorProvider instance = CUSTOM_REPLICA_PLAN_ITERATOR_PROVIDER_PROPERTY.isPresent()
                                     ? FBUtilities.construct(CUSTOM_REPLICA_PLAN_ITERATOR_PROVIDER_PROPERTY.getString(),
                                                             "Replica Plan Iterator Provider")
                                     : new DefaultReplicaPlanIteratorProvider();

    AbstractReplicaPlanIterator getReplicaPlanIterator(PartitionRangeReadCommand command, Keyspace keyspace, ConsistencyLevel consistencyLevel);

    class DefaultReplicaPlanIteratorProvider implements ReplicaPlanIteratorProvider
    {
        @Override
        public AbstractReplicaPlanIterator getReplicaPlanIterator(PartitionRangeReadCommand command, Keyspace keyspace, ConsistencyLevel consistencyLevel)
        {
            return new ReplicaPlanIterator(command.dataRange().keyRange(), command.indexQueryPlan(), keyspace, consistencyLevel);
        }
    }
}
