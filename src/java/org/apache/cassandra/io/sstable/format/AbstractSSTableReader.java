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

package org.apache.cassandra.io.sstable.format;

import java.util.Set;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.AbstractBigTableReader.UniqueIdentifier;
import org.apache.cassandra.io.util.DiskOptimizationStrategy;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.SelfRefCounted;

public abstract class AbstractSSTableReader<T extends AbstractSSTableReader<T>> extends SSTable implements SelfRefCounted<T>
{
    public final UniqueIdentifier instanceId = new UniqueIdentifier();
    public final AbstractBigTableReader.OpenReason openReason;

    protected AbstractSSTableReader(Descriptor descriptor,
                                    Set<Component> components,
                                    TableMetadataRef metadata,
                                    DiskOptimizationStrategy optimizationStrategy)
    {
        super(descriptor, components, metadata, optimizationStrategy);
    }

    public interface Factory<T>
    {
        T open(SSTableReaderBuilder builder);

        PartitionIndexIterator indexIterator(Descriptor descriptor, TableMetadata metadata);
    }
}
