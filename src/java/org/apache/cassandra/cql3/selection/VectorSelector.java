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
package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.filter.ColumnFilter.Builder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

final class VectorSelector extends Selector
{
    /**
     * The vector type.
     */
    private final VectorType<?> type;

    /**
     * The list elements
     */
    private final List<Selector> elements;

    public static Factory newFactory(final AbstractType<?> type, final SelectorFactories factories)
    {
        assert type.isVector() : String.format("Unable to create vector selector from type %s", type.asCQL3Type());
        VectorType<?> vt = (VectorType<?>) type;
        return new MultiElementFactory(type, factories)
        {
            protected String getColumnName()
            {
                return Lists.listToString(factories, Factory::getColumnName);
            }

            public Selector newInstance(final QueryOptions options)
            {
                return new VectorSelector(vt, factories.newInstances(options));
            }
        };
    }

    @Override
    public void addFetchedColumns(Builder builder)
    {
        for (int i = 0, m = elements.size(); i < m; i++)
            elements.get(i).addFetchedColumns(builder);
    }

    public void addInput(ProtocolVersion protocolVersion, ResultSetBuilder rs) throws InvalidRequestException
    {
        for (int i = 0, m = elements.size(); i < m; i++)
            elements.get(i).addInput(protocolVersion, rs);
    }

    public ByteBuffer getOutput(ProtocolVersion protocolVersion) throws InvalidRequestException
    {
        List<ByteBuffer> buffers = new ArrayList<>(elements.size());
        for (int i = 0, m = elements.size(); i < m; i++)
        {
            buffers.add(elements.get(i).getOutput(protocolVersion));
        }
        return type.decomposeRaw(buffers);
    }

    public void reset()
    {
        for (int i = 0, m = elements.size(); i < m; i++)
            elements.get(i).reset();
    }

    public VectorType<?> getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return Lists.listToString(elements);
    }

    private VectorSelector(VectorType<?> type, List<Selector> elements)
    {
        Preconditions.checkArgument(elements.size() == type.dimension,
                                    "Unable to create a vector select of type %s from %s elements",
                                    type.asCQL3Type(),
                                    elements.size());
        this.type = type;
        this.elements = elements;
    }
}
