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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.SetSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class SetType<T> extends CollectionType<Set<T>>
{
    // interning instances
    private static final ConcurrentHashMap<AbstractType<?>, SetType> instances = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<AbstractType<?>, SetType> frozenInstances = new ConcurrentHashMap<>();

    private final SetSerializer<T> serializer;

    public static SetType<?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 1)
            throw new ConfigurationException("SetType takes exactly 1 type parameter");

        return getInstance(l.get(0), true);
    }

    @SuppressWarnings("unchecked")
    public static <T> SetType<T> getInstance(AbstractType<T> elements, boolean isMultiCell)
    {
        return getInstance(isMultiCell ? instances : frozenInstances,
                           elements,
                           () -> new SetType<>(elements, isMultiCell));
    }

    @Override
    public SetType<T> overrideKeyspace(Function<String, String> overrideKeyspace)
    {
        AbstractType<T> oldType = getElementsType();
        AbstractType<T> newType = oldType.overrideKeyspace(overrideKeyspace);

        if (newType == oldType)
            return this;

        return getInstance(newType, isMultiCell());
    }

    private SetType(AbstractType<T> elements, boolean isMultiCell)
    {
        super(Kind.SET, ImmutableList.of(elements), isMultiCell);
        this.serializer = SetSerializer.getInstance(elements.getSerializer(), elements.comparatorSet);
    }

    @Override
    @SuppressWarnings("unchecked")
    public SetType<T> with(ImmutableList<AbstractType<?>> subTypes, boolean isMultiCell)
    {
        Preconditions.checkArgument(subTypes.size() == 1,
                                    "Invalid number of subTypes for SetType (got %s)", subTypes.size());
        return getInstance((AbstractType<T>) subTypes.get(0), isMultiCell);
    }

    @Override
    public SetType<?> withUpdatedUserType(UserType udt)
    {
        // If our subtypes do contain the UDT and this is requested, there is a fair chance we won't be re-using an
        // instance with our own exact subtypes, so clean up the interned instance.
        if (referencesUserType(udt.name))
            (isMultiCell() ? instances : frozenInstances).remove(getElementsType());

        return (SetType<?>) super.withUpdatedUserType(udt);
    }

    @SuppressWarnings("unchecked")
    public AbstractType<T> getElementsType()
    {
        return (AbstractType<T>) subTypes.get(0);
    }

    public AbstractType<T> nameComparator()
    {
        return getElementsType();
    }

    public AbstractType<?> valueComparator()
    {
        return EmptyType.instance;
    }

    @Override
    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        return ListType.compareListOrSet(getElementsType(), left, accessorL, right, accessorR);
    }

    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, ByteComparable.Version version)
    {
        return ListType.asComparableBytesListOrSet(getElementsType(), accessor, data, version);
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        return ListType.fromComparableBytesListOrSet(accessor, comparableBytes, version, getElementsType());
    }

    public SetSerializer<T> getSerializer()
    {
        return serializer;
    }

    @Override
    public String toString(boolean ignoreFreezing)
    {
        boolean includeFrozenType = !ignoreFreezing && !isMultiCell();

        StringBuilder sb = new StringBuilder();
        if (includeFrozenType)
            sb.append(FrozenType.class.getName()).append('(');
        sb.append(getClass().getName());
        sb.append(TypeParser.stringifyTypeParameters(subTypes, ignoreFreezing || !isMultiCell()));
        if (includeFrozenType)
            sb.append(')');
        return sb.toString();
    }

    public List<ByteBuffer> serializedValues(Iterator<Cell<?>> cells)
    {
        List<ByteBuffer> bbs = new ArrayList<>();
        while (cells.hasNext())
            bbs.add(cells.next().path().get(0));
        return bbs;
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String)
            parsed = Json.decodeJson((String) parsed);

        if (!(parsed instanceof List))
            throw new MarshalException(String.format(
                    "Expected a list (representing a set), but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

        List<?> list = (List<?>) parsed;
        Set<Term> terms = new HashSet<>(list.size());
        AbstractType<T> elements = getElementsType();
        for (Object element : list)
        {
            if (element == null)
                throw new MarshalException("Invalid null element in set");
            terms.add(elements.fromJSONObject(element));
        }

        return new Sets.DelayedValue(elements, terms);
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return ListType.setOrListToJsonString(buffer, getElementsType(), protocolVersion);
    }

    @Override
    public boolean contains(ByteBuffer set, ByteBuffer element)
    {
        return CollectionSerializer.contains(getElementsType(), set, element, false, false, ProtocolVersion.V3);
    }
}
