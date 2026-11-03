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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * The path from a column to a value nested in it: a field of a user-defined type, a component of a tuple, possibly
 * through the elements of lists and sets and the values of maps, at any depth (for example {@code addrs.loc.lat} on
 * a {@code list<frozen<address>>} column).
 * <p>
 * A path is a list of {@link Step steps}, resolved against the type of the column:
 * <ul>
 *     <li>{@link Step.Kind#FIELD}: the field of a UDT or the component of a tuple at a position. Tuple components
 *     have no names: in CQL they are named {@code field1}, {@code field2}, ... (1-based, as DSE Search does).</li>
 *     <li>{@link Step.Kind#ELEMENTS}: every element of a list or a set, or every value of a map. It is implicit in
 *     CQL: it is inserted when a field is accessed on a collection whose elements are UDTs or tuples.</li>
 * </ul>
 * Paths use positions, not names: they are not affected by adding fields to a type ({@code ALTER TYPE ... ADD}),
 * and values written before a field was added are shorter, so the missing trailing fields read as {@code null}.
 * <p>
 * Instances are immutable.
 */
public final class FieldPath
{
    private static final Pattern TUPLE_FIELD_NAME = Pattern.compile("field([1-9][0-9]*)");
    private static final String TUPLE_FIELD_PREFIX = "field";
    /**
     * The maximum number of steps of a deserialized path: far more than the nesting of any type, it guards against
     * corrupted or malicious sizes.
     */
    private static final int MAX_SERIALIZED_STEPS = 64;

    private final ImmutableList<Step> steps;
    private final boolean hasElements;
    // the cell path of the field of a non-frozen UDT read by the first step, null if it's not a field step
    @Nullable
    private final CellPath firstFieldCellPath;

    private FieldPath(ImmutableList<Step> steps)
    {
        assert !steps.isEmpty() : "A field path must have at least one step";
        this.steps = steps;
        this.hasElements = steps.contains(Step.ELEMENTS);
        Step first = steps.get(0);
        // the cell path of a field is its position, which, unlike its name, doesn't change with ALTER TYPE
        this.firstFieldCellPath = first.kind == Step.Kind.FIELD ? CellPath.create(ByteBufferUtil.bytes((short) first.position)) : null;
    }

    /**
     * @param steps the steps of the path, in order from the column
     * @return the path made of the given steps
     */
    public static FieldPath of(List<Step> steps)
    {
        return new FieldPath(ImmutableList.copyOf(steps));
    }

    /**
     * Resolves the given field names against the type of a column.
     *
     * @param columnType the type of the column the path starts from
     * @param names the names of the fields, in order; tuple components are named {@code field1}, {@code field2}, ...
     * @return the resolved path
     * @throws InvalidRequestException if a name does not resolve to a field
     */
    public static FieldPath resolve(AbstractType<?> columnType, List<FieldIdentifier> names)
    {
        if (names.isEmpty())
            throw new InvalidRequestException("A field path must name at least one field");

        List<Step> steps = new ArrayList<>(names.size() + 1);
        AbstractType<?> type = columnType.unwrap();
        StringBuilder prefix = new StringBuilder();
        for (FieldIdentifier name : names)
        {
            // cross the collections, whose elements (map values) must be UDTs or tuples to access a field of them
            while (type.isCollection())
            {
                CollectionType<?> collection = (CollectionType<?>) type;
                if (collection.kind == CollectionType.Kind.MAP)
                {
                    MapType<?, ?> map = (MapType<?, ?>) collection;
                    if (!isTupleOrUDT(map.getValuesType()) && isTupleOrUDT(map.getKeysType()))
                        throw new InvalidRequestException(String.format("Cannot access field %s of %s: field paths through the keys of a map are not supported",
                                                                        name, describe(prefix)));
                    type = map.getValuesType();
                }
                else
                {
                    type = collection.kind == CollectionType.Kind.LIST
                           ? ((ListType<?>) collection).getElementsType()
                           : ((SetType<?>) collection).getElementsType();
                }
                steps.add(Step.ELEMENTS);
            }

            int position;
            if (type.isUDT())
            {
                UserType userType = (UserType) type;
                position = userType.fieldPosition(name);
                if (position < 0)
                    throw new InvalidRequestException(String.format("Undefined field %s in %s of type %s",
                                                                    name, describe(prefix), userType.getNameAsString()));
            }
            else if (type.isTuple())
            {
                TupleType tupleType = (TupleType) type;
                position = tupleFieldPosition(name);
                if (position < 0 || position >= tupleType.size())
                    throw new InvalidRequestException(String.format("Undefined field %s in %s of type %s: tuple components are named field1 to field%d",
                                                                    name, describe(prefix), tupleType.asCQL3Type(), tupleType.size()));
            }
            else
            {
                throw new InvalidRequestException(String.format("Cannot access field %s of %s of type %s, which is neither a user-defined type nor a tuple",
                                                                name, describe(prefix), type.asCQL3Type()));
            }

            steps.add(Step.field(position));
            type = ((TupleType) type).type(position);
            if (prefix.length() > 0)
                prefix.append('.');
            prefix.append(ColumnIdentifier.maybeQuote(name.toString()));
        }
        return new FieldPath(ImmutableList.copyOf(steps));
    }

    private static String describe(CharSequence prefix)
    {
        if (prefix.length() == 0)
            return "the column";
        return String.format("field %s", prefix);
    }

    private static boolean isTupleOrUDT(AbstractType<?> type)
    {
        return type.isTuple() || type.isUDT();
    }

    /**
     * @param name a field name
     * @return the 0-based position of the tuple component with that name ({@code field1} is 0), or -1 if the name
     * does not have the form of a tuple component name
     */
    private static int tupleFieldPosition(FieldIdentifier name)
    {
        Matcher matcher = TUPLE_FIELD_NAME.matcher(name.toString());
        if (!matcher.matches())
            return -1;
        try
        {
            return Integer.parseInt(matcher.group(1)) - 1;
        }
        catch (NumberFormatException e)
        {
            return -1;
        }
    }

    /**
     * @param position the 0-based position of a tuple component
     * @return the CQL name of that component: {@code field1} for position 0
     */
    public static String tupleFieldName(int position)
    {
        return TUPLE_FIELD_PREFIX + (position + 1);
    }

    public List<Step> steps()
    {
        return steps;
    }

    /**
     * @return whether the path crosses a collection, and so reaches any number of values
     */
    public boolean hasElements()
    {
        return hasElements;
    }

    /**
     * @param columnType the type of the column the path starts from
     * @param unpackLeaf whether a collection leaf is read as its elements (map values) rather than as one value
     * @return whether the path can reach more than one value per row: when it crosses a collection, or when its
     * leaf is a collection that is unpacked
     */
    public boolean isMultiValued(AbstractType<?> columnType, boolean unpackLeaf)
    {
        return hasElements() || (unpackLeaf && leafType(columnType).isCollection());
    }

    /**
     * @param columnType the type of the column the path starts from
     * @return the type of the values reached by the path
     */
    public AbstractType<?> leafType(AbstractType<?> columnType)
    {
        AbstractType<?> type = columnType.unwrap();
        for (Step step : steps)
            type = step.apply(type);
        return type;
    }

    /**
     * @param columnType the type of the column the path starts from
     * @return the names of the fields of the path, as resolved by {@link #resolve(AbstractType, List)}
     */
    public List<FieldIdentifier> names(AbstractType<?> columnType)
    {
        List<FieldIdentifier> names = new ArrayList<>(steps.size());
        AbstractType<?> type = columnType.unwrap();
        for (Step step : steps)
        {
            if (step.kind == Step.Kind.FIELD)
                names.add(type.isUDT()
                          ? ((UserType) type).fieldName(step.position)
                          : FieldIdentifier.forInternalString(tupleFieldName(step.position)));
            type = step.apply(type);
        }
        return names;
    }

    /**
     * @param columnType the type of the column the path starts from
     * @return the path in CQL form, without the column name: the field names separated by dots, quoted when needed,
     * for example {@code loc.lat}
     */
    public String toCQLString(AbstractType<?> columnType)
    {
        return toCQLString(names(columnType));
    }

    /**
     * @param names field names
     * @return the names separated by dots, quoted when needed
     */
    public static String toCQLString(List<FieldIdentifier> names)
    {
        StringBuilder builder = new StringBuilder();
        for (FieldIdentifier name : names)
        {
            if (builder.length() > 0)
                builder.append('.');
            builder.append(ColumnIdentifier.maybeQuote(name.toString()));
        }
        return builder.toString();
    }

    /**
     * Calls the consumer with each user-defined type crossed by the path and the position of the field of it that
     * the path reads.
     *
     * @param columnType the type of the column the path starts from
     * @param consumer the consumer of the user types and field positions
     */
    public void forEachUserTypeField(AbstractType<?> columnType, BiConsumer<UserType, Integer> consumer)
    {
        AbstractType<?> type = columnType.unwrap();
        for (Step step : steps)
        {
            if (step.kind == Step.Kind.FIELD && type.isUDT())
                consumer.accept((UserType) type, step.position);
            type = step.apply(type);
        }
    }

    /**
     * Extracts the values reached by this path from a serialized (single-cell) value of the column: the value of a
     * frozen type, of a partition key or clustering column, or the value of a cell.
     *
     * @param columnType the type of the column the path starts from
     * @param value the serialized value of the column, possibly {@code null}
     * @param unpackLeaf whether a collection leaf is read as its elements (map values) rather than as one value
     * @param consumer the consumer of the values; {@code null} values (unset fields) are not given to it
     */
    public void collectValues(AbstractType<?> columnType, @Nullable ByteBuffer value, boolean unpackLeaf, Consumer<ByteBuffer> consumer)
    {
        if (value == null)
            return;
        collect(columnType.unwrap(), value, 0, unpackLeaf, consumer);
    }

    /**
     * Extracts the values reached by this path from the cells of a multi-cell column: a non-frozen UDT or a
     * non-frozen collection.
     *
     * @param columnType the type of the column the path starts from, multi-cell
     * @param data the cells of the column, possibly {@code null}
     * @param nowInSec the current time, the cells that are not live at that time are ignored
     * @param unpackLeaf whether a collection leaf is read as its elements (map values) rather than as one value
     * @param consumer the consumer of the values; {@code null} values (unset fields) are not given to it
     */
    public void collectValues(AbstractType<?> columnType,
                              @Nullable ComplexColumnData data,
                              long nowInSec,
                              boolean unpackLeaf,
                              Consumer<ByteBuffer> consumer)
    {
        collectValues(columnType, data, nowInSec, unpackLeaf, null, consumer);
    }

    /**
     * As {@link #collectValues(AbstractType, ComplexColumnData, long, boolean, Consumer)}, also giving the local
     * expiration time of each live expiring cell (with a TTL) read by the path to the given consumer, if not
     * {@code null}.
     */
    public void collectValues(AbstractType<?> columnType,
                              @Nullable ComplexColumnData data,
                              long nowInSec,
                              boolean unpackLeaf,
                              @Nullable LongConsumer expiringCells,
                              Consumer<ByteBuffer> consumer)
    {
        if (data == null)
            return;

        AbstractType<?> type = columnType.unwrap();
        assert type.isMultiCell() : "Not a multi-cell type: " + type.asCQL3Type();
        Step first = steps.get(0);
        if (type.isUDT())
        {
            assert first.kind == Step.Kind.FIELD;
            Cell<?> cell = data.getCell(firstFieldCellPath);
            if (cell != null && cell.isLive(nowInSec))
            {
                if (expiringCells != null && cell.isExpiring())
                    expiringCells.accept(cell.localDeletionTime());
                collect(((UserType) type).type(first.position), cell.buffer(), 1, unpackLeaf, consumer);
            }
        }
        else
        {
            assert type.isCollection() && first.kind == Step.Kind.ELEMENTS;
            CollectionType<?> collection = (CollectionType<?>) type;
            AbstractType<?> elementType = first.apply(type);
            for (Cell<?> cell : data)
            {
                if (cell == null || !cell.isLive(nowInSec))
                    continue;
                if (expiringCells != null && cell.isExpiring())
                    expiringCells.accept(cell.localDeletionTime());
                // the elements of sets are the cell paths, those of lists and the values of maps are the cell values
                ByteBuffer element = collection.kind == CollectionType.Kind.SET ? cell.path().get(0) : cell.buffer();
                collect(elementType, element, 1, unpackLeaf, consumer);
            }
        }
    }

    /**
     * Extracts the value reached by a path without {@link #hasElements() elements} from a serialized (single-cell)
     * value of the column, as {@link #collectValues(AbstractType, ByteBuffer, boolean, Consumer)} does without
     * unpacking the leaf, but without any allocation other than that of the returned buffer.
     *
     * @param value the serialized value of the column, possibly {@code null}
     * @return the value of the field, or {@code null} if it's unset
     */
    @Nullable
    public ByteBuffer singleValue(@Nullable ByteBuffer value)
    {
        assert !hasElements : "Multi-valued path " + this;
        return value == null ? null : fieldValue(value, 0);
    }

    /**
     * Extracts the value reached by a path without {@link #hasElements() elements} from the cells of a non-frozen
     * UDT column, as {@link #collectValues(AbstractType, ComplexColumnData, long, boolean, Consumer)} does without
     * unpacking the leaf.
     *
     * @param data the cells of the column, possibly {@code null}
     * @param nowInSec the current time, the cells that are not live at that time are ignored
     * @return the value of the field, or {@code null} if it's unset
     */
    @Nullable
    public ByteBuffer singleValue(@Nullable ComplexColumnData data, long nowInSec)
    {
        assert !hasElements : "Multi-valued path " + this;
        assert firstFieldCellPath != null;
        if (data == null)
            return null;
        Cell<?> cell = data.getCell(firstFieldCellPath);
        if (cell == null || !cell.isLive(nowInSec))
            return null;
        ByteBuffer value = cell.buffer();
        return steps.size() == 1 ? value : fieldValue(value, 1);
    }

    /**
     * @return the value reached by the field steps from the given one, all of them {@link Step.Kind#FIELD}, from
     * a serialized tuple or UDT value; a single buffer is created, for the result
     */
    @Nullable
    private ByteBuffer fieldValue(ByteBuffer value, int firstStep)
    {
        int start = value.position();
        int end = value.limit();
        for (int i = firstStep; i < steps.size(); i++)
        {
            long bounds = locateComponent(value, start, end, steps.get(i).position);
            if (bounds < 0)
                return null;
            start = (int) (bounds >>> 32);
            end = (int) bounds;
        }
        ByteBuffer result = value.duplicate();
        result.position(start);
        result.limit(end);
        return result;
    }

    private void collect(AbstractType<?> type, @Nullable ByteBuffer value, int stepIndex, boolean unpackLeaf, Consumer<ByteBuffer> consumer)
    {
        if (value == null)
            return;

        if (stepIndex == steps.size())
        {
            if (unpackLeaf && type.isCollection())
                forEachElement((CollectionType<?>) type, value, consumer);
            else
                consumer.accept(value);
            return;
        }

        Step step = steps.get(stepIndex);
        AbstractType<?> next = step.apply(type);
        if (step.kind == Step.Kind.FIELD)
            collect(next, component(value, step.position), stepIndex + 1, unpackLeaf, consumer);
        else
            forEachElement((CollectionType<?>) type, value, element -> collect(next, element, stepIndex + 1, unpackLeaf, consumer));
    }

    /**
     * Reads one component of a serialized tuple or frozen UDT value.
     * <p>
     * Unlike {@link TupleType#split}, it accepts values with any number of components: values written before a
     * field was added to a type have fewer components (the missing ones are {@code null}), and the type known to the
     * reader may be older than the value (more components).
     *
     * @param value the serialized tuple or UDT value
     * @param position the 0-based position of the component
     * @return the value of the component, or {@code null} if it is null or missing
     */
    static ByteBuffer component(ByteBuffer value, int position)
    {
        long bounds = locateComponent(value, value.position(), value.limit(), position);
        if (bounds < 0)
            return null;
        ByteBuffer component = value.duplicate();
        component.position((int) (bounds >>> 32));
        component.limit((int) bounds);
        return component;
    }

    /**
     * Finds one component of the serialized tuple or frozen UDT value between the given absolute positions of the
     * buffer, accepting any number of components (see {@link #component(ByteBuffer, int)}).
     *
     * @return the absolute start (high 32 bits) and end (low 32 bits) of the component, or -1 if it is null or missing
     */
    private static long locateComponent(ByteBuffer value, int start, int end, int position)
    {
        int offset = start;
        for (int i = 0; ; i++)
        {
            if (offset == end)
                return -1;

            if (end - offset < 4)
                throw new MarshalException(String.format("Not enough bytes to read %dth component", i));

            int size = value.getInt(offset);
            offset += 4;
            if (size >= 0 && size > end - offset)
                throw new MarshalException(String.format("Not enough bytes to read %dth component", i));

            if (i == position)
                return size < 0 ? -1 : ((long) offset << 32) | (offset + size);

            if (size > 0)
                offset += size;
        }
    }

    /**
     * Calls the consumer with the elements of a serialized frozen list or set, or with the values of a serialized
     * frozen map; the {@code null} elements are skipped.
     */
    public static void forEachElement(CollectionType<?> type, ByteBuffer value, Consumer<ByteBuffer> consumer)
    {
        ValueAccessor<ByteBuffer> accessor = ByteBufferAccessor.instance;
        int count = CollectionSerializer.readCollectionSize(value, accessor);
        int offset = CollectionSerializer.sizeOfCollectionSize();
        for (int i = 0; i < count; i++)
        {
            if (type.kind == CollectionType.Kind.MAP)
                offset += CollectionSerializer.skipValue(value, accessor, offset);

            ByteBuffer element = CollectionSerializer.readValue(value, accessor, offset);
            offset += CollectionSerializer.sizeOfValue(element, accessor);
            if (element != null)
                consumer.accept(element);
        }
    }

    /**
     * @param column the specification of the column the path starts from
     * @return the specification of the value reached by this path, without {@link #hasElements() elements}: its type
     * is the {@link #leafType leaf type} and its name is the dotted path in CQL form, the column name and the field
     * names quoted when needed ({@code addr.loc.lat}, {@code "Addr"."City"}), so that the bind markers of a
     * restriction on the field get the type and the name of the field
     */
    public ColumnSpecification leafSpecification(ColumnSpecification column)
    {
        assert !hasElements : "Multi-valued path " + this;
        return new ColumnSpecification(column.ksName,
                                       column.cfName,
                                       new ColumnIdentifier(column.name.toCQLString() + '.' + toCQLString(column.type), true),
                                       leafType(column.type));
    }

    /**
     * Writes this path: the number of steps (unsigned vint), then for each step a tag byte ({@code 0} for a
     * {@link Step.Kind#FIELD field}, {@code 1} for {@link Step.Kind#ELEMENTS elements}), followed, for a field, by its
     * position (unsigned vint).
     */
    public void serialize(DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt32(steps.size());
        for (Step step : steps)
        {
            out.writeByte(step.kind.tag);
            if (step.kind == Step.Kind.FIELD)
                out.writeUnsignedVInt32(step.position);
        }
    }

    /**
     * Reads a path written by {@link #serialize(DataOutputPlus)}.
     */
    public static FieldPath deserialize(DataInputPlus in) throws IOException
    {
        int size = in.readUnsignedVInt32();
        if (size <= 0 || size > MAX_SERIALIZED_STEPS)
            throw new IOException("Invalid field path size " + Integer.toUnsignedString(size));
        List<Step> steps = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            byte tag = in.readByte();
            Step.Kind kind = Step.Kind.fromTag(tag);
            if (kind == null)
                throw new IOException("Unknown field path step " + tag);
            if (kind == Step.Kind.FIELD)
            {
                int position = in.readUnsignedVInt32();
                if (position < 0)
                    throw new IOException("Invalid field position " + Integer.toUnsignedString(position));
                steps.add(Step.field(position));
            }
            else
            {
                steps.add(Step.ELEMENTS);
            }
        }
        return of(steps);
    }

    /**
     * @return the size of this path as written by {@link #serialize(DataOutputPlus)}
     */
    public long serializedSize()
    {
        long size = TypeSizes.sizeofUnsignedVInt(steps.size());
        for (Step step : steps)
        {
            size += 1;
            if (step.kind == Step.Kind.FIELD)
                size += TypeSizes.sizeofUnsignedVInt(step.position);
        }
        return size;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof FieldPath))
            return false;
        return steps.equals(((FieldPath) o).steps);
    }

    @Override
    public int hashCode()
    {
        return steps.hashCode();
    }

    @Override
    public String toString()
    {
        return steps.toString();
    }

    /**
     * A step of a {@link FieldPath}.
     */
    public static final class Step
    {
        public enum Kind
        {
            /** The field of a UDT or the component of a tuple at a position. */
            FIELD(0),
            /** The elements of a list or a set, or the values of a map. */
            ELEMENTS(1);

            /** The tag of the steps of this kind in the serialized paths, see {@link #serialize}. Never change it. */
            public final int tag;

            Kind(int tag)
            {
                this.tag = tag;
            }

            /**
             * @return the kind with the given serialization tag, {@code null} if there is none
             */
            @Nullable
            public static Kind fromTag(int tag)
            {
                for (Kind kind : values())
                    if (kind.tag == tag)
                        return kind;
                return null;
            }
        }

        public static final Step ELEMENTS = new Step(Kind.ELEMENTS, -1);

        public final Kind kind;
        /** The 0-based position of the field, -1 for {@link Kind#ELEMENTS}. */
        public final int position;

        private Step(Kind kind, int position)
        {
            this.kind = kind;
            this.position = position;
        }

        /**
         * @param position the 0-based position of the field of a UDT or the component of a tuple
         * @return the step to that field
         */
        public static Step field(int position)
        {
            if (position < 0)
                throw new IllegalArgumentException("Invalid field position " + position);
            return new Step(Kind.FIELD, position);
        }

        /**
         * @param type the type the step is applied to
         * @return the type the step leads to
         */
        public AbstractType<?> apply(AbstractType<?> type)
        {
            if (kind == Kind.FIELD)
            {
                if (!type.isTuple() && !type.isUDT())
                    throw new IllegalStateException(String.format("Cannot read field %d of non-tuple type %s", position, type.asCQL3Type()));
                TupleType tuple = (TupleType) type;
                if (position >= tuple.size())
                    throw new IllegalStateException(String.format("Cannot read field %d of type %s with %d fields", position, type.asCQL3Type(), tuple.size()));
                return tuple.type(position);
            }

            if (!type.isCollection())
                throw new IllegalStateException("Cannot read the elements of non-collection type " + type.asCQL3Type());
            CollectionType<?> collection = (CollectionType<?>) type;
            switch (collection.kind)
            {
                case LIST:
                    return ((ListType<?>) collection).getElementsType();
                case SET:
                    return ((SetType<?>) collection).getElementsType();
                case MAP:
                    return ((MapType<?, ?>) collection).getValuesType();
                default:
                    throw new AssertionError("Unknown collection kind " + collection.kind);
            }
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (!(o instanceof Step))
                return false;
            Step other = (Step) o;
            return kind == other.kind && position == other.position;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(kind, position);
        }

        @Override
        public String toString()
        {
            return kind == Kind.FIELD ? Integer.toString(position) : "*";
        }
    }
}
