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

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;

/**
 * Base class for all types that can be multi-cell (when not frozen).
 * <p/>
 * A multi-cell type is one whose value is composed of multiple sub-values that are laid out on multiple {@link Cell}
 * instances (one for each sub-value), typically collections. This layout allows partial updates (of only some of
 * the sub-values) without requiring a read-before-write operation.
 * <p/>
 * All multi-cell capable types can either be used as truly multi-cell types or can be used in a frozen state.
 * In the latter case, the values are not laid out in multiple cells; instead, the entire value (with all its sub-values)
 * is packed within a single cell value. This implies that partial updates without read-before-write are not possible.
 * The {@link AbstractType#isMultiCell()} method indicates whether a given type is a multi-cell variant or a frozen one.
 * Both variants are technically different types but represent the same values from a user perspective, with different
 * capabilities.
 *
 * @param <T> the type of the values of this type.
 */
public abstract class MultiCellCapableType<T> extends AbstractType<T>
{
    protected MultiCellCapableType(ComparisonType comparisonType, boolean isMultiCell, ImmutableList<AbstractType<?>> subTypes)
    {
        super(comparisonType, isMultiCell, subTypes);
    }

    /**
     * Returns the subtype/comparator to use for the {@link CellPath} part of cells forming values for this type when
     * used in its multi-cell variant.
     * <p/>
     * Note: In theory, this method should not be accessed on frozen instances (where {@code isMultiCell() == false}).
     * However, for convenience, it is expected that this method always returns a proper value "as if" the type was a
     * multi-cell variant, even if it is not.
     *
     * @return the comparator for the {@link CellPath} component of cells of this type, regardless of whether the type
     * is frozen or not.
     */
    public abstract AbstractType<?> nameComparator();

    @Override
    public final boolean isCompatibleWith(AbstractType<?> previous)
    {
        if (equals(previous))
            return true;

        if (!(previous instanceof MultiCellCapableType))
            return false;

        if (this.isMultiCell() != previous.isMultiCell())
            return false;

        MultiCellCapableType<?> prevType = (MultiCellCapableType<?>) previous;
        return this.isMultiCell() ? isCompatibleWithMultiCell(prevType)
                                  : isCompatibleWithFrozen(prevType);
    }

    /**
     * Whether {@code this} type is compatible (including for sorting) with {@code previous}, assuming both are
     * of the same class (so {@code previous} can be safely cast to whichever class implements this) and both are
     * frozen.
     */
    protected abstract boolean isCompatibleWithFrozen(@Nonnull MultiCellCapableType<?> previous);

    /**
     * Whether {@code this} type is compatible (including for sorting) with {@code previous}, assuming both are
     * of the same class (so {@code previous} can be safely cast to whichever class implements this) but neither
     * are frozen.
     */
    protected abstract boolean isCompatibleWithMultiCell(@Nonnull MultiCellCapableType<?> previous);

    @Override
    public final boolean isSerializationCompatibleWith(AbstractType<?> previous)
    {
        if (equals(previous))
            return true;

        if (!(previous instanceof MultiCellCapableType))
            return false;

        if (this.isMultiCell() != previous.isMultiCell())
            return false;

        MultiCellCapableType<?> prevType = (MultiCellCapableType<?>) previous;
        return isMultiCell() ? isSerializationCompatibleWithMultiCell(prevType)
                             : isSerializationCompatibleWithFrozen(prevType);
    }

    /**
     * Determines if the current type is serialization compatible with the given previous type.
     * <p>
     * Serialization compatibility is primarily concerned with the ability to read the serialized value from a buffer
     * that contains other data after the value. This means the value must either have a fixed length or its length must
     * be explicitly stored. In frozen collections or tuples, all serialized values are prefixed with their length,
     * regardless of whether the value has a fixed or variable length. Therefore, to ensure serialization compatibility,
     * it is sufficient to verify whether the types are value-compatible when frozen, in addition to checking the
     * isMultiCell and exact type conditions.
     * </p>
     *
     * @param previous the previous type to check compatibility against
     * @return {@code true} if the current type is serialization compatible with the previous type, false otherwise
     */
    protected boolean isSerializationCompatibleWithFrozen(MultiCellCapableType<?> previous)
    {
        return isValueCompatibleWithFrozen(previous);
    }

    protected boolean isSerializationCompatibleWithMultiCell(MultiCellCapableType<?> previous)
    {
        return isCompatibleWithMultiCell(previous);
    }

    @Override
    protected final boolean isValueCompatibleWithInternal(AbstractType<?> previous)
    {
        if (equals(previous))
            return true;

        if (!(previous instanceof MultiCellCapableType))
            return false;

        if (this.isMultiCell() != previous.isMultiCell())
            return false;

        MultiCellCapableType<?> prevType = (MultiCellCapableType<?>) previous;
        return isMultiCell() ? isValueCompatibleWithMultiCell(prevType)
                             : isValueCompatibleWithFrozen(prevType);
    }

    /**
     * Whether {@code this} type is value-compatible with {@code previous}, assuming both are of the same class (so
     * {@code previous} can be safely cast to whichever class implements this) and both are are frozen.
     */
    protected abstract boolean isValueCompatibleWithFrozen(MultiCellCapableType<?> previous);

    protected boolean isValueCompatibleWithMultiCell(MultiCellCapableType<?> previous)
    {
        return isCompatibleWithMultiCell(previous);
    }

}
