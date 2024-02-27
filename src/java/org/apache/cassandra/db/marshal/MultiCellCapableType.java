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

import java.util.List;

import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;

/**
 * Base class for all types that have the capacity of being multi-cell (when not frozen).
 *
 * <p>A multi-cell type is one whose value is composed of multiple sub-values that are layout on multiple {@link Cell}
 * (one for each sub-value), typically collections. Being layout on multiple cell allows partial update (of only some
 * of the sub-values) without read-before write.
 *
 * <p>All multi-cell capable types can either be used as truly multi-cell types, or can be used frozen. In the later
 * case, the values are not layout in multiple cells, but instead the whole value (with all its sub-values) is packed
 * within a single cell value (which imply no partial update without read-before-write in particular).
 * {@link #isMultiCell} allows to know if a given type is a multi-cell variant or a frozen one (both are technically
 * 2 different types, that have the same values from a user point of view, just with different capability).
 *
 * @param <T> the type of the values of this type.
 */
public abstract class MultiCellCapableType<T> extends AbstractType<T>
{

    protected MultiCellCapableType(boolean multiCell, List<AbstractType<?>> components)
    {
        super(ComparisonType.CUSTOM, multiCell, components);
    }

    // Overriding as abstract to prevent subclasses from relying on the default implementation, as it's not appropriate.
    @Override
    public abstract AbstractType<?> with(List<AbstractType<?>> subTypes, boolean isMultiCell);

    /**
     * The subtype/comparator to use for the {@link CellPath} part of cells forming values for this type when used in
     * its multi-cell variant.
     *
     * <p>Note: in theory, we shouldn't have to access this on frozen instances (where {@code isMultiCell == false}),
     * but for convenience, it is expected that this method always returns a proper value "as if" the type was a
     * multi-cell variant, even if it is not.
     *
     * @return the comparator for the {@link CellPath} component of cells of this type.
     */
    public abstract AbstractType<?> nameComparator();

    @Override
    public AbstractType<?> unfreeze()
    {
        return isMultiCell ? this : with(subTypes, true);
    }
}
