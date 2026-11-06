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
package org.apache.cassandra.cql3.statements.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.FieldPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public class IndexTarget
{
    public static final String TARGET_OPTION_NAME = "target";
    public static final String CUSTOM_INDEX_OPTION_NAME = "class_name";

    public final ColumnIdentifier column;
    public final Type type;

    /**
     * The names of the fields of the indexed value inside the column, empty if the whole column is indexed. They are
     * in canonical form: the elements of the collections crossed by the path are implicit (see {@link FieldPath}).
     */
    public final List<FieldIdentifier> fieldNames;

    /**
     * The path to the indexed value inside the column, resolved from {@link #fieldNames}, or {@code null} if the
     * whole column is indexed. When there is a path, {@link #type} applies to the value it reaches (the leaf).
     */
    @Nullable
    public final FieldPath fieldPath;

    public IndexTarget(ColumnIdentifier column, Type type)
    {
        this(column, type, Collections.emptyList(), null);
    }

    public IndexTarget(ColumnIdentifier column, Type type, List<FieldIdentifier> fieldNames, @Nullable FieldPath fieldPath)
    {
        assert fieldNames.isEmpty() == (fieldPath == null) : "Field names and path must be given together";
        this.column = column;
        this.type = type;
        this.fieldNames = ImmutableList.copyOf(fieldNames);
        this.fieldPath = fieldPath;
    }

    public boolean hasFieldPath()
    {
        return fieldPath != null;
    }

    /**
     * @return the target in CQL form: {@code col}, {@code values(col)}, ..., and for a field path {@code col.f.g},
     * {@code values(col.f)} or {@code full(col.f)}. This is the form stored in the {@link #TARGET_OPTION_NAME}
     * option of the index, which is also what {@code DESCRIBE} prints.
     */
    public String asCqlString()
    {
        String target = fieldNames.isEmpty()
                        ? column.toCQLString()
                        : column.toCQLString() + '.' + FieldPath.toCQLString(fieldNames);
        return type == Type.SIMPLE
             ? target
             : String.format("%s(%s)", type.toString(), target);
    }

    public static class Raw
    {
        private final ColumnIdentifier column;
        private final Type type;
        // the field names inside the parentheses of values(...)/full(...), or after the column of a simple target
        private final List<FieldIdentifier> path;
        // the field names after values(...), which then designates the elements of a collection
        private final List<FieldIdentifier> elementPath;

        private Raw(ColumnIdentifier column, Type type)
        {
            this(column, type, null, null);
        }

        private Raw(ColumnIdentifier column, Type type, @Nullable List<FieldIdentifier> path, @Nullable List<FieldIdentifier> elementPath)
        {
            this.column = column;
            this.type = type;
            this.path = path == null ? Collections.emptyList() : path;
            this.elementPath = elementPath == null ? Collections.emptyList() : elementPath;
        }

        public static Raw simpleIndexOn(ColumnIdentifier c)
        {
            return new Raw(c, Type.SIMPLE);
        }

        /**
         * @return the target {@code c.path}
         */
        public static Raw simpleIndexOn(ColumnIdentifier c, @Nullable List<FieldIdentifier> path)
        {
            return new Raw(c, Type.SIMPLE, path, null);
        }

        public static Raw valuesOf(ColumnIdentifier c)
        {
            return new Raw(c, Type.VALUES);
        }

        /**
         * @return the target {@code values(c.path).elementPath}
         */
        public static Raw valuesOf(ColumnIdentifier c, @Nullable List<FieldIdentifier> path, @Nullable List<FieldIdentifier> elementPath)
        {
            return new Raw(c, Type.VALUES, path, elementPath);
        }

        public static Raw keysOf(ColumnIdentifier c)
        {
            return new Raw(c, Type.KEYS);
        }

        public static Raw keysAndValuesOf(ColumnIdentifier c)
        {
            return new Raw(c, Type.KEYS_AND_VALUES);
        }

        public static Raw fullCollection(ColumnIdentifier c)
        {
            return new Raw(c, Type.FULL);
        }

        /**
         * @return the target {@code full(c.path)}
         */
        public static Raw fullCollection(ColumnIdentifier c, @Nullable List<FieldIdentifier> path)
        {
            return new Raw(c, Type.FULL, path, null);
        }

        public IndexTarget prepare(TableMetadata table)
        {
            // Until we've prepared the target column, we can't be certain about the target type
            // because (for backwards compatibility) an index on a collection's values uses the
            // same syntax as an index on a regular column (i.e. the 'values' in
            // 'CREATE INDEX on table(values(collection));' is optional). So we correct the target type
            // when the target column is a collection & the target type is SIMPLE.
            ColumnMetadata columnDef = table.getExistingColumn(column);
            if (path.isEmpty() && elementPath.isEmpty())
            {
                Type actualType = (type == Type.SIMPLE && columnDef.type.isCollection()) ? Type.VALUES : type;
                return new IndexTarget(columnDef.name, actualType);
            }

            Type leafType = type;
            if (!elementPath.isEmpty())
            {
                // values(c.path).elementPath: the elements of the collection reached by c.path, which are implicit in
                // the canonical form c.path.elementPath
                AbstractType<?> collectionType = path.isEmpty() ? columnDef.type.unwrap() : FieldPath.resolve(columnDef.type, path).leafType(columnDef.type);
                if (!collectionType.isCollection())
                    throw new InvalidRequestException(String.format("Cannot use values() on %s of type %s, which is not a collection",
                                                                    targetName(columnDef, path), collectionType.asCQL3Type()));
                leafType = Type.SIMPLE;
            }

            List<FieldIdentifier> names = new ArrayList<>(path.size() + elementPath.size());
            names.addAll(path);
            names.addAll(elementPath);
            FieldPath fieldPath = FieldPath.resolve(columnDef.type, names);
            // the same rule as for columns applies to the leaf: a simple target on a collection means its values
            if (leafType == Type.SIMPLE && fieldPath.leafType(columnDef.type).isCollection())
                leafType = Type.VALUES;
            return new IndexTarget(columnDef.name, leafType, fieldPath.names(columnDef.type), fieldPath);
        }

        private static String targetName(ColumnMetadata column, List<FieldIdentifier> path)
        {
            return path.isEmpty() ? column.name.toCQLString() : column.name.toCQLString() + '.' + FieldPath.toCQLString(path);
        }
    }

    public enum Type
    {
        VALUES, KEYS, KEYS_AND_VALUES, FULL, SIMPLE;

        public String toString()
        {
            switch (this)
            {
                case KEYS: return "keys";
                case KEYS_AND_VALUES: return "entries";
                case FULL: return "full";
                case VALUES: return "values";
                case SIMPLE: return "";
                default: return "";
            }
        }

        public static Type fromString(String s)
        {
            if ("".equals(s))
                return SIMPLE;
            else if ("values".equals(s))
                return VALUES;
            else if ("keys".equals(s))
                return KEYS;
            else if ("entries".equals(s))
                return KEYS_AND_VALUES;
            else if ("full".equals(s))
                return FULL;

            throw new AssertionError("Unrecognized index target type " + s);
        }
    }
    
    @Override
    public String toString()
    {
        return asCqlString();
    }
}
