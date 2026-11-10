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
package org.apache.cassandra.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.FieldPath;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.Pair;

public class TargetParser
{
    private static final Pattern TARGET_REGEX = Pattern.compile("^(keys|entries|values|full)\\((.+)\\)$");
    private static final Pattern TWO_QUOTES = Pattern.compile("\"\"");
    private static final String QUOTE = "\"";

    public static Pair<ColumnMetadata, IndexTarget.Type> parse(TableMetadata metadata, IndexMetadata indexDef)
    {
        String target = indexDef.options.get("target");
        assert target != null : String.format("No target definition found for index %s", indexDef.name);
        Pair<ColumnMetadata, IndexTarget.Type> result = parse(metadata, target);
        if (result == null)
            throw new ConfigurationException(String.format("Unable to parse targets for index %s (%s)", indexDef.name, target));
        return result;
    }

    public static Pair<ColumnMetadata, IndexTarget.Type> parse(TableMetadata metadata, String target)
    {
        // if the regex matches then the target is in the form "keys(foo)", "entries(bar)" etc
        // if not, then it must be a simple column name and implictly its type is VALUES
        Matcher matcher = TARGET_REGEX.matcher(target);
        String columnName;
        IndexTarget.Type targetType;
        if (matcher.matches())
        {
            targetType = IndexTarget.Type.fromString(matcher.group(1));
            columnName = matcher.group(2);
        }
        else
        {
            columnName = target;
            targetType = IndexTarget.Type.VALUES;
        }

        // in the case of a quoted column name the name in the target string
        // will be enclosed in quotes, which we need to unwrap. It may also
        // include quote characters internally, escaped like so:
        //      abc"def -> abc""def.
        // Because the target string is stored in a CQL compatible form, we
        // need to un-escape any such quotes to get the actual column name
        if (columnName.startsWith(QUOTE))
        {
            columnName = StringUtils.substring(StringUtils.substring(columnName, 1), 0, -1);
            columnName = TWO_QUOTES.matcher(columnName).replaceAll(QUOTE);
        }

        // if it's not a CQL table, we can't assume that the column name is utf8, so
        // in that case we have to do a linear scan of the cfm's columns to get the matching one.
        // After dropping compact storage (see CASSANDRA-10857), we can't distinguish between the
        // former compact/thrift table, so we have to fall back to linear scan in both cases.
        ColumnMetadata cd = metadata.getColumn(new ColumnIdentifier(columnName, true));
        if (cd != null)
            return Pair.create(cd, targetType);

        for (ColumnMetadata column : metadata.columns())
            if (column.name.toString().equals(columnName))
                return Pair.create(column, targetType);

        return null;
    }

    /**
     * Parses the target of an index, including the targets on a field of a UDT or tuple column (see
     * {@link IndexTarget#asCqlString()}).
     *
     * @return the parsed target
     * @throws ConfigurationException if the target can't be parsed
     */
    public static Target parseTarget(TableMetadata metadata, IndexMetadata indexDef)
    {
        String target = indexDef.options.get(IndexTarget.TARGET_OPTION_NAME);
        assert target != null : String.format("No target definition found for index %s", indexDef.name);
        Target result = parseTarget(metadata, target);
        if (result == null)
            throw new ConfigurationException(String.format("Unable to parse targets for index %s (%s)", indexDef.name, target));
        return result;
    }

    /**
     * Parses the target of an index, including the targets on a field of a UDT or tuple column: {@code col.f.g},
     * {@code values(col.f)} and {@code full(col.f)}, as written by {@link IndexTarget#asCqlString()}.
     * <p>
     * As for {@link #parse(TableMetadata, String)}, a simple target has type {@link IndexTarget.Type#VALUES}.
     *
     * @return the parsed target, or {@code null} if the target doesn't designate a column of the table, or a field
     * of one
     */
    @Nullable
    public static Target parseTarget(TableMetadata metadata, String target)
    {
        // A target that designates a whole column, parsed as it always was. Column names can contain dots when
        // quoted: the target is that column if it's written as the CQL form of its name (a target "addr.city" is
        // the column named addr.city, while addr.city is the field city of column addr).
        Pair<ColumnMetadata, IndexTarget.Type> columnTarget = parse(metadata, target);
        if (columnTarget != null && columnTarget.left.name.toCQLString().equals(columnText(target)))
            return new Target(columnTarget.left, columnTarget.right, null);

        Target fieldTarget = parseFieldTarget(metadata, target);
        if (fieldTarget != null)
            return fieldTarget;

        // the targets that may have been stored in other forms (for example by the drivers or old versions)
        return columnTarget == null ? null : new Target(columnTarget.left, columnTarget.right, null);
    }

    /**
     * @return the column part of a target: the target without its {@code keys(...)}, {@code values(...)}, ... wrapper
     */
    private static String columnText(String target)
    {
        Matcher matcher = TARGET_REGEX.matcher(target);
        return matcher.matches() ? matcher.group(2) : target;
    }

    @Nullable
    private static Target parseFieldTarget(TableMetadata metadata, String target)
    {
        List<String> names = new ArrayList<>();
        IndexTarget.Type type = IndexTarget.Type.VALUES;
        Matcher matcher = TARGET_REGEX.matcher(target);
        String path = target;
        if (matcher.matches())
        {
            type = IndexTarget.Type.fromString(matcher.group(1));
            path = matcher.group(2);
        }

        if (!splitNames(path, names) || names.size() < 2)
            return null;

        String columnName = names.get(0);
        ColumnMetadata column = metadata.getColumn(new ColumnIdentifier(columnName, true));
        if (column == null)
            return null;

        List<FieldIdentifier> fieldNames = new ArrayList<>(names.size() - 1);
        for (String name : names.subList(1, names.size()))
            fieldNames.add(FieldIdentifier.forInternalString(name));

        try
        {
            return new Target(column, type, FieldPath.resolve(column.type, fieldNames));
        }
        catch (InvalidRequestException e)
        {
            return null;
        }
    }

    /**
     * Splits a dotted path of CQL identifiers ({@code a.b}, {@code "A"."b.c"}), unquoting the quoted ones.
     *
     * @return {@code false} if the string is not a dotted path of identifiers
     */
    private static boolean splitNames(String path, List<String> names)
    {
        int i = 0;
        int length = path.length();
        while (true)
        {
            if (i >= length)
                return false;

            StringBuilder name = new StringBuilder();
            if (path.charAt(i) == '"')
            {
                i++;
                while (true)
                {
                    if (i >= length)
                        return false;
                    char c = path.charAt(i++);
                    if (c == '"')
                    {
                        // a doubled quote is an escaped quote, a single one closes the name
                        if (i < length && path.charAt(i) == '"')
                        {
                            name.append('"');
                            i++;
                        }
                        else
                        {
                            break;
                        }
                    }
                    else
                    {
                        name.append(c);
                    }
                }
            }
            else
            {
                while (i < length && isUnquotedIdentifierChar(path.charAt(i)))
                    name.append(path.charAt(i++));
                if (name.length() == 0)
                    return false;
            }
            names.add(name.toString());

            if (i == length)
                return true;
            if (path.charAt(i++) != '.')
                return false;
        }
    }

    private static boolean isUnquotedIdentifierChar(char c)
    {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
    }

    /**
     * A parsed index target: a column, the type of target and, for an index on a field of the column, the path to
     * the field.
     */
    public static final class Target
    {
        public final ColumnMetadata column;
        public final IndexTarget.Type type;
        @Nullable
        public final FieldPath fieldPath;

        public Target(ColumnMetadata column, IndexTarget.Type type, @Nullable FieldPath fieldPath)
        {
            this.column = column;
            this.type = type;
            this.fieldPath = fieldPath;
        }

        public boolean hasFieldPath()
        {
            return fieldPath != null;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (!(o instanceof Target))
                return false;
            Target other = (Target) o;
            return column.equals(other.column) && type == other.type && Objects.equals(fieldPath, other.fieldPath);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(column, type, fieldPath);
        }

        @Override
        public String toString()
        {
            return fieldPath == null
                   ? String.format("%s(%s)", type, column.name)
                   : String.format("%s(%s.%s)", type, column.name, fieldPath.toCQLString(column.type));
        }
    }
}
