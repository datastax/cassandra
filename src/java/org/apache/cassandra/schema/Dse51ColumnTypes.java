/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.FrozenType;
import org.apache.cassandra.db.marshal.PartitionerDefinedOrder;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * The column types of the big {@code me} sstables written for DSE 5.1 (the DSE 5.1 compatibility mode). DSE 5.1.51
 * parses the type names of the serialization header of an sstable as Cassandra 3.11 does, so:
 * <ul>
 *     <li>only the types of Cassandra 3.11, and DSE's geometric and date range types, can be written: a column of
 *     another type (a vector, a non-frozen tuple, a type of another package) is refused, at the schema statement that
 *     would create it in the mode ({@link #refusedStatementChanges}), in a schema change received from a node not in
 *     the mode ({@link #refusedSchemaMutations}), at start ({@link #verifyAtStartup}) and, as a last line, when the
 *     header is written ({@link #sstableTypeName});</li>
 *     <li>a frozen tuple is named without {@code FrozenType}, as Cassandra 3.x names it (a tuple is always frozen
 *     there); a frozen user type keeps it, as DSE 5.1 writes it.</li>
 * </ul>
 * The names of DSE's geometric and date range types are those this software reads in DSE 6's sstables; that DSE 5.1.51
 * reads them is for a check on a DSE 5.1.51 node.
 */
public final class Dse51ColumnTypes
{
    private static final String MARSHAL_PACKAGE = AbstractType.class.getPackageName();

    /** The types of {@code org.apache.cassandra.db.marshal} a DSE 5.1.51 node parses, by simple class name. */
    private static final Set<String> PARSED_BY_DSE51 = Set.of("AsciiType", "BooleanType", "BytesType", "ByteType",
                                                              "CompositeType", "CounterColumnType", "DateType",
                                                              "DecimalType", "DoubleType", "DurationType",
                                                              "DynamicCompositeType", "EmptyType", "FloatType",
                                                              "InetAddressType", "Int32Type", "IntegerType",
                                                              "LexicalUUIDType", "ListType", "LongType", "MapType",
                                                              "PartitionerDefinedOrder", "ReversedType", "SetType",
                                                              "ShortType", "SimpleDateType", "TimestampType",
                                                              "TimeType", "TimeUUIDType", "TupleType", "UserType",
                                                              "UTF8Type", "UUIDType",
                                                              // DSE's own, read by DSE 5.1 too
                                                              "DateRangeType", "PointType", "LineStringType",
                                                              "PolygonType");

    private static final String FROZEN_TUPLE = FrozenType.class.getName() + '(' + TupleType.class.getName() + '(';

    private static final String PARTITIONER_DEFINED_ORDER = PartitionerDefinedOrder.class.getName() + '(';

    /** The reason under which the refused statements are counted. */
    public static final String REFUSED_SCHEMA_CHANGE = "schema change with a column type DSE 5.1 does not read";

    private Dse51ColumnTypes()
    {
    }

    /**
     * The part of {@code type} DSE 5.1.51 cannot parse, named as this software names it, or null when it parses all of
     * it: a type outside the types of Cassandra 3.11 and DSE's own, or a non-frozen tuple.
     */
    @Nullable
    public static String unreadable(AbstractType<?> type)
    {
        Class<?> klass = type.getClass();
        // a class of that package itself, not of a package under it
        boolean parsed = klass.getPackageName().equals(MARSHAL_PACKAGE) &&
                         PARSED_BY_DSE51.contains(klass.getSimpleName());
        if (!parsed)
            return type.toString();
        if (type instanceof TupleType && !(type instanceof UserType) && type.isMultiCell())
            return "non-frozen " + type.asCQL3Type();
        for (AbstractType<?> subType : type.subTypes())
        {
            String unreadable = unreadable(subType);
            if (unreadable != null)
                return unreadable;
        }
        return null;
    }

    /**
     * The name of {@code type} in the serialization header of a big {@code me} sstable, as DSE 5.1.51 and Cassandra
     * 3.x write it: this software's name without the {@code FrozenType} of a frozen tuple.
     *
     * @throws IllegalStateException when DSE 5.1.51 cannot parse the type ({@link #unreadable})
     */
    public static String sstableTypeName(AbstractType<?> type)
    {
        String unreadable = unreadable(type);
        if (unreadable != null)
            throw new IllegalStateException("A big me sstable for DSE 5.1 cannot hold a column of type " + type +
                                            ": DSE 5.1 cannot read " + unreadable);
        return withoutPartitionKeyTypes(withoutFrozenTuples(type.toString()));
    }

    /**
     * {@code name} with every {@code PartitionerDefinedOrder} (the clustering of the table of a secondary index) named
     * by its partitioner alone, as Cassandra 3.x names it: without the storage mode (an offline tool) this software
     * appends the partition key type after a {@code :}, which the type parser of Cassandra 3.11 refuses. A name whose
     * parenthesis is never closed, which the type parser never gives, is returned as it is.
     */
    @VisibleForTesting
    static String withoutPartitionKeyTypes(String name)
    {
        StringBuilder result = new StringBuilder(name.length());
        int from = 0;
        int at;
        while ((at = name.indexOf(PARTITIONER_DEFINED_ORDER, from)) >= 0)
        {
            int open = at + PARTITIONER_DEFINED_ORDER.length();
            int depth = 1;
            int close = open;
            int colon = -1;
            while (close < name.length() && depth > 0)
            {
                char c = name.charAt(close);
                if (c == '(')
                    depth++;
                else if (c == ')')
                    depth--;
                else if (c == ':' && depth == 1 && colon < 0)
                    colon = close;
                if (depth > 0)
                    close++;
            }
            if (depth > 0)
                return name;
            result.append(name, from, open).append(name, open, colon < 0 ? close : colon).append(')');
            from = close + 1;
        }
        return result.append(name, Math.min(from, name.length()), name.length()).toString();
    }

    /** {@code name} without the {@code FrozenType(...)} that wraps a tuple, wherever it is. */
    private static String withoutFrozenTuples(String name)
    {
        StringBuilder result = new StringBuilder(name.length());
        // the closing parentheses to drop, by depth
        List<Integer> dropped = new ArrayList<>();
        int depth = 0;
        for (int i = 0; i < name.length(); i++)
        {
            if (name.startsWith(FROZEN_TUPLE, i))
            {
                dropped.add(depth);
                depth++;
                i += FrozenType.class.getName().length();
                continue;
            }
            char c = name.charAt(i);
            if (c == '(')
            {
                depth++;
            }
            else if (c == ')')
            {
                depth--;
                if (!dropped.isEmpty() && dropped.get(dropped.size() - 1) == depth)
                {
                    dropped.remove(dropped.size() - 1);
                    continue;
                }
            }
            result.append(c);
        }
        return result.toString();
    }

    /**
     * In {@code DSE_5_1}, the refusal of a schema statement that changes {@code before} into {@code after} so that a
     * table or a view has a column, or a user type a field, of a type DSE 5.1 cannot read ({@link #unreadable}); null
     * when there is none, or outside the mode. Only the tables, views and user types the change creates or changes
     * are checked.
     */
    @Nullable
    public static String refusedStatementChanges(Keyspaces before, Keyspaces after)
    {
        if (!DatabaseDescriptor.isDse51CompatibilityMode())
            return null;
        List<String> refused = new ArrayList<>();
        for (KeyspaceMetadata keyspace : after)
        {
            KeyspaceMetadata previous = before.getNullable(keyspace.name);
            if (previous == keyspace)
                continue;
            // a user type, used by a table or not: a schema change that carries it is refused by the other nodes in
            // the mode (refusedSchemaMutations)
            for (UserType type : keyspace.types)
            {
                UserType old = previous == null ? null : previous.types.getNullable(type.name);
                if (type.equals(old))
                    continue;
                for (int i = 0; i < type.size(); i++)
                {
                    String unreadable = unreadable(type.fieldType(i));
                    if (unreadable != null)
                        refused.add(keyspace.name + '.' + type.getNameAsString() + '.' + type.fieldName(i) +
                                    " (" + unreadable + ')');
                }
            }
            for (TableMetadata table : keyspace.tablesAndViews())
            {
                TableMetadata old = previous == null ? null : previous.getTableOrViewNullable(table.name);
                if (table.equals(old))
                    continue;
                for (ColumnMetadata column : table.columns())
                {
                    String unreadable = unreadable(column.type);
                    if (unreadable != null)
                        refused.add(table.keyspace + '.' + table.name + '.' + column.name + " (" + unreadable + ')');
                }
            }
        }
        if (refused.isEmpty())
            return null;
        refused.sort(null);
        DseCompatRefusals.refused(REFUSED_SCHEMA_CHANGE);
        return Dse51Compressors.STATEMENT_REFUSAL + String.join(", ", refused) +
               (refused.size() == 1 ? " is" : " are") + " of a type DSE 5.1 cannot read, so a DSE 5.1 node could " +
               "not open the sstables this node writes for " + (refused.size() == 1 ? "it" : "them");
    }

    /**
     * Every column of a table or a view of {@code keyspaces} of a type DSE 5.1 cannot read, as
     * {@code keyspace.table.column (part)}, sorted.
     */
    @VisibleForTesting
    static List<String> unreadableColumns(Iterable<KeyspaceMetadata> keyspaces)
    {
        List<String> refused = new ArrayList<>();
        for (KeyspaceMetadata keyspace : keyspaces)
            for (TableMetadata table : keyspace.tablesAndViews())
                for (ColumnMetadata column : table.columns())
                {
                    String unreadable = unreadable(column.type);
                    if (unreadable != null)
                        refused.add(table.keyspace + '.' + table.name + '.' + column.name + " (" + unreadable + ')');
                }
        refused.sort(null);
        return refused;
    }

    /**
     * In {@code DSE_5_1}, refuses to start while a table or a view of the schema has a column of a type DSE 5.1 cannot
     * read: this node could write no sstable of it. Only a node not in the mode can have given the schema such a
     * column, since the statements and the schema changes received in the mode are refused. Does nothing in the other
     * modes.
     */
    public static void verifyAtStartup() throws StartupException
    {
        if (!DatabaseDescriptor.isDse51CompatibilityMode())
            return;
        List<KeyspaceMetadata> keyspaces = new ArrayList<>();
        for (String name : Schema.instance.getKeyspaces())
        {
            KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(name);
            if (keyspace != null)
                keyspaces.add(keyspace);
        }
        List<String> refused = unreadableColumns(keyspaces);
        if (!refused.isEmpty())
            throw new StartupException(StartupException.ERR_WRONG_CONFIG, startRefusal(refused));
    }

    /**
     * The refusal of the start while a column of the schema has a type DSE 5.1 cannot read. Its only remedy is the
     * flag drop, where the refusal of a compressor has a start in the mode that keeps the rollback (one start with
     * {@code ignore_dse51_compression}): such a column enters the schema only while a node of this software runs
     * outside the mode (a statement in the mode refuses it, and a node in the mode does not apply a schema change that
     * carries one), so the schema the node holds is no longer the one DSE 5.1 nodes hold, and the data written for the
     * column outside the mode is in sstables DSE 5.1 does not read.
     */
    @VisibleForTesting
    static String startRefusal(List<String> refused)
    {
        return "internode_compatibility_mode DSE_5_1 refused: " + String.join(", ", refused) +
               (refused.size() == 1 ? " is" : " are") + " of a type DSE 5.1 cannot read, so this node cannot write " +
               "the sstables DSE 5.1 reads for " + (refused.size() == 1 ? "it" : "them") + ", and its rollback " +
               "to DSE 5.1 is no longer possible. End the mode on this node: the flag drop " +
               "(internode_compatibility_mode: NONE with storage_compatibility_mode: HCD_1), with -D" +
               CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS.getKey() + "=true, needed " +
               "while the marker of the DSE 5.1 upgrade exists on this node or its record of DSE 5.1 peers lists any " +
               "(or cannot be read); out of the mode, the node no longer reaches the DSE 5.1 nodes that remain";
    }

    /**
     * In {@code DSE_5_1}, the columns and the fields of user types to which the schema {@code mutations} give a type
     * DSE 5.1 cannot read, as {@code keyspace.table.column (type)} or {@code keyspace.type.field (type)}, sorted;
     * empty when there is none, or outside the mode. Read from the rows of {@code system_schema.columns} and
     * {@code system_schema.types} they carry, the types parsed with the user types the keyspace has on this node; a
     * type that does not parse so (a user type the change itself creates) is refused when its text names a vector or
     * is a non-frozen tuple ({@code tuple<...>} not under {@code frozen<...>}), which DSE 5.1 does not read (a user
     * type named after {@code vector<} would be taken for one); the user type, when created, is checked through its
     * own row.
     */
    public static List<String> refusedSchemaMutations(Collection<Mutation> mutations)
    {
        if (!DatabaseDescriptor.isDse51CompatibilityMode())
            return List.of();
        List<String> refused = new ArrayList<>();
        for (Mutation mutation : mutations)
        {
            if (!SchemaConstants.SCHEMA_KEYSPACE_NAME.equals(mutation.getKeyspaceName()))
                continue;
            for (PartitionUpdate update : mutation.getPartitionUpdates())
            {
                String table = update.metadata().name;
                boolean columns = SchemaKeyspaceTables.COLUMNS.equals(table);
                if (!columns && !SchemaKeyspaceTables.TYPES.equals(table))
                    continue;
                String keyspace = UTF8Type.instance.compose(update.partitionKey().getKey());
                KeyspaceMetadata current = Schema.instance.getKeyspaceMetadata(keyspace);
                Types types = current == null ? Types.none() : current.types;
                String typeName = columns ? "type" : "field_types";
                ColumnMetadata typeColumn = update.metadata().getColumn(ByteBufferUtil.bytes(typeName));
                ColumnMetadata namesColumn = columns ? null
                                                     : update.metadata().getColumn(ByteBufferUtil.bytes("field_names"));
                for (Row row : (Iterable<Row>) update::rowIterator)
                {
                    Cell<?> cell = typeColumn == null ? null : row.getCell(typeColumn);
                    if (cell == null || !cell.isLive(FBUtilities.nowInSeconds()))
                        continue;
                    String owner = keyspace + '.' + UTF8Type.instance.compose(row.clustering().bufferAt(0));
                    if (columns)
                    {
                        String column = UTF8Type.instance.compose(row.clustering().bufferAt(1));
                        String type = UTF8Type.instance.compose(cell.buffer());
                        if (isUnreadable(keyspace, type, types))
                            refused.add(owner + '.' + column + " (" + type + ')');
                        continue;
                    }
                    @SuppressWarnings("unchecked")
                    List<String> fieldTypes = (List<String>) typeColumn.type.compose(cell.buffer());
                    Cell<?> namesCell = namesColumn == null ? null : row.getCell(namesColumn);
                    List<String> fieldNames = null;
                    if (namesCell != null)
                    {
                        @SuppressWarnings("unchecked")
                        List<String> names = (List<String>) namesColumn.type.compose(namesCell.buffer());
                        fieldNames = names;
                    }
                    for (int i = 0; i < fieldTypes.size(); i++)
                    {
                        if (!isUnreadable(keyspace, fieldTypes.get(i), types))
                            continue;
                        String field = fieldNames != null && i < fieldNames.size() ? fieldNames.get(i)
                                                                                   : String.valueOf(i);
                        refused.add(owner + '.' + field + " (" + fieldTypes.get(i) + ')');
                    }
                }
            }
        }
        refused.sort(null);
        return refused;
    }

    /** Whether the CQL type {@code type} of a schema row is one DSE 5.1 cannot read, see refusedSchemaMutations. */
    private static boolean isUnreadable(String keyspace, String type, Types types)
    {
        try
        {
            return unreadable(CQLTypeParser.parse(keyspace, type, types)) != null;
        }
        catch (RuntimeException e)
        {
            String text = type.toLowerCase(Locale.ROOT).trim();
            return text.contains("vector<") || text.startsWith("tuple<");
        }
    }
}
