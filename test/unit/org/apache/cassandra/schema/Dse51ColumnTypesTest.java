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

import java.util.Collection;
import java.util.List;

import ch.qos.logback.classic.spi.ILoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DateRangeType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LegacyTimeUUIDType;
import org.apache.cassandra.db.marshal.LineStringType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.PartitionerDefinedOrder;
import org.apache.cassandra.db.marshal.PointType;
import org.apache.cassandra.db.marshal.PolygonType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The column types of the big me sstables written for DSE 5.1: the types DSE 5.1.51 reads, their names as Cassandra
 * 3.x writes them, and the refusal, in {@code internode_compatibility_mode: DSE_5_1}, of a schema statement that would
 * give a table a type DSE 5.1 cannot read.
 */
public class Dse51ColumnTypesTest extends CQLTester
{
    private static final String MARSHAL = "org.apache.cassandra.db.marshal.";
    private static final TupleType TUPLE = new TupleType(List.of(Int32Type.instance, UTF8Type.instance));
    private static final String TUPLE_NAME = MARSHAL + "TupleType(" + MARSHAL + "Int32Type," + MARSHAL + "UTF8Type)";

    private InternodeCompatibilityMode modeBefore;

    @Before
    public void saveMode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
    }

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    @Test
    public void theTypesOfCassandra311AndDseAreRead()
    {
        List<AbstractType<?>> read = List.of(Int32Type.instance, UTF8Type.instance, TUPLE,
                                             ListType.getInstance(TUPLE, true),
                                             SetType.getInstance(UTF8Type.instance, false),
                                             MapType.getInstance(UTF8Type.instance, TUPLE, true),
                                             ReversedType.getInstance(Int32Type.instance), PointType.instance,
                                             LineStringType.instance, PolygonType.instance, DateRangeType.instance,
                                             DurationType.instance, CounterColumnType.instance);
        for (AbstractType<?> type : read)
            assertThat(Dse51ColumnTypes.unreadable(type)).as(type.toString()).isNull();
    }

    @Test
    public void theOtherTypesAreNamed()
    {
        VectorType<Float> vector = VectorType.getInstance(FloatType.instance, 3);
        assertThat(Dse51ColumnTypes.unreadable(vector)).isEqualTo(vector.toString());
        // inside another type, the part DSE 5.1 cannot read
        assertThat(Dse51ColumnTypes.unreadable(ListType.getInstance(vector, false))).isEqualTo(vector.toString());
        assertThat(Dse51ColumnTypes.unreadable(new TupleType(List.of(Int32Type.instance, vector))))
        .isEqualTo(vector.toString());
        assertThat(Dse51ColumnTypes.unreadable(LegacyTimeUUIDType.instance))
        .isEqualTo(LegacyTimeUUIDType.instance.toString());
        // a tuple that is not frozen: Cassandra 3.x has none
        TupleType nonFrozen = new TupleType(List.of(Int32Type.instance, UTF8Type.instance), true);
        assertThat(Dse51ColumnTypes.unreadable(nonFrozen)).isEqualTo("non-frozen " + nonFrozen.asCQL3Type());
        assertThatThrownBy(() -> Dse51ColumnTypes.sstableTypeName(vector))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("A big me sstable for DSE 5.1 cannot hold a column of type " + vector + ": DSE 5.1 cannot read " +
                    vector);
    }

    @Test
    public void aFrozenTupleIsNamedWithoutFrozenType()
    {
        assertThat(TUPLE.toString()).isEqualTo(MARSHAL + "FrozenType(" + TUPLE_NAME + ')');
        assertThat(Dse51ColumnTypes.sstableTypeName(TUPLE)).isEqualTo(TUPLE_NAME);
        assertThat(Dse51ColumnTypes.sstableTypeName(ListType.getInstance(TUPLE, true)))
        .isEqualTo(MARSHAL + "ListType(" + TUPLE_NAME + ')');
        assertThat(Dse51ColumnTypes.sstableTypeName(MapType.getInstance(TUPLE, TUPLE, true)))
        .isEqualTo(MARSHAL + "MapType(" + TUPLE_NAME + ',' + TUPLE_NAME + ')');
        // a frozen collection of tuples: FrozenType at the top, kept, as 3.x writes it
        String frozenList = ListType.getInstance(TUPLE, false).toString();
        assertThat(Dse51ColumnTypes.sstableTypeName(ListType.getInstance(TUPLE, false))).isEqualTo(frozenList);
        assertThat(frozenList).isEqualTo(MARSHAL + "FrozenType(" + MARSHAL + "ListType(" + TUPLE_NAME + "))");
        // a type without a tuple keeps its name
        assertThat(Dse51ColumnTypes.sstableTypeName(SetType.getInstance(UTF8Type.instance, false)))
        .isEqualTo(SetType.getInstance(UTF8Type.instance, false).toString());
    }

    /**
     * The clustering of the table of a secondary index, which this software names with the partition key type after
     * a {@code :} without the storage mode (an offline tool), is named by its partitioner alone, as Cassandra 3.11
     * parses it; a type that holds one is named so at any depth.
     */
    @Test
    public void thePartitionerDefinedOrderIsNamedByItsPartitionerAlone()
    {
        PartitionerDefinedOrder order = new PartitionerDefinedOrder(Murmur3Partitioner.instance, TUPLE);
        String plain = MARSHAL + "PartitionerDefinedOrder(" + Murmur3Partitioner.class.getName() + ')';
        StorageCompatibilityMode before = DatabaseDescriptor.getStorageCompatibilityMode();
        try
        {
            DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
            assertThat(order.toString()).startsWith(MARSHAL + "PartitionerDefinedOrder(" +
                                                    Murmur3Partitioner.class.getName() + ':');
            assertThat(Dse51ColumnTypes.sstableTypeName(order)).isEqualTo(plain);
            assertThat(Dse51ColumnTypes.sstableTypeName(ReversedType.getInstance(order)))
            .isEqualTo(MARSHAL + "ReversedType(" + plain + ')');
            DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
            assertThat(order.toString()).isEqualTo(plain);
            assertThat(Dse51ColumnTypes.sstableTypeName(order)).isEqualTo(plain);
            // a name whose parenthesis is never closed is left as it is
            String unterminated = MARSHAL + "PartitionerDefinedOrder(" + Murmur3Partitioner.class.getName() + ":x";
            assertThat(Dse51ColumnTypes.withoutPartitionKeyTypes(unterminated)).isEqualTo(unterminated);
        }
        finally
        {
            DatabaseDescriptor.setStorageCompatibilityMode(before);
        }
    }

    /**
     * In {@code DSE_5_1}, with {@code allow_ddl}, a table or a column of a vector is refused, counted, and changes
     * nothing; the other types run; with {@code NONE} the statement runs.
     */
    @Test
    public void aStatementGivingATableAVectorIsRefusedInDse51Mode() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
        {
            long before = DseCompatRefusals.count(Dse51ColumnTypes.REFUSED_SCHEMA_CHANGE);
            VectorType<Float> vector = VectorType.getInstance(FloatType.instance, 2);
            assertThatThrownBy(() -> execute("CREATE TABLE " + KEYSPACE + ".vectors (k int PRIMARY KEY, " +
                                             "e vector<float, 2>)"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessage("internode_compatibility_mode DSE_5_1 refuses the statement: " + KEYSPACE + ".vectors.e (" +
                        vector + ") is of a type DSE 5.1 cannot read, so a DSE 5.1 node could not open the sstables " +
                        "this node writes for it");
            assertThatThrownBy(() -> execute("ALTER TABLE " + KEYSPACE + '.' + table + " ADD e vector<float, 2>"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining(KEYSPACE + '.' + table + ".e (" + vector + ") is of a type DSE 5.1 cannot read");
            assertThat(Schema.instance.getTableMetadata(KEYSPACE, "vectors")).isNull();
            assertThat(Schema.instance.getTableMetadata(KEYSPACE, table).getColumn(UTF8Type.instance.decompose("e")))
            .isNull();
            assertThat(DseCompatRefusals.count(Dse51ColumnTypes.REFUSED_SCHEMA_CHANGE)).isEqualTo(before + 2);

            // refused for its compression too, the statement names both
            assertThatThrownBy(() -> execute("CREATE TABLE " + KEYSPACE + ".both (k int PRIMARY KEY, " +
                                             "e vector<float, 2>) WITH compression = {'class': 'ZstdCompressor'}"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageStartingWith("internode_compatibility_mode DSE_5_1 refuses the statement: " + KEYSPACE +
                                    ".both (ZstdCompressor)")
            .hasMessageEndingWith("compression; " + KEYSPACE + ".both.e (" + vector + ") is of a type DSE 5.1 cannot " +
                                  "read, so a DSE 5.1 node could not open the sstables this node writes for it")
            .satisfies(e -> assertThat(e.getMessage().split("refuses the statement", -1)).hasSize(2));
            assertThat(Schema.instance.getTableMetadata(KEYSPACE, "both")).isNull();
            assertThat(DseCompatRefusals.count(Dse51ColumnTypes.REFUSED_SCHEMA_CHANGE)).isEqualTo(before + 3);

            execute("ALTER TABLE " + KEYSPACE + '.' + table + " ADD t frozen<tuple<int, text>>");
            assertThat(DseCompatRefusals.count(Dse51ColumnTypes.REFUSED_SCHEMA_CHANGE)).isEqualTo(before + 3);
        }
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        execute("ALTER TABLE " + KEYSPACE + '.' + table + " ADD e vector<float, 2>");
        assertThat(Schema.instance.getTableMetadata(KEYSPACE, table).getColumn(UTF8Type.instance.decompose("e")))
        .isNotNull();
    }

    /** In {@code DSE_5_1}, with {@code allow_ddl}, a field of a vector added to a user type a table uses is refused. */
    @Test
    public void aVectorFieldAddedToAUserTypeIsRefusedInDse51Mode() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int)");
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, u frozen<" + type + ">)");
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
        {
            assertThatThrownBy(() -> execute("ALTER TYPE " + KEYSPACE + '.' + type + " ADD e vector<float, 2>"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageStartingWith("internode_compatibility_mode DSE_5_1 refuses the statement: " + KEYSPACE + '.' +
                                    table + ".u (")
            .hasMessageContaining("VectorType");
        }
        UserType unchanged = Schema.instance.getKeyspaceMetadata(KEYSPACE).types
                                           .getNullable(UTF8Type.instance.decompose(type));
        assertThat(unchanged.fieldPosition(FieldIdentifier.forUnquoted("e"))).isNegative();
    }

    /**
     * In {@code DSE_5_1}, with {@code allow_ddl}, a user type created with a field of a vector is refused, used by a
     * table or not, since the other nodes in the mode would not apply the schema change that carries it.
     */
    @Test
    public void aUserTypeWithAVectorIsRefusedInDse51Mode() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
        {
            assertThatThrownBy(() -> execute("CREATE TYPE " + KEYSPACE + ".vt (a int, e vector<float, 2>)"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessage("internode_compatibility_mode DSE_5_1 refuses the statement: " + KEYSPACE + ".vt.e (" +
                        VectorType.getInstance(FloatType.instance, 2) + ") is of a type DSE 5.1 cannot read, so a " +
                        "DSE 5.1 node could not open the sstables this node writes for it");
        }
        Types types = Schema.instance.getKeyspaceMetadata(KEYSPACE).types;
        assertThat(types.getNullable(UTF8Type.instance.decompose("vt"))).isNull();
    }

    /**
     * In {@code DSE_5_1} a push of a schema change that gives a table a column of a vector, from a node of this
     * software not in the mode, is not applied, counted and logged at WARN.
     */
    @Test
    public void aPushedSchemaChangeWithAVectorIsNotAppliedInDse51Mode() throws Throwable
    {
        InetAddressAndPort sender = InetAddressAndPort.getByName("127.0.0.241");
        MessagingService.instance().versions.set(sender, MessagingService.current_version);
        try
        {
            pushVectorTableFrom(sender);
        }
        finally
        {
            MessagingService.instance().versions.reset(sender);
        }
    }

    private void pushVectorTableFrom(InetAddressAndPort sender) throws Throwable
    {
        TableMetadata vectors = TableMetadata.builder(KEYSPACE, "pushed_vectors_handler")
                                             .addPartitionKeyColumn("k", Int32Type.instance)
                                             .addRegularColumn("e", VectorType.getInstance(FloatType.instance, 2))
                                             .build();
        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(KEYSPACE);
        long timestamp = FBUtilities.timestampMicros();
        Mutation push = SchemaKeyspace.makeCreateTableMutation(keyspace, vectors, timestamp).build();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        long before = DseCompatRefusals.count(Dse51ColumnTypes.REFUSED_SCHEMA_CHANGE);
        withLogAppender(DefaultSchemaUpdateHandler.class, appender -> {
            Collection<Mutation> pushed = List.of(push);
            SchemaPushVerbHandler.instance.doVerb(Message.builder(Verb.SCHEMA_PUSH_REQ, pushed).from(sender).build());
            assertThat(appender.list.stream().map(ILoggingEvent::getFormattedMessage))
            .contains("Not applying a schema change from " + sender + " in internode_compatibility_mode DSE_5_1: it " +
                      "gives [" + KEYSPACE + ".pushed_vectors_handler.e (vector<float, 2>)] a type DSE 5.1 cannot " +
                      "read. Only a node not in the mode sends such a change: this node's schema disagrees with that " +
                      "node's, and the schema this node pulls from it is not applied either, until the change is " +
                      "reverted there");
        });
        assertThat(Schema.instance.getTableMetadata(KEYSPACE, "pushed_vectors_handler")).isNull();
        assertThat(DseCompatRefusals.count(Dse51ColumnTypes.REFUSED_SCHEMA_CHANGE)).isEqualTo(before + 1);
    }

    /**
     * In {@code DSE_5_1} a schema change from a node not in the mode that gives a table a column, or a user type a
     * field, of a vector is named, so that the update handler does not apply it; with {@code NONE} nothing is named.
     */
    @Test
    public void aSchemaChangeWithAVectorIsNamedInDse51Mode()
    {
        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(KEYSPACE);
        TableMetadata vectors = TableMetadata.builder(KEYSPACE, "pushed_vectors")
                                             .addPartitionKeyColumn("k", Int32Type.instance)
                                             .addRegularColumn("e", VectorType.getInstance(FloatType.instance, 2))
                                             .build();
        UserType withVector = new UserType("vks", UTF8Type.instance.decompose("vt"),
                                           List.of(FieldIdentifier.forUnquoted("e")),
                                           List.of(VectorType.getInstance(FloatType.instance, 2)), true);
        KeyspaceMetadata newKeyspace = KeyspaceMetadata.create("vks", KeyspaceParams.simple(1),
                                                               Tables.none(), Views.none(), Types.of(withVector),
                                                               UserFunctions.none());
        long timestamp = FBUtilities.timestampMicros();
        List<Mutation> mutations = List.of(SchemaKeyspace.makeCreateTableMutation(keyspace, vectors, timestamp).build(),
                                           SchemaKeyspace.makeCreateKeyspaceMutation(newKeyspace, timestamp).build());
        assertThat(Dse51ColumnTypes.refusedSchemaMutations(mutations)).isEmpty();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        assertThat(Dse51ColumnTypes.refusedSchemaMutations(mutations))
        .containsExactly(KEYSPACE + ".pushed_vectors.e (vector<float, 2>)", "vks.vt.e (vector<float, 2>)");
        // a table without one is not named
        TableMetadata plain = TableMetadata.builder(KEYSPACE, "plain").addPartitionKeyColumn("k", Int32Type.instance)
                                           .build();
        Mutation plainTable = SchemaKeyspace.makeCreateTableMutation(keyspace, plain, timestamp).build();
        assertThat(Dse51ColumnTypes.refusedSchemaMutations(List.of(plainTable))).isEmpty();
    }

    /**
     * A type of a schema change naming a user type the change itself creates does not parse against this node's
     * schema: in {@code DSE_5_1} it is named when it is a non-frozen tuple, and not when the tuple is frozen.
     */
    @Test
    public void aNonFrozenTupleOfAUserTypeCreatedByTheSameChangeIsNamedInDse51Mode()
    {
        UserType u = new UserType("tks", UTF8Type.instance.decompose("u"), List.of(FieldIdentifier.forUnquoted("a")),
                                  List.of(Int32Type.instance), true);
        AbstractType<?> frozenTuple = new TupleType(List.of(Int32Type.instance, u.freeze()), false);
        UserType w = new UserType("tks", UTF8Type.instance.decompose("w"), List.of(FieldIdentifier.forUnquoted("f")),
                                  List.of(frozenTuple), true);
        AbstractType<?> tuple = new TupleType(List.of(Int32Type.instance, u.freeze()), true);
        TableMetadata table = TableMetadata.builder("tks", "tuples")
                                           .addPartitionKeyColumn("k", Int32Type.instance)
                                           .addRegularColumn("frozen_tuple", frozenTuple)
                                           .addRegularColumn("tuple", tuple)
                                           .build();
        KeyspaceMetadata newKeyspace = KeyspaceMetadata.create("tks", KeyspaceParams.simple(1), Tables.of(table),
                                                               Views.none(), Types.of(u, w), UserFunctions.none());
        long timestamp = FBUtilities.timestampMicros();
        List<Mutation> mutations = List.of(SchemaKeyspace.makeCreateKeyspaceMutation(newKeyspace, timestamp).build());
        assertThat(Schema.instance.getKeyspaceMetadata("tks")).isNull();
        assertThat(Dse51ColumnTypes.refusedSchemaMutations(mutations)).isEmpty();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        assertThat(Dse51ColumnTypes.refusedSchemaMutations(mutations))
        .containsExactly("tks.tuples.tuple (" + tuple.asCQL3Type() + ')');
        assertThat(tuple.asCQL3Type().toString()).startsWith("tuple<");
    }

    /** A schema holding a column of a vector refuses the start in {@code DSE_5_1}, and in that mode only. */
    @Test
    public void aVectorColumnRefusesTheStartInDse51ModeOnly() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, e vector<float, 2>)");
        String column = KEYSPACE + '.' + table + ".e (" + VectorType.getInstance(FloatType.instance, 2) + ')';
        assertThat(Dse51ColumnTypes.unreadableColumns(List.of(Schema.instance.getKeyspaceMetadata(KEYSPACE))))
        .containsExactly(column);
        assertThatCode(Dse51ColumnTypes::verifyAtStartup).doesNotThrowAnyException();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        assertThatThrownBy(Dse51ColumnTypes::verifyAtStartup)
        .isInstanceOf(StartupException.class)
        .hasMessage("internode_compatibility_mode DSE_5_1 refused: " + column + " is of a type DSE 5.1 cannot read, " +
                    "so this node cannot write the sstables DSE 5.1 reads for it, and its rollback to DSE 5.1 is no " +
                    "longer possible. End the mode on this node: the flag drop (internode_compatibility_mode: NONE " +
                    "with storage_compatibility_mode: HCD_1), with " +
                    "-Dcassandra.internode_compatibility_mode.ignore_dse_peers=true, needed while the marker of " +
                    "the DSE 5.1 upgrade exists on this node or its record of DSE 5.1 peers lists any (or cannot be " +
                    "read); out of the mode, the node no longer reaches the DSE 5.1 nodes that remain");
    }
}
