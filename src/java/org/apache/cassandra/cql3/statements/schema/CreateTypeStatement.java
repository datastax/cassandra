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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.UTName;
import org.apache.cassandra.cql3.statements.RawKeyspaceAwareStatement;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public final class CreateTypeStatement extends AlterSchemaStatement
{
    private final String typeName;
    private final List<FieldIdentifier> fieldNames;
    private final List<CQL3Type.Raw> rawFieldTypes;
    private final boolean ifNotExists;

    public CreateTypeStatement(String queryString,
                               String keyspaceName,
                               String typeName,
                               List<FieldIdentifier> fieldNames,
                               List<CQL3Type.Raw> rawFieldTypes,
                               boolean ifNotExists)
    {
        super(queryString, keyspaceName);
        this.typeName = typeName;
        this.fieldNames = fieldNames;
        this.rawFieldTypes = rawFieldTypes;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public void validate(ClientState state)
    {
        super.validate(state);

        Guardrails.fieldsPerUDT.guard(fieldNames.size(), typeName, false, state);

        for (int i = 0; i < rawFieldTypes.size(); i++)
        {
            rawFieldTypes.get(i).validate(state, "Field " + fieldNames.get(i));
        }
    }

    public Keyspaces apply(Keyspaces schema)
    {
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
        if (null == keyspace)
            throw ire("Keyspace '%s' doesn't exist", keyspaceName);

        UserType existingType = keyspace.types.getNullable(bytes(typeName));
        if (null != existingType)
        {
            if (ifNotExists)
                return schema;

            throw ire("A user type with name '%s' already exists", typeName);
        }

        Set<FieldIdentifier> usedNames = new HashSet<>();
        for (FieldIdentifier name : fieldNames)
            if (!usedNames.add(name))
                throw ire("Duplicate field name '%s' in type '%s'", name, typeName);

        List<AbstractType<?>> fieldTypes =
            rawFieldTypes.stream()
                         .map(t -> t.prepare(keyspaceName, keyspace.types).getType())
                         .collect(toList());

        UserType udt = new UserType(keyspaceName, bytes(typeName), fieldNames, fieldTypes, true);
        validate(udt);

        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.types.with(udt)));
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.CREATED, Target.TYPE, keyspaceName, typeName);
    }

    public void authorize(ClientState client)
    {
        client.ensureAllTablesPermission(keyspaceName, Permission.CREATE);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_TYPE, keyspaceName, typeName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, typeName);
    }

    public static UserType parse(String cql, String keyspace)
    {
        return parse(cql, keyspace, Types.none());
    }

    public static UserType parse(String cql, String keyspace, Types userTypes)
    {
        return CQLFragmentParser.parseAny(CqlParser::createTypeStatement, cql, "CREATE TYPE")
                                .keyspace(keyspace)
                                .prepare(null) // works around a messy ClientState/QueryProcessor class init deadlock
                                .createType(userTypes);
    }

    /**
     * Build the {@link UserType} this statement creates.
     *
     * @param existingTypes the user-types existing in the keyspace in which the type is created (and thus on which
     *                      the created type may depend on).
     * @return the created type.
     */
    private UserType createType(Types existingTypes)
    {
        List<AbstractType<?>> fieldTypes = rawFieldTypes.stream()
                                                        .map(t -> t.prepare(keyspaceName, existingTypes).getType())
                                                        .collect(toList());
        UserType type = new UserType(keyspaceName, bytes(typeName), fieldNames, fieldTypes, true);
        validate(type);
        return type;
    }

    /**
     * Ensures that the created User-Defined Type (UDT) is valid and allowed.
     * <p/>
     * Note: Most type validation is performed through {@link AbstractType#validateForColumn} because almost no type
     * is intrinsically invalid unless used as a column type. For instance, while we don't declare a column with a
     * {@code set<counter>} type, there is no reason to forbid a UDF in a SELECT clause that takes two separate
     * counter values and puts them in a set. Thus, {@code set<counter>} is not intrinsically invalid, and this applies
     * to almost all validation in {@link AbstractType#validateForColumn}.
     * <p/>
     * However, since UDTs are created separately from their use, it makes sense for user-friendliness to be a bit
     * more restrictive: if a UDT cannot ever be used as a column type, it is almost certainly a user error. Waiting
     * until the type is used to throw an error might be annoying. Therefore, we do not allow creating types that
     * cannot ever be used as column types, even if this is an arbitrary limitation in some ways (e.g., a user may
     * "legitimately" want to create a type solely for use as the return type of a UDF, similar to the {@code set<counter>}
     * example above, but we disallow that).
     *
     * @param type the User-Defined Type to validate
     * @throws IllegalArgumentException if the UDT contains counters, as counters are always disallowed in UDTs
     */
    static void validate(UserType type)
    {
        // The only thing that is always disallowed is the use of counters within a UDT. Anything else might be acceptable,
        // though possibly only if the type is used frozen.
        if (type.referencesCounter())
            throw ire("A user type cannot contain counters");
    }

    public static final class Raw extends RawKeyspaceAwareStatement<CreateTypeStatement>
    {
        private final UTName name;
        private final boolean ifNotExists;

        private final List<FieldIdentifier> fieldNames = new ArrayList<>();
        private final List<CQL3Type.Raw> rawFieldTypes = new ArrayList<>();

        public Raw(UTName name, boolean ifNotExists)
        {
            this.name = name;
            this.ifNotExists = ifNotExists;
        }

        public Raw keyspace(String keyspace)
        {
            name.setKeyspace(keyspace);
            return this;
        }

        @Override
        public CreateTypeStatement prepare(ClientState state, UnaryOperator<String> keyspaceMapper)
        {
            String keyspaceName = keyspaceMapper.apply(name.hasKeyspace() ? name.getKeyspace() : state.getKeyspace());
            if (keyspaceMapper != Constants.IDENTITY_STRING_MAPPER)
                rawFieldTypes.forEach(t -> t.forEachUserType(utName -> utName.updateKeyspaceIfDefined(keyspaceMapper)));
            return new CreateTypeStatement(rawCQLStatement, keyspaceName,
                                           name.getStringTypeName(), fieldNames,
                                           rawFieldTypes, ifNotExists);
        }

        public void addField(FieldIdentifier name, CQL3Type.Raw type)
        {
            fieldNames.add(name);
            rawFieldTypes.add(type);
        }

        public void addToRawBuilder(Types.RawBuilder builder)
        {
            builder.add(name.getStringTypeName(),
                        fieldNames.stream().map(FieldIdentifier::toString).collect(toList()),
                        rawFieldTypes.stream().map(CQL3Type.Raw::toString).collect(toList()));
        }
    }
}
