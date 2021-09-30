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

package org.apache.cassandra.schema;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.marshal.UserType;

public class SchemaChangeNotifier
{
    private final List<SchemaChangeListener> changeListeners = new CopyOnWriteArrayList<>();

    public void registerListener(SchemaChangeListener listener)
    {
        changeListeners.add(listener);
    }

    @SuppressWarnings("unused")
    public void unregisterListener(SchemaChangeListener listener)
    {
        changeListeners.remove(listener);
    }

    public void notifyKeyspaceCreated(KeyspaceMetadata keyspace)
    {
        notifyCreateKeyspace(keyspace);
        keyspace.types.forEach(this::notifyCreateType);
        keyspace.tables.forEach(this::notifyCreateTable);
        keyspace.views.forEach(this::notifyCreateView);
        keyspace.functions.udfs().forEach(this::notifyCreateFunction);
        keyspace.functions.udas().forEach(this::notifyCreateAggregate);
        SchemaDiagnostics.keyspaceCreated(SchemaManager.instance.schema(), keyspace);
    }

    public void notifyKeyspaceAltered(KeyspaceMetadata.KeyspaceDiff delta)
    {
        // notify on everything dropped
        delta.udas.dropped.forEach(uda -> notifyDropAggregate((UDAggregate) uda));
        delta.udfs.dropped.forEach(udf -> notifyDropFunction((UDFunction) udf));
        delta.views.dropped.forEach(this::notifyDropView);
        delta.tables.dropped.forEach(this::notifyDropTable);
        delta.types.dropped.forEach(this::notifyDropType);

        // notify on everything created
        delta.types.created.forEach(this::notifyCreateType);
        delta.tables.created.forEach(this::notifyCreateTable);
        delta.views.created.forEach(this::notifyCreateView);
        delta.udfs.created.forEach(udf -> notifyCreateFunction((UDFunction) udf));
        delta.udas.created.forEach(uda -> notifyCreateAggregate((UDAggregate) uda));

        // notify on everything altered
        if (!delta.before.params.equals(delta.after.params))
            notifyAlterKeyspace(delta.before, delta.after);
        delta.types.altered.forEach(diff -> notifyAlterType(diff.before, diff.after));
        delta.tables.altered.forEach(diff -> notifyAlterTable(diff.before, diff.after));
        delta.views.altered.forEach(diff -> notifyAlterView(diff.before, diff.after));
        delta.udfs.altered.forEach(diff -> notifyAlterFunction(diff.before, diff.after));
        delta.udas.altered.forEach(diff -> notifyAlterAggregate(diff.before, diff.after));
        SchemaDiagnostics.keyspaceAltered(SchemaManager.instance.schema(), delta);
    }

    public void notifyKeyspaceDropped(KeyspaceMetadata keyspace)
    {
        keyspace.functions.udas().forEach(this::notifyDropAggregate);
        keyspace.functions.udfs().forEach(this::notifyDropFunction);
        keyspace.views.forEach(this::notifyDropView);
        keyspace.tables.forEach(this::notifyDropTable);
        keyspace.types.forEach(this::notifyDropType);
        notifyDropKeyspace(keyspace);
        SchemaDiagnostics.keyspaceDropped(SchemaManager.instance.schema(), keyspace);
    }

    private void notifyCreateKeyspace(KeyspaceMetadata ksm)
    {
        changeListeners.forEach(l -> l.onCreateKeyspace(ksm.name));
    }

    private void notifyCreateTable(TableMetadata metadata)
    {
        changeListeners.forEach(l -> l.onCreateTable(metadata.keyspace, metadata.name));
    }

    private void notifyCreateView(ViewMetadata view)
    {
        changeListeners.forEach(l -> l.onCreateView(view.keyspace(), view.name()));
    }

    private void notifyCreateType(UserType ut)
    {
        changeListeners.forEach(l -> l.onCreateType(ut.keyspace, ut.getNameAsString()));
    }

    private void notifyCreateFunction(UDFunction udf)
    {
        changeListeners.forEach(l -> l.onCreateFunction(udf.name().keyspace, udf.name().name, udf.argTypes()));
    }

    private void notifyCreateAggregate(UDAggregate udf)
    {
        changeListeners.forEach(l -> l.onCreateAggregate(udf.name().keyspace, udf.name().name, udf.argTypes()));
    }

    private void notifyAlterKeyspace(KeyspaceMetadata before, KeyspaceMetadata after)
    {
        changeListeners.forEach(l -> l.onAlterKeyspace(after.name));
    }

    private void notifyAlterTable(TableMetadata before, TableMetadata after)
    {
        boolean changeAffectedPreparedStatements = before.changeAffectsPreparedStatements(after);
        changeListeners.forEach(l -> l.onAlterTable(after.keyspace, after.name, changeAffectedPreparedStatements));
    }

    private void notifyAlterView(ViewMetadata before, ViewMetadata after)
    {
        boolean changeAffectedPreparedStatements = before.metadata.changeAffectsPreparedStatements(after.metadata);
        changeListeners.forEach(l -> l.onAlterView(after.keyspace(), after.name(), changeAffectedPreparedStatements));
    }

    private void notifyAlterType(UserType before, UserType after)
    {
        changeListeners.forEach(l -> l.onAlterType(after.keyspace, after.getNameAsString()));
    }

    private void notifyAlterFunction(UDFunction before, UDFunction after)
    {
        changeListeners.forEach(l -> l.onAlterFunction(after.name().keyspace, after.name().name, after.argTypes()));
    }

    private void notifyAlterAggregate(UDAggregate before, UDAggregate after)
    {
        changeListeners.forEach(l -> l.onAlterAggregate(after.name().keyspace, after.name().name, after.argTypes()));
    }

    private void notifyDropKeyspace(KeyspaceMetadata ksm)
    {
        changeListeners.forEach(l -> l.onDropKeyspace(ksm.name));
    }

    private void notifyDropTable(TableMetadata metadata)
    {
        changeListeners.forEach(l -> l.onDropTable(metadata.keyspace, metadata.name));
    }

    private void notifyDropView(ViewMetadata view)
    {
        changeListeners.forEach(l -> l.onDropView(view.keyspace(), view.name()));
    }

    private void notifyDropType(UserType ut)
    {
        changeListeners.forEach(l -> l.onDropType(ut.keyspace, ut.getNameAsString()));
    }

    private void notifyDropFunction(UDFunction udf)
    {
        changeListeners.forEach(l -> l.onDropFunction(udf.name().keyspace, udf.name().name, udf.argTypes()));
    }

    private void notifyDropAggregate(UDAggregate udf)
    {
        changeListeners.forEach(l -> l.onDropAggregate(udf.name().keyspace, udf.name().name, udf.argTypes()));
    }
}
