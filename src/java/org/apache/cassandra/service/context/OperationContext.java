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

package org.apache.cassandra.service.context;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.OPERATION_CONTEXT_FACTORY;

/**
 * Represents some context about a "top-level" operation.
 * <p>
 * This interface is fairly open on purpose, as implementations for different operations could look fairly different.
 * But it is also open-ended as it is an extension point: the {@link #FACTORY} used to create the context instances
 * is configurable, and meant to allow extensions to add whatever information they need to the context.
 * <p>
 * Also note that what consistute a "top-level" operation is not strictly defined. At the time of this writing, those
 * context are not serialized across nodes, so "top-level" is understood as "for a node", and so correspond to
 * operations like "a `ReadCommand` execution on a replica".
 * <p>
 * The context of executing operation is tracked by {@link OperationContextTracker} which use the {@link ExecutorLocal}
 * concept to make that context available to any methods that execute as part of the operation. Basically, this is a way
 * to make the context available everwhere along the path of execution of the operation, without needing to pass that
 * context as argument of every single method that could be involved by the operation execution (which in most cases
 * would be <b>a lot of methods</b>).
*/
public interface OperationContext extends AutoCloseable
{
    Factory FACTORY = OPERATION_CONTEXT_FACTORY.getString() == null
                      ? new DefaultOperationContext.Factory()
                      : FBUtilities.construct(OPERATION_CONTEXT_FACTORY.getString(), "operation context factory");


    /**
     * Called when the operation this is a context of terminates, and thus when the context will not be used/retrieved
     * anymore.
     */
    @Override
    void close();

    /**
     * Factory used to create {@link OperationContext} instances.
     * <p>
     * The intent is that every operation that wants to set a context should have its own method in this interface, but
     * operations are added as needed (instead of trying to cover every possible operation upfront).
     * <p>
     * Do note however that there can only be one operation context "active" at any given time (meaning, any thread
     * execute can only see at most one context), so the context should be set at the higher level that make sense
     * (and if necessary, sub-operations can enrich the context of their parent, assuming the parent context make room
     * for this).
     */
    interface Factory
    {
        OperationContext forRead(ReadCommand command, ColumnFamilyStore cfs);
    }
}
