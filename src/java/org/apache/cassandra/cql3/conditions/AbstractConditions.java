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
package org.apache.cassandra.cql3.conditions;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.cql3.functions.Function;

/**
 * Base class for <code>Conditions</code> classes.
 *
 */
abstract class AbstractConditions implements Conditions
{
    public void addFunctionsTo(List<Function> functions)
    {
    }

    @Override
    public Iterable<ColumnMetadata> getColumns()
    {
        return null;
    }

    @Override
    public Set<ColumnMetadata> getAnalyzedColumns(IndexRegistry indexRegistry, IndexHints indexHints)
    {
        return Collections.emptySet();
    }

    public boolean isEmpty()
    {
        return false;
    }

    public boolean appliesToStaticColumns()
    {
        return false;
    }

    public boolean appliesToRegularColumns()
    {
        return false;
    }

    public boolean isIfExists()
    {
        return false;
    }

    public boolean isIfNotExists()
    {
        return false;
    }
}
