/*
 * Copyright DataStax, Inc.
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
package org.apache.cassandra.cql3.statements;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.db.filter.ANNOptions;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;

/**
 * {@code WITH option1=... AND option2=...} options for SELECT statements.
 */
public class SelectOptions extends PropertyDefinitions
{
    public static final SelectOptions EMPTY = new SelectOptions();
    public static final String ANN_OPTIONS = "ann_options";
    public static final String INCLUDED_INDEXES = "included_indexes";
    public static final String EXCLUDED_INDEXES = "excluded_indexes";

    private static final Set<String> keywords = ImmutableSet.of(ANN_OPTIONS, INCLUDED_INDEXES, EXCLUDED_INDEXES);

    /**
     * Validates all the {@code SELECT} options.
     *
     * @param state the query state
     * @param limit the {@code SELECT} query user-provided limit
     * @param indexRegistry the index registry for the queried table
     * @param indexQueryPlan the index query plan for the query, if any
     * @throws InvalidRequestException if any of the options are invalid
     */
    public void validate(QueryState state,
                         TableMetadata table,
                         int limit,
                         IndexRegistry indexRegistry,
                         @Nullable Index.QueryPlan indexQueryPlan) throws RequestValidationException
    {
        validate(keywords, Collections.emptySet());
        parseANNOptions().validate(state, table.keyspace, limit);
        parseIndexHints(table, indexRegistry).validate(indexQueryPlan);
    }

    /**
     * Parse the ANN Options. Does not validate values of the options or whether peers will be able to process them.
     *
     * @return the ANN options within these options, or {@link ANNOptions#NONE} if no options are present
     * @throws InvalidRequestException if the ANN options are invalid
     */
    public ANNOptions parseANNOptions() throws RequestValidationException
    {
        Map<String, String> options = getMap(ANN_OPTIONS);

        return options == null
               ? ANNOptions.NONE
               : ANNOptions.fromMap(options);
    }

    /**
     * @return {@code true} if these options contain ANN options, {@code false} otherwise
     */
    public boolean hasANNOptions()
    {
        return properties.containsKey(ANN_OPTIONS);
    }

    /**
     * Parse the {@link IndexHints}, performing query-independent validation. Query-dependent validation should be done
     * later, when the query plan is built, by calling {@link IndexHints#validate(Index.QueryPlan)}.
     *
     * @return the parsed index hints, {@link IndexHints#NONE} if no hints are present, or they are empty
     * @throws InvalidRequestException if the index hints are invalid
     */
    public IndexHints parseIndexHints(TableMetadata table, IndexRegistry indexRegistry) throws RequestValidationException
    {
        Set<QualifiedName> included = getQualifiedNames(INCLUDED_INDEXES);
        Set<QualifiedName> excluded = getQualifiedNames(EXCLUDED_INDEXES);
        return IndexHints.fromCQLNames(included, excluded, table, indexRegistry);
    }
}
