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

package org.apache.cassandra.index.sai.analyzer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.ClientWarn;

/**
 * Index config property for defining the behaviour of the equals operator (=) when the index is analyzed.
 * </p>
 * Analyzers transform the indexed value, so EQ queries using an analyzed index can return results different to those of
 * an equivalent query without indexes. Having EQ queries returning different results depending on if/how the column is
 * indexed can be confusing for users, so probably the safest approach is to reject EQ queries on analyzed indexes, and
 * let users use the analyzer matches operator (:) instead. However, for backwards compatibility reasons, we should
 * allow users to let equality queries behave same as match queries through this index config property. We use an enum
 * value rather than a boolean to allow for future extensions.
 */
public class AnalyzerEqOperatorSupport
{
    public static final String OPTION = "equals_behaviour_when_analyzed";
    public static final Value DEFAULT = Value.MATCH; // default to : behaviour for backwards compatibility

    @VisibleForTesting
    static final String NOT_ANALYZED_ERROR = "The behaviour of the equals operator (=) cannot be " +
                                             "defined with the '" + OPTION + "' index option because " +
                                             "the index is not analyzed.";

    @VisibleForTesting
    static final String WRONG_OPTION_ERROR = String.format("Invalid value for '%s' option. " +
                                                           "Possible values are %s but found ",
                                                           OPTION, Arrays.toString(Value.values()));

    public static final String EQ_RESTRICTION_ON_ANALYZED_WARNING =
    String.format("Columns [%%s] are restricted by '=' and have analyzed indexes [%%s] able to process those restrictions. " +
                  "Analyzed indexes might process '=' restrictions in a way that is inconsistent with non-indexed queries. " +
                  "While '=' is still supported on analyzed indexes for backwards compatibility, " +
                  "it is recommended to use the ':' operator instead to prevent the ambiguity. " +
                  "Future versions will remove support for '=' on analyzed indexes. " +
                  "If you want to forbid the use of '=' on analyzed indexes now, " +
                  "please use '%s':'%s' in the index options.",
                  OPTION, Value.UNSUPPORTED.toString().toLowerCase());

    public enum Value
    {
        /**
         * The index won't support equality (=) expressions on analyzed indexes.
         */
        UNSUPPORTED,
        /**
         * Allow equality (=) expressions on analyzed indexes. They will behave same as match queries (:).
         */
        MATCH
    }

    public static boolean supportsEqualsFromOptions(Map<String, String> options)
    {
        return fromMap(options) == Value.MATCH;
    }

    public static Value fromMap(Map<String, String> options)
    {
        if (options == null || !options.containsKey(OPTION))
            return DEFAULT;

        if (!AbstractAnalyzer.isAnalyzed(options))
            throw new InvalidRequestException(NOT_ANALYZED_ERROR);

        String option = options.get(OPTION).toUpperCase();
        try
        {
            return Value.valueOf(option);
        }
        catch (IllegalArgumentException e)
        {
            throw new InvalidRequestException(WRONG_OPTION_ERROR + option);
        }
    }

    /**
     * Emits a client warning if the filter contains EQ restrictions on columns having an analyzed index.
     *
     * @param filter the filter to check
     * @param indexes the existing indexes
     */
    public static void maybeWarn(RowFilter filter, Set<Index> indexes)
    {
        Warner warner = new Warner(indexes);
        maybeWarn(filter.root(), warner);
        warner.maybeWarn();
    }

    private static void maybeWarn(RowFilter.FilterElement element, Warner warner)
    {
        for (RowFilter.Expression expression : element.expressions())
        {
            if (expression.operator() == Operator.EQ)
                warner.addEqRestriction(expression.column());
        }

        for (RowFilter.FilterElement child : element.children())
        {
            maybeWarn(child, warner);
        }
    }

    /**
     * Class for emitting a client warning when a query has EQ restrictions on columns having an analyzed index.
     */
    private static class Warner
    {
        private final Set<Index> allIndexes;

        private Set<ColumnMetadata> columns;
        private Set<Index> indexes;

        private Warner(Set<Index> allIndexes)
        {
            this.allIndexes = allIndexes;
        }

        private void addEqRestriction(ColumnMetadata column)
        {
            for (Index index : allIndexes)
            {
                if (index.supportsExpression(column, Operator.EQ) &&
                    index.supportsExpression(column, Operator.ANALYZER_MATCHES))
                {
                    if (columns == null)
                        columns = new HashSet<>();
                    columns.add(column);

                    if (indexes == null)
                        indexes = new HashSet<>();
                    indexes.add(index);
                }
            }
        }

        private void maybeWarn()
        {
            if (columns == null || indexes == null)
                return;

            StringJoiner columnNames = new StringJoiner(", ");
            StringJoiner indexNames = new StringJoiner(", ");
            columns.forEach(column -> columnNames.add(column.name.toString()));
            indexes.forEach(index -> indexNames.add(index.getIndexMetadata().name));

            ClientWarn.instance.warn(String.format(EQ_RESTRICTION_ON_ANALYZED_WARNING, columnNames, indexNames));
        }
    }
}
