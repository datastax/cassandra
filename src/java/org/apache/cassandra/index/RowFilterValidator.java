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

package org.apache.cassandra.index;

import java.util.HashSet;
import java.util.Set;
import java.util.StringJoiner;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.sai.analyzer.AnalyzerEqOperatorSupport;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.ClientWarn;

/**
 * Class for validating the index-related aspects of a {@link RowFilter}, without considering what index is actually used.
 * </p>
 * It will emit a client warning when a query has EQ restrictions on columns having an analyzed index.
 */
class RowFilterValidator
{
    private final Iterable<Index> allIndexes;

    private Set<ColumnMetadata> columns;
    private Set<Index> indexes;

    private RowFilterValidator(Iterable<Index> allIndexes)
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

    private void validate()
    {
        if (columns == null || indexes == null)
            return;

        StringJoiner columnNames = new StringJoiner(", ");
        StringJoiner indexNames = new StringJoiner(", ");
        columns.forEach(column -> columnNames.add(column.name.toString()));
        indexes.forEach(index -> indexNames.add(index.getIndexMetadata().name));

        ClientWarn.instance.warn(String.format(AnalyzerEqOperatorSupport.EQ_RESTRICTION_ON_ANALYZED_WARNING, columnNames, indexNames));
    }

    /**
     * Emits a client warning if the filter contains EQ restrictions on columns having an analyzed index.
     *
     * @param filter the filter to validate
     * @param indexes the existing indexes
     */
    public static void validate(RowFilter filter, Iterable<Index> indexes)
    {
        RowFilterValidator validator = new RowFilterValidator(indexes);
        validate(filter.root(), validator);
        validator.validate();
    }

    private static void validate(RowFilter.FilterElement element, RowFilterValidator validator)
    {
        for (RowFilter.Expression expression : element.expressions())
        {
            if (expression.operator() == Operator.EQ)
                validator.addEqRestriction(expression.column());
        }

        for (RowFilter.FilterElement child : element.children())
        {
            validate(child, validator);
        }
    }
}
