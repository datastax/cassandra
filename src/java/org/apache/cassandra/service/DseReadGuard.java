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

package org.apache.cassandra.service;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.filter.OptimizerOptions;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.DseCompatRefusals;

/**
 * The never-send guard of the reads in DSE 6 compatibility mode: a read that only this software can serve is refused
 * at the coordinator, because a DSE 6 replica cannot read it at the pre-4.0 dialect. That dialect has the operator
 * codes of Cassandra 3.0.14 (none above 14, {@code LIKE}), its row-filter expression kinds (none from 100, and kind 1
 * is an equality on a map entry) and its read-command layout (no disjunction, no index hints); the row filter's SAI
 * optimizer options ({@code WITH optimizer_options}), which this software sends only from {@code VERSION_DS_20}, are
 * not in that layout either. Their own check, {@code OptimizerOptions.validate}, refuses them earlier, uncounted, when
 * this node has a connection pool, established or not, below that version to a node of the keyspace's ring; the guard
 * covers a DSE 6 peer with no pool yet, and every such read once no DSE 6 peer is left while the mode is still on. Two
 * more are refused because DSE has no way to evaluate them: a user-defined expression names a class registered on
 * this node only, and GROUP BY with a selector function is an aggregation kind DSE 6 does not define. The refusal
 * happens before any replica is chosen, whichever replicas would have been selected: an application is not expected to
 * adopt new features during an upgrade, and a silent replica exclusion would hide the mistake. Every refusal is
 * counted in {@code nodetool compatstatus}.
 * <p>
 * Multi-range reads are not checked here: they are never built in the mode ({@code RangeCommandIterator} queries
 * every range on its own), and their verb is refused on the way to a DSE 6 peer like any other verb the dialect lacks.
 * <p>
 * Kept apart from {@link DseCompat}, which the messaging code loads, so that messaging does not initialise the CQL and
 * read classes.
 */
public final class DseReadGuard
{
    /** The highest {@link Operator} code Cassandra 3.0.14 defines ({@code LIKE}); DSE refuses anything above it at the dialect. */
    public static final int MAX_PRE40_OPERATOR_CODE = Operator.LIKE.getValue();

    /** The first row-filter expression kind that Cassandra 3.0 does not define: the DS-only kinds start at 100. */
    public static final int FIRST_DS_ONLY_EXPRESSION_KIND = 100;

    /** The row-filter expression kind of a map-entry comparison: an equality only in Cassandra 3.0 and DSE. */
    public static final int MAP_ENTRY_KIND = 1;

    private DseReadGuard()
    {
    }

    /**
     * Refuses, in DSE 6 compatibility mode, a read that a DSE 6 replica cannot parse; a no-op when the mode is off.
     *
     * @throws InvalidRequestException when the mode is on and the command uses a feature the dialect cannot carry
     */
    public static void checkReadCommand(ReadCommand command)
    {
        if (!DseCompat.enabled())
            return;
        String feature = unsupportedReadFeature(command);
        if (feature != null)
        {
            DseCompatRefusals.refused("read with " + feature);
            throw new InvalidRequestException("DSE 6 compatibility mode: " + feature + " is not available while the upgrade from DSE 6 is in progress; "
                                              + "run the query once internode_compatibility_mode is NONE on every node");
        }
    }

    /** {@link #checkReadCommand} for each command of a read; a no-op, with no iteration, when the mode is off. */
    public static void checkReadCommands(List<? extends ReadCommand> commands)
    {
        if (!DseCompat.enabled())
            return;
        for (ReadCommand command : commands)
            checkReadCommand(command);
    }

    /** The first DS-only read feature the command uses, or null when the dialect can carry it. */
    @VisibleForTesting
    static String unsupportedReadFeature(ReadCommand command)
    {
        // A filter that needs reconciliation (every filtered read above CL ONE) is not refused: the flag is left out at
        // the dialect (ReadCommand.Serializer) and the DSE replica filters strictly, as before CASSANDRA-19018.
        RowFilter filter = command.rowFilter();
        if (filter.indexHints != null && !(filter.indexHints.included.isEmpty() && filter.indexHints.excluded.isEmpty()))
            return "an index hint";
        if (filter.optimizerOptions != null && filter.optimizerOptions != OptimizerOptions.NONE)
            return "an SAI optimizer option";
        if (hasDisjunction(filter.root()))
            return "a disjunction (OR) in the WHERE clause";
        // the 3.0 layout has a flat list of expressions only
        if (!filter.root().children().isEmpty())
            return "a nested group of conditions in the WHERE clause";
        for (RowFilter.Expression expression : filter.expressions())
        {
            if (expression.kindCode() >= FIRST_DS_ONLY_EXPRESSION_KIND)
                return "a row-filter expression of kind " + expression.kindCode();
            if (expression.isUserDefined())
                return "a user-defined row-filter expression";
            // kind 1 is MAP_EQUALITY in Cassandra 3.0 and DSE: a map entry compared with anything but = is not
            if (expression.kindCode() == MAP_ENTRY_KIND && expression.operator() != Operator.EQ)
                return "a map entry compared with " + expression.operator();
            if (expression.operator() != null && expression.operator().getValue() > MAX_PRE40_OPERATOR_CODE)
                return "the " + expression.operator() + " operator";
        }

        AggregationSpecification groupBy = command.limits().groupBySpecification();
        if (groupBy != null && groupBy.kind() == AggregationSpecification.Kind.AGGREGATE_BY_PK_PREFIX_WITH_SELECTOR)
            return "GROUP BY with a selector function";
        return null;
    }

    private static boolean hasDisjunction(RowFilter.FilterElement element)
    {
        if (element.isDisjunction())
            return true;
        for (RowFilter.FilterElement child : element.children())
            if (hasDisjunction(child))
                return true;
        return false;
    }
}
