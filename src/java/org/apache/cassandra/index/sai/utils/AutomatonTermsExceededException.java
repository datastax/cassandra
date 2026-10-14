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

package org.apache.cassandra.index.sai.utils;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;

/**
 * Thrown when an automaton intersection with a SAI terms dictionary (see {@code TermsReader#intersect} and
 * {@code TrieMemoryIndex#automatonMatch}) exhausts the caller-supplied visited-terms budget
 * ({@link AutomatonQueries.ExpansionsBudget}). On the query path the budget is shared by all the sstable segments
 * and memtable shards a read command visits on a replica ({@code QueryContext#automatonExpansionsBudget()}), so
 * the cap bounds the work done by a pattern-matching read; the read then fails (there is no fallback).
 * <p>
 * As an {@link InternalRequestExecutionException}, this failure is reported to the coordinator with
 * {@link RequestFailureReason#SAI_AUTOMATON_EXPANSIONS_EXCEEDED}, following the
 * {@code FeatureNeedsIndexRebuildException} precedent. The failure protocol only carries the reason code: the client
 * message names the reason and suggests a more selective pattern (see {@link RequestFailureReason#hint()}), and the
 * detailed message with the restricted column is logged (rate-limited) by the replica.
 */
public final class AutomatonTermsExceededException extends RuntimeException implements InternalRequestExecutionException
{
    private final int maxVisitedTerms;

    public AutomatonTermsExceededException(int maxVisitedTerms)
    {
        super(String.format("Automaton query visited more than the maximum allowed %d terms", maxVisitedTerms));
        this.maxVisitedTerms = maxVisitedTerms;
    }

    /**
     * @param message a message with full query context (see {@code AutomatonQueries#EXPANSIONS_EXCEEDED_MESSAGE}),
     *                used when re-throwing the raw dictionary-level failure with column/operator information
     */
    public AutomatonTermsExceededException(String message, int maxVisitedTerms)
    {
        super(message);
        this.maxVisitedTerms = maxVisitedTerms;
    }

    public int maxVisitedTerms()
    {
        return maxVisitedTerms;
    }

    /**
     * @return a copy of this exception whose message names the restriction whose pattern exhausted the budget (see
     * {@link AutomatonQueries#EXPANSIONS_EXCEEDED_MESSAGE})
     */
    public AutomatonTermsExceededException forRestriction(Operator patternOperator, Object column)
    {
        return new AutomatonTermsExceededException(String.format(AutomatonQueries.EXPANSIONS_EXCEEDED_MESSAGE,
                                                                 patternOperator,
                                                                 column,
                                                                 maxVisitedTerms),
                                                   maxVisitedTerms);
    }

    @Override
    public RequestFailureReason getReason()
    {
        return RequestFailureReason.SAI_AUTOMATON_EXPANSIONS_EXCEEDED;
    }
}
