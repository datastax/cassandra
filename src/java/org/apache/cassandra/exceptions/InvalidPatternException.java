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

package org.apache.cassandra.exceptions;

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.Operator;

/**
 * Thrown when a {@code LIKE} pattern can't be compiled into an automaton (see
 * {@link org.apache.cassandra.cql3.LikePatterns#forPatternOperator}): it is syntactically invalid or too complex to
 * determinize.
 * <p>
 * Coordinators compile the patterns of {@code LIKE} restrictions when the restriction is bound, so the client gets
 * this {@link InvalidRequestException} before the query reaches the replicas. The operator evaluating the pattern
 * ({@link Operator#isSatisfiedBy}) doesn't know the restricted column, which its caller adds with
 * {@link #withColumn}.
 */
public final class InvalidPatternException extends InvalidRequestException
{
    private static final String MESSAGE = "Invalid pattern for %s restriction: %s";
    private static final String MESSAGE_WITH_COLUMN = "Invalid pattern for %s restriction on column %s: %s";

    private final Operator operator;
    private final String reason;

    /**
     * @param operator the pattern operator
     * @param column the restricted column, or {@code null} if it isn't known
     * @param reason why the pattern can't be compiled
     */
    public InvalidPatternException(Operator operator, @Nullable Object column, String reason)
    {
        super(column == null ? String.format(MESSAGE, operator, reason)
                             : String.format(MESSAGE_WITH_COLUMN, operator, column, reason));
        this.operator = operator;
        this.reason = reason;
    }

    /**
     * @return a copy of this exception naming the given restricted column
     */
    public InvalidPatternException withColumn(Object column)
    {
        return new InvalidPatternException(operator, column, reason);
    }
}
