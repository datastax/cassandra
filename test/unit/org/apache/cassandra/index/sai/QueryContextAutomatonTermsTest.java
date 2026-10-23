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
package org.apache.cassandra.index.sai;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.index.sai.utils.AbortedOperationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * The query context counts the terms visited by the automaton (non-prefix LIKE) scans of a query and checks the
 * query for a timeout every {@link QueryContext#AUTOMATON_CHECKPOINT_INTERVAL} visited terms, so that such scans are
 * bounded by the query timeout.
 */
public class QueryContextAutomatonTermsTest
{
    @Test
    public void countsVisitedTerms()
    {
        QueryContext context = new QueryContext(TimeUnit.HOURS.toMillis(1));
        assertEquals(0, context.automatonTermsVisited());
        for (int i = 0; i < 200; i++)
            context.onAutomatonTermVisited();
        assertEquals(200, context.automatonTermsVisited());
    }

    @Test
    public void checkpointsPeriodically()
    {
        // ignore the test property disabling SAI query timeouts, if set
        if (QueryContext.DISABLE_TIMEOUT)
            return;

        // a query whose quota has already elapsed is aborted at the next periodic timeout check, not at every term
        QueryContext expired = new QueryContext(0);
        for (int i = 0; i < QueryContext.AUTOMATON_CHECKPOINT_INTERVAL - 1; i++)
            expired.onAutomatonTermVisited();
        assertThrows(AbortedOperationException.class, expired::onAutomatonTermVisited);
        assertEquals(QueryContext.AUTOMATON_CHECKPOINT_INTERVAL, expired.automatonTermsVisited());
    }
}
