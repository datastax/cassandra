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

package org.apache.cassandra.db.monitoring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import static java.lang.Thread.sleep;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

public class SaiSlowestQueriesQueueTest
{
    @Test
    public void shouldMaintainMaxSize() throws InterruptedException, ExecutionException
    {
        // given
        int maxSize = 16;
        SaiSlowestQueriesQueue q = new SaiSlowestQueriesQueue(maxSize, 1);

        // when
        for (int duration = 0; duration < maxSize * 10; duration++)
        {
            q.addAsync(slowQuery(duration));
        }
        waitUntilNewQueriesAreProcessed(q);

        // then
        assertThat(q.getAndReset(), hasSize(maxSize));
    }

    @Test
    public void shouldEmptyOnGetAndReset() throws InterruptedException, ExecutionException
    {
        // given
        SaiSlowestQueriesQueue q = new SaiSlowestQueriesQueue(10, 1);

        for (int duration = 0; duration < 100; duration++)
        {
            q.addAsync(slowQuery(duration));
        }
        waitUntilNewQueriesAreProcessed(q);

        // when
        q.getAndReset();

        // then
        assertThat(q.getAndReset(), hasSize(0));
    }

    @Test
    public void shouldKeepSlowestQueries() throws InterruptedException, ExecutionException
    {
        // given
        int maxSize = 16;
        int maxDuration = 100;
        SaiSlowestQueriesQueue q = new SaiSlowestQueriesQueue(maxSize, 1);

        ArrayList<Integer> durations = IntStream.range(0, maxDuration)
                                                .boxed().collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(durations);

        // when
        durations.forEach(duration -> q.addAsync(slowQuery(duration)));
        waitUntilNewQueriesAreProcessed(q);

        // then
        List<SaiSlowLog.SlowSaiQuery> slowest = q.getAndReset();
        for (int duration = maxDuration - maxSize; duration < maxDuration; duration++)
        {
            assertThat(slowest, hasItem(queryWithDuration(duration)));
        }
    }

    @Test
    public void shouldRemoveFastestQueriesOnResize() throws InterruptedException, ExecutionException
    {
        // given
        int maxSize = 16;
        int resizedSize = 5;
        SaiSlowestQueriesQueue q = new SaiSlowestQueriesQueue(maxSize, 1);

        for (int duration = 0; duration < maxSize; duration++)
        {
            q.addAsync(slowQuery(duration));
        }
        waitUntilNewQueriesAreProcessed(q);

        // when
        q.resize(resizedSize);

        // then
        List<SaiSlowLog.SlowSaiQuery> slowest = q.getAndReset();
        for (int duration = maxSize - resizedSize; duration < maxSize; duration++)
        {
            assertThat(slowest, hasItem(queryWithDuration(duration)));
        }
    }

    @Test
    public void shouldPreserveExistingQueriesOnResizeToBigger() throws InterruptedException, ExecutionException
    {
        // given
        int maxSize = 16;
        SaiSlowestQueriesQueue q = new SaiSlowestQueriesQueue(maxSize, 1);

        for (int duration = 0; duration < maxSize; duration++)
        {
            q.addAsync(slowQuery(duration));
        }
        waitUntilNewQueriesAreProcessed(q);

        // when
        q.resize(25);

        // then
        List<SaiSlowLog.SlowSaiQuery> slowest = q.getAndReset();
        for (int duration = 0; duration < maxSize; duration++)
        {
            assertThat(slowest, hasItem(queryWithDuration(duration)));
        }
    }

    private void waitUntilNewQueriesAreProcessed(SaiSlowestQueriesQueue q) throws InterruptedException, ExecutionException
    {
        while (!q.bufferIsEmpty())
            sleep(1);
    }

    private SaiSlowLog.SlowSaiQuery slowQuery(int i)
    {
        return new SaiSlowLog.SlowSaiQuery(i,
                                           "optimizedPlan",
                                           "tracingSessionId");
    }

    private Matcher<SaiSlowLog.SlowSaiQuery> queryWithDuration(long duration)
    {
        return new QueryWithDuration(duration);
    }

    private static class QueryWithDuration extends TypeSafeMatcher<SaiSlowLog.SlowSaiQuery>
    {
        private final long duration;

        private QueryWithDuration(long duration)
        {
            this.duration = duration;
        }

        protected boolean matchesSafely(SaiSlowLog.SlowSaiQuery slowSaiQuery)
        {
            return slowSaiQuery.getDuration() == duration;
        }

        public void describeTo(Description description)
        {
            description.appendText("query with duration " + duration);
        }
    }
}
