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

package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.v1.TermsReader;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Range queries on a {@code text} column with an SAI index with few posting lists open at a time, see
 * {@link LiteralRangeRandomizedQueryTester}.
 */
public class LiteralRangeFewOpenPostingListsQueryTest extends LiteralRangeRandomizedQueryTester
{
    /**
     * Above a number of terms in the range only some posting lists of a segment are open, and the terms are walked
     * again when the query goes past them: same results with nearly all the ranges above that number.
     */
    @Test
    public void testRandomizedRangesWithFewOpenPostingLists()
    {
        int maxLazyPostingLists = TermsReader.getMaxLazyPostingLists();
        TermsReader.setMaxLazyPostingLists(64 + random.nextInt(64));
        try
        {
            randomizedRanges();
        }
        finally
        {
            TermsReader.setMaxLazyPostingLists(maxLazyPostingLists);
        }
    }

    /**
     * An intersection with another index advances the range past the posting lists that are open: the terms are
     * walked again from the target of each skip, with very few posting lists open.
     */
    @Test
    public void testIntersectionWithFewOpenPostingLists()
    {
        int maxLazyPostingLists = TermsReader.getMaxLazyPostingLists();
        TermsReader.setMaxLazyPostingLists(4);
        try
        {
            createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text, n int)");
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
            disableCompaction();

            List<Integer> expected = new ArrayList<>();
            for (int pk = 0; pk < 2000; pk++)
            {
                boolean matches = pk % 97 == 0;
                execute("INSERT INTO %s (pk, v, n) VALUES (?, ?, ?)", pk, String.format("v%05d", pk), matches ? 1 : 0);
                if (matches && pk >= 100)
                    expected.add(pk);
                if (pk == 999)
                    flush();
            }
            flush();

            // the optimizer would not intersect with an unselective range
            String query = "SELECT pk FROM %s WHERE v >= 'v00100' AND n = 1 WITH optimizer_options = {'query_optimization_level': 0}";
            TableQueryMetrics.PerQuery metrics = StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore())
                                                                          .queryMetrics().perQueryMetrics.get(TableQueryMetrics.QueryKind.ALL);
            for (int i = 0; i < 2; i++)
            {
                assertEquals(expected, keys(execute(query)));
                // the walks of the terms after the skips reopen posting lists: more than the 1900 terms of the range
                assertTrue(metrics.postingListsOpened.getSnapshot().getMax() > 1900);
                List<Integer> limited = keys(execute(query.replace(" WITH", " LIMIT 5 WITH")));
                assertEquals(5, limited.size());
                assertTrue(expected.containsAll(limited));
                compact();
            }
        }
        finally
        {
            TermsReader.setMaxLazyPostingLists(maxLazyPostingLists);
        }
    }
}
