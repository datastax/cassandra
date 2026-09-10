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

import org.junit.Test;

import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.plan.Plan;

public class VectorPlaningTest extends VectorTester
{
    @Test
    public void testPlaningOnHybridQueries()
    {
        createTable("CREATE TABLE %s (k int, c int, v vector<float, 2>, n int, PRIMARY KEY(k, c))");
        String vectorIndex = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        String numericIndex = createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 0, [0, 0], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 1, [0, 1], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 2, [0, 2], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 3, [0, 3], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 4, [0, 4], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 5, [0, 5], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 6, [0, 6], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 7, [0, 7], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 8, [0, 8], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 9, [0, 9], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 10, [0, 10], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 11, [0, 11], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 12, [0, 12], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 13, [0, 13], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 14, [0, 14], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 15, [0, 15], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 16, [0, 16], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 17, [0, 17], 1)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 18, [0, 18], 0)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (0, 19, [0, 19], 0)");

        // hybrid query that prefers the filtering index due to selectivity
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5",
                              Plan.NumericIndexScan.class,
                              row(18), row(19));
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 WITH included_indexes = {" + numericIndex + '}',
                              Plan.NumericIndexScan.class,
                              row(18), row(19));
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 WITH included_indexes = {" + vectorIndex + '}',
                              Plan.AnnIndexScan.class,
                              row(18), row(19));
        assertInvalidMessage(IndexContext.MULTIPLE_HINTS_WITH_ORDER_BY_ERRROR_MESSAGE,
                             "SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 WITH included_indexes = {" + numericIndex + ',' + vectorIndex + '}');
        assertInvalidMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE,
                             "SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 WITH excluded_indexes = {" + numericIndex + '}');
        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, 'v'),
                             "SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 WITH excluded_indexes = {" + vectorIndex + '}');
        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, 'v'),
                             "SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 WITH excluded_indexes = {" + numericIndex + ',' + vectorIndex + '}');

        // hybrid query that prefers the ordering index due to selectivity
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 1 ORDER BY v ANN OF [0, 0] LIMIT 5",
                              Plan.AnnIndexScan.class,
                              row(0), row(1), row(2), row(3), row(4));
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 1 ORDER BY v ANN OF [0, 0] LIMIT 5 WITH included_indexes = {" + numericIndex + '}',
                              Plan.NumericIndexScan.class,
                              row(0), row(1), row(2), row(3), row(4));
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 1 ORDER BY v ANN OF [0, 0] LIMIT 5 WITH included_indexes = {" + vectorIndex + '}',
                              Plan.AnnIndexScan.class,
                              row(0), row(1), row(2), row(3), row(4));
        assertInvalidMessage(IndexContext.MULTIPLE_HINTS_WITH_ORDER_BY_ERRROR_MESSAGE,
                             "SELECT c FROM %s WHERE n = 1 ORDER BY v ANN OF [0, 0] LIMIT 5 WITH included_indexes = {" + numericIndex + ',' + vectorIndex + '}');
        assertInvalidMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE,
                             "SELECT c FROM %s WHERE n = 1 ORDER BY v ANN OF [0, 0] WITH excluded_indexes = {" + numericIndex + '}');
        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, 'v'),
                             "SELECT c FROM %s WHERE n = 1 ORDER BY v ANN OF [0, 0] LIMIT 5 WITH excluded_indexes = {" + vectorIndex + '}');
        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, 'v'),
                             "SELECT c FROM %s WHERE n = 1 ORDER BY v ANN OF [0, 0] LIMIT 5 WITH excluded_indexes = {" + numericIndex + ',' + vectorIndex + '}');
    }
}
