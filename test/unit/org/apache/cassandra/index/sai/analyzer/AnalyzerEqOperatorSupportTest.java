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

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.service.ClientWarn;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;

import static java.lang.String.format;
import static org.apache.cassandra.index.sai.analyzer.AnalyzerEqOperatorSupport.EQ_RESTRICTION_ON_ANALYZED_WARNING;

/**
 * Tests for {@link AnalyzerEqOperatorSupport}.
 */
public class AnalyzerEqOperatorSupportTest extends SAITester
{
    @Before
    public void createTable()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
    }

    private void populateTable()
    {
        execute("INSERT INTO %s (k, v) VALUES (1, 'Quick fox')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'Lazy fox')");
    }

    @Test
    public void testWithoutAnyIndex()
    {
        populateTable();

        // equals (=)
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'Quick fox' ALLOW FILTERING", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'quick fox' ALLOW FILTERING");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'fox' ALLOW FILTERING");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'Quick fox' OR v = 'Lazy fox' ALLOW FILTERING", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'quick fox' OR v = 'lazy fox' ALLOW FILTERING");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'Quick' OR v = 'Lazy' ALLOW FILTERING");
        assertInvalidMessage("v cannot be restricted by more than one relation if it includes an Equal",
                             "SELECT k FROM %s WHERE v = 'Quick' AND v = 'fox' ALLOW FILTERING");

        // matches (:)
        assertInvalidMessage(": restriction is only supported on properly indexed columns. v : 'Quick fox' is not valid.",
                             "SELECT k FROM %s WHERE v : 'Quick fox' ALLOW FILTERING");
    }

    @Test
    public void testWithLegacyIndex()
    {
        populateTable();

        createIndex("CREATE INDEX ON %s(v)");
        waitForIndexQueryable();

        // equals (=)
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'Quick fox'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'quick fox'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'fox'");
        assertInvalidMessage("v cannot be restricted by more than one relation if it includes an Equal",
                             "SELECT k FROM %s WHERE v = 'Quick' AND v = 'fox'");
        assertInvalidMessage(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_DISJUNCTION,
                             "SELECT k FROM %s WHERE v = 'Quick fox' OR v = 'Lazy fox'");

        // matches (:)
        assertInvalidMessage(format(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_ANALYZER_MATCHES_MESSAGE, 'v'),
                             "SELECT k FROM %s WHERE v : 'Quick fox'");
    }

    @Test
    public void testNonAnalyzedIndexWithDefaults()
    {
        assertIndexQueries("{}", () -> {

            // equals (=)
            assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'Quick fox'", row(1));
            assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'quick fox'");
            assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'fox'");
            assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'Quick fox' OR v = 'Lazy fox'", row(1), row(2));
            assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'quick fox' OR v = 'lazy fox'");
            assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'Quick' OR v = 'Lazy'");
            assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'quick' OR v = 'lazy'");
            assertInvalidMessage("v cannot be restricted by more than one relation if it includes an Equal",
                                 "SELECT k FROM %s WHERE v = 'Quick' AND v = 'fox'");

            // matches (:)
            assertIndexDoesNotSupportMatches();
        });
    }

    @Test
    public void testNonAnalyzedIndexWithMatch()
    {
        assertIndexThrowsNotAnalyzedError("{'equals_behaviour_when_analyzed': 'MATCH'}");
    }

    @Test
    public void testNonAnalyzedIndexWithUnsupported()
    {
        assertIndexThrowsNotAnalyzedError("{'equals_behaviour_when_analyzed': 'UNSUPPORTED'}");
    }

    @Test
    public void testNonAnalyzedIndexWithWrongValue()
    {
        assertIndexThrowsNotAnalyzedError("{'equals_behaviour_when_analyzed': 'WRONG'}");
    }

    @Test
    public void testNonTokenizedIndexWithDefaults()
    {
        assertIndexQueries("{'case_sensitive': 'false'}", () -> {
            assertNonTokenizedIndexSupportsEquality();
            assertNonTokenizedIndexSupportsMatches();
            assertNonTokenizedIndexSupportsMixedEqualityAndMatches();
        });
    }

    @Test
    public void testNonTokenizedIndexWithMatch()
    {
        assertIndexQueries("{'case_sensitive': 'false', 'equals_behaviour_when_analyzed': 'MATCH'}", () -> {
            assertNonTokenizedIndexSupportsEquality();
            assertNonTokenizedIndexSupportsMatches();
            assertNonTokenizedIndexSupportsMixedEqualityAndMatches();
        });
    }

    @Test
    public void testNonTokenizedIndexWithUnsupported()
    {
        assertIndexQueries("{'case_sensitive': 'false', 'equals_behaviour_when_analyzed': 'UNSUPPORTED'}", () -> {
            assertIndexDoesNotSupportEquals();
            assertNonTokenizedIndexSupportsMatches();
        });
    }

    @Test
    public void testNonTokenizedIndexWithWrongValue()
    {
        assertIndexThrowsUnrecognizedOptionError("{'case_sensitive': 'false', 'equals_behaviour_when_analyzed': 'WRONG'}");
    }

    private void assertNonTokenizedIndexSupportsEquality()
    {
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick fox'", row(1));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick fox'", row(1));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'fox'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick fox' OR v = 'Lazy fox'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick fox' OR v = 'lazy fox'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick' OR v = 'Lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick' OR v = 'lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick' AND v = 'fox'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick' AND v = 'fox'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick' AND v = 'Lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick' AND v = 'lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE (v = 'quick' AND v = 'fox') OR v = 'dog'");
        assertRowsWithWarning("SELECT k FROM %s WHERE (v = 'quick' AND v = 'fox') OR v = 'Lazy'");
    }

    private void assertNonTokenizedIndexSupportsMatches()
    {
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick fox'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick fox'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'fox'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick fox' OR v : 'Lazy fox'", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick fox' OR v : 'lazy fox'", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick' OR v : 'Lazy'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick' OR v : 'lazy'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick' AND v : 'fox'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick' AND v : 'fox'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick' AND v : 'Lazy'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick' AND v : 'lazy'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE (v : 'quick' AND v : 'fox') OR v : 'dog'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE (v : 'quick' AND v : 'fox') OR v : 'Lazy'");
    }

    private void assertNonTokenizedIndexSupportsMixedEqualityAndMatches()
    {
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick fox' OR v : 'Lazy fox'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick fox' OR v : 'lazy fox'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick' OR v : 'Lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick' OR v : 'lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick' AND v : 'fox'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick' AND v : 'fox'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick' AND v : 'Lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick' AND v : 'lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE (v = 'quick' AND v : 'fox') OR v = 'dog'");
        assertRowsWithWarning("SELECT k FROM %s WHERE (v = 'quick' AND v : 'fox') OR v = 'Lazy'");

        assertRowsWithWarning("SELECT k FROM %s WHERE v : 'Quick fox' OR v = 'Lazy fox'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v : 'quick fox' OR v = 'lazy fox'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v : 'Quick' OR v = 'Lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v : 'quick' OR v = 'lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v : 'Quick' AND v = 'fox'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v : 'quick' AND v = 'fox'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v : 'Quick' AND v = 'Lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v : 'quick' AND v = 'lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE (v : 'quick' AND v = 'fox') OR v : 'dog'");
        assertRowsWithWarning("SELECT k FROM %s WHERE (v : 'quick' AND v = 'fox') OR v : 'Lazy'");
    }

    @Test
    public void testTokenizedIndexWithDefaults()
    {
        assertIndexQueries("{'index_analyzer': 'standard'}", () -> {
            assertTokenizedIndexSupportsEquality();
            assertTokenizedIndexSupportsMatches();
            assertTokenizedIndexSupportsMixedEqualityAndMatches();
        });
    }

    @Test
    public void testTokenizedIndexWithMatch()
    {
        assertIndexQueries("{'index_analyzer': 'standard', 'equals_behaviour_when_analyzed': 'MATCH'}", () -> {
            assertTokenizedIndexSupportsEquality();
            assertTokenizedIndexSupportsMatches();
            assertTokenizedIndexSupportsMixedEqualityAndMatches();
        });
    }

    @Test
    public void testTokenizedIndexWithUnsupported()
    {
        assertIndexQueries("{'index_analyzer': 'standard', 'equals_behaviour_when_analyzed': 'UNSUPPORTED'}", () -> {
            assertIndexDoesNotSupportEquals();
            assertTokenizedIndexSupportsMatches();
        });
    }

    @Test
    public void testTokenizedIndexWithWrongValue()
    {
        assertIndexThrowsUnrecognizedOptionError("{'index_analyzer': 'standard', 'equals_behaviour_when_analyzed': 'WRONG'}");
    }

    private void assertTokenizedIndexSupportsEquality()
    {
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick fox'", row(1));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick fox'", row(1));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick'", row(1));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'fox'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick fox' OR v = 'Lazy fox'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick fox' OR v = 'lazy fox'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick' OR v = 'Lazy'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick' OR v = 'lazy'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick' AND v = 'fox'", row(1));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick' AND v = 'fox'", row(1));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick' AND v = 'Lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick' AND v = 'lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE (v = 'quick' AND v = 'fox') OR v = 'dog'", row(1));
        assertRowsWithWarning("SELECT k FROM %s WHERE (v = 'quick' AND v = 'fox') OR v = 'Lazy'", row(1), row(2));
    }

    private void assertTokenizedIndexSupportsMatches()
    {
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick fox'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick fox'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'fox'", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick fox' OR v : 'Lazy fox'", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick fox' OR v : 'lazy fox'", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick' OR v : 'Lazy'", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick' OR v : 'lazy'", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick' AND v : 'fox'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick' AND v : 'fox'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick' AND v : 'Lazy'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick' AND v : 'lazy'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE (v : 'quick' AND v : 'fox') OR v : 'dog'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE (v : 'quick' AND v : 'fox') OR v : 'Lazy'", row(1), row(2));
    }

    private void assertTokenizedIndexSupportsMixedEqualityAndMatches()
    {
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick fox' OR v : 'Lazy fox'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick fox' OR v : 'lazy fox'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick' OR v : 'Lazy'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick' OR v : 'lazy'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick' AND v : 'fox'", row(1));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick' AND v : 'fox'", row(1));
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'Quick' AND v : 'Lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v = 'quick' AND v : 'lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE (v = 'quick' AND v : 'fox') OR v = 'dog'", row(1));
        assertRowsWithWarning("SELECT k FROM %s WHERE (v = 'quick' AND v : 'fox') OR v = 'Lazy'", row(1), row(2));

        assertRowsWithWarning("SELECT k FROM %s WHERE v : 'Quick fox' OR v = 'Lazy fox'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v : 'quick fox' OR v = 'lazy fox'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v : 'Quick' OR v = 'Lazy'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v : 'quick' OR v = 'lazy'", row(1), row(2));
        assertRowsWithWarning("SELECT k FROM %s WHERE v : 'Quick' AND v = 'fox'", row(1));
        assertRowsWithWarning("SELECT k FROM %s WHERE v : 'quick' AND v = 'fox'", row(1));
        assertRowsWithWarning("SELECT k FROM %s WHERE v : 'Quick' AND v = 'Lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE v : 'quick' AND v = 'lazy'");
        assertRowsWithWarning("SELECT k FROM %s WHERE (v : 'quick' AND v = 'fox') OR v : 'dog'", row(1));
        assertRowsWithWarning("SELECT k FROM %s WHERE (v : 'quick' AND v = 'fox') OR v : 'Lazy'", row(1), row(2));
    }

    private void assertIndexDoesNotSupportEquals()
    {
        // the EQ query should not be supported by the index
        String query = "SELECT k FROM %s WHERE v = 'Quick fox'";
        assertInvalidMessage("Column 'v' has an index but does not support the operators specified in the query", query);

        // the EQ query should stil be supported with filtering without index intervention
        assertRowsWithoutWarning(query + " ALLOW FILTERING", row(1));

        // the EQ query should not use any kind of transformation when filtered without index intervention
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'quick fox' ALLOW FILTERING");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'fox' ALLOW FILTERING");
    }

    private void assertIndexDoesNotSupportMatches()
    {
        String query = "SELECT k FROM %s WHERE v : 'Quick fox'";
        String errorMessage = "Index on column v does not support ':' restrictions.";
        assertInvalidMessage(errorMessage, query);
        assertInvalidMessage(errorMessage, query + " ALLOW FILTERING");
    }

    private void assertIndexThrowsNotAnalyzedError(String indexOptions)
    {
        assertInvalidMessage(AnalyzerEqOperatorSupport.NOT_ANALYZED_ERROR,
                             "CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS =" + indexOptions);
    }

    private void assertIndexThrowsUnrecognizedOptionError(String indexOptions)
    {
        assertInvalidMessage(AnalyzerEqOperatorSupport.WRONG_OPTION_ERROR,
                             "CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS =" + indexOptions);
    }

    private void assertIndexQueries(String indexOptions, Runnable queries)
    {
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = " + indexOptions);
        waitForIndexQueryable();
        populateTable();

        queries.run();
        flush();
        queries.run();
    }

    private void assertRowsWithoutWarning(String query, Object[]... rows)
    {
        assertRows(query, rows).isNullOrEmpty();
    }

    private void assertRowsWithWarning(String query, Object[]... rows)
    {
        assertRows(query, rows).hasSize(1).contains(format(EQ_RESTRICTION_ON_ANALYZED_WARNING, 'v', currentIndex()));
    }

    private ListAssert<String> assertRows(String query, Object[]... rows)
    {
        ClientWarn.instance.captureWarnings();
        CQLTester.disablePreparedReuseForTest();
        assertRows(execute(query), rows);
        ListAssert<String> assertion = Assertions.assertThat(ClientWarn.instance.getWarnings());
        ClientWarn.instance.resetWarnings();
        return assertion;
    }
}
