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

package org.apache.cassandra.db.guardrails;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;

public class GuardrailSAIAnnRerankKTest extends ThresholdTester
{
    private static final int WARN_THRESHOLD = 50;
    private static final int FAIL_THRESHOLD = 100;

    private int defaultWarnThreshold = DatabaseDescriptor.getGuardrailsConfig().getSaiAnnRerankKWarnThreshold();
    private int defaultFailThreshold = DatabaseDescriptor.getGuardrailsConfig().getSaiAnnRerankKFailThreshold();
    private int defaultMaxTopK = CassandraRelevantProperties.SAI_VECTOR_SEARCH_MAX_TOP_K.getInt();

    public GuardrailSAIAnnRerankKTest()
    {
        super(WARN_THRESHOLD,
              FAIL_THRESHOLD,
              Guardrails.annRerankKMaxValue,
              Guardrails::setSaiAnnRerankKThreshold,
              Guardrails::getSaiAnnRerankKWarnThreshold,
              Guardrails::getSaiAnnRerankKFailThreshold);
    }

    @After
    public void after()
    {
        DatabaseDescriptor.getGuardrailsConfig().setSaiAnnRerankKThreshold(defaultWarnThreshold, defaultFailThreshold);
        CassandraRelevantProperties.SAI_VECTOR_SEARCH_MAX_TOP_K.setInt(defaultMaxTopK);
    }

    @Test
    public void testDefaultValues()
    {
        // Reset to defaults
        DatabaseDescriptor.getGuardrailsConfig().setSaiAnnRerankKThreshold(defaultWarnThreshold, defaultFailThreshold);

        // Test that default failure threshold is 4 times the max top K
        int maxTopK = CassandraRelevantProperties.SAI_VECTOR_SEARCH_MAX_TOP_K.getInt();
        assertEquals(-1, (int) DatabaseDescriptor.getGuardrailsConfig().getSaiAnnRerankKWarnThreshold());
        assertEquals(4 * maxTopK, (int) DatabaseDescriptor.getGuardrailsConfig().getSaiAnnRerankKFailThreshold());
    }

    @Test
    public void testSAIAnnRerankKThresholds() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // Test values below and at warning threshold
        assertValid("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': 10}");
        assertValid("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': " + (WARN_THRESHOLD - 1) + '}');
        assertValid("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': " + WARN_THRESHOLD + '}');

        // Test values between warning and failure thresholds
        assertWarns("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': " + (WARN_THRESHOLD + 1) + '}',
                    String.format("ANN options specifies rerank_k=%d, this exceeds the warning threshold of %d.",
                                  WARN_THRESHOLD + 1, WARN_THRESHOLD));

        // Test values at failure threshold (should still warn)
        assertWarns("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': " + FAIL_THRESHOLD + '}',
                    String.format("ANN options specifies rerank_k=%d, this exceeds the warning threshold of %d.",
                                FAIL_THRESHOLD, WARN_THRESHOLD));

        // Test values above failure threshold
        assertFails("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': " + (FAIL_THRESHOLD + 1) + '}',
                    String.format("ANN options specifies rerank_k=%d, this exceeds the failure threshold of %d.",
                                FAIL_THRESHOLD + 1, FAIL_THRESHOLD));
    }

    @Test
    public void testDisabledThresholds() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // Test with warning threshold disabled
        int failThreashold = DatabaseDescriptor.getGuardrailsConfig().getSaiAnnRerankKFailThreshold();
        DatabaseDescriptor.getGuardrailsConfig().setSaiAnnRerankKThreshold(-1, failThreashold);
        assertValid("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': " + (WARN_THRESHOLD + 1) + '}');

        // Test with failure threshold disabled
        DatabaseDescriptor.getGuardrailsConfig().setSaiAnnRerankKThreshold(-1, -1);
        assertValid("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': " + (FAIL_THRESHOLD + 1) + '}');
    }

    @Ignore // TODO: e-enable this test when we support negative rerank_k values
    public void testNegativeRerankK() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // Negative rerank_k values should be valid and not trigger warnings
        assertValid("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': -1}");
        assertValid("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': -1000}");
    }

    @Test
    public void testMissingRerankK() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // Queries without rerank_k should be valid and not trigger warnings.
        assertValid("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10");
        assertValid("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {}");
    }
}