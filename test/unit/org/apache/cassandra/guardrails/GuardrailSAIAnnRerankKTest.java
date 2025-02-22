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

package org.apache.cassandra.guardrails;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;

public class GuardrailSAIAnnRerankKTest extends GuardrailTester
{
    private static final int WARN_THRESHOLD = 50;
    private static final int FAIL_THRESHOLD = 100;

    private int defaultWarnThreshold;
    private int defaultFailThreshold;
    private int defaultMaxTopK;

    @Before
    public void before()
    {
        defaultWarnThreshold = DatabaseDescriptor.getGuardrailsConfig().sai_ann_rerank_k_warn_threshold;
        defaultFailThreshold = DatabaseDescriptor.getGuardrailsConfig().sai_ann_rerank_k_fail_threshold;
        defaultMaxTopK = CassandraRelevantProperties.SAI_VECTOR_SEARCH_MAX_TOP_K.getInt();

        DatabaseDescriptor.getGuardrailsConfig().sai_ann_rerank_k_warn_threshold = WARN_THRESHOLD;
        DatabaseDescriptor.getGuardrailsConfig().sai_ann_rerank_k_fail_threshold = FAIL_THRESHOLD;
    }

    @After
    public void after()
    {
        DatabaseDescriptor.getGuardrailsConfig().sai_ann_rerank_k_warn_threshold = defaultWarnThreshold;
        DatabaseDescriptor.getGuardrailsConfig().sai_ann_rerank_k_fail_threshold = defaultFailThreshold;
        CassandraRelevantProperties.SAI_VECTOR_SEARCH_MAX_TOP_K.setInt(defaultMaxTopK);
    }

    @Test
    public void testConfigValidation()
    {
        DatabaseDescriptor.getGuardrailsConfig().sai_ann_rerank_k_fail_threshold = -1;
        testValidationOfStrictlyPositiveProperty((c, v) -> c.sai_ann_rerank_k_warn_threshold = v.intValue(),
                                               "sai_ann_rerank_k_warn_threshold");

        DatabaseDescriptor.getGuardrailsConfig().sai_ann_rerank_k_warn_threshold = -1;
        testValidationOfStrictlyPositiveProperty((c, v) -> c.sai_ann_rerank_k_fail_threshold = v.intValue(),
                                               "sai_ann_rerank_k_fail_threshold");
    }

    @Test
    public void testDefaultValues()
    {
        // Reset to defaults
        DatabaseDescriptor.getGuardrailsConfig().sai_ann_rerank_k_warn_threshold = defaultWarnThreshold;
        DatabaseDescriptor.getGuardrailsConfig().sai_ann_rerank_k_fail_threshold = defaultFailThreshold;

        // Test that default failure threshold is 4 times the max top K
        int maxTopK = CassandraRelevantProperties.SAI_VECTOR_SEARCH_MAX_TOP_K.getInt();
        assertEquals(-1, (int) DatabaseDescriptor.getGuardrailsConfig().sai_ann_rerank_k_warn_threshold);
        assertEquals(4 * maxTopK, (int) DatabaseDescriptor.getGuardrailsConfig().sai_ann_rerank_k_fail_threshold);
    }

    @Test
    public void testSAIAnnRerankKThresholds() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // Test values below and at warning threshold
        assertValid("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': 10}");
        assertValid("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': " + (WARN_THRESHOLD - 1) + '}');
        assertValid("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': " + (WARN_THRESHOLD) + '}');

        // Test values between warning and failure thresholds
        assertWarns(String.format("ANN options specifies rerank_k=%d, this exceeds the warning threshold of %d.",
                                WARN_THRESHOLD + 1, WARN_THRESHOLD),
                   "SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': " + (WARN_THRESHOLD + 1) + '}');

        // Test values at failure threshold (should still warn)
        assertWarns(String.format("ANN options specifies rerank_k=%d, this exceeds the warning threshold of %d.",
                                FAIL_THRESHOLD, WARN_THRESHOLD),
                   "SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': " + FAIL_THRESHOLD + '}');

        // Test values above failure threshold
        assertFails(String.format("ANN options specifies rerank_k=%d, this exceeds the failure threshold of %d.",
                                FAIL_THRESHOLD + 1, FAIL_THRESHOLD),
                   "SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': " + (FAIL_THRESHOLD + 1) + '}');
    }

    @Test
    public void testDisabledThresholds() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // Test with warning threshold disabled
        DatabaseDescriptor.getGuardrailsConfig().sai_ann_rerank_k_warn_threshold = -1;
        assertValid("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': " + (WARN_THRESHOLD + 1) + '}');

        // Test with failure threshold disabled
        DatabaseDescriptor.getGuardrailsConfig().sai_ann_rerank_k_fail_threshold = -1;
        assertValid("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {'rerank_k': " + (FAIL_THRESHOLD + 1) + '}');
    }

    @Ignore
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

        // Queries without rerank_k should be valid
        assertValid("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10");
        assertValid("SELECT * FROM %s ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 10 WITH ann_options = {}");
    }
}