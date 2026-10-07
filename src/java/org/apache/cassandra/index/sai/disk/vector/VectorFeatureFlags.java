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

package org.apache.cassandra.index.sai.disk.vector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Strict, fail-fast configuration surface for the recently-added vector feature switches.
 *
 * <p>Every switch listed here is REQUIRED-EXPLICIT: it has no default, and a missing, empty, or
 * unrecognized value throws {@link ConfigurationException} naming the property and its accepted
 * values. In addition, {@link #validate()} scans the whole {@code cassandra.sai.vector} system
 * property namespace and rejects any key that is not a declared {@link CassandraRelevantProperties}
 * entry — so a typo'd property name (the historical
 * {@code cassandra.sai.vector.latest.version} silent no-op) fails the node loudly instead of
 * silently running some implicit mode and skewing a measurement.
 *
 * <p>Validation runs once (memoized) at the first access — wired into SAI index construction so a
 * misconfigured node fails at startup, not mid-benchmark on its first flush or merge.
 */
public final class VectorFeatureFlags
{
    private static final Logger logger = LoggerFactory.getLogger(VectorFeatureFlags.class);

    /** Property-namespace prefix this class polices. */
    private static final String NAMESPACE_PREFIX = "cassandra.sai.vector";

    private static volatile boolean validated = false;

    private VectorFeatureFlags()
    {
    }

    /** Whether memtable graphs encode PQ codes incrementally during ingest. */
    public static boolean amortizePqEncoding()
    {
        return requiredBoolean(CassandraRelevantProperties.SAI_VECTOR_AMORTIZE_PQ_ENCODING);
    }

    /** Whether residual flush-time PQ work is serialized node-wide. */
    /**
     * Whether vector graph merges use the experimental retain-largest strategy
     * (jvector {@code OnDiskGraphIndexCompactor#setRetainLargest}).
     *
     * <p>jvector still refuses the shortcut per-merge when the retained source does
     * not dominate the surviving nodes, falling back to the symmetric merge — so
     * enabling this asks for the strategy where it applies, it does not force it.
     */
    public static boolean compactionRetainLargest()
    {
        return requiredBoolean(CassandraRelevantProperties.SAI_VECTOR_COMPACTION_RETAIN_LARGEST);
    }

    public static boolean serializeFlushPq()
    {
        return requiredBoolean(CassandraRelevantProperties.SAI_VECTOR_SERIALIZE_FLUSH_PQ);
    }

    /**
     * Whether flush/rebuild graph builders run cleanup()'s final improveConnections re-refinement
     * (a per-node beam re-search duplicating what insert-time construction already computed).
     * false = flush index build is enforceDegree + serialization only.
     */
    public static boolean flushRefineFinalGraph()
    {
        return requiredBoolean(CassandraRelevantProperties.SAI_VECTOR_FLUSH_REFINE_FINAL_GRAPH);
    }

    /**
     * Validates the whole vector feature-flag surface: every required-explicit switch parses, and
     * every {@code cassandra.sai.vector*} system property is a declared key. Memoized after the
     * first successful pass; failures are not memoized (each caller re-throws the same clear error).
     */
    public static void validate()
    {
        if (validated)
            return;

        // Unknown-key scan: any property in our namespace that is not a declared
        // CassandraRelevantProperties entry is a typo or a removed flag; reject it.
        Set<String> knownKeys = new HashSet<>();
        for (CassandraRelevantProperties p : CassandraRelevantProperties.values())
            knownKeys.add(p.getKey());
        List<String> unknown = new ArrayList<>();
        for (Object k : System.getProperties().keySet())
        {
            String key = String.valueOf(k);
            if (key.startsWith(NAMESPACE_PREFIX) && !knownKeys.contains(key))
                unknown.add(key);
        }
        if (!unknown.isEmpty())
            throw new ConfigurationException("Unrecognized " + NAMESPACE_PREFIX + "* system properties " + unknown
                                             + " — not declared in CassandraRelevantProperties. A typo'd flag runs NOTHING; "
                                             + "fix the name or remove the setting.");

        // Every required-explicit switch must be present and parseable.
        boolean amortize = amortizePqEncoding();
        boolean serialize = serializeFlushPq();
        boolean refineFlush = flushRefineFinalGraph();
        boolean retainLargest = compactionRetainLargest();

        logger.info("Vector feature flags (all explicit): amortize_pq_encoding={}, serialize_flush_pq={}, "
                    + "flush_refine_final_graph={}, compaction_retain_largest={}",
                    amortize, serialize, refineFlush, retainLargest);
        validated = true;
    }

    private static String requiredString(CassandraRelevantProperties property)
    {
        String raw = property.getString();
        if (raw == null || raw.trim().isEmpty())
            throw new ConfigurationException("Required vector feature flag -D" + property.getKey()
                                             + " is not set. These switches have NO defaults: set it explicitly "
                                             + "(jvm-server.options) so the run's configuration is unambiguous.");
        return raw.trim();
    }

    private static boolean requiredBoolean(CassandraRelevantProperties property)
    {
        String raw = requiredString(property);
        if (raw.equals("true"))
            return true;
        if (raw.equals("false"))
            return false;
        throw unrecognized(property, raw, "true, false");
    }

    private static ConfigurationException unrecognized(CassandraRelevantProperties property, String raw, String accepted)
    {
        return new ConfigurationException("Unrecognized value '" + raw + "' for -D" + property.getKey()
                                          + "; accepted: " + accepted);
    }
}
