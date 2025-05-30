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
package org.apache.cassandra.index.sai.disk.v1;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.disk.vector.VectorSourceModel;
import org.apache.cassandra.index.sai.utils.TypeUtil;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VECTOR_SEARCH_MAX_TOP_K;

/**
 * Per-index config for storage-attached index writers.
 */
public class IndexWriterConfig
{
    public static final String POSTING_LIST_LVL_MIN_LEAVES = "bkd_postings_min_leaves";
    public static final String POSTING_LIST_LVL_SKIP_OPTION = "bkd_postings_skip";

    private static final int DEFAULT_POSTING_LIST_MIN_LEAVES = 64;
    private static final int DEFAULT_POSTING_LIST_LVL_SKIP = 3;

    public static final String MAXIMUM_NODE_CONNECTIONS = "maximum_node_connections";
    public static final String CONSTRUCTION_BEAM_WIDTH = "construction_beam_width";
    public static final String NEIGHBORHOOD_OVERFLOW = "neighborhood_overflow";
    public static final String ALPHA = "alpha";
    public static final String ENABLE_HIERARCHY = "enable_hierarchy";
    public static final String SIMILARITY_FUNCTION = "similarity_function";
    public static final String SOURCE_MODEL = "source_model";
    public static final String OPTIMIZE_FOR = "optimize_for"; // unused, retained for compatibility w/ old schemas

    public static final int MAXIMUM_MAXIMUM_NODE_CONNECTIONS = 512;
    public static final int MAXIMUM_CONSTRUCTION_BEAM_WIDTH = 3200;

    public static final int DEFAULT_MAXIMUM_NODE_CONNECTIONS = 16;
    public static final int DEFAULT_CONSTRUCTION_BEAM_WIDTH = 100;
    public static final boolean DEFAULT_ENABLE_HIERARCHY = false;

    public static final int MAX_TOP_K = SAI_VECTOR_SEARCH_MAX_TOP_K.getInt();

    public static final String validSimilarityFunctions = Arrays.stream(VectorSimilarityFunction.values())
                                                                .map(Enum::name)
                                                                .collect(Collectors.joining(", "));

    private static final VectorSourceModel DEFAULT_SOURCE_MODEL = VectorSourceModel.OTHER;

    private static final IndexWriterConfig EMPTY_CONFIG = new IndexWriterConfig(null, -1, -1, -1, -1, null, DEFAULT_SOURCE_MODEL);

    /**
     * Fully qualified index name, in the format "<keyspace>.<table>.<index_name>".
     */
    private final String indexName;

    /**
     * Skip, or the sampling interval, for selecting a bkd tree level that is eligible for an auxiliary posting list.
     * Sampling starts from 0, but bkd tree root node is at level 1. For skip = 4, eligible levels are 4, 8, 12, etc (no
     * level 0, because there is no node at level 0).
     */
    private final int bkdPostingsSkip;

    /**
     * Min. number of reachable leaves for a given node to be eligible for an auxiliary posting list.
     */
    private final int bkdPostingsMinLeaves;

    private final int maximumNodeConnections;
    private final int constructionBeamWidth;
    private final VectorSimilarityFunction similarityFunction;
    private final VectorSourceModel sourceModel;

    private final Float neighborhoodOverflow; // default varies for in memory/compaction build
    private final Float alpha; // default varies for in memory/compaction build
    private final boolean enableHierarchy; // defaults to false

    public IndexWriterConfig(String indexName,
                             int bkdPostingsSkip,
                             int bkdPostingsMinLeaves)
    {
        this(indexName,
             bkdPostingsSkip,
             bkdPostingsMinLeaves,
             DEFAULT_MAXIMUM_NODE_CONNECTIONS,
             DEFAULT_CONSTRUCTION_BEAM_WIDTH,
             DEFAULT_SOURCE_MODEL.defaultSimilarityFunction,
             DEFAULT_SOURCE_MODEL
        );
    }

    public IndexWriterConfig(String indexName,
                             int bkdPostingsSkip,
                             int bkdPostingsMinLeaves,
                             int maximumNodeConnections,
                             int constructionBeamWidth,
                             VectorSimilarityFunction similarityFunction,
                             VectorSourceModel sourceModel)
    {
        this(indexName, bkdPostingsSkip, bkdPostingsMinLeaves, maximumNodeConnections, constructionBeamWidth,
             similarityFunction, sourceModel, null, null, false);
    }

    public IndexWriterConfig(String indexName,
                             int bkdPostingsSkip,
                             int bkdPostingsMinLeaves,
                             int maximumNodeConnections,
                             int constructionBeamWidth,
                             VectorSimilarityFunction similarityFunction,
                             VectorSourceModel sourceModel,
                             Float neighborhoodOverflow,
                             Float alpha,
                             boolean enableHierarchy)
    {
        this.indexName = indexName;
        this.bkdPostingsSkip = bkdPostingsSkip;
        this.bkdPostingsMinLeaves = bkdPostingsMinLeaves;
        this.maximumNodeConnections = maximumNodeConnections;
        this.constructionBeamWidth = constructionBeamWidth;
        this.similarityFunction = similarityFunction;
        this.sourceModel = sourceModel;
        this.neighborhoodOverflow = neighborhoodOverflow;
        this.alpha = alpha;
        this.enableHierarchy = enableHierarchy;
    }

    public String getIndexName()
    {
        return indexName;
    }

    public int getBkdPostingsMinLeaves()
    {
        return bkdPostingsMinLeaves;
    }

    public int getBkdPostingsSkip()
    {
        return bkdPostingsSkip;
    }

    public int getAnnMaxDegree()
    {
        // For historical reasons (Lucene doubled the maximum node connections for its HNSW),
        // maximumNodeConnections represents half of the graph degree, so double it
        return 2 * maximumNodeConnections;
    }

    /** you should probably use getAnnMaxDegree instead */
    @VisibleForTesting
    @Deprecated
    public int getMaximumNodeConnections()
    {
        return maximumNodeConnections;
    }

    public int getConstructionBeamWidth()
    {
        return constructionBeamWidth;
    }

    public VectorSimilarityFunction getSimilarityFunction()
    {
        return similarityFunction;
    }

    public VectorSourceModel getSourceModel()
    {
        return sourceModel;
    }

    public float getNeighborhoodOverflow(float defaultValue)
    {
        return neighborhoodOverflow == null ? defaultValue : neighborhoodOverflow;
    }

    public float getAlpha(float defaultValue)
    {
        return alpha == null ? defaultValue : alpha;
    }

    public boolean isHierarchyEnabled()
    {
        return enableHierarchy;
    }

    public static IndexWriterConfig fromOptions(String indexName, AbstractType<?> type, Map<String, String> options)
    {
        int minLeaves = DEFAULT_POSTING_LIST_MIN_LEAVES;
        int skip = DEFAULT_POSTING_LIST_LVL_SKIP;
        int maximumNodeConnections = DEFAULT_MAXIMUM_NODE_CONNECTIONS;
        int queueSize = DEFAULT_CONSTRUCTION_BEAM_WIDTH;
        VectorSourceModel sourceModel = DEFAULT_SOURCE_MODEL;
        VectorSimilarityFunction similarityFunction = sourceModel.defaultSimilarityFunction; // don't leave null in case no options at all are given

        Float neighborhoodOverflow = null;
        Float alpha = null;
        boolean enableHierarchy = DEFAULT_ENABLE_HIERARCHY;

        if (options.get(POSTING_LIST_LVL_MIN_LEAVES) != null || options.get(POSTING_LIST_LVL_SKIP_OPTION) != null)
        {
            if (TypeUtil.isLiteral(type))
            {
                throw new InvalidRequestException(String.format("CQL type %s cannot have auxiliary posting lists on index %s.", type.asCQL3Type(), indexName));
            }

            for (Map.Entry<String, String> entry : options.entrySet())
            {
                switch (entry.getKey())
                {
                    case POSTING_LIST_LVL_MIN_LEAVES:
                    {
                        minLeaves = Integer.parseInt(entry.getValue());

                        if (minLeaves < 1)
                        {
                            throw new InvalidRequestException(String.format("Posting list min. leaves count can't be less than 1 on index %s.", indexName));
                        }

                        break;
                    }

                    case POSTING_LIST_LVL_SKIP_OPTION:
                    {
                        skip = Integer.parseInt(entry.getValue());

                        if (skip < 1)
                        {
                            throw new InvalidRequestException(String.format("Posting list skip can't be less than 1 on index %s.", indexName));
                        }

                        break;
                    }
                }
            }
        }
        else if (options.get(MAXIMUM_NODE_CONNECTIONS) != null ||
                 options.get(CONSTRUCTION_BEAM_WIDTH) != null ||
                 options.get(OPTIMIZE_FOR) != null ||
                 options.get(SIMILARITY_FUNCTION) != null ||
                 options.get(SOURCE_MODEL) != null ||
                 options.get(NEIGHBORHOOD_OVERFLOW) != null ||
                 options.get(ALPHA) != null ||
                 options.get(ENABLE_HIERARCHY) != null)
        {
            if (!type.isVector())
                throw new InvalidRequestException(String.format("CQL type %s cannot have vector options", type.asCQL3Type()));

            if (options.containsKey(MAXIMUM_NODE_CONNECTIONS))
            {
                try
                {
                    maximumNodeConnections = Integer.parseInt(options.get(MAXIMUM_NODE_CONNECTIONS));
                }
                catch (NumberFormatException e)
                {
                    throw new InvalidRequestException(String.format("Maximum number of connections %s is not a valid integer for index %s",
                                                                    options.get(MAXIMUM_NODE_CONNECTIONS), indexName));
                }
                if (maximumNodeConnections <= 0 || maximumNodeConnections > MAXIMUM_MAXIMUM_NODE_CONNECTIONS)
                    throw new InvalidRequestException(String.format("Maximum number of connections for index %s cannot be <= 0 or > %s, was %s", indexName, MAXIMUM_MAXIMUM_NODE_CONNECTIONS, maximumNodeConnections));
            }
            if (options.containsKey(CONSTRUCTION_BEAM_WIDTH))
            {
                try
                {
                    queueSize = Integer.parseInt(options.get(CONSTRUCTION_BEAM_WIDTH));
                }
                catch (NumberFormatException e)
                {
                    throw new InvalidRequestException(String.format("Construction beam width %s is not a valid integer for index %s",
                                                                    options.get(CONSTRUCTION_BEAM_WIDTH), indexName));
                }
                if (queueSize <= 0 || queueSize > MAXIMUM_CONSTRUCTION_BEAM_WIDTH)
                    throw new InvalidRequestException(String.format("Construction beam width for index %s cannot be <= 0 or > %s, was %s", indexName, MAXIMUM_CONSTRUCTION_BEAM_WIDTH, queueSize));
            }
            if (options.containsKey(SOURCE_MODEL))
            {
                String option = options.get(SOURCE_MODEL).toUpperCase().replace("-", "_");
                try
                {
                    sourceModel = VectorSourceModel.valueOf(option);
                }
                catch (IllegalArgumentException e)
                {
                    var validSourceModels = Arrays.stream(VectorSourceModel.values())
                                                  .map(Enum::name)
                                                  .collect(Collectors.joining(", "));
                    throw new InvalidRequestException(String.format("source_model '%s' was not recognized for index %s. Valid values are: %s",
                                                                    option, indexName, validSourceModels));
                }
            }
            if (options.containsKey(SIMILARITY_FUNCTION))
            {
                String option = options.get(SIMILARITY_FUNCTION).toUpperCase();
                try
                {
                    similarityFunction = VectorSimilarityFunction.valueOf(option);
                }
                catch (IllegalArgumentException e)
                {
                    throw new InvalidRequestException(String.format("Similarity function %s was not recognized for index %s. Valid values are: %s",
                                                                    option, indexName, validSimilarityFunctions));
                }
            }
            else
            {
                similarityFunction = sourceModel.defaultSimilarityFunction;
            }

            if (options.containsKey(NEIGHBORHOOD_OVERFLOW))
            {
                try
                {
                    neighborhoodOverflow = Float.parseFloat(options.get(NEIGHBORHOOD_OVERFLOW));
                    if (neighborhoodOverflow < 1.0f)
                        throw new InvalidRequestException(String.format("Neighborhood overflow for index %s must be >= 1.0, was %s",
                                                                      indexName, neighborhoodOverflow));
                }
                catch (NumberFormatException e)
                {
                    throw new InvalidRequestException(String.format("Neighborhood overflow %s is not a valid float for index %s",
                                                                    options.get(NEIGHBORHOOD_OVERFLOW), indexName));
                }
            }

            if (options.containsKey(ALPHA))
            {
                try
                {
                    alpha = Float.parseFloat(options.get(ALPHA));
                    if (alpha <= 0)
                        throw new InvalidRequestException(String.format("Alpha for index %s must be > 0, was %s", 
                                                                      indexName, alpha));
                }
                catch (NumberFormatException e)
                {
                    throw new InvalidRequestException(String.format("Alpha %s is not a valid float for index %s",
                                                                  options.get(ALPHA), indexName));
                }
            }

            if (options.containsKey(ENABLE_HIERARCHY))
            {
                String value = options.get(ENABLE_HIERARCHY).toLowerCase();
                if (!value.equals("true") && !value.equals("false"))
                    throw new InvalidRequestException(String.format("Enable hierarchy must be 'true' or 'false' for index %s, was '%s'",
                                                                  indexName, value));
                enableHierarchy = Boolean.parseBoolean(value);
            }
        }

        return new IndexWriterConfig(indexName, skip, minLeaves, maximumNodeConnections, queueSize, similarityFunction, sourceModel, neighborhoodOverflow, alpha, enableHierarchy);
    }

    public static IndexWriterConfig defaultConfig(String indexName)
    {
        return new IndexWriterConfig(indexName,
                                     DEFAULT_POSTING_LIST_LVL_SKIP,
                                     DEFAULT_POSTING_LIST_MIN_LEAVES,
                                     DEFAULT_MAXIMUM_NODE_CONNECTIONS,
                                     DEFAULT_CONSTRUCTION_BEAM_WIDTH,
                                     DEFAULT_SOURCE_MODEL.defaultSimilarityFunction,
                                     DEFAULT_SOURCE_MODEL
        );
    }

    public static IndexWriterConfig emptyConfig()
    {
        return EMPTY_CONFIG;
    }

    @Override
    public String toString()
    {
        return String.format("IndexWriterConfig{%s=%d, %s=%d, %s=%d, %s=%d, %s=%s, %s=%s, %s=%f, %s=%f, %s=%b}",
                             POSTING_LIST_LVL_SKIP_OPTION, bkdPostingsSkip,
                             POSTING_LIST_LVL_MIN_LEAVES, bkdPostingsMinLeaves,
                             MAXIMUM_NODE_CONNECTIONS, maximumNodeConnections,
                             CONSTRUCTION_BEAM_WIDTH, constructionBeamWidth,
                             SIMILARITY_FUNCTION, similarityFunction,
                             SOURCE_MODEL, sourceModel,
                             NEIGHBORHOOD_OVERFLOW, neighborhoodOverflow,
                             ALPHA, alpha,
                             ENABLE_HIERARCHY, enableHierarchy);
    }
}
