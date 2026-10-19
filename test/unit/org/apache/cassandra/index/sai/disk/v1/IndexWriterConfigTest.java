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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class IndexWriterConfigTest
{
    @Test
    public void defaultsTest()
    {
        IndexWriterConfig config = IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), new HashMap<>());

        assertThat(config.getMaximumNodeConnections()).isEqualTo(16);
        assertThat(config.getConstructionBeamWidth()).isEqualTo(100);
        assertThat(config.getSimilarityFunction()).isEqualTo(VectorSimilarityFunction.COSINE);
    }

    @Test
    public void maximumNodeConnectionsTest()
    {
        Map<String, String> options = new HashMap<>();
        options.put(IndexWriterConfig.MAXIMUM_NODE_CONNECTIONS, "10");
        IndexWriterConfig config = IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options);
        assertThat(config.getMaximumNodeConnections()).isEqualTo(10);

        options.put(IndexWriterConfig.MAXIMUM_NODE_CONNECTIONS, "-1");
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Maximum number of connections for index test cannot be <= 0 or > 512, was -1");

        options.put(IndexWriterConfig.MAXIMUM_NODE_CONNECTIONS, Integer.toString(IndexWriterConfig.MAXIMUM_MAXIMUM_NODE_CONNECTIONS + 1));
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Maximum number of connections for index test cannot be <= 0 or > 512, was " + (IndexWriterConfig.MAXIMUM_MAXIMUM_NODE_CONNECTIONS + 1));

        options.put(IndexWriterConfig.MAXIMUM_NODE_CONNECTIONS, "abc");
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Maximum number of connections abc is not a valid integer for index test");
    }

    @Test
    public void queueSizeTest()
    {
        Map<String, String> options = new HashMap<>();
        options.put(IndexWriterConfig.CONSTRUCTION_BEAM_WIDTH, "150");
        IndexWriterConfig config = IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options);
        assertThat(config.getConstructionBeamWidth()).isEqualTo(150);

        options.put(IndexWriterConfig.CONSTRUCTION_BEAM_WIDTH, "-1");
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Construction beam width for index test cannot be <= 0 or > 3200, was -1");

        options.put(IndexWriterConfig.CONSTRUCTION_BEAM_WIDTH, Integer.toString(IndexWriterConfig.MAXIMUM_CONSTRUCTION_BEAM_WIDTH + 1));
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Construction beam width for index test cannot be <= 0 or > 3200, was " + (IndexWriterConfig.MAXIMUM_CONSTRUCTION_BEAM_WIDTH + 1));

        options.put(IndexWriterConfig.CONSTRUCTION_BEAM_WIDTH, "abc");
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Construction beam width abc is not a valid integer for index test");
    }

    @Test
    public void similarityFunctionTest()
    {
        Map<String, String> options = new HashMap<>();
        options.put(IndexWriterConfig.SIMILARITY_FUNCTION, "DOT_PRODUCT");
        IndexWriterConfig config = IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options);
        assertThat(config.getSimilarityFunction()).isEqualTo(VectorSimilarityFunction.DOT_PRODUCT);

        options.put(IndexWriterConfig.SIMILARITY_FUNCTION, "euclidean");
        config = IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options);
        assertThat(config.getSimilarityFunction()).isEqualTo(VectorSimilarityFunction.EUCLIDEAN);

        options.put(IndexWriterConfig.SIMILARITY_FUNCTION, "blah");
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Similarity function BLAH was not recognized for index test. Valid values are: EUCLIDEAN, DOT_PRODUCT, COSINE");
    }

    @Test
    public void alphaTest()
    {
        Map<String, String> options = new HashMap<>();
        // Provide a valid alpha
        options.put(IndexWriterConfig.ALPHA, "1.7");
        IndexWriterConfig config = IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options);
        assertThat(config.getAlpha(999f)).isEqualTo(1.7f);

        // Provide an invalid (negative) alpha
        options.put(IndexWriterConfig.ALPHA, "-5");
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Alpha for index test must be > 0, was -5");

        // Provide a non-float alpha
        options.put(IndexWriterConfig.ALPHA, "abc");
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Alpha abc is not a valid float for index test");
    }

    @Test
    public void neighborhoodOverflowTest()
    {
        Map<String, String> options = new HashMap<>();
        // Provide a valid neighborhood_overflow
        options.put(IndexWriterConfig.NEIGHBORHOOD_OVERFLOW, "2.3");
        IndexWriterConfig config = IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options);
        assertThat(config.getNeighborhoodOverflow(999f)).isEqualTo(2.3f);

        // Provide invalid (<=0) overflow
        options.put(IndexWriterConfig.NEIGHBORHOOD_OVERFLOW, "0");
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Neighborhood overflow for index test must be >= 1.0, was 0");

        // Provide a non-float overflow
        options.put(IndexWriterConfig.NEIGHBORHOOD_OVERFLOW, "abc");
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Neighborhood overflow abc is not a valid float for index test");
    }

    @Test
    public void enableHierarchyTest()
    {
        Map<String, String> options = new HashMap<>();
        // Provide a valid enable_hierarchy
        options.put(IndexWriterConfig.ENABLE_HIERARCHY, "true");
        IndexWriterConfig config = IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options);
        assertThat(config.isHierarchyEnabled()).isTrue();

        // Provide an invalid enable_hierarchy
        options.put(IndexWriterConfig.ENABLE_HIERARCHY, "foo");
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Enable hierarchy must be 'true' or 'false' for index test, was 'foo'");
    }
}
