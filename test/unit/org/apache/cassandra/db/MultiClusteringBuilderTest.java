/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db;

import java.util.NavigableSet;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.TableMetadata;
import org.assertj.core.api.Assertions;

public class MultiClusteringBuilderTest extends CQLTester
{
    @Test
    public void testEmpty()
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2))");
        TableMetadata table = currentTableMetadata();
        MultiClusteringBuilder builder = MultiClusteringBuilder.create(table.comparator);
        NavigableSet<Clustering<?>> clusterings = builder.build();
        Assertions.assertThat(clusterings)
                  .hasSize(1)
                  .allMatch(Clustering.EMPTY::equals);
        Assertions.assertThat(builder.buildBound(true)).isEmpty();
        Assertions.assertThat(builder.buildBound(false)).isEmpty();
    }
}
