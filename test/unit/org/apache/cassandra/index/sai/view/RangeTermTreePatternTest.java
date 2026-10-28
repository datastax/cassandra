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

package org.apache.cassandra.index.sai.view;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The term tree prunes the sstable indexes of prefix and pattern expressions by the range of the terms starting
 * with the prefix (the common prefix of the automaton for a pattern).
 */
public class RangeTermTreePatternTest
{
    @BeforeClass
    public static void setup()
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void shouldPrunePrefixesAndPatterns()
    {
        IndexContext context = indexContext(UTF8Type.instance);
        SSTableIndex a = index(context, "aaa", "abz");
        SSTableIndex b = index(context, "b00", "b99");
        SSTableIndex c = index(context, "c", "d");
        TermTree tree = tree(context, a, b, c);

        assertEquals(Set.of(a), tree.search(expression(context, Operator.LIKE_PREFIX, "ab")));
        // the search is inclusive, so an index whose min term is nextOf(prefix) itself ("c") is admitted too
        assertEquals(Set.of(b, c), tree.search(expression(context, Operator.LIKE_PREFIX, "b")));
        assertEquals(Set.of(), tree.search(expression(context, Operator.LIKE_PREFIX, "x")));
        // patterns: the common prefix of the automaton, if any
        assertEquals(Set.of(b, c), tree.search(expression(context, Operator.LIKE_MATCHES, "b%5")));
        assertEquals(Set.of(c), tree.search(expression(context, Operator.LIKE_MATCHES, "c%")));
        assertEquals(Set.of(a, b, c), tree.search(expression(context, Operator.LIKE_SUFFIX, "z")));
        assertEquals(Set.of(a, b, c), tree.search(expression(context, Operator.LIKE_CONTAINS, "b")));
        assertEquals(Set.of(a), tree.search(expression(context, Operator.LIKE_PREFIX, "a")));
    }

    @Test
    public void shouldPruneReversedColumns()
    {
        // the min/max terms of a DESC clustering column are in the reversed order of the column type
        IndexContext context = indexContext(ReversedType.getInstance(UTF8Type.instance));
        SSTableIndex a = index(context, "abz", "aaa");
        SSTableIndex b = index(context, "b99", "b00");
        TermTree tree = tree(context, a, b);

        assertEquals(Set.of(a), tree.search(expression(context, Operator.LIKE_PREFIX, "ab")));
        assertEquals(Set.of(b), tree.search(expression(context, Operator.LIKE_MATCHES, "b%5")));
        assertEquals(Set.of(), tree.search(expression(context, Operator.LIKE_PREFIX, "x")));
    }

    private static TermTree tree(IndexContext context, SSTableIndex... indexes)
    {
        TermTree.Builder builder = new RangeTermTree.Builder(context.getValidator());
        for (SSTableIndex index : indexes)
            builder.add(index);
        return builder.build();
    }

    private static SSTableIndex index(IndexContext context, String minTerm, String maxTerm)
    {
        SSTableIndex index = mock(SSTableIndex.class);
        when(index.minTerm()).thenReturn(UTF8Type.instance.decompose(minTerm));
        when(index.maxTerm()).thenReturn(UTF8Type.instance.decompose(maxTerm));
        when(index.getVersion()).thenReturn(Version.LATEST);
        when(index.getIndexContext()).thenReturn(context);
        // read by the trace logging of RangeTermTree.Builder#addIndex (CI runs the tests with TRACE on)
        when(index.getSSTable()).thenReturn(mock(SSTableReader.class));
        return index;
    }

    private static Expression expression(IndexContext context, Operator operator, String value)
    {
        return new Expression(context).add(operator, UTF8Type.instance.decompose(value));
    }

    private static IndexContext indexContext(AbstractType<?> clusteringType)
    {
        TableMetadata table = TableMetadata.builder("ks", "tb")
                                           .addPartitionKeyColumn("pk", Int32Type.instance)
                                           .addClusteringColumn("ck", clusteringType)
                                           .partitioner(Murmur3Partitioner.instance)
                                           .build();

        Map<String, String> options = new HashMap<>();
        options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getCanonicalName());
        options.put("target", "ck");
        IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata("ck_index", IndexMetadata.Kind.CUSTOM, options);
        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(table, indexMetadata);
        return new IndexContext(table.keyspace,
                                table.name,
                                table.id,
                                table.partitionKeyType,
                                table.comparator,
                                target.left,
                                target.right,
                                indexMetadata,
                                MockSchema.newCFS(table));
    }
}
