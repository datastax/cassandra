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

package org.apache.cassandra.schema;

import java.math.BigInteger;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class TableMetadataTest
{
    @Test
    public void testPartitionKeyAsCQLLiteral()
    {
        String keyspaceName = "keyspace";
        String tableName = "table";
        CompositeType compositeType1 = CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance, UTF8Type.instance);
        TableMetadata metadata1 = TableMetadata.builder(keyspaceName, tableName)
                                               .addPartitionKeyColumn("key", compositeType1)
                                               .build();

        String keyAsCQLLiteral = metadata1.partitionKeyAsCQLLiteral(compositeType1.decompose("test:", "composite!", "type)"));
        assertThat(keyAsCQLLiteral).isEqualTo("('test:', 'composite!', 'type)')");

        CompositeType compositeType2 = CompositeType.getInstance(new TupleType(Arrays.asList(FloatType.instance,
                                                                                             UTF8Type.instance)),
                                                                 IntegerType.instance);
        TableMetadata metadata2 = TableMetadata.builder(keyspaceName, tableName)
                                               .addPartitionKeyColumn("key", compositeType2)
                                               .build();
        String keyAsCQLLiteral2 = metadata2.partitionKeyAsCQLLiteral(compositeType2.decompose(TupleType.buildValue(FloatType.instance.decompose(0.33f),
                                                                                                                   UTF8Type.instance.decompose("tuple test")),
                                                                                              BigInteger.valueOf(10)));
        assertThat(keyAsCQLLiteral2).isEqualTo("((0.33, 'tuple test'), 10)");

        TableMetadata metadata3 = TableMetadata.builder(keyspaceName, tableName).addPartitionKeyColumn("key", UTF8Type.instance).build();
        assertEquals("'non-composite test'", metadata3.partitionKeyAsCQLLiteral(UTF8Type.instance.decompose("non-composite test")));
    }
}
