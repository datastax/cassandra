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

package org.apache.cassandra.index.sai.utils;

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.index.sai.disk.v2.RowAwarePrimaryKeyFactory;

public class StaticClusteringRowAwarePrimaryKeyFactoryTest extends RowAwarePrimaryKeyFactoryTest
{
    @Test
    public void simplePartitonStaticAndSingleClusteringAscTest()
    {
        PrimaryKey.Factory factory = new RowAwarePrimaryKeyFactory(simplePartitionStaticAndSingleClusteringAsc.comparator);
        int rows = nextInt(10, 100);
        PrimaryKey[] keys = new PrimaryKey[rows];
        int partition = 0;
        int clustering = 0;
        for (int index = 0; index < rows; index++)
        {
            if (clustering == 0)
            {
                keys[index] = factory.create(makeKey(simplePartitionSingleClusteringAsc, partition), Clustering.STATIC_CLUSTERING);
                clustering++;
            }
            else
                keys[index] = factory.create(makeKey(simplePartitionSingleClusteringAsc, partition),
                                             makeClustering(simplePartitionSingleClusteringAsc, Integer.toString(clustering++)));
            if (clustering == 5)
            {
                clustering = 0;
                partition++;
            }
        }

        Arrays.sort(keys);

        assertCorrectComparison(factory, keys);
    }

    @Override
    protected void assertCorrectComparison(PrimaryKey.Factory factory, PrimaryKey[] keys)
    {
        compareTokenOnlyWithByteComparison(factory, keys);
    }
}
