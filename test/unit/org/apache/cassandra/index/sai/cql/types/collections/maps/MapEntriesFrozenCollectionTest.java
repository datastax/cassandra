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
package org.apache.cassandra.index.sai.cql.types.collections.maps;

import java.util.Collection;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.cql.types.DataSet;
import org.apache.cassandra.index.sai.cql.types.IndexingTypeSupport;
import org.apache.cassandra.index.sai.cql.types.collections.CollectionDataSet;
import org.apache.cassandra.index.sai.disk.format.Version;

@RunWith(Parameterized.class)
public class MapEntriesFrozenCollectionTest extends IndexingTypeSupport
{
    @Parameterized.Parameters(name = "version={0},dataset={1},wide={2},scenario={3}")
    public static Collection<Object[]> generateParameters()
    {
        DataSet<Map<Integer, Integer>> frozen = new CollectionDataSet.FrozenMapValuesDataSet<>(new DataSet.IntDataSet());
        return generateParameters(new CollectionDataSet.MapEntriesDataSet<>(frozen));
    }

    public MapEntriesFrozenCollectionTest(Version version, DataSet<?> dataset, boolean widePartitions, Scenario scenario)
    {
        super(version, dataset, widePartitions, scenario);
    }

    @Test
    public void test() throws Throwable
    {
        runIndexQueryScenarios();
    }
}
