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

package org.apache.cassandra.distributed.test.sai.datamodels;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.cql.datamodels.DataModel;
import org.apache.cassandra.index.sai.cql.datamodels.IndexQuerySupport;

/**
 * Long-running variant of {@link MultiNodeQueryTester} that uses larger and random datasets
 * to test SAI with more realistic data volumes.
 */
abstract class LongMultiNodeQueryTester extends MultiNodeQueryTester
{
    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> params() throws Throwable
    {
        List<Object[]> scenarios = new LinkedList<>();

        scenarios.add(new Object[]{ "BaseDataModel",
                                    (Supplier<DataModel>) () -> new DataModel.BaseDataModel(DataModel.NORMAL_COLUMNS, DataModel.NORMAL_COLUMN_DATA_LARGE),
                                    IndexQuerySupport.BASE_QUERY_SETS });

        scenarios.add(new Object[]{ "CompoundKeyDataModel",
                                    (Supplier<DataModel>) () -> new DataModel.CompoundKeyDataModel(DataModel.NORMAL_COLUMNS, DataModel.NORMAL_COLUMN_DATA_LARGE),
                                    IndexQuerySupport.BASE_QUERY_SETS });

        scenarios.add(new Object[]{ "CompoundKeyWithStaticsDataModel",
                                    (Supplier<DataModel>) () -> new DataModel.CompoundKeyWithStaticsDataModel(DataModel.STATIC_COLUMNS, DataModel.STATIC_COLUMN_DATA_LARGE),
                                    IndexQuerySupport.STATIC_QUERY_SETS });

        scenarios.add(new Object[]{ "CompositePartitionKeyDataModel",
                                    (Supplier<DataModel>) () -> new DataModel.CompositePartitionKeyDataModel(DataModel.NORMAL_COLUMNS, DataModel.NORMAL_COLUMN_DATA_LARGE),
                                    ImmutableList.builder().addAll(IndexQuerySupport.BASE_QUERY_SETS).addAll(IndexQuerySupport.COMPOSITE_PARTITION_QUERY_SETS).build() });

        return scenarios;
    }
}
