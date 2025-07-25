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

package org.apache.cassandra.index.sai.disk.format;

/**
 * The {@code IndexFeatureSet} represents the set of features available that are available
 * to an {@code OnDiskFormat}.
 *
 * The baseline features included in the V1 on-disk format are not included in the feature set.
 * Thus, V1 on-disk format features should only be added here if support for them is dropped in
 * a future version.
 */
public interface IndexFeatureSet
{
    /**
     * Returns whether the index supports row-awareness. Row-awareness means that the per-sstable
     * index supports mapping rowID -> {@code PrimaryKey} where the {@code PrimaryKey} contains both
     * partition key and clustering information.
     *
     * @return true if the index supports row-awareness
     */
    boolean isRowAware();

    /**
     * @return true if index metadata contains term histograms for fast cardinality estimation
     */
    boolean hasTermsHistogram();

    /**
     * The {@code Accumulator} is used to accumulate the {@link IndexFeatureSet} responses from
     * multiple sources. This will include all the SSTables included in a query and all the indexes
     * attached to those SSTables, added using {@link Accumulator#accumulate}.
     * <p>
     * The feature set of the current version denoted by {@link Version#current()}
     * is implicitly added, so the result feature set will include only the features supported by the
     * current version.
     * <p>
     * The {@code Accumulator} creates an {@code IndexFeatureSet} this contains the features from
     * all the associated feature sets where {@code false} is the highest priority. This means if any
     * on-disk format on any SSTable doesn't support a feature then that feature isn't supported
     * by the query.
     */
    class Accumulator
    {
        boolean isRowAware = true;
        boolean hasTermsHistogram = true;
        boolean complete = false;

        public Accumulator()
        {
            accumulate(Version.current().onDiskFormat().indexFeatureSet());
        }

        /**
         * Add another {@code IndexFeatureSet} to the accumulation
         *
         * @param indexFeatureSet the feature set to accumulate
         */
        public void accumulate(IndexFeatureSet indexFeatureSet)
        {
            assert !complete : "Cannot accumulate after complete has been called";
            if (!indexFeatureSet.isRowAware())
                isRowAware = false;
            if (!indexFeatureSet.hasTermsHistogram())
                hasTermsHistogram = false;
        }

        /**
         * Complete the accumulation of feature sets and return the
         * result of the accumulation.
         *
         * @return an {@link IndexFeatureSet} containing the accumulated feature set
         */
        public IndexFeatureSet complete()
        {
            complete = true;
            return new IndexFeatureSet()
            {
                @Override
                public boolean isRowAware()
                {
                    return isRowAware;
                }

                @Override
                public boolean hasTermsHistogram()
                {
                    return hasTermsHistogram;
                }
            };
        }
    }
}
