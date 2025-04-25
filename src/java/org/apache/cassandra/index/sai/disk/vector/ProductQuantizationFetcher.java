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

import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Stream;

import io.github.jbellis.jvector.quantization.ProductQuantization;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.io.util.FileHandle;

public class ProductQuantizationFetcher
{

    /**
     * Return the best previous CompressedVectors for this column that matches the `matcher` predicate.
     * "Best" means the most recent one that hits the row count target of {@link ProductQuantization#MAX_PQ_TRAINING_SET_SIZE},
     * or the one with the most rows if none are larger than that.
     */
    public static PqInfo getPqIfPresent(IndexContext indexContext) throws IOException
    {
        // TODO when compacting, this view is likely the whole table, is it worth only considering the sstables that
        //  are being compacted?
        // Flatten all segments, sorted by size then timestamp (size is capped to MAX_PQ_TRAINING_SET_SIZE)
        var sortedSegments = indexContext.getView().getIndexes().stream()
                                         .flatMap(CustomSegmentSorter::streamSegments)
                                         .filter(customSegment -> customSegment.numRowsOrMaxPQTrainingSetSize > CassandraOnHeapGraph.MIN_PQ_ROWS)
                                         .sorted()
                                         .iterator();

        while (sortedSegments.hasNext())
        {
            var customSegment = sortedSegments.next();
            // Because we sorted based on size then timestamp, this is the best match (assuming it exists)
            var pqInfo = customSegment.getPqInfo();
            if (pqInfo != null)
                return pqInfo;
        }

        return null;  // nothing matched
    }

    public static class PqInfo
    {
        public final ProductQuantization pq;
        /** an empty Optional indicates that the index was written with an older version that did not record this information */
        public final boolean unitVectors;
        public final long rowCount;

        public PqInfo(ProductQuantization pq, boolean unitVectors, long rowCount)
        {
            this.pq = pq;
            this.unitVectors = unitVectors;
            this.rowCount = rowCount;
        }
    }

    private static class CustomSegmentSorter implements Comparable<CustomSegmentSorter>
    {
        private final SSTableIndex sstableIndex;
        private final int numRowsOrMaxPQTrainingSetSize;
        private final long timestamp;
        private final int segmentPosition;

        private CustomSegmentSorter(SSTableIndex sstableIndex, long numRows, int segmentPosition)
        {
            this.sstableIndex = sstableIndex;
            // TODO give the size cost of larger PQ sets, is it worth trying to get the PQ object closest to this
            // value? I'm concerned that we'll grab something pretty large for mem.
            this.numRowsOrMaxPQTrainingSetSize = (int) Math.min(numRows, ProductQuantization.MAX_PQ_TRAINING_SET_SIZE);
            this.timestamp = sstableIndex.getSSTable().getMaxTimestamp();
            this.segmentPosition = segmentPosition;
        }

        @SuppressWarnings("resource")
        private PqInfo getPqInfo() throws IOException
        {
            if (sstableIndex.areSegmentsLoaded())
            {
                var segment = sstableIndex.getSegments().get(segmentPosition);
                V2VectorIndexSearcher searcher = (V2VectorIndexSearcher) segment.getIndexSearcher();
                // Skip segments that don't have PQ
                if (VectorCompression.CompressionType.PRODUCT_QUANTIZATION != searcher.getCompression().type)
                    return null;

                // Because we sorted based on size then timestamp, this is the best match
                return new PqInfo(searcher.getPQ(), searcher.containsUnitVectors(), segment.metadata.numRows);
            }
            else
            {
                // We have to load from disk here
                var segmentMetadata = sstableIndex.getSegmentMetadatas().get(segmentPosition);
                try (var pq = sstableIndex.indexFiles().pq())
                {
                    // Returns null if wrong uses wrong compression type.
                    return maybeReadPqFromSegment(segmentMetadata, pq);
                }
            }
        }

        @Override
        public int compareTo(CustomSegmentSorter o)
        {
            // Sort by size descending, then timestamp descending for sstables
            int cmp = Long.compare(numRowsOrMaxPQTrainingSetSize, o.numRowsOrMaxPQTrainingSetSize);
            if (cmp != 0)
                return cmp;
            return Long.compare(timestamp, o.timestamp);
        }

        private static Stream<CustomSegmentSorter> streamSegments(SSTableIndex index)
        {
            var results = new ArrayList<CustomSegmentSorter>();

            var metadatas = index.getSegmentMetadatas();
            for (int i = 0; i < metadatas.size(); i++)
                results.add(new CustomSegmentSorter(index, metadatas.get(i).numRows, i));

            return results.stream();
        }
    }

    /**
     * Takes a segment metadata and file handle and returns the PQ info if the segment has PQ that uses
     * compression type {@link VectorCompression.CompressionType#PRODUCT_QUANTIZATION}.
     *
     * @param sm segment metadata
     * @param fh PQ file handle
     * @return PqInfo if the segment has PQ, null otherwise
     * @throws IOException if an I/O error occurs
     */
    public static PqInfo maybeReadPqFromSegment(SegmentMetadata sm, FileHandle fh) throws IOException
    {
        try (var reader = fh.createReader())
        {
            long offset = sm.componentMetadatas.get(IndexComponentType.PQ).offset;
            // close parallel to code in CassandraDiskANN constructor, but different enough
            // (we only want the PQ codebook) that it's difficult to extract into a common method
            reader.seek(offset);
            boolean unitVectors;
            if (reader.readInt() == CassandraDiskAnn.PQ_MAGIC)
            {
                reader.readInt(); // skip over version
                unitVectors = reader.readBoolean();
            }
            else
            {
                unitVectors = true;
                reader.seek(offset);
            }
            var compressionType = VectorCompression.CompressionType.values()[reader.readByte()];
            if (compressionType == VectorCompression.CompressionType.PRODUCT_QUANTIZATION)
            {
                // TODO if this is really big, is there any way to either limit what we read or to just leave this
                // as a disk backed PQ to reduce memory utilization? This is only used to help seed the initial
                // training, so it seems excessive to load the whole thing into memory for large segments.
                var pq = ProductQuantization.load(reader);
                return new PqInfo(pq, unitVectors, sm.numRows);
            }
        }
        return null;
    }
}
