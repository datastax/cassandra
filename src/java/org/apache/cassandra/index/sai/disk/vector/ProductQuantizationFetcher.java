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
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.slf4j.Logger;

import io.github.jbellis.jvector.quantization.ProductQuantization;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.io.util.FileHandle;

public class ProductQuantizationFetcher
{
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(ProductQuantizationFetcher.class);

    /**
     * Return the best previous CompressedVectors for this column that matches the `matcher` predicate.
     * "Best" means the most recent one that hits the row count target of {@link ProductQuantization#MAX_PQ_TRAINING_SET_SIZE},
     * or the one with the most rows if none are larger than that.
     */
    public static PqInfo getPqIfPresent(IndexContext indexContext)
    {
        // We get a referenced view becuase we might actually read a segment from disk, which requires that we
        // hold a lock on the index to prevent it from getting deleted concurrently. The PqInfo object is all in
        // memory, though, so we don't need the view when we return.
        var view = indexContext.getReferencedView(TimeUnit.SECONDS.toNanos(5));
        if (view == null)
        {
            logger.warn("Unable to get view of already built indexes for {}", indexContext);
            return null;
        }

        try
        {
            // Retrieve the first compressed vectors for a segment with at least MAX_PQ_TRAINING_SET_SIZE rows
            // or the one with the most rows if none reach that size.
            // Flatten all segments, sorted by size (capped to MAX_PQ_TRAINING_SET_SIZE) then timestamp
            return view.getIndexes().stream()
                       .flatMap(CustomSegmentSorter::streamSegments)
                       .filter(customSegment -> customSegment.numRowsOrMaxPQTrainingSetSize >= CassandraOnHeapGraph.MIN_PQ_ROWS)
                       .sorted()
                       .map(CustomSegmentSorter::getPqInfo)
                       .filter(Objects::nonNull)
                       .findFirst()
                       .orElse(null);
        }
        finally
        {
            view.release();
        }
    }

    public static class PqInfo
    {
        public final ProductQuantization pq;
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
            // Note to reviewers, it looks like we use the PQ in the BuildScoreProvider, so we might actually use more
            // of the PQ vector space than I initially though.
            this.numRowsOrMaxPQTrainingSetSize = (int) Math.min(numRows, ProductQuantization.MAX_PQ_TRAINING_SET_SIZE);
            this.timestamp = sstableIndex.getSSTable().getMaxTimestamp();
            this.segmentPosition = segmentPosition;
        }

        @SuppressWarnings("resource")
        private PqInfo getPqInfo()
        {
            return sstableIndex.getPqInfo(segmentPosition);
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
    public static PqInfo maybeReadPqFromSegment(SegmentMetadata sm, FileHandle fh)
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
        catch (IOException e)
        {
            logger.warn("Failed to read PQ from segment {} for {}. Skipping", sm, fh, e);
        }
        return null;
    }
}
