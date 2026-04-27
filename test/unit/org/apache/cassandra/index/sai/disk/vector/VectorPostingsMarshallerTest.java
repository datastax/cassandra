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

package org.apache.cassandra.index.sai.disk.vector;

import java.util.List;

import org.junit.Test;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.agrona.collections.Int2IntHashMap;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings.CompactionVectorPostings;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings.Marshaller;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VectorPostingsMarshallerTest
{
    private final Marshaller marshaller = new Marshaller();

    @Test
    public void testWriteAndReadSinglePosting()
    {
        // Create a CompactionVectorPostings with a single posting
        int ordinal = 42;
        int rowId = 100;
        CompactionVectorPostings postings = new CompactionVectorPostings(ordinal, rowId);

        // Write to bytes
        Bytes<?> bytes = Bytes.allocateElasticOnHeap();
        marshaller.write(bytes, postings);

        // Read back
        bytes.readPosition(0);
        CompactionVectorPostings read = marshaller.read(bytes, null);

        // Verify
        assertNotNull(read);
        assertEquals(ordinal, read.getOrdinal());
        assertEquals(1, read.size());
        assertEquals(rowId, read.getPostings().get(0).intValue());
        
        bytes.releaseLast();
    }

    @Test
    public void testWriteAndReadMultiplePostings()
    {
        // Create a CompactionVectorPostings with multiple postings
        int ordinal = 10;
        List<Integer> rowIds = List.of(5, 15, 25, 35);
        CompactionVectorPostings postings = new CompactionVectorPostings(ordinal, rowIds);

        // Write to bytes
        Bytes<?> bytes = Bytes.allocateElasticOnHeap();
        marshaller.write(bytes, postings);

        // Read back
        bytes.readPosition(0);
        CompactionVectorPostings read = marshaller.read(bytes, null);

        // Verify
        assertNotNull(read);
        assertEquals(ordinal, read.getOrdinal());
        assertEquals(4, read.size());
        assertEquals(rowIds, read.getPostings());
        
        bytes.releaseLast();
    }

    @Test
    public void testWriteAndReadEmptyPostings()
    {
        // Create a CompactionVectorPostings with empty list
        int ordinal = 7;
        List<Integer> rowIds = List.of();
        CompactionVectorPostings postings = new CompactionVectorPostings(ordinal, rowIds);

        // Write to bytes
        Bytes<?> bytes = Bytes.allocateElasticOnHeap();
        marshaller.write(bytes, postings);

        // Read back
        bytes.readPosition(0);
        CompactionVectorPostings read = marshaller.read(bytes, null);

        // Verify
        assertNotNull(read);
        assertEquals(ordinal, read.getOrdinal());
        assertEquals(0, read.size());
        assertTrue(read.getPostings().isEmpty());
        
        bytes.releaseLast();
    }

    @Test
    public void testWriteAndReadLargeOrdinalAndRowIds()
    {
        // Test with large values to ensure proper integer handling
        int ordinal = Integer.MAX_VALUE - 1;
        List<Integer> rowIds = List.of(Integer.MAX_VALUE - 10, Integer.MAX_VALUE - 5, Integer.MAX_VALUE - 1);
        CompactionVectorPostings postings = new CompactionVectorPostings(ordinal, rowIds);

        // Write to bytes
        Bytes<?> bytes = Bytes.allocateElasticOnHeap();
        marshaller.write(bytes, postings);

        // Read back
        bytes.readPosition(0);
        CompactionVectorPostings read = marshaller.read(bytes, null);

        // Verify
        assertNotNull(read);
        assertEquals(ordinal, read.getOrdinal());
        assertEquals(3, read.size());
        assertEquals(rowIds, read.getPostings());
        
        bytes.releaseLast();
    }

    @Test
    public void testRecordExtraOrdinalsWithSinglePosting()
    {
        // Test recordExtraOrdinals with a single posting (should not add anything to extraOrdinals)
        try (ChronicleMap<Integer, CompactionVectorPostings> map = ChronicleMapBuilder
                .of(Integer.class, CompactionVectorPostings.class)
                .entries(10)
                .averageValueSize(100)
                .valueMarshaller(marshaller)
                .create())
        {
            int key = 1;
            int ordinal = 50;
            int rowId = 50; // First rowId must equal ordinal
            CompactionVectorPostings postings = new CompactionVectorPostings(ordinal, rowId);
            
            map.put(key, postings);

            Int2IntHashMap extraOrdinals = new Int2IntHashMap(-1);
            
            map.forEachEntry(entry -> {
                Marshaller.recordExtraOrdinals(entry, extraOrdinals);
            });

            // With only one posting, no extra ordinals should be recorded
            assertEquals(0, extraOrdinals.size());
        }
    }

    @Test
    public void testRecordExtraOrdinalsWithMultiplePostings()
    {
        // Test recordExtraOrdinals with multiple postings
        try (ChronicleMap<Integer, CompactionVectorPostings> map = ChronicleMapBuilder
                .of(Integer.class, CompactionVectorPostings.class)
                .entries(10)
                .averageValueSize(100)
                .valueMarshaller(marshaller)
                .create())
        {
            int key = 1;
            int ordinal = 100;
            // First rowId must equal ordinal for ONE_TO_MANY
            List<Integer> rowIds = List.of(100, 200, 300, 400);
            CompactionVectorPostings postings = new CompactionVectorPostings(ordinal, rowIds);
            
            map.put(key, postings);

            Int2IntHashMap extraOrdinals = new Int2IntHashMap(-1);
            
            map.forEachEntry(entry -> {
                Marshaller.recordExtraOrdinals(entry, extraOrdinals);
            });

            // Should have 3 extra ordinals (all except the first one)
            assertEquals(3, extraOrdinals.size());
            assertEquals(ordinal, extraOrdinals.get(200));
            assertEquals(ordinal, extraOrdinals.get(300));
            assertEquals(ordinal, extraOrdinals.get(400));
        }
    }

    @Test
    public void testRecordExtraOrdinalsWithMultipleEntries()
    {
        // Test recordExtraOrdinals with multiple map entries
        try (ChronicleMap<Integer, CompactionVectorPostings> map = ChronicleMapBuilder
                .of(Integer.class, CompactionVectorPostings.class)
                .entries(10)
                .averageValueSize(100)
                .valueMarshaller(marshaller)
                .create())
        {
            // Entry 1
            int ordinal1 = 10;
            List<Integer> rowIds1 = List.of(10, 20, 30);
            map.put(1, new CompactionVectorPostings(ordinal1, rowIds1));

            // Entry 2
            int ordinal2 = 40;
            List<Integer> rowIds2 = List.of(40, 50);
            map.put(2, new CompactionVectorPostings(ordinal2, rowIds2));

            Int2IntHashMap extraOrdinals = new Int2IntHashMap(-1);
            
            map.forEachEntry(entry -> {
                Marshaller.recordExtraOrdinals(entry, extraOrdinals);
            });

            // Should have 3 extra ordinals total (2 from first entry, 1 from second)
            assertEquals(3, extraOrdinals.size());
            assertEquals(ordinal1, extraOrdinals.get(20));
            assertEquals(ordinal1, extraOrdinals.get(30));
            assertEquals(ordinal2, extraOrdinals.get(50));
        }
    }

    @Test
    public void testRoundTripWithZeroOrdinal()
    {
        // Test edge case with ordinal = 0
        int ordinal = 0;
        int rowId = 0;
        CompactionVectorPostings postings = new CompactionVectorPostings(ordinal, rowId);

        Bytes<?> bytes = Bytes.allocateElasticOnHeap();
        marshaller.write(bytes, postings);

        bytes.readPosition(0);
        CompactionVectorPostings read = marshaller.read(bytes, null);

        assertEquals(ordinal, read.getOrdinal());
        assertEquals(1, read.size());
        assertEquals(rowId, read.getPostings().get(0).intValue());
        
        bytes.releaseLast();
    }

    @Test
    public void testSerializedSizeConsistency()
    {
        // Verify that the serialized size is consistent and predictable
        int ordinal = 100;
        List<Integer> rowIds = List.of(1, 2, 3, 4, 5);
        CompactionVectorPostings postings = new CompactionVectorPostings(ordinal, rowIds);

        Bytes<?> bytes = Bytes.allocateElasticOnHeap();
        marshaller.write(bytes, postings);

        // Expected size: 4 bytes (ordinal) + 4 bytes (size) + 5 * 4 bytes (row IDs) = 28 bytes
        long expectedSize = 4 + 4 + (5 * 4);
        assertEquals(expectedSize, bytes.writePosition());
        
        bytes.releaseLast();
    }
}

// Made with Bob
