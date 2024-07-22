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

package org.apache.cassandra.index.sai.utils;

import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.ArrayCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.memory.HeapCloner;

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CellWithSourceTableTest {

    private ColumnMetadata column;
    private Cell<?> wrappedCell;
    private Object sourceTable;
    private CellWithSourceTable<?> cellWithSourceTable;

    private final long timestamp = System.currentTimeMillis();
    // We use a 4 byte array because the Int32Type is used in the test
    private final byte[] value = new byte[]{0,0,0,1};

    @Before
    public void setUp()
    {
        column = ColumnMetadata.regularColumn("keyspace1", "table1", "name1", Int32Type.instance);
        wrappedCell = new ArrayCell(column, timestamp, Cell.NO_TTL, Cell.NO_DELETION_TIME, value, null);
        sourceTable = new Object();
        cellWithSourceTable = new CellWithSourceTable<>(wrappedCell, sourceTable);
    }

    @Test
    public void testSourceTable()
    {
        assertEquals(sourceTable, cellWithSourceTable.sourceTable());
    }

    @Test
    public void testIsCounterCell()
    {
        assertEquals(wrappedCell.isCounterCell(), cellWithSourceTable.isCounterCell());
    }

    @Test
    public void testValue()
    {
        assertEquals(wrappedCell.value(), cellWithSourceTable.value());
    }

    @Test
    public void testAccessor()
    {
        assertEquals(wrappedCell.accessor(), cellWithSourceTable.accessor());
    }

    @Test
    public void testTimestamp()
    {
        assertEquals(wrappedCell.timestamp(), cellWithSourceTable.timestamp());
    }

    @Test
    public void testTtl()
    {
        assertEquals(wrappedCell.ttl(), cellWithSourceTable.ttl());
    }

    @Test
    public void testLocalDeletionTime()
    {
        assertEquals(wrappedCell.localDeletionTime(), cellWithSourceTable.localDeletionTime());
    }

    @Test
    public void testIsTombstone()
    {
        assertEquals(wrappedCell.isTombstone(), cellWithSourceTable.isTombstone());
    }

    @Test
    public void testIsExpiring()
    {
        assertEquals(wrappedCell.isExpiring(), cellWithSourceTable.isExpiring());
    }

    @Test
    public void testIsLive()
    {
        var nowInSec = 0;
        assertEquals(wrappedCell.isLive(nowInSec), cellWithSourceTable.isLive(nowInSec));
    }

    @Test
    public void testPath()
    {
        assertEquals(wrappedCell.path(), cellWithSourceTable.path());
    }

    @Test
    public void testWithUpdatedColumn()
    {
        var originalColumn = cellWithSourceTable.column();
        var newColumn = ColumnMetadata.regularColumn("keyspace1", "table1", "name2", Int32Type.instance);
        var resultColumn = cellWithSourceTable.withUpdatedColumn(newColumn).column();
        assertNotEquals(originalColumn, resultColumn);
        assertEquals(newColumn, resultColumn);
    }

    @Test
    public void testWithUpdatedValue()
    {
        ByteBuffer newValue = ByteBuffer.allocate(4);
        var oldValue = cellWithSourceTable.value();
        var resultValue = cellWithSourceTable.withUpdatedValue(newValue).value();
        assertNotEquals(oldValue, resultValue);
        assertTrue(resultValue instanceof byte[]);
        assertArrayEquals(newValue.array(), (byte[]) resultValue);
    }

    @Test
    public void testWithUpdatedTimestampAndLocalDeletionTime()
    {
        long newTimestamp = 1234567890L;
        int newLocalDeletionTime = 987654321;
        var originalTimestamp = cellWithSourceTable.timestamp();
        var originalDeletionTime = cellWithSourceTable.localDeletionTime();
        var resultTimestamp = cellWithSourceTable.withUpdatedTimestampAndLocalDeletionTime(newTimestamp, newLocalDeletionTime).timestamp();
        var resultDeletionTime = cellWithSourceTable.withUpdatedTimestampAndLocalDeletionTime(newTimestamp, newLocalDeletionTime).localDeletionTime();
        assertNotEquals(originalTimestamp, resultTimestamp);
        assertEquals(newTimestamp, resultTimestamp);
        assertNotEquals(originalDeletionTime, resultDeletionTime);
        assertEquals(newLocalDeletionTime, resultDeletionTime);
    }

    @Test
    public void testWithSkippedValue()
    {
        var originalValue = cellWithSourceTable.value();
        var resultValue = cellWithSourceTable.withSkippedValue().value();
        assertNotEquals(originalValue, resultValue);
    }

    @Test
    public void testClone()
    {
        var resultClone = cellWithSourceTable.clone(HeapCloner.instance);
        // The reference is not equal here because we have a non-zero length value
        assertNotSame(cellWithSourceTable, resultClone);
        // Now make the value 0 length and we should get the same reference
        var skippedCell = cellWithSourceTable.withSkippedValue();
        var clonedSkippedCell = skippedCell.clone(HeapCloner.instance);
        assertSame(skippedCell, clonedSkippedCell);
    }

    @Test
    public void testDataSize()
    {
        assertEquals(wrappedCell.dataSize(), cellWithSourceTable.dataSize());
    }

    @Test
    public void testUnsharedHeapSizeExcludingData()
    {
        assertEquals(wrappedCell.unsharedHeapSizeExcludingData(), cellWithSourceTable.unsharedHeapSizeExcludingData());
    }

    @Test
    public void testValidate()
    {
        wrappedCell.validate();
        cellWithSourceTable.validate();
    }

    @Test
    public void testHasInvalidDeletions()
    {
        assertEquals(wrappedCell.hasInvalidDeletions(), cellWithSourceTable.hasInvalidDeletions());
    }

    @Test
    public void testDigest()
    {
        var digest1 = Digest.forValidator();
        cellWithSourceTable.digest(digest1);
        var digest2 = Digest.forValidator();
        wrappedCell.digest(digest2);
        assertArrayEquals(digest1.digest(), digest2.digest());
    }

    @Test
    public void testUpdateAllTimestamp()
    {
        long newTimestamp = 1234567890L;
        var resultData = cellWithSourceTable.updateAllTimestamp(newTimestamp);
        assertEquals(newTimestamp, resultData.minTimestamp());
        assertEquals(newTimestamp, resultData.minTimestamp());
    }

    @Test
    public void testMarkCounterLocalToBeCleared()
    {
        var resultCell = cellWithSourceTable.markCounterLocalToBeCleared();
        assertSame(cellWithSourceTable, resultCell);
    }

    @Test
    public void testPurge()
    {
        DeletionPurger purger = mock(DeletionPurger.class);
        var mockCell = mock(Cell.class);
        int purgeNull = 1234567890;
        int purgeSame = 98765;
        when(mockCell.purge(any(), eq(purgeNull))).thenReturn(null);
        when(mockCell.purge(any(), eq(purgeSame))).thenReturn(mockCell);
        var cell = new CellWithSourceTable<>(mockCell, sourceTable);
        assertNull(cell.purge(purger, purgeNull));
        assertSame(cell, cell.purge(purger, purgeSame));
    }

    @Test
    public void testMaxTimestamp()
    {
        assertEquals(timestamp, cellWithSourceTable.maxTimestamp());
    }

    @Test
    public void testMinTimestamp()
    {
        assertEquals(timestamp, cellWithSourceTable.minTimestamp());
    }
}