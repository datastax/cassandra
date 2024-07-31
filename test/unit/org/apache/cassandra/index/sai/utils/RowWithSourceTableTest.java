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

import java.nio.ByteBuffer;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.rows.ArrayCell;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.HeapCloner;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class RowWithSourceTableTest {

    private RowWithSourceTable rowWithSourceTable;
    private TableMetadata tableMetadata;
    private ColumnMetadata complexColumn;
    private ColumnMetadata column;
    private CellPath complexCellPath;
    private Cell<?> complexCell;
    private Cell<?> cell;
    private Row originalRow;
    private final Object source = new Object();
    // We use a 4 byte array because the Int32Type is used in the test
    private final byte[] value = new byte[]{0,0,0,1};

    @Before
    public void setUp()
    {
        var listType = ListType.getInstance(Int32Type.instance, true);
        complexColumn = ColumnMetadata.regularColumn("keyspace1", "table1", "complex", listType);
        column = ColumnMetadata.regularColumn("keyspace1", "table1", "name1", Int32Type.instance);
        tableMetadata = TableMetadata.builder("keyspace1", "table1")
                                     .addPartitionKeyColumn("pk", Int32Type.instance)
                                     .addColumn(complexColumn)
                                     .addColumn(column).build();
        complexCellPath = CellPath.create(ByteBuffer.allocate(0));
        complexCell = new ArrayCell(complexColumn, System.currentTimeMillis(), Cell.NO_TTL, Cell.NO_DELETION_TIME, value, complexCellPath);
        cell = new ArrayCell(column, System.currentTimeMillis(), Cell.NO_TTL, Cell.NO_DELETION_TIME, value, null);
        // Use unsorted builder to avoid the need to manually sort cells here
        var builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.EMPTY);
        builder.addCell(complexCell);
        builder.addCell(cell);
        originalRow = builder.build();
        rowWithSourceTable = new RowWithSourceTable(originalRow, source);
    }

    @Test
    public void testKind()
    {
        assertEquals(originalRow.kind(), rowWithSourceTable.kind());
    }

    @Test
    public void testClustering()
    {
        assertEquals(originalRow.clustering(), rowWithSourceTable.clustering());
    }

    @Test
    public void testDigest()
    {
        var digest1 = Digest.forValidator();
        rowWithSourceTable.digest(digest1);
        var digest2 = Digest.forValidator();
        originalRow.digest(digest2);
        assertArrayEquals(digest1.digest(), digest2.digest());
    }

    @Test
    public void testValidateData()
    {
        rowWithSourceTable.validateData(tableMetadata);
    }

    @Test
    public void testHasInvalidDeletions()
    {
        assertFalse(rowWithSourceTable.hasInvalidDeletions());
    }

    @Test
    public void testColumns()
    {
        assertEquals(2, rowWithSourceTable.columns().size());
        assertTrue(rowWithSourceTable.columns().contains(complexColumn));
        assertTrue(rowWithSourceTable.columns().contains(column));
    }

    @Test
    public void testColumnCount()
    {
        assertEquals(2, rowWithSourceTable.columnCount());
        assertEquals(originalRow.columnCount(), rowWithSourceTable.columnCount());
    }

    @Test
    public void testDeletion()
    {
        assertEquals(originalRow.deletion(), rowWithSourceTable.deletion());
    }

    @Test
    public void testPrimaryKeyLivenessInfo()
    {
        assertEquals(originalRow.primaryKeyLivenessInfo(), rowWithSourceTable.primaryKeyLivenessInfo());
    }

    @Test
    public void testIsStatic()
    {
        assertEquals(originalRow.isStatic(), rowWithSourceTable.isStatic());
    }

    @Test
    public void testIsEmpty()
    {
        assertFalse(rowWithSourceTable.isEmpty());
        assertEquals(originalRow.isEmpty(), rowWithSourceTable.isEmpty());
    }

    @Test
    public void testToString()
    {
       assertEquals(originalRow.toString(tableMetadata), rowWithSourceTable.toString(tableMetadata));
    }

    @Test
    public void testHasLiveData()
    {
        assertTrue(originalRow.hasLiveData(1000, false));
        assertTrue(rowWithSourceTable.hasLiveData(1000, false));
    }

    @Test
    public void testGetCellWithCorrectColumn()
    {
        var resultCell = rowWithSourceTable.getCell(column);
        assertTrue(resultCell instanceof CellWithSourceTable);
        // This mapping is the whole point of these two classes.
        assertSame(source, ((CellWithSourceTable<?>)resultCell).sourceTable());
        assertSame(cell.value(), resultCell.value());
    }

    @Test
    public void testGetCellWithMissingColumn()
    {
        var diffCol = ColumnMetadata.regularColumn("keyspace1", "table1", "name2", Int32Type.instance);
        assertNull(rowWithSourceTable.getCell(diffCol));
    }

    @Test
    public void testGetCellWithPath()
    {
        Cell<?> resultCell = rowWithSourceTable.getCell(complexColumn, complexCellPath);
        assertTrue(resultCell instanceof CellWithSourceTable);
        // This mapping is the whole point of these two classes.
        assertSame(source, ((CellWithSourceTable<?>)resultCell).sourceTable());
        assertSame(cell.value(), resultCell.value());
    }

    @Test
    public void testGetComplexColumnData()
    {
        var complexColumnData = rowWithSourceTable.getComplexColumnData(complexColumn);
        var firstCell = complexColumnData.iterator().next();
        assertTrue(firstCell instanceof CellWithSourceTable);
        assertSame(source, ((CellWithSourceTable<?>)firstCell).sourceTable());
    }

    @Test
    public void testGetColumnData()
    {
        var simpleColumnData = rowWithSourceTable.getColumnData(column);
        assertTrue(simpleColumnData instanceof CellWithSourceTable);
        assertSame(source, ((CellWithSourceTable<?>)simpleColumnData).sourceTable());
        var complexColumnData = rowWithSourceTable.getColumnData(complexColumn);
        assertTrue(complexColumnData instanceof ComplexColumnData);
        var firstCell = ((ComplexColumnData)complexColumnData).iterator().next();
        assertTrue(firstCell instanceof CellWithSourceTable);
        assertSame(source, ((CellWithSourceTable<?>)firstCell).sourceTable());
    }

    @Test
    public void testCells()
    {
        var cells = originalRow.cells().iterator();
        var wrappedCells = rowWithSourceTable.cells().iterator();
        while (cells.hasNext())
        {
            var cell = cells.next();
            var wrappedCell = wrappedCells.next();
            assertTrue(wrappedCell instanceof CellWithSourceTable);
            assertSame(source, ((CellWithSourceTable<?>)wrappedCell).sourceTable());
            assertSame(cell.value(), wrappedCell.value());
        }
        assertFalse(wrappedCells.hasNext());
    }

    @Test
    public void testColumnData()
    {
        var columnDataCollection = rowWithSourceTable.columnData();
        assertEquals(2, columnDataCollection.size());
        var iter = columnDataCollection.iterator();
        while (iter.hasNext())
        {
            var columnData = iter.next();
            if (columnData instanceof CellWithSourceTable)
            {
                assertSame(source, ((CellWithSourceTable<?>)columnData).sourceTable());
            }
            else if (columnData instanceof ComplexColumnData)
            {
                var complexIter = ((ComplexColumnData)columnData).iterator();
                while (complexIter.hasNext())
                {
                    var cell = complexIter.next();
                    assertTrue(cell instanceof CellWithSourceTable);
                    assertSame(source, ((CellWithSourceTable<?>)cell).sourceTable());
                }
            }
            else
            {
                fail("Unexpected column data type");
            }
        }

    }

    @Test
    public void testCellsInLegacyOrder()
    {
        var cells = originalRow.cellsInLegacyOrder(tableMetadata, false).iterator();
        var wrappedCells = rowWithSourceTable.cellsInLegacyOrder(tableMetadata, false).iterator();
        while (cells.hasNext())
        {
            var cell = cells.next();
            var wrappedCell = wrappedCells.next();
            assertTrue(wrappedCell instanceof CellWithSourceTable);
            assertSame(source, ((CellWithSourceTable<?>)wrappedCell).sourceTable());
            assertSame(cell.value(), wrappedCell.value());
        }
        assertFalse(wrappedCells.hasNext());
    }

    @Test
    public void testHasComplexDeletion()
    {
        assertFalse(rowWithSourceTable.hasComplexDeletion());
    }

    @Test
    public void testHasComplex()
    {
        assertTrue(rowWithSourceTable.hasComplex());
    }

    @Test
    public void testHasDeletion()
    {
        assertFalse(rowWithSourceTable.hasDeletion(1000));
    }

    @Test
    public void testSearchIterator()
    {
        var iterator = rowWithSourceTable.searchIterator();
        var columnData = iterator.next(column);
        assertTrue(columnData instanceof CellWithSourceTable);
        assertSame(source, ((CellWithSourceTable<?>)columnData).sourceTable());
        assertNull(iterator.next(column));
    }

    @Test
    public void testFilter()
    {
        assertSame(rowWithSourceTable, rowWithSourceTable.filter(ColumnFilter.all(tableMetadata), tableMetadata));
    }

    @Test
    public void testFilterWithDeletion()
    {
        assertSame(rowWithSourceTable, rowWithSourceTable.filter(ColumnFilter.all(tableMetadata), DeletionTime.LIVE, true, tableMetadata));
    }

    @Test
    public void testTransformAndFilter()
    {
        assertSame(rowWithSourceTable, rowWithSourceTable.transformAndFilter(LivenessInfo.EMPTY, Row.Deletion.LIVE, c -> c));
    }

    @Test
    public void testTransformAndFilterWithFunction() 
    {
        assertNull(rowWithSourceTable.transformAndFilter(c -> null));
        assertSame(rowWithSourceTable, rowWithSourceTable.transformAndFilter(c -> c));
    }

    @Test
    public void testClone()
    {
        assertTrue(rowWithSourceTable.clone(HeapCloner.instance) instanceof RowWithSourceTable);
    }

    @Test
    public void testDataSize()
    {
        assertEquals(originalRow.dataSize(), rowWithSourceTable.dataSize());
    }

    @Test
    public void testUnsharedHeapSizeExcludingData()
    {
        var wrapperSize = ObjectSizes.measure(new RowWithSourceTable(null, null));
        assertEquals(originalRow.unsharedHeapSizeExcludingData() + wrapperSize,
                     rowWithSourceTable.unsharedHeapSizeExcludingData());
    }

}
