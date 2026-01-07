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

import org.apache.cassandra.db.CellSourceIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.rows.ArrayCell;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.HeapCloner;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class RowWithSourceTest {

    private RowWithSource rowWithSource;
    private TableMetadata tableMetadata;
    private ColumnMetadata complexColumn;
    private ColumnMetadata column;
    private CellPath complexCellPath;
    private Cell<?> complexCell;
    private Cell<?> cell;
    private Row originalRow;
    static final CellSourceIdentifier source = new SSTableId()
    {
        @Override
        public int compareTo(Object o)
        {
            return 0;
        }

        @Override
        public ByteBuffer asBytes()
        {
            return null;
        }

        @Override
        public boolean isEqualSource(CellSourceIdentifier other)
        {
            return other == this;
        }
    };
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
        rowWithSource = new RowWithSource(originalRow, source);
    }

    @Test
    public void testKind()
    {
        assertEquals(originalRow.kind(), rowWithSource.kind());
    }

    @Test
    public void testClustering()
    {
        assertEquals(originalRow.clustering(), rowWithSource.clustering());
    }

    @Test
    public void testDigest()
    {
        var digest1 = Digest.forValidator();
        rowWithSource.digest(digest1);
        var digest2 = Digest.forValidator();
        originalRow.digest(digest2);
        assertArrayEquals(digest1.digest(), digest2.digest());
    }

    @Test
    public void testValidateData()
    {
        rowWithSource.validateData(tableMetadata);
    }

    @Test
    public void testHasInvalidDeletions()
    {
        assertFalse(rowWithSource.hasInvalidDeletions());
    }

    @Test
    public void testColumns()
    {
        assertEquals(2, rowWithSource.columns().size());
        assertTrue(rowWithSource.columns().contains(complexColumn));
        assertTrue(rowWithSource.columns().contains(column));
    }

    @Test
    public void testColumnCount()
    {
        assertEquals(2, rowWithSource.columnCount());
        assertEquals(originalRow.columnCount(), rowWithSource.columnCount());
    }

    @Test
    public void testDeletion()
    {
        assertEquals(originalRow.deletion(), rowWithSource.deletion());
    }

    @Test
    public void testPrimaryKeyLivenessInfo()
    {
        assertEquals(originalRow.primaryKeyLivenessInfo(), rowWithSource.primaryKeyLivenessInfo());
    }

    @Test
    public void testIsStatic()
    {
        assertEquals(originalRow.isStatic(), rowWithSource.isStatic());
    }

    @Test
    public void testIsEmpty()
    {
        assertFalse(rowWithSource.isEmpty());
        assertEquals(originalRow.isEmpty(), rowWithSource.isEmpty());
    }

    @Test
    public void testToString()
    {
       assertEquals(originalRow.toString(tableMetadata), rowWithSource.toString(tableMetadata));
    }

    @Test
    public void testHasLiveData()
    {
        assertTrue(originalRow.hasLiveData(1000, false));
        assertTrue(rowWithSource.hasLiveData(1000, false));
    }

    @Test
    public void testGetCellWithCorrectColumn()
    {
        var resultCell = rowWithSource.getCell(column);
        assertTrue(resultCell instanceof CellWithSource);
        // This mapping is the whole point of these two classes.
        assertSame(source, ((CellWithSource<?>)resultCell).sourceTable());
        assertSame(cell.value(), resultCell.value());
    }

    @Test
    public void testGetCellWithMissingColumn()
    {
        var diffCol = ColumnMetadata.regularColumn("keyspace1", "table1", "name2", Int32Type.instance);
        assertNull(rowWithSource.getCell(diffCol));
    }

    @Test
    public void testGetCellWithPath()
    {
        Cell<?> resultCell = rowWithSource.getCell(complexColumn, complexCellPath);
        assertTrue(resultCell instanceof CellWithSource);
        // This mapping is the whole point of these two classes.
        assertSame(source, ((CellWithSource<?>)resultCell).sourceTable());
        assertSame(cell.value(), resultCell.value());
    }

    @Test
    public void testGetComplexColumnData()
    {
        var complexColumnData = rowWithSource.getComplexColumnData(complexColumn);
        var firstCell = complexColumnData.iterator().next();
        assertTrue(firstCell instanceof CellWithSource);
        assertSame(source, ((CellWithSource<?>)firstCell).sourceTable());
    }

    @Test
    public void testGetColumnData()
    {
        var simpleColumnData = rowWithSource.getColumnData(column);
        assertTrue(simpleColumnData instanceof CellWithSource);
        assertSame(source, ((CellWithSource<?>)simpleColumnData).sourceTable());
        var complexColumnData = rowWithSource.getColumnData(complexColumn);
        assertTrue(complexColumnData instanceof ComplexColumnData);
        var firstCell = ((ComplexColumnData)complexColumnData).iterator().next();
        assertTrue(firstCell instanceof CellWithSource);
        assertSame(source, ((CellWithSource<?>)firstCell).sourceTable());
    }

    @Test
    public void testCells()
    {
        var cells = originalRow.cells().iterator();
        var wrappedCells = rowWithSource.cells().iterator();
        while (cells.hasNext())
        {
            var cell = cells.next();
            var wrappedCell = wrappedCells.next();
            assertTrue(wrappedCell instanceof CellWithSource);
            assertSame(source, ((CellWithSource<?>)wrappedCell).sourceTable());
            assertSame(cell.value(), wrappedCell.value());
        }
        assertFalse(wrappedCells.hasNext());
    }

    @Test
    public void testColumnData()
    {
        var columnDataCollection = rowWithSource;
        assertEquals(2, columnDataCollection.columnCount());
        var iter = columnDataCollection.iterator();
        while (iter.hasNext())
        {
            var columnData = iter.next();
            if (columnData instanceof CellWithSource)
            {
                assertSame(source, ((CellWithSource<?>)columnData).sourceTable());
            }
            else if (columnData instanceof ComplexColumnData)
            {
                var complexIter = ((ComplexColumnData)columnData).iterator();
                while (complexIter.hasNext())
                {
                    var cell = complexIter.next();
                    assertTrue(cell instanceof CellWithSource);
                    assertSame(source, ((CellWithSource<?>)cell).sourceTable());
                }
            }
            else
            {
                fail("Unexpected column data type");
            }
        }

    }

    @Test
    public void testHasComplexDeletion()
    {
        assertFalse(rowWithSource.hasComplexDeletion());
    }

    @Test
    public void testHasDeletion()
    {
        assertFalse(rowWithSource.hasDeletion(1000));
    }

    @Test
    public void testFilter()
    {
        assertSame(rowWithSource, rowWithSource.filter(ColumnFilter.all(tableMetadata), tableMetadata));
    }

    @Test
    public void testFilterWithDeletion()
    {
        assertSame(rowWithSource, rowWithSource.filter(ColumnFilter.all(tableMetadata), DeletionTime.LIVE, true, tableMetadata));
    }

    @Test
    public void testTransformAndFilter()
    {
        assertNull(rowWithSource.transformAndFilter(li -> li, RowWithSourceTest::toNull));
        assertSame(rowWithSource, rowWithSource.transformAndFilter(li -> li, RowWithSourceTest::unchanged));
    }

    private static <C extends CellData<?, C>> C toNull(C c)
    {
        return null;
    }

    private static <C extends CellData<?, C>> C unchanged(C c)
    {
        return c;
    }

    @Test
    public void testClone()
    {
        assertTrue(rowWithSource.clone(HeapCloner.instance) instanceof RowWithSource);
    }

    @Test
    public void testDataSize()
    {
        assertEquals(originalRow.dataSize(), rowWithSource.dataSize());
    }

    @Test
    public void testUnsharedHeapSizeExcludingData()
    {
        var wrapperSize = ObjectSizes.measure(new RowWithSource(null, null));
        assertEquals(originalRow.unsharedHeapSizeExcludingData() + wrapperSize,
                     rowWithSource.unsharedHeapSizeExcludingData());
    }

}
