package org.apache.cassandra.index.sai.disk.v1;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import org.junit.Test;

import org.agrona.collections.LongArrayList;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import static org.apache.cassandra.index.sai.disk.v1.BKDReaderTest.buildQuery;
import static org.apache.cassandra.index.sai.metrics.QueryEventListeners.NO_OP_BKD_LISTENER;

public class SortedPostingsTest extends NdiRandomizedTest
{
    public static class Row
    {
        public final long rowID;
        public final Comparable[] array;

        public Row(long rowID, Comparable... array)
        {
            this.rowID = rowID;
            this.array = array;
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                              .add("rowID", rowID)
                              .add("array", array)
                              .toString();
        }
    }

    public static LongArrayList find(int min, int max, int sortCol, int filterCol, List<Row> rows)
    {
        LongArrayList rowids = new LongArrayList();

        List<Row> rowsCopy = new ArrayList<>(rows);
        Collections.sort(rowsCopy, (o1, o2) -> o1.array[sortCol].compareTo(o2.array[sortCol]));

        final List<Row> filteredRows = rows.stream().filter(o -> ((Number)o.array[filterCol]).intValue() >= min && ((Number)o.array[filterCol]).intValue() <= max).collect(Collectors.toList());
        System.out.println("find filteredRows="+filteredRows);
        for (Row row : filteredRows)
        {
            rowids.add(row.rowID);
        }
        return rowids;
    }

    public BKDReader create(int col, List<Row> rows) throws IOException
    {
        List<Row> rowsCopy = new ArrayList<>(rows);
        Collections.sort(rowsCopy, (o1, o2) -> {
            int cmp = o1.array[col].compareTo(o2.array[col]);
            if (cmp == 0)
            {
                return Long.compare(o1.rowID, o2.rowID);
            }
            return cmp;
        });

        final IndexComponents indexComponents = newIndexComponents();
        byte[] scratch = new byte[5];
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, 5);
        long maxRowID = -1;
        for (Row row : rowsCopy)
        {
            maxRowID = Math.max(row.rowID, maxRowID);
            //                  value, rowid
            int intVal = ((Number) row.array[col]).intValue();
            NumericUtils.intToSortableBytes(intVal, scratch, 1);
            if (intVal == Integer.MAX_VALUE)
            {
                BKDWriter.genMissingValue(scratch);
            }
            buffer.addPackedValue((int) row.rowID, new BytesRef(scratch));
        }

        BKDReader reader = BKDReaderTest.finishAndOpenReaderOneDim(10, buffer, indexComponents, maxRowID+1);

        final MetadataWriter metaWriter = new MetadataWriter(indexComponents);

        final BKDRowOrdinalsWriter rowOrdinalsWriter = new BKDRowOrdinalsWriter(reader,
                                                                                indexComponents,
                                                                                metaWriter);
        metaWriter.close();
        return reader;
    }

    @Test
    public void test() throws Exception
    {
        int numRows = 1001;

        final List<Row> rows = new ArrayList();
        //int i = 0;
        File file = new File("/tmp/DSP-19512-test-rows.csv");
        System.out.println("file.exists="+file.exists());
//        if (!file.exists())
//        {
        FileWriter fileWriter = new FileWriter(file);

        for (long docID = numRows; docID >= 0; docID--)
        {
            int i = nextInt(0, 1000);
            if (nextInt(0, 10) == 0)
            {
                //i = 9000;
                i = Integer.MAX_VALUE;
            }
            rows.add(new Row(docID, i, docID));
            fileWriter.write(docID + "," + i + "," + docID+"\n");
        }
        fileWriter.close();
//        }
//        else
//        {
//            List<String> lines = FileUtils.readLines(file);
//
//            //assert numRows == lines.size() : "lines.size="+lines.size();
//
//            for (int x=0; x < numRows; x++)
//            {
//                String line = lines.get(x);
//                String[] strs = line.split(",");
//                rows.add(new Row(Integer.parseInt(strs[0]), Integer.parseInt(strs[1]), Integer.parseInt(strs[2])));
//            }
//        }

        BKDReader reader = create(0, rows);

//        List<BKDReader.RowIDAndPointIDIterator> list = reader.rowIDIterators();
//
//        RowIDPointIDMergeIterator merger = new RowIDPointIDMergeIterator(list.toArray(new BKDReader.RowIDAndPointIDIterator[0]));
//
//        while (merger.hasNext())
//        {
//            BKDReader.RowIDAndPointID obj = merger.next();
//            System.out.println("obj="+obj);
//        }
//
//        merger.close();

//        int lfidx = 0;
//        for (Map.Entry<Long, Integer> entry : reader.leafFPToLeafNode.entrySet())
//        {
//            IndexInput bkdInput = reader.indexComponents.openInput(reader.kdtreeFile);
//            IndexInput bkdPostingsInput = reader.indexComponents.openInput(reader.postingsFile);
//            Iterator<BKDReader.RowIDAndPointID> iterator = reader.leafRowIDIterator(lfidx++,
//                                                                        entry.getValue(),
//                                                                        entry.getKey(),
//                                                                        bkdInput,
//                                                                        bkdPostingsInput);
//            while (iterator.hasNext())
//            {
//                BKDReader.RowIDAndPointID obj = iterator.next();
//                System.out.println("obj="+obj);
//            }
//            bkdInput.close();
//            bkdPostingsInput.close();
//        }

        final BlockPackedReader rowIDToPointIDReader = BKDRowOrdinalsWriter.openReader(reader.indexComponents, MetadataSource.loadColumnMetadata(reader.indexComponents));
        final LongArray rowIDToPointIDMap = rowIDToPointIDReader.open();

        BKDReader reader2 = create(1, rows);

        final SortedPostingsWriter sortedPostingsWriter = new SortedPostingsWriter(reader2);

        IndexOutputWriter sortedOrderMapOut = reader2.indexComponents.createOutput(reader2.indexComponents.termsData);

        final long postingsIndexFilePointer = sortedPostingsWriter.finish(() -> {
                                                                              try
                                                                              {
                                                                                  return reader2.indexComponents.createOutput(reader2.indexComponents.postingLists);
                                                                              }
                                                                              catch (Exception ex)
                                                                              {
                                                                                  throw new RuntimeException(ex);
                                                                              }
                                                                          },
                                                                          sortedOrderMapOut,
                                                                          () -> {
                                                                              FileHandle handle = reader2.indexComponents.createFileHandle(reader2.indexComponents.postingLists);
                                                                              return reader2.indexComponents.openInput(handle);
                                                                          },
                                                                          (rowID -> rowIDToPointIDMap.get(rowID)));

        rowIDToPointIDMap.close();
        sortedOrderMapOut.close();

        String postingIndexName = "first";

        SortedPostingsIndex sortedPostingsIndex = new SortedPostingsIndex(reader2.indexComponents.openBlockingInput(reader2.indexComponents.postingLists), postingsIndexFilePointer);
        reader2.postingIndexMap.put(postingIndexName, sortedPostingsIndex);

        int min = nextInt(0, numRows/2);
        int max = nextInt(numRows/2, numRows);

        System.out.println("min=" + min + " max=" + max);

        BKDReader.IntersectVisitor visitor = buildQuery(min, max);

        final List<PostingList.PeekablePostingList> pointIDLists = reader2.intersect(visitor,
                                                                                     NO_OP_BKD_LISTENER,
                                                                                     new QueryContext(),
                                                                                     postingIndexName,
                                                                                     new BKDReader.SortedPostingInputs()
                                                                                     {
                                                                                         @Override
                                                                                         public IndexInput openPostingsInput() throws IOException
                                                                                         {
                                                                                             return reader2.indexComponents.openBlockingInput(reader2.indexComponents.postingLists);
                                                                                         }

                                                                                         @Override
                                                                                         public IndexInput openOrderMapInput() throws IOException
                                                                                         {
                                                                                             return reader2.indexComponents.openBlockingInput(reader2.indexComponents.termsData);
                                                                                         }
                                                                                     });
        PriorityQueue<PostingList.PeekablePostingList> queue = new PriorityQueue(Comparator.comparingLong(PostingList.PeekablePostingList::peek));
        queue.addAll(pointIDLists);
        PostingList pointIDList = MergePostingList.merge(queue);

        BKDReader.IteratorState iterator = reader.iteratorState();

        LongArrayList rowIDs = new LongArrayList();
        while (true)
        {
            long pointID = pointIDList.nextPosting();

            if (pointID == PostingList.END_OF_STREAM) break;

            iterator.seekTo(pointID);

            byte[] termBytes = iterator.getRawTermValue();

            int value = NumericUtils.sortableBytesToInt(termBytes, 1);
            final long rowID = iterator.rowID();

            System.out.println("termBytes=" + Arrays.toString(termBytes) + " pointID=" + pointID + " value=" + value + " rowID=" + rowID);

            rowIDs.add(rowID);
        }
        iterator.close();

        LongArrayList checkRowIDs = find(min, max, 0, 1, rows);

        reader2.close();

        for (PostingList.PeekablePostingList list : pointIDLists)
        {
            list.close();
        }

        checkRowIDs.sort(Comparator.naturalOrder());
        rowIDs.sort(Comparator.naturalOrder());

        System.out.println("rowIDs1=" + checkRowIDs);
        System.out.println("rowIDs2=" + rowIDs);

        assertArrayEquals(checkRowIDs.toLongArray(), rowIDs.toLongArray());
    }
}