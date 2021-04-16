package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexOutput;

public class BKDRowOrdinalsWriter
{
    // note: BKDReader must be closed by the caller
    public BKDRowOrdinalsWriter(BKDReader reader,
                                IndexComponents components,
                                MetadataWriter metadataWriter) throws IOException
    {
        List<BKDReader.RowIDAndPointIDIterator> iterators = reader.rowIDIterators();
        RowIDPointIDMergeIterator merger = new RowIDPointIDMergeIterator(iterators.toArray(new BKDReader.RowIDAndPointIDIterator[0]));

        final IndexOutput output = components.createOutput(components.kdTreeRowOrdinals, true);

        //System.out.println("rowIDToPointOrdinal2="+Arrays.toString(rowIDToPointOrdinal2));

        final NumericValuesWriter writer = new NumericValuesWriter(components.kdTreeRowOrdinals,
                                                                   output,
                                                                   metadataWriter,
                                                                   false);

        while (merger.hasNext())
        {
            final BKDReader.RowIDAndPointID obj = merger.next();
            writer.add(obj.pointID);
        }

//        for (BKDReader.RowIDAndPointIDIterator iterator : iterators)
//        {
//            iterator.close();
//        }

        merger.close();

        writer.close();
        output.close();
    }

    public static BlockPackedReader openReader(IndexComponents components, MetadataSource meta) throws IOException
    {
        final FileHandle file = components.createFileHandle(components.kdTreeRowOrdinals);
        return new BlockPackedReader(file, components.kdTreeRowOrdinals, components, meta);
    }
}
