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

package org.apache.cassandra.index.sai.disk.v4;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class V4OnDiskFormat extends V3OnDiskFormat
{
    public static final V4OnDiskFormat instance = new V4OnDiskFormat();

    @Override
    public IndexSearcher newIndexSearcher(SSTableContext sstableContext,
                                          IndexContext indexContext,
                                          PerIndexFiles indexFiles,
                                          SegmentMetadata segmentMetadata) throws IOException
    {
        if (indexContext.isLiteral())
            return new V4InvertedIndexSearcher(sstableContext.primaryKeyMapFactory(), indexFiles, segmentMetadata, sstableContext.indexDescriptor, indexContext);
        return super.newIndexSearcher(sstableContext, indexContext, indexFiles, segmentMetadata);
    }

    @Override
    public ByteComparable encodeForInMemoryTrie(ByteBuffer input, AbstractType<?> type)
    {
        // Composite types use their individual type to ensure they sorted correctly in the trie so we can do
        // range queries over entries.
        return TypeUtil.isLiteral(type) && !TypeUtil.isComposite(type)
               ? version -> ByteSource.appendTerminator(ByteSource.of(input, version), ByteSource.TERMINATOR)
               : TypeUtil.asComparableBytes(input, type);
    }

    @Override
    public ByteComparable convertFromInMemoryToOnDiskEncoding(ByteComparable term, AbstractType<?> type)
    {
        // Composite types are not escaped, so we don't need to unescape them. The ByteSource.of call above
        // is what requires unescaping. Note unescape removes the terminator for affected types.
        return TypeUtil.isLiteral(type) && !TypeUtil.isComposite(type)
               ? v -> ByteSourceInverse.unescape(ByteSource.peekable(term.asComparableBytes(v)))
               : term;
    }

    @Override
    public ByteComparable encodeForOnDiskTrie(ByteBuffer term, AbstractType<?> type)
    {
        // Note that fixedLength is the same as unescape(escape(input, type), type), but we skip some steps, so this
        // is a bit faster.
        return TypeUtil.isLiteral(type) && !TypeUtil.isComposite(type)
               ? ByteComparable.fixedLength(term)
               : TypeUtil.asComparableBytes(term, type);
    }
}