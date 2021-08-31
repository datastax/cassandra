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

package org.apache.cassandra.index.sai.disk.v2.blockindex;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.IndexSearcher;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.Segment;
import org.apache.cassandra.index.sai.disk.v1.trie.TermsReader;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Executes {@link Expression}s against the trie-based terms dictionary for an individual index segment.
 */
public class BlockIndexSearcher extends IndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    public BlockIndexSearcher(Segment segment, IndexContext indexContext) throws IOException
    {
        super(segment, indexContext);

        long root = metadata.getIndexRoot(IndexComponent.Type.TERMS_DATA);
        assert root >= 0;

        Map<String,String> map = metadata.componentMetadatas.get(IndexComponent.Type.TERMS_DATA).attributes;
        String footerPointerString = map.get(SAICodecUtils.FOOTER_POINTER);
        long footerPointer = footerPointerString == null ? -1 : Long.parseLong(footerPointerString);

//        reader = new TermsReader(indexContext,
//                                 segment.primaryKeyMap.copyOf(),
//                                 indexFiles.get(IndexComponent.Type.TERMS_DATA).sharedCopy(),
//                                 indexFiles.get(IndexComponent.Type.POSTING_LISTS).sharedCopy(),
//                                 root,
//                                 footerPointer);
    }

    @Override
    public long indexFileCacheSize()
    {
        // trie has no pre-allocated memory.
        // TODO: Is this still the case now the trie isn't using the chunk cache?
        return 0;
    }

    @Override
    @SuppressWarnings("resource")
    public List<RangeIterator> search(Expression exp, SSTableQueryContext context) throws IOException
    {
        return null;
//        if (logger.isTraceEnabled())
//            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);
//
//        if (!exp.getOp().isEquality())
//            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression: " + exp));
//
//        final ByteComparable term = ByteComparable.fixedLength(exp.lower.value.encoded);
//        QueryEventListener.TrieIndexEventListener listener = MulticastQueryEventListeners.of(context.queryContext, perColumnEventListener);
//
//        PostingList postingList = reader.exactMatch(term, listener, context.queryContext);
//
//        return toIterators(postingList == null ? Collections.EMPTY_LIST : Collections.singletonList(postingList.peekable()), context);
    }

    public static int openPerIndexFiles()
    {
        return TermsReader.openPerIndexFiles();
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("indexContext", indexContext)
                          .toString();
    }

    @Override
    public void close()
    {
        //reader.close();
    }
}
