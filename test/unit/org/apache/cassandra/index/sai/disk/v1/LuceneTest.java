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

package org.apache.cassandra.index.sai.disk.v1;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Test;

import org.agrona.collections.IntArrayList;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import static org.junit.Assert.assertArrayEquals;

public class LuceneTest
{
    @Test
    public void test() throws Exception
    {
        Path luceneDir = Files.createTempDirectory("jmh_lucene_test");
        Directory directory = FSDirectory.open(luceneDir);
        IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
        IndexWriter indexWriter = new IndexWriter(directory, config);

        int[] rowIds = new int[] {10, 20, 30};

        int lastRowID = rowIds[rowIds.length - 1];

        StringField field = new StringField("columnA", "value", Field.Store.NO);
        Document document = new Document();

        int i = 0;
        for (int x = 0; x <= lastRowID; x++)
        {
            if (x == rowIds[i])
            {
                document.add(field);
                i++;
            }
            else
            {
                document.clear();
            }
            indexWriter.addDocument(document);
        }
        indexWriter.forceMerge(1);
        indexWriter.close();

        IntArrayList docs = new IntArrayList();

        try (DirectoryReader reader = DirectoryReader.open(directory))
        {
            LeafReaderContext context = reader.leaves().get(0);
            PostingsEnum postings = context.reader().postings(new Term("columnA", new BytesRef("value")));
            while (true)
            {
                int doc = postings.nextDoc();
                if (doc == PostingsEnum.NO_MORE_DOCS)
                {
                    break;
                }
                docs.add(doc);
            }
            assertArrayEquals(rowIds, docs.toIntArray());
        }
    }
}
