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

package org.apache.cassandra.io.sstable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.tools.JsonTransformer;
import org.apache.cassandra.tools.SSTableExport;
import org.apache.cassandra.tools.Util;
import org.assertj.core.api.Assertions;


public class MultipleSSTableFormatsTest extends CQLTester
{
    private final static Logger logger = LoggerFactory.getLogger(MultipleSSTableFormatsTest.class);
    private final static int cnt = 100;
    private final static int overlap = 70;
    private final static int deletionCount = 30;

    private final long seed = 55;//System.nanoTime();
    private Random random;

    private SSTableFormat<?, ?> savedSSTableFormat;
    
    @Before
    public void before() 
    {
        savedSSTableFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        random = new Random(seed);
        logger.info("Using random seed = {}", seed);
    }

    @After
    public void after() 
    {
        DatabaseDescriptor.setSelectedSSTableFormat(savedSSTableFormat);
    }

    Map<Integer, SSTableFormat> delformat = Maps.newHashMap();

    private Map<Integer, Integer> createSSTables() throws IOException
    {
        Map<Integer, Integer> content = Maps.newHashMap();
        delformat.clear();

        createTable("CREATE TABLE %s (id INT, val INT, PRIMARY KEY (id))");
        disableCompaction();

        int offset = 0;
        for (SSTableFormat<?, ?> format : DatabaseDescriptor.getSSTableFormats().values())
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);

            for (int i = 0; i < cnt; i++)
            {
                int v = random.nextInt();
                content.put(i + offset, v);
                execute("INSERT INTO %s (id, val) VALUES (?, ?)", i + offset, v);
            }
            offset += cnt - overlap;
            System.out.println(((TrieMemtable) getCurrentColumnFamilyStore().getCurrentMemtable()).dump());

            flush();
        }

        for (SSTableFormat<?, ?> format : DatabaseDescriptor.getSSTableFormats().values())
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);

            for (int i = 0; i < deletionCount; i++)
            {
                int key = random.nextInt(offset + overlap);
                content.remove(key);
                delformat.put(key, format);
                execute("DELETE FROM %s WHERE id = ?", key);
            }
            System.out.println(((TrieMemtable) getCurrentColumnFamilyStore().getCurrentMemtable()).dump());

            flush();
        }

        for (SSTableReader s : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            System.out.println(s);
            try (ISSTableScanner scanner = s.getScanner())
            {
                OutputStream os = new ByteArrayOutputStream();
                JsonTransformer.toJsonLines(scanner, Util.iterToStream(scanner), false, getCurrentColumnFamilyStore().metadata(), os);
                System.out.println(os.toString());
            }
        }

        List<SSTableFormat<?, ?>> createdFormats = createdFormats();
        Assertions.assertThat(createdFormats).hasSameElementsAs(Sets.newHashSet(DatabaseDescriptor.getSSTableFormats().values()));

        return content;
    }

    private void checkRead(Map<Integer, Integer> content) 
    {
        for (Map.Entry<Integer, Integer> entry : content.entrySet())
        {
            UntypedResultSet r = execute("SELECT val FROM %s WHERE id = ?", entry.getKey());
            Assertions.assertThat(r.one().getInt("val")).isEqualTo(entry.getValue());
        }

        for (Map.Entry<Integer, SSTableFormat> entry : delformat.entrySet())
        {
            UntypedResultSet r = execute("SELECT val FROM %s WHERE id = ?", entry.getKey());
            Assertions.assertThat(r.isEmpty());
        }

        Iterator<UntypedResultSet.Row> it = execute("SELECT id, val FROM %s").iterator();
        Map<Integer, Integer> results = Maps.newHashMap();
        while (it.hasNext()) {
            UntypedResultSet.Row row = it.next();
            results.put(row.getInt("id"), row.getInt("val"));
        }
        if (!results.equals(content))
        {
            Set<Integer> r = Sets.newHashSet(results.keySet());
            r.removeAll(content.keySet());
            for (var k : r)
            {
                System.out.println(k + " from " + delformat.get(k));
                UntypedResultSet rr = execute("SELECT val FROM %s WHERE id = ?", k);
                Assertions.assertThat(rr.isEmpty());
            }
            Iterator<UntypedResultSet.Row> itt = execute("SELECT id, val FROM %s").iterator();
            while (itt.hasNext()) {
                UntypedResultSet.Row row = itt.next();
                System.out.println(row.getInt("id"));
            }
        }
        Assertions.assertThat(results).isEqualTo(content);
    }

    @Test
    public void testRead() throws Throwable
    {
        Map<Integer, Integer> content = createSSTables();
        checkRead(content);
    }

    @Test
    public void testCompactionToBigFormat() throws Throwable
    {
        testCompaction(BigFormat.getInstance());
    }

    @Test
    public void testCompactionToBtiFormat() throws Throwable
    {
        testCompaction(BtiFormat.getInstance());
    }

    private void testCompaction(SSTableFormat<?, ?> format) throws Throwable
    {
        Map<Integer, Integer> content = createSSTables();
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        compact();
        List<SSTableFormat<?, ?>> createdFormats = createdFormats();
        Assertions.assertThat(createdFormats).hasSize(1);
        Assertions.assertThat(createdFormats.get(0)).isEqualTo(format);
        checkRead(content);
    }

    private List<SSTableFormat<?, ?>> createdFormats()
    {
        return ColumnFamilyStore.getIfExists(KEYSPACE, currentTable())
                                .getLiveSSTables()
                                .stream()
                                .map(sstr -> sstr.descriptor.getFormat())
                                .collect(Collectors.toList());
    }
}
