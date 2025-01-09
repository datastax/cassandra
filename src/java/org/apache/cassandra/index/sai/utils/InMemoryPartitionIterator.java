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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;

public class InMemoryPartitionIterator implements PartitionIterator
{
    private final Iterator<InMemoryRowIterator> partitions;

    private InMemoryPartitionIterator(List<InMemoryRowIterator> partitions)
    {
        this.partitions = partitions.iterator();
    }

    public static InMemoryPartitionIterator create(ReadCommand command, List<Pair<PartitionInfo, Row>> sortedRows)
    {
        if (sortedRows.isEmpty())
            return new InMemoryPartitionIterator(Collections.emptyList());

        List<InMemoryRowIterator> partitions = new ArrayList<>();

        PartitionInfo currentPartitionInfo = null;
        List<Row> currentRows = null;

        for (Pair<PartitionInfo, Row> pair : sortedRows)
        {
            PartitionInfo partitionInfo = pair.left;
            Row row = pair.right;

            if (currentPartitionInfo == null || !currentPartitionInfo.key.equals(partitionInfo.key))
            {
                if (currentPartitionInfo != null)
                    partitions.add(new InMemoryRowIterator(command, currentPartitionInfo, currentRows));

                currentPartitionInfo = partitionInfo;
                currentRows = new ArrayList<>(1);
            }

            currentRows.add(row);
        }

        partitions.add(new InMemoryRowIterator(command, currentPartitionInfo, currentRows));

        return new InMemoryPartitionIterator(partitions);
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean hasNext()
    {
        return partitions.hasNext();
    }

    @Override
    public RowIterator next()
    {
        return partitions.next();
    }


    private static class InMemoryRowIterator implements RowIterator
    {
        private final ReadCommand command;
        private final PartitionInfo partitionInfo;
        private final Iterator<Row> rows;

        public InMemoryRowIterator(ReadCommand command, PartitionInfo partitionInfo, List<Row> rows)
        {
            this.command = command;
            this.partitionInfo = partitionInfo;
            this.rows = rows.iterator();
        }

        @Override
        public void close()
        {
        }

        @Override
        public boolean hasNext()
        {
            return rows.hasNext();
        }

        @Override
        public Row next()
        {
            return rows.next();
        }

        @Override
        public TableMetadata metadata()
        {
            return command.metadata();
        }

        @Override
        public boolean isReverseOrder()
        {
            return command.isReversed();
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return command.metadata().regularAndStaticColumns();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return partitionInfo.key;
        }

        @Override
        public Row staticRow()
        {
            return partitionInfo.staticRow;
        }
    }
}
