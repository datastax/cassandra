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
package org.apache.cassandra.db.compaction;

import java.util.function.LongPredicate;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.ScannerList;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class Upgrader
{
    private final CompactionRealm realm;
    private final SSTableReader sstable;
    private final LifecycleTransaction transaction;
    private final File directory;

    private final CompactionController controller;

    private final OutputHandler outputHandler;

    public Upgrader(CompactionRealm realm, LifecycleTransaction txn, OutputHandler outputHandler)
    {
        this.realm = realm;
        this.transaction = txn;
        this.sstable = txn.onlyOne();
        this.outputHandler = outputHandler;
        this.directory = new File(sstable.getFilename()).parent();
        this.controller = new UpgradeController(realm);
    }

    private SSTableWriter createCompactionWriter(StatsMetadata metadata)
    {
        MetadataCollector sstableMetadataCollector = new MetadataCollector(realm.metadata().comparator);
        sstableMetadataCollector.sstableLevel(sstable.getSSTableLevel());

        Descriptor descriptor = realm.newSSTableDescriptor(directory);
        return descriptor.getFormat().getWriterFactory().builder(descriptor)
                         .setKeyCount(metadata.totalRows) // TODO is it correct? I don't know why did we estimate that value instead of just copying it from metadata
                         .setRepairedAt(metadata.repairedAt)
                         .setPendingRepair(metadata.pendingRepair)
                         .setTransientSSTable(metadata.isTransient)
                         .setTableMetadataRef(realm.metadataRef())
                         .setMetadataCollector(sstableMetadataCollector)
                         .setSerializationHeader(SerializationHeader.make(realm.metadata(), Sets.newHashSet(sstable)))
                         .addDefaultComponents(realm.getIndexManager().listIndexGroups())
                         .setSecondaryIndexGroups(realm.getIndexManager().listIndexGroups())
                         .build(transaction, realm);
    }

    public void upgrade(boolean keepOriginals)
    {
        outputHandler.output("Upgrading " + sstable);
        long nowInSec = FBUtilities.nowInSeconds();
        try (SSTableRewriter writer = SSTableRewriter.construct(realm, transaction, keepOriginals, CompactionTask.getMaxDataAge(transaction.originals()));
             ScannerList scanners = ScannerList.of(transaction.originals(), null);
             CompactionIterator iter = new CompactionIterator(transaction.opType(), scanners.scanners, controller, nowInSec, nextTimeUUID()))
        {
            writer.switchWriter(createCompactionWriter(sstable.getSSTableMetadata()));
            iter.setTargetDirectory(writer.currentWriter().getFilename());
            while (iter.hasNext())
                writer.append(iter.next());

            writer.finish();
            outputHandler.output("Upgrade of " + sstable + " complete.");
        }
        catch (Exception e)
        {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        finally
        {
            controller.close();
        }
    }

    private static class UpgradeController extends CompactionController
    {
        public UpgradeController(CompactionRealm cfs)
        {
            super(cfs, Integer.MAX_VALUE);
        }

        @Override
        public LongPredicate getPurgeEvaluator(DecoratedKey key)
        {
            return time -> false;
        }
    }
}

