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
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.ScannerList;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
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
            // the DSE 5.1 compatibility mode: big me, whose deletion times are 32-bit, written from an sstable whose
            // deletion times go past 2038 (oa)
            Version written = writer.currentWriter().descriptor.version;
            boolean refuse = written.hasPre40Encoding() && sstable.descriptor.version.hasUIntDeletionTime();
            RefusePast2038 refusePast2038 = refuse ? new RefusePast2038(written) : null;
            // a refusal of the partition deletion or the static row throws from apply, the partition then closed
            // with the scanners of the compaction iterator, the rewrite aborted
            while (iter.hasNext())
                writer.append(refusePast2038 != null ? Transformation.apply(iter.next(), refusePast2038) : iter.next());

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

    /**
     * The rewrite at big {@code me} (a version with {@link Version#hasPre40Encoding()}, written in the DSE 5.1
     * compatibility mode only) of an sstable whose deletion times go past 2038 ({@code oa}): a deletion or expiration
     * time past the cap of {@code me}, which its writer would write wrong, is refused before it is written
     * ({@link UnfilteredSerializer#checkPre40SSTableDeletionTimes}), so that the rewrite fails and the source is kept.
     */
    private static final class RefusePast2038 extends Transformation<UnfilteredRowIterator>
    {
        private final Version written;

        private RefusePast2038(Version written)
        {
            this.written = written;
        }

        @Override
        protected DeletionTime applyToDeletion(DeletionTime deletionTime)
        {
            UnfilteredSerializer.checkPre40SSTableDeletionTime(deletionTime, written);
            return deletionTime;
        }

        @Override
        protected Row applyToStatic(Row row)
        {
            UnfilteredSerializer.checkPre40SSTableDeletionTimes(row, written);
            return row;
        }

        @Override
        protected Row applyToRow(Row row)
        {
            UnfilteredSerializer.checkPre40SSTableDeletionTimes(row, written);
            return row;
        }

        @Override
        protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            UnfilteredSerializer.checkPre40SSTableDeletionTimes(marker, written);
            return marker;
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

