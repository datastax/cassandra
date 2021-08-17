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

package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

import org.junit.Assert;
import org.junit.Ignore;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.schema.KeyspaceParams;

@Ignore
abstract class CommitLogSyncFailureTest
{
    private static final String KEYSPACE1 = "CommitLogTest";
    private static final String STANDARD1 = "CommitLogChainedMarkersTest";

    ColumnFamilyStore cfs1;

    void setUp(Config.DiskAccessMode accessMode)
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setDiskAccessMode(accessMode);
        DatabaseDescriptor.setCommitLogSegmentSize(5);
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.periodic);
        DatabaseDescriptor.setCommitLogSyncPeriod(10000 * 1000);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD1, 0, AsciiType.instance, BytesType.instance));

        CompactionManager.instance.disableAutoCompaction();

        cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
    }

    void replayCommitLogAfterSyncFailure(boolean doSync) throws IOException
    {
        if (CommitLog.instance.configuration.useCompression() || CommitLog.instance.configuration.useEncryption())
        {
            System.out.println("Test does not make sense with commit log compression.");
            return;
        }

        byte[] entropy = new byte[1024];
        new Random().nextBytes(entropy);
        final Mutation m = new RowUpdateBuilder(cfs1.metadata.get(), 0, "k")
        .clustering("bytes")
        .add("val", ByteBuffer.wrap(entropy))
        .build();

        int samples = 10000;
        for (int i = 0; i < samples; i++)
            CommitLog.instance.add(m);

        if (doSync)
            CommitLog.instance.sync(false);

        ArrayList<File> toCheck = CommitLogReaderTest.getCommitLogs();
        CommitLogReader reader = new CommitLogReader();
        CommitLogReaderTest.TestCLRHandler testHandler = new CommitLogReaderTest.TestCLRHandler(cfs1.metadata.get());
        for (File f : toCheck)
            reader.readCommitLogSegment(testHandler, f, CommitLogReader.ALL_MUTATIONS, false);

        Assert.assertEquals(samples, testHandler.seenMutationCount());
    }
}
