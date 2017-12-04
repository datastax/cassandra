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

package org.apache.cassandra.db;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DiskBoundaryManagerTest extends CQLTester
{
    private static final List<Directories.DataDirectory> DIRS1 = Lists.newArrayList(new Directories.DataDirectory(new File("/tmp/1")),
                                                                                    new Directories.DataDirectory(new File("/tmp/2")),
                                                                                    new Directories.DataDirectory(new File("/tmp/3")));

    private static final List<Directories.DataDirectory> DIRS2 = Lists.newArrayList(new Directories.DataDirectory(new File("/tmp/11")),
                                                                                    new Directories.DataDirectory(new File("/tmp/22")),
                                                                                    new Directories.DataDirectory(new File("/tmp/33")),
                                                                                    new Directories.DataDirectory(new File("/tmp/44")));

    private static final List<Directories.DataDirectory> DIRS3 = Lists.newArrayList(new Directories.DataDirectory(new File("/tmp/111")),
                                                                                    new Directories.DataDirectory(new File("/tmp/222")),
                                                                                    new Directories.DataDirectory(new File("/tmp/333")),
                                                                                    new Directories.DataDirectory(new File("/tmp/444")),
                                                                                    new Directories.DataDirectory(new File("/tmp/555")));

    private static final List<List<Directories.DataDirectory>> ALL_DIRS = Arrays.asList(DIRS1, DIRS2, DIRS3);

    private DiskBoundaryManager dbm;
    private MockCFS mock;
    private Directories cfDirectories;
    private Directories custom1Directories;
    private Directories custom2Directories;
    private Directories[] allDirectories;

    @Before
    public void setup()
    {
        BlacklistedDirectories.clearUnwritableUnsafe();
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.updateNormalTokens(BootStrapper.getRandomTokens(metadata, 10), FBUtilities.getBroadcastAddress());
        createTable("create table %s (id int primary key, x text)");
        ColumnFamilyStore currentColumnFamilyStore = getCurrentColumnFamilyStore();
        dbm = currentColumnFamilyStore.diskBoundaryManager;

        cfDirectories = new Directories(getCurrentColumnFamilyStore().metadata, DIRS1);
        custom1Directories = new Directories(getCurrentColumnFamilyStore().metadata, DIRS2);
        custom2Directories = new Directories(getCurrentColumnFamilyStore().metadata, DIRS3);
        allDirectories = new Directories[]{ cfDirectories, custom1Directories, custom2Directories };
        mock = new MockCFS(getCurrentColumnFamilyStore(), cfDirectories);
    }

    @Test
    public void getBoundariesTest()
    {
        for (int i = 0; i < allDirectories.length; i++)
        {
            Directories dir = allDirectories[i];
            DiskBoundaries dbv = dbm.getDiskBoundaries(mock, dir);
            Assert.assertEquals(ALL_DIRS.get(i).size(), dbv.positions.size());
            Assert.assertEquals(Arrays.asList(dir.getWriteableLocations()), dbv.directories);
            // fetch again and check that reference is cached
            assertSame(dbv, dbm.getDiskBoundaries(mock, dir));
        }
    }

    @Test
    public void blackListTest()
    {
        // Fetch disk boundaries
        DiskBoundaries[] oldBoundaries = getAll();

        // Blacklist directory from cfDir and from custom2
        File[] blacklisted = new File[]{new File("/tmp/3"), null, new File("/tmp/444")};
        BlacklistedDirectories.maybeMarkUnwritable(blacklisted[0]);
        BlacklistedDirectories.maybeMarkUnwritable(blacklisted[2]);

        DiskBoundaries[] newBoundaries = getAll();
        for (int i = 0; i < allDirectories.length; i++)
        {
            // Check that all boundaries were invalidated
            assertNotSame(oldBoundaries[i], newBoundaries[i]);
            // Check that blacklisted directories are not returned
            List<Directories.DataDirectory> newDirs = Lists.newArrayList(ALL_DIRS.get(i));
            if (blacklisted[i] != null)
            {
                Directories.DataDirectory blacklistedDir = new Directories.DataDirectory(blacklisted[i]);
                newDirs.remove(blacklistedDir);
            }
            Assert.assertEquals(newDirs.size(), newBoundaries[i].positions.size());
            Assert.assertEquals(newDirs, newBoundaries[i].directories);
        }
    }

    private DiskBoundaries[] getAll()
    {
        DiskBoundaries[] boundaries = new DiskBoundaries[allDirectories.length];
        for (int i = 0; i < allDirectories.length; i++)
        {
            boundaries[i] = dbm.getDiskBoundaries(mock, allDirectories[i]);
        }
        return boundaries;
    }

    @Test
    public void updateTokensTest() throws UnknownHostException
    {
        DiskBoundaries[] oldBoundaries = getAll();

        StorageService.instance.getTokenMetadata().updateNormalTokens(BootStrapper.getRandomTokens(StorageService.instance.getTokenMetadata(), 10), InetAddress.getByName("127.0.0.10"));

        DiskBoundaries[] newBoundaries = getAll();
        for (int i = 0; i < allDirectories.length; i++)
        {
            assertFalse(oldBoundaries.equals(newBoundaries));
        }
    }

    @Test
    public void alterKeyspaceTest() throws Throwable
    {
        DiskBoundaries[] oldBoundaries = getAll();

        execute("alter keyspace "+keyspace()+" with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");

        DiskBoundaries[] newBoundaries = getAll();
        for (int i = 0; i < allDirectories.length; i++)
        {
            assertNotSame(oldBoundaries[i], newBoundaries[i]);
            assertSame(newBoundaries[i], dbm.getDiskBoundaries(mock, allDirectories[i]));
        }
    }

    // just to be able to override the data directories
    private static class MockCFS extends ColumnFamilyStore
    {
        MockCFS(ColumnFamilyStore cfs, Directories dirs)
        {
            super(cfs.keyspace, cfs.getTableName(), 0, cfs.metadata, dirs, false, false, true);
        }
    }
}
