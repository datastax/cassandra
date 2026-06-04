/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.disk;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;

/**
 * Abstract base class for row-aware {@link PrimaryKeyMap} tests.
 * Provides common setup, test methods, and helper classes to reduce code duplication
 * across skinny, wide, and static clustering test variants.
 */
@RunWith(Parameterized.class)
public abstract class RowAwarePrimaryKeyMapTester extends SAITester
{
    @Parameterized.Parameter
    public Version version;
    protected SSTableReader sstable;
    protected PrimaryKey.Factory pkFactory;
    protected IndexComponents.ForRead perSSTableComponents;
    protected IPartitioner partitioner;

    @Parameterized.Parameters(name = "version={0}")
    public static List<Object[]> data()
    {
        return Version.ALL.stream()
                          .filter(v -> v.onDiskFormat().indexFeatureSet().isRowAware())
                          .map(v -> new Object[]{ v })
                          .collect(Collectors.toList());
    }

    /**
     * Subclasses must implement this to return the set of index contexts
     * needed for their specific test scenario.
     */
    protected abstract Set<IndexContext> getIndexContexts();

    /**
     * Subclasses must implement this to create the table schema
     * appropriate for their test scenario.
     */
    protected abstract void createTableSchema() throws Throwable;

    /**
     * Subclasses must implement this to insert test data
     * appropriate for their test scenario.
     */
    protected abstract void insertTestData() throws Throwable;

    /**
     * Subclasses must implement this to create a MapWalker instance
     * with test-specific position generators.
     */
    protected abstract MapWalker createMapWalker(PrimaryKeyMap map, PrimaryKeyMapFunction function);

    @Before
    public void setup() throws Throwable
    {
        SAIUtil.setCurrentVersion(version);

        createTableSchema();
        insertTestData();

        // Flush to generate SSTable and SAI components
        flush();

        // Obtain the just-flushed SSTable
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        this.sstable = cfs.getLiveSSTables().iterator().next();

        // Build IndexDescriptor from the live SSTable using the matching index contexts
        IndexDescriptor indexDescriptor = IndexDescriptor.load(sstable, getIndexContexts());
        this.pkFactory = indexDescriptor.perSSTableComponents().version().onDiskFormat().newPrimaryKeyFactory(cfs.metadata.get().comparator);

        this.perSSTableComponents = indexDescriptor.perSSTableComponents();
        this.partitioner = sstable.metadata().partitioner;
    }

    @Test
    public void testExactRowIdOrInvertedCeiling() throws Throwable
    {
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            MapWalker mapWalker = createMapWalker(map, map::exactRowIdOrInvertedCeiling);
            mapWalker.testExactRowIdOrInvertedCeiling();
        }
    }

    @Test
    public void testCeiling() throws Throwable
    {
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            MapWalker mapWalker = createMapWalker(map, map::ceiling);
            mapWalker.testCeiling();
        }
    }

    @Test
    public void testFloor() throws Throwable
    {
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            MapWalker mapWalker = createMapWalker(map, map::floor);
            mapWalker.testFloor();
        }
    }

    /**
     * Functional interface for PrimaryKeyMap API methods that take a PrimaryKey and return a row ID.
     */
    @FunctionalInterface
    protected interface PrimaryKeyMapFunction
    {
        long apply(PrimaryKey pk);
    }

    /**
     * Abstract helper class for testing PrimaryKeyMap operations.
     * Provides common functionality and requires subclasses to implement
     * test-specific assertion logic.
     */
    protected abstract class MapWalker
    {
        protected final long count;
        protected final PrimaryKeyMapFunction rowIdFromPKMethod;
        protected final PrimaryKey firstPk;
        protected final PrimaryKey lastPk;
        protected final long firstToken;
        protected final long lastToken;

        protected MapWalker(PrimaryKeyMap map, PrimaryKeyMapFunction rowIdFromPKMethod)
        {
            this.rowIdFromPKMethod = rowIdFromPKMethod;
            this.count = map.count();
            this.firstPk = map.primaryKeyFromRowId(0);
            this.lastPk = map.primaryKeyFromRowId(count - 1);
            this.firstToken = firstPk.token().getLongValue();
            this.lastToken = lastPk.token().getLongValue();
        }

        /**
         * Helper method to build a PrimaryKey with partition key and clustering.
         */
        protected PrimaryKey buildPk(IPartitioner partitioner, int pk, int ck)
        {
            ByteBuffer pkBuf = Int32Type.instance.decompose(pk);
            Token token = partitioner.getToken(pkBuf);
            DecoratedKey key = new BufferDecoratedKey(token, pkBuf);
            Clustering<ByteBuffer> clustering = Clustering.make(Int32Type.instance.decompose(ck));
            return pkFactory.create(key, clustering);
        }

        /**
         * Helper method to build a PrimaryKey with static clustering.
         */
        protected PrimaryKey buildPkStatic(IPartitioner partitioner, int pk)
        {
            ByteBuffer pkBuf = Int32Type.instance.decompose(pk);
            Token token = partitioner.getToken(pkBuf);
            DecoratedKey key = new BufferDecoratedKey(token, pkBuf);
            return pkFactory.create(key, Clustering.STATIC_CLUSTERING);
        }

        protected void assertResult(PrimaryKey pk, long expected, String expectationMessage)
        {
            long actual = rowIdFromPKMethod.apply(pk);
            assertEquals(expectationMessage, expected, actual);
        }

        /**
         * Subclasses must implement test-specific assertions for exactRowIdOrInvertedCeiling.
         */
        protected abstract void testExactRowIdOrInvertedCeiling();

        /**
         * Subclasses must implement test-specific assertions for ceiling.
         */
        protected abstract void testCeiling();

        /**
         * Subclasses must implement test-specific assertions for floor.
         */
        protected abstract void testFloor();
    }
}
