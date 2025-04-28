/*
 * Copyright DataStax, Inc.
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
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;

import com.google.common.collect.Sets;

import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;

/**
 * User-provided directives about what indexes should be used by a {@code SELECT} query. It consists of a set of indexes
 * that should be preferred and a set of indexes that should not be used. Other than that, indexes that are applicable
 * to the query and that are not mentioned on these two sets might or might not be used depending on the index query
 * planner.
 */
public class IndexHints
{
    static final String CONFLICTING_INDEXES_ERROR = "Indexes cannot be both preferred and excluded: ";
    static final String WRONG_KEYSPACE_ERROR = "Index %s is not in the same keyspace as the queried table.";
    static final String MISSING_INDEX_ERROR = "Table %s doesn't have an index named %s";

    public static final IndexHints NONE = new IndexHints(Collections.emptySet(), Collections.emptySet());

    public static final Serializer serializer = new Serializer();

    /**
     * The indexes to prefer when executing a query.
     */
    public final Set<IndexMetadata> preferred;

    /**
     * The indexes not to use when executing the query.
     */
    public final Set<IndexMetadata> excluded;

    private IndexHints(Set<IndexMetadata> preferred, Set<IndexMetadata> excluded)
    {
        this.preferred = preferred;
        this.excluded = excluded;
    }

    public boolean prefers(String indexName)
    {
        return preferred.stream().anyMatch(i -> i.name.equals(indexName));
    }

    public boolean excludes(Index index)
    {
        return excluded.contains(index.getIndexMetadata());
    }

    public boolean excludes(String indexName)
    {
        return excluded.stream().anyMatch(i -> i.name.equals(indexName));
    }

    public Optional<Index> getBestIndexFor(Collection<Index> indexes, Predicate<Index> filter)
    {
        if (indexes.isEmpty())
            return Optional.empty();

        // filter excluded indexes
        Set<Index> candidates = new HashSet<>(indexes.size());
        for (Index index : indexes)
        {
            if (!excluded.contains(index.getIndexMetadata()) && filter.test(index))
                candidates.add(index);
        }

        // if all indexes are excluded, return empty
        if (candidates.isEmpty())
            return Optional.empty();

        // try to find a preferred index
        for (Index index : candidates)
        {
            if (preferred.contains(index.getIndexMetadata()))
                return Optional.of(index);
        }

        return Optional.of(candidates.iterator().next());
    }

    public static IndexHints create(Set<IndexMetadata> preferred, Set<IndexMetadata> excluded)
    {
        assert preferred != null && excluded != null;

        if (preferred.isEmpty() && excluded.isEmpty())
            return NONE;

        return new IndexHints(preferred, excluded);
    }

    public void validate(String keyspace)
    {
        if (preferred.isEmpty() && excluded.isEmpty())
            return;

        // Ensure that no index is both preferred and excluded
        Set<IndexMetadata> conflictingIndexes = Sets.intersection(preferred, excluded);
        if (!conflictingIndexes.isEmpty())
        {
            // collect the names of the conflicting indexes in order to provide a consistent error message
            SortedSet<String> names = new TreeSet<>();
            for (IndexMetadata i : conflictingIndexes)
                names.add(i.name);

            throw new InvalidRequestException(CONFLICTING_INDEXES_ERROR + String.join(", ", names));
        }

        // Ensure that all nodes in the cluster are in a version that supports index hints, including this one
        assert keyspace != null;
        Set<InetAddressAndPort> badNodes = MessagingService.instance().endpointsWithConnectionsOnVersionBelow(keyspace, MessagingService.VERSION_DS_12);
        if (MessagingService.current_version < MessagingService.VERSION_DS_12)
            badNodes.add(FBUtilities.getBroadcastAddressAndPort());
        if (!badNodes.isEmpty())
            throw new InvalidRequestException("Index hints are not supported in clusters below DS 12.");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexHints that = (IndexHints) o;
        return Objects.equals(preferred, that.preferred) &&
               Objects.equals(excluded, that.excluded);
    }

    /**
     * Returns the index hints represented by the specified sets of CQL names for the specified queried table.
     * </p>
     * All the mentioned indexes should exist in the index registry of the queried table,
     * or an {@link InvalidRequestException} will be thrown.
     *
     * @param preferred the names of the indexes to prefer when executing the query
     * @param excluded the names of the indexes to exclude when executing the query
     * @param table the queried table
     * @param indexRegistry the index registry of the queried table
     * @return the index hints represented by the specified sets of CQL names
     * @throws InvalidRequestException if any of the specified indexes do not exist in the specified index registry
     */
    public static IndexHints fromCQLNames(Set<QualifiedName> preferred,
                                          Set<QualifiedName> excluded,
                                          TableMetadata table,
                                          IndexRegistry indexRegistry)
    {
        return IndexHints.create(fetchIndexes(preferred, table, indexRegistry),
                                 fetchIndexes(excluded, table, indexRegistry));
    }

    private static Set<IndexMetadata> fetchIndexes(Set<QualifiedName> indexNames, TableMetadata table, IndexRegistry indexRegistry)
    {
        if (indexNames == null || indexNames.isEmpty())
            return Collections.emptySet();

        Set<IndexMetadata> indexes = new HashSet<>(indexNames.size());

        for (QualifiedName indexName : indexNames)
        {
            IndexMetadata index = fetchIndex(indexName, table, indexRegistry);
            indexes.add(index);
        }

        return indexes;
    }

    private static IndexMetadata fetchIndex(QualifiedName indexName, TableMetadata table, IndexRegistry indexRegistry)
    {
        String name = indexName.getName();
        String keyspace = indexName.getKeyspace();

        if (keyspace != null && !table.keyspace.equals(keyspace))
            throw new InvalidRequestException(format(WRONG_KEYSPACE_ERROR, indexName));

        Index index = indexRegistry.getIndexByName(name);
        if (index == null)
            throw new InvalidRequestException(format(MISSING_INDEX_ERROR, table.name, name));

        return index.getIndexMetadata();
    }

    public Comparator<Index.QueryPlan> comparator()
    {
        return Comparator.comparing(plan -> Sets.intersection(preferred, metadatas(plan.getIndexes())).size());
    }

    private static Set<IndexMetadata> metadatas(Collection<Index> indexes)
    {
        Set<IndexMetadata> metadatas = new HashSet<>(indexes.size());
        for (Index index : indexes)
            metadatas.add(index.getIndexMetadata());
        return metadatas;
    }

    /**
     * Serializer for {@link IndexHints}.
     * </p>
     * This serializer writes a short containing bit flags that indicate which types of hints are present, allowing the
     * future addition of new types of hints without necessarily increasing the messaging version. We should be able to
     * create compatible messages in the future if we add new types of hints and those are not explicitly set in the
     * user query. If we receive a message with unknown newer types of hints from a newer node, we will reject it.
     * </p>
     * Also, the bit flags are used to skip writing empty sets of indexes, which is the common case.
     */
    public static class Serializer
    {
        /** Bit flags mask to check if there are preferred indexes. */
        private static final short PREFERRED_MASK = 1;

        /** Bit flags mask to check if there are excluded indexes. */
        private static final short EXCLUDED_MASK = 2;

        /** Bit flags mask to check if there are any unknown hints. It's the negation of all the known flags. */
        private static final short UNKNOWN_HINTS_MASK = ~(PREFERRED_MASK | EXCLUDED_MASK);

        private static final IndexSetSerializer indexSetSerializer = new IndexSetSerializer();

        public void serialize(IndexHints hints, DataOutputPlus out, int version) throws IOException
        {
            // index hints are only supported in DS 12 and above, so don't serialize anything if the messaging version is lower
            if (version < MessagingService.VERSION_DS_12)
            {
                if (hints != NONE)
                    throw new IllegalStateException("Unable to serialize index hints with messaging version: " + version);
                return;
            }

            short flags = flags(hints);
            out.writeShort(flags);

            indexSetSerializer.serialize(hints.preferred, out, version);
            indexSetSerializer.serialize(hints.excluded, out, version);
        }

        public IndexHints deserialize(DataInputPlus in, int version, TableMetadata table) throws IOException
        {
            // index hints are only supported in DS 12 and above, so don't read anything if the messaging version is lower
            if (version < MessagingService.VERSION_DS_12)
                return IndexHints.NONE;

            // read the flags first to determine which types of hints are present
            short flags = in.readShort();

            // Reject any flags for unknown hints that may have been written by a node running newer code.
            if ((flags & UNKNOWN_HINTS_MASK) != 0)
                throw new IOException("Found unsupported index hints, likely due to the index hints containing " +
                                      "new types of hint that are not supported by this node.");

            // read preferred and excluded indexes
            Set<IndexMetadata> preferred = hasPreferred(flags) ? indexSetSerializer.deserialize(in, version, table) : Collections.emptySet();
            Set<IndexMetadata> excluded = hasExcluded(flags) ? indexSetSerializer.deserialize(in, version, table) : Collections.emptySet();

            return IndexHints.create(preferred, excluded);
        }

        public long serializedSize(IndexHints hints, int version)
        {
            // index hints are only supported in DS 12 and above, so no size if the messaging version is lower
            if (version < MessagingService.VERSION_DS_12)
                return 0;

            // size of flags
            long size = TypeSizes.SHORT_SIZE;

            // size of preferred and excluded indexes
            size += indexSetSerializer.serializedSize(hints.preferred, version);
            size += indexSetSerializer.serializedSize(hints.excluded, version);

            return size;
        }

        private static short flags(IndexHints hints)
        {
            short flags = 0;

            if (hints == NONE)
                return flags;

            if (!hints.preferred.isEmpty())
                flags |= PREFERRED_MASK;

            if (!hints.excluded.isEmpty())
                flags |= EXCLUDED_MASK;

            return flags;
        }

        private static boolean hasPreferred(int flags)
        {
            return (flags & PREFERRED_MASK) == PREFERRED_MASK;
        }

        private static boolean hasExcluded(int flags)
        {
            return (flags & EXCLUDED_MASK) == EXCLUDED_MASK;
        }
    }

    /**
     * Serializer for a set of indexes. Nothing is written if the set is empty. Otherwise, we write first the number of
     * indexes and then the indexes themselves. Each index is represented by the serialization of its metadata.
     */
    private static class IndexSetSerializer
    {
        private void serialize(Set<IndexMetadata> indexes, DataOutputPlus out, int version) throws IOException
        {
            if (indexes.isEmpty())
                return;

            int n = indexes.size();
            out.writeShort(n);
            for (IndexMetadata index : indexes)
                IndexMetadata.serializer.serialize(index, out, version);
        }

        private Set<IndexMetadata> deserialize(DataInputPlus in, int version, TableMetadata table) throws IOException
        {
            short n = in.readShort();
            Set<IndexMetadata> indexes = new HashSet<>(n);
            for (short i = 0; i < n; i++)
            {
                IndexMetadata metadata = IndexMetadata.serializer.deserialize(in, version, table);
                indexes.add(metadata);
            }
            return indexes;
        }

        private long serializedSize(Set<IndexMetadata> indexes, int version)
        {
            if (indexes.isEmpty())
                return 0;

            long size = 0;
            size += TypeSizes.SHORT_SIZE; // number of indexes
            for (IndexMetadata index : indexes)
                size += IndexMetadata.serializer.serializedSize(index, version);
            return size;
        }
    }
}
