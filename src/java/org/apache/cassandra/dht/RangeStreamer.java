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
package org.apache.cassandra.dht;

import java.net.InetAddress;
import java.util.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.dht.SourceFilters.*;

/**
 * Assists in streaming ranges to a node.
 */
public class RangeStreamer
{
    private static final Logger logger = LoggerFactory.getLogger(RangeStreamer.class);

    /* bootstrap tokens. can be null if replacing the node. */
    private final Collection<Token> tokens;
    /* current token ring */
    private final TokenMetadata metadata;
    /* address of this node */
    private final InetAddress address;
    /* streaming description */
    private final String description;
    private final Multimap<String, Map.Entry<InetAddress, Collection<Range<Token>>>> toFetch = HashMultimap.create();
    private final ISourceFilter sourceFilter;
    private final StreamPlan streamPlan;
    private final boolean useStrictConsistency;
    private final IEndpointSnitch snitch;
    private final StreamStateStore stateStore;
    private StreamResultFuture streamFuture;
    private final StreamConsistency streamConsistency;

    /**
     * A filter applied to sources to stream from when constructing a fetch map.
     */
    public static interface ISourceFilter
    {
        public boolean shouldInclude(InetAddress endpoint);
    }

    public RangeStreamer(TokenMetadata metadata,
                         Collection<Token> tokens,
                         InetAddress address,
                         String description,
                         boolean useStrictConsistency,
                         StreamConsistency streamConsistency,
                         IEndpointSnitch snitch,
                         StreamStateStore stateStore,
                         boolean connectSequentially,
                         ISourceFilter sourceFilter)
    {
        this.metadata = metadata;
        this.tokens = tokens;
        this.address = address;
        this.description = description;
        this.streamPlan = new StreamPlan(description, true);
        this.useStrictConsistency = useStrictConsistency;
        this.streamConsistency = streamConsistency;
        this.snitch = snitch;
        this.stateStore = stateStore;
        streamPlan.listeners(this.stateStore);
        this.sourceFilter = sourceFilter;
    }

    /**
     * Add ranges to be streamed for given keyspace.
     *
     * @param keyspaceName keyspace name
     * @param ranges ranges to be streamed
     */
    public void addRanges(String keyspaceName, Collection<Range<Token>> ranges)
    {
        if (Keyspace.open(keyspaceName).getReplicationStrategy() instanceof LocalStrategy)
        {
            logger.info("Not adding ranges for Local Strategy keyspace={}", keyspaceName);
            return;
        }

        boolean useStrictSource = useStrictSourcesForRanges(keyspaceName);
        Multimap<Range<Token>, InetAddress> rangesForKeyspace = useStrictSource
                                                                ? getAllRangesWithStrictSourcesFor(keyspaceName, ranges)
                                                                : getAllRangesWithSourcesFor(keyspaceName, ranges);

        logger.info("Adding keyspace '{}'{} for ranges {}",
                    keyspaceName,
                    useStrictSource ? " with strict sources" : "",
                    rangesForKeyspace.keySet());

        for (Map.Entry<Range<Token>, InetAddress> entry : rangesForKeyspace.entries())
            logger.info("{}: range {} exists on {} for keyspace {}", description, entry.getKey(), entry.getValue(), keyspaceName);

        Multimap<InetAddress, Range<Token>> sourceRanges = getRangeFetchMap(rangesForKeyspace,
                                                                            sourceFilter,
                                                                            keyspaceName,
                                                                            useStrictConsistency,
                                                                            streamConsistency);
        for (Map.Entry<InetAddress, Collection<Range<Token>>> entry : sourceRanges.asMap().entrySet())
        {
            if (logger.isTraceEnabled())
            {
                for (Range<Token> r : entry.getValue())
                    logger.trace(String.format("%s: range %s from source %s for keyspace %s", description, r, entry.getKey(), keyspaceName));
            }
            toFetch.put(keyspaceName, entry);
        }
    }

    /**
     * @param keyspaceName keyspace name to check
     * @return true when the node is bootstrapping, useStrictConsistency is true and # of nodes in the cluster is more than # of replica
     */
    private boolean useStrictSourcesForRanges(String keyspaceName)
    {
        AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();
        return useStrictConsistency
                && tokens != null
                && metadata.getAllEndpoints().size() != strat.getReplicationFactor();
    }

    /**
     * Get a map of all ranges and their respective sources that are candidates for streaming the given ranges
     * to us. For each range, the list of sources is sorted by proximity relative to the given destAddress.
     *
     * @throws java.lang.IllegalStateException when there is no source to get data streamed
     */
    private Multimap<Range<Token>, InetAddress> getAllRangesWithSourcesFor(String keyspaceName, Collection<Range<Token>> desiredRanges)
    {
        AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();
        Multimap<Range<Token>, InetAddress> rangeAddresses = strat.getRangeAddresses(metadata.cloneOnlyTokenMap());

        Multimap<Range<Token>, InetAddress> rangeSources = ArrayListMultimap.create();
        for (Range<Token> desiredRange : desiredRanges)
        {
            for (Range<Token> range : rangeAddresses.keySet())
            {
                if (range.contains(desiredRange))
                {
                    List<InetAddress> preferred = snitch.getSortedListByProximity(address, rangeAddresses.get(range));
                    rangeSources.putAll(desiredRange, preferred);
                    break;
                }
            }

            if (!rangeSources.keySet().contains(desiredRange))
                throw new IllegalStateException("No sources found for " + desiredRange);
        }
        return rangeSources;
    }

    /**
     * Get a map of all ranges and the source that will be cleaned up once this bootstrapped node is added for the given ranges.
     * For each range, the list should only contain a single source. This allows us to consistently migrate data without violating
     * consistency.
     *
     * @throws java.lang.IllegalStateException when there is no source to get data streamed, or more than 1 source found.
     */
    private Multimap<Range<Token>, InetAddress> getAllRangesWithStrictSourcesFor(String keyspace, Collection<Range<Token>> desiredRanges)
    {
        assert tokens != null;
        AbstractReplicationStrategy strat = Keyspace.open(keyspace).getReplicationStrategy();

        // Active ranges
        TokenMetadata metadataClone = metadata.cloneOnlyTokenMap();
        Multimap<Range<Token>, InetAddress> addressRanges = strat.getRangeAddresses(metadataClone);

        // Pending ranges
        metadataClone.updateNormalTokens(tokens, address);
        Multimap<Range<Token>, InetAddress> pendingRangeAddresses = strat.getRangeAddresses(metadataClone);

        // Collects the source that will have its range moved to the new node
        Multimap<Range<Token>, InetAddress> rangeSources = ArrayListMultimap.create();

        for (Range<Token> desiredRange : desiredRanges)
        {
            for (Map.Entry<Range<Token>, Collection<InetAddress>> preEntry : addressRanges.asMap().entrySet())
            {
                if (preEntry.getKey().contains(desiredRange))
                {
                    Set<InetAddress> oldEndpoints = Sets.newHashSet(preEntry.getValue());
                    Set<InetAddress> newEndpoints = Sets.newHashSet(pendingRangeAddresses.get(desiredRange));

                    // Due to CASSANDRA-5953 we can have a higher RF then we have endpoints.
                    // So we need to be careful to only be strict when endpoints == RF
                    if (oldEndpoints.size() == strat.getReplicationFactor())
                    {
                        oldEndpoints.removeAll(newEndpoints);
                        assert oldEndpoints.size() == 1 : "Expected 1 endpoint but found " + oldEndpoints.size();
                    }

                    rangeSources.put(desiredRange, oldEndpoints.iterator().next());
                }
            }

            // Validate
            Collection<InetAddress> addressList = rangeSources.get(desiredRange);
            if (addressList == null || addressList.isEmpty())
                throw new IllegalStateException("No sources found for " + desiredRange);

            if (addressList.size() > 1)
                throw new IllegalStateException("Multiple endpoints found for " + desiredRange);

            InetAddress sourceIp = addressList.iterator().next();
            EndpointState sourceState = Gossiper.instance.getEndpointStateForEndpoint(sourceIp);
            if (Gossiper.instance.isEnabled() && (sourceState == null || !sourceState.isAlive()))
                throw new RuntimeException("A node required to move the data consistently is down (" + sourceIp + "). " +
                                           "If you wish to move the data from a potentially inconsistent replica, restart the node with -Dcassandra.consistent.rangemovement=false");
        }

        return rangeSources;
    }

    /**
     * @param rangesWithSources The ranges we want to fetch (key) and their potential sources (value)
     * @param filter A (possibly empty) collection of source filters to apply. In addition to any filters given
     *                      here, we always exclude ourselves.
     * @param keyspace keyspace name
     * @return Map of source endpoint to collection of ranges
     */
    private static Multimap<InetAddress, Range<Token>> getRangeFetchMap(Multimap<Range<Token>, InetAddress> rangesWithSources,
                                                                        ISourceFilter filter,
                                                                        String keyspace,
                                                                        boolean useStrictConsistency,
                                                                        StreamConsistency streamConsistency)
    {
        Keyspace ks = Keyspace.open(keyspace);
        AbstractReplicationStrategy strategy = ks.getReplicationStrategy();
        int requiredSources = streamConsistency.requiredSources(ks);

        Multimap<InetAddress, Range<Token>> rangeFetchMapMap = HashMultimap.create();
        for (Range<Token> range : rangesWithSources.keySet())
        {
            int foundSources = 0;
            for (InetAddress address : rangesWithSources.get(range))
            {
                if (!filter.shouldInclude(address) || streamConsistency.shouldSkipSource(strategy, address))
                    continue;

                if (address.equals(FBUtilities.getBroadcastAddress()))
                {
                    // If localhost is a source, we have found one, but we don't add it to the map to avoid
                    // streaming locally. This is used for relocate/move.
                    foundSources++;
                    continue;
                }

                logger.info("Including {} for streaming range {} in keyspace {}", address, range, keyspace);
                rangeFetchMapMap.put(address, range);
                foundSources++;
                // ensure we only stream from required number of replicas for each range
                if (foundSources == requiredSources)
                    break;
            }

            if (foundSources < requiredSources)
            {
                handleSourceNotFound(keyspace, useStrictConsistency, range, foundSources, requiredSources, streamConsistency);
            }
        }

        return rangeFetchMapMap;
    }

    // Do not rename or remove this method without adopting the byteman rules in the utest RangeStreamerBootstrapTest
    static void handleSourceNotFound(String keyspace, boolean useStrictConsistency, Range<Token> range, int foundSources,
                                     int requiredSources, StreamConsistency streamConsistency)
    {
        AbstractReplicationStrategy strat = Keyspace.isInitialized() ? Keyspace.open(keyspace).getReplicationStrategy() : null;
        if (strat != null && strat.getReplicationFactor() == 1)
        {
            if (useStrictConsistency)
                throw new IllegalStateException("Unable to find sufficient sources for streaming range " + range + " in keyspace " + keyspace + " with RF=1. " +
                                                "Ensure this keyspace contains replicas in the source datacenter.");
            else
                logger.warn("Unable to find sufficient sources for streaming range {} in keyspace {} with RF=1. " +
                            "Keyspace might be missing data.", range, keyspace);
        }
        else if (DatabaseDescriptor.isReplacing() && strat != null && requiredSources > strat.getReplicationFactor() - 1)
        {
            logger.warn("Cannot ensure replace consistency {} for range {} in keyspace {} (RF={}). " +
                        "Required sources: {}, found sources {}.", streamConsistency, range, keyspace,
                        strat.getReplicationFactor(), requiredSources, foundSources);
        }
        else
            throw new IllegalStateException("Unable to find sufficient sources for streaming range " + range + " in keyspace " +
                                            keyspace + " with consistency " + streamConsistency + ". Found: " +
                                            foundSources + " but require: " + requiredSources);
    }

    public static Multimap<InetAddress, Range<Token>> getWorkMapForMove(Multimap<Range<Token>, InetAddress> rangesWithSourceTarget, String keyspace,
                                                                        IFailureDetector fd,
                                                                        boolean useStrictConsistency)
    {
        // do *not* exclude the local host via the source-filter - this is meant for relocate/move, which likely
        // involves the local node
        return getRangeFetchMap(rangesWithSourceTarget,
                                failureDetectorFilter(fd),
                                keyspace,
                                useStrictConsistency,
                                StreamConsistency.ONE);
    }

    // For testing purposes
    @VisibleForTesting
    Multimap<String, Map.Entry<InetAddress, Collection<Range<Token>>>> toFetch()
    {
        return toFetch;
    }

    public StreamResultFuture fetchAsync()
    {
        for (Map.Entry<String, Map.Entry<InetAddress, Collection<Range<Token>>>> entry : toFetch.entries())
        {
            String keyspace = entry.getKey();
            InetAddress source = entry.getValue().getKey();
            InetAddress preferred = SystemKeyspace.getPreferredIP(source);
            Collection<Range<Token>> ranges = entry.getValue().getValue();

            // filter out already streamed ranges
            Set<Range<Token>> availableRanges = stateStore.getAvailableRanges(keyspace, StorageService.instance.getTokenMetadata().partitioner);
            if (ranges.removeAll(availableRanges))
            {
                logger.info("Some ranges of {} are already available. Skipping streaming those ranges.", availableRanges);
            }

            if (logger.isTraceEnabled())
                logger.trace("{}ing from {} ranges {}", description, source, StringUtils.join(ranges, ", "));
            /* Send messages to respective folks to stream data over to me */
            streamPlan.requestRanges(source, preferred, keyspace, ranges);
        }

        streamFuture = streamPlan.execute();

        return streamFuture;
    }

    public void abort(String reason)
    {
        if (streamFuture == null)
            throw new IllegalStateException("Range streaming has not been started");

        streamFuture.abort(reason);
    }

    public enum StreamConsistency
    {
        ONE(ConsistencyLevel.ONE), GLOBAL_QUORUM(ConsistencyLevel.QUORUM), LOCAL_DC_QUORUM(ConsistencyLevel.LOCAL_QUORUM);

        final ConsistencyLevel correspondingCL;

        StreamConsistency(ConsistencyLevel cl)
        {
            correspondingCL = cl;
        }

        public int requiredSources(Keyspace ks)
        {
            return correspondingCL.blockFor(ks);
        }

        public boolean shouldSkipSource(AbstractReplicationStrategy strategy, InetAddress address)
        {
            return strategy instanceof NetworkTopologyStrategy && correspondingCL.isDatacenterLocal() && !correspondingCL.isLocal(address);
        }
    }
}
