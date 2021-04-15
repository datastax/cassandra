/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.UnmodifiableArrayList;

/**
 * Strategy that replicate data on every {@code live} node.
 *
 * <p>This strategy is a {@code MultiDatacentersStrategy}. By consequence, it will handle properly local consistency levels.
 * Nevertheless, as the data is replicated on every node, consistency levels such as QUORUM should not be used
 * on clusters having more than 5 nodes.<p>
 *
 * <p>During bootstrap the time at which the data will be available is unknown and if the bootstrap is performed with
 * autobootstrap=false on a seed node, there will be no data locally until rebuild is run.</p>
 *
 */
public class EverywhereStrategy extends AbstractReplicationStrategy implements MultiDatacentersStrategy
{
    public EverywhereStrategy(String keyspaceName,
                              TokenMetadata tokenMetadata,
                              IEndpointSnitch snitch,
                              Map<String, String> configOptions) throws ConfigurationException
    {
        super(keyspaceName, tokenMetadata, snitch, configOptions);
    }

    @Override
    public List<InetAddress> calculateNaturalEndpoints(Token token, TokenMetadata tokenMetadata)
    {
        // Even if primary range repairs do not make a lot of sense for this strategy we want the behavior to be
        // correct if somebody use it.
        // Primary range repair expect the first endpoint of the list to be the primary range owner.
        Set<InetAddress> endpoints = new LinkedHashSet<>();
        Iterator<Token> iter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), token, false);
        while (iter.hasNext())
        {
            endpoints.add(tokenMetadata.getEndpoint(iter.next()));
        }
        return new ArrayList<>(endpoints);
    }

    @Override
    public int getReplicationFactor()
    {
        return getAllRingMembers().size();
    }

    @Override
    public boolean isReplicatedInDatacenter(String dc)
    {
        return true;
    }

    @Override
    public int getReplicationFactor(String dc)
    {
        int count = 0;
        for (InetAddress address : getAllRingMembers())
        {
            if (Objects.equals(snitch.getDatacenter(address), dc))
                count++;
        }
        return count;
    }

    @Override
    public Set<String> getDatacenters()
    {
        Set<String> datacenters = new HashSet<>();
        for (InetAddress address : getAllRingMembers())
        {
            datacenters.add(snitch.getDatacenter(address));
        }
        return datacenters;
    }

    @Override
    public Multimap<InetAddress, Range<Token>> getAddressRanges(TokenMetadata metadata)
    {
        Multimap<InetAddress, Range<Token>> map = HashMultimap.create();
        Collection<Range<Token>> ranges = metadata.getPrimaryRangesFor(metadata.sortedTokens());

        for (InetAddress address : metadata.getAllRingMembers())
        {
            map.putAll(address, ranges);
        }

        return map;
    }

    @Override
    public Multimap<Range<Token>, InetAddress> getRangeAddresses(TokenMetadata metadata)
    {
        Multimap<Range<Token>, InetAddress> map = HashMultimap.create();
        Collection<Range<Token>> ranges = metadata.getPrimaryRangesFor(metadata.sortedTokens());
        Set<InetAddress> allRingMembers = metadata.getAllRingMembers();

        for (Range<Token> range : ranges)
        {
            map.putAll(range, allRingMembers);
        }

        return map;
    }

    @Override
    public void validateOptions() throws ConfigurationException
    {
    }

    @Override
    public Collection<String> recognizedOptions()
    {
        return UnmodifiableArrayList.emptyList();
    }

    /**
     * @return <code>false</code> because the data is not partitioned across the ring.
     * See APOLLO-589 for details about why this was introduced.
     */
    @Override
    public boolean isPartitioned()
    {
        return false;
    }
}
