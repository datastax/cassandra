package org.apache.cassandra.hadoop;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.thrift.TokenRange;

/**
 * Restricts token range set to the given datacenter(s) and if there are several consecutive
 * ranges with the exactly same endpoints within those datacenters, it merges them into one single range.
 * This is useful when we want to run a Hadoop job in a DC that is configured without vnodes, yet some other DC
 * is configured with vnodes and scatters the token ring into many small ranges.
 */
public class TokenRangeMerger
{

    private final IPartitioner partitioner;

    public TokenRangeMerger(IPartitioner partitioner)
    {
        assert partitioner != null : "partitioner cannot be null";
        this.partitioner = partitioner;
    }

    public List<TokenRange> mergeRanges(Collection<TokenRange> ranges, Set<String> datacenters)
    {
        List<TokenRange> filtered = filterRangesByDC(ranges, datacenters);
        return mergeRanges(filtered);
    }

    private List<TokenRange> filterRangesByDC(Collection<TokenRange> ranges, Set<String> datacenters)
    {
        List<TokenRange> result = Lists.newArrayListWithExpectedSize(ranges.size());
        for (TokenRange tokenRange : ranges)
            result.add(filterEndpointsByDC(tokenRange, datacenters));
        return result;
    }

    private TokenRange filterEndpointsByDC(TokenRange tokenRange, Set<String> datacenters)
    {
        assert tokenRange.endpoint_details != null
                : "endpoint_details must not be null";
        assert tokenRange.endpoints.size() == tokenRange.endpoint_details.size()
                : "endpoint_details must be of the same size as endpoints";

        TokenRange result = new TokenRange();
        result.start_token = tokenRange.start_token;
        result.end_token = tokenRange.end_token;

        for (int i = 0; i < tokenRange.endpoints.size(); i++)
        {
            EndpointDetails ed = tokenRange.endpoint_details.get(i);
            if (datacenters.contains(ed.datacenter))
            {
                result.addToEndpoints(tokenRange.endpoints.get(i));
                result.addToEndpoint_details(ed);

                if (tokenRange.rpc_endpoints != null)
                    result.addToRpc_endpoints(tokenRange.rpc_endpoints.get(i));
            }
        }

        return result;
    }

    private List<TokenRange> mergeRanges(Collection<TokenRange> ranges)
    {
        Queue<TokenRange> sortedRanges = sortRanges(ranges);
        List<TokenRange> result = Lists.newArrayList();

        while (!sortedRanges.isEmpty())
            result.add(dequeueMerged(sortedRanges));

        return result;
    }

    private TokenRange dequeueMerged(Queue<TokenRange> ranges)
    {
        TokenRange head = ranges.poll();
        String startToken = head.start_token;
        String endToken = head.end_token;
        Set<String> endpoints = Sets.newHashSet(head.endpoints);

        while (!ranges.isEmpty() &&
                ranges.peek().start_token.equals(endToken) &&
                Sets.newHashSet(ranges.peek().endpoints).equals(endpoints))
        {
            endToken = ranges.poll().end_token;
        }

        TokenRange result = new TokenRange(startToken, endToken, head.endpoints);
        result.endpoint_details = head.endpoint_details;
        result.rpc_endpoints = head.rpc_endpoints;
        return result;
    }

    private Queue<TokenRange> sortRanges(Collection<TokenRange> ranges)
    {
        List<TokenRange> sortedRanges = Lists.newArrayList(ranges);
        Collections.sort(sortedRanges, new Comparator<TokenRange>()
        {
            @Override
            public int compare(TokenRange o1, TokenRange o2)
            {
                Token.TokenFactory tf = partitioner.getTokenFactory();
                Token t1 = tf.fromString(o1.start_token);
                Token t2 = tf.fromString(o2.start_token);
                return t1.compareTo(t2);
            }
        });
        return Lists.newLinkedList(sortedRanges);
    }
}