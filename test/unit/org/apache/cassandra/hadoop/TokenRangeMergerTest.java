package org.apache.cassandra.hadoop;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.thrift.TokenRange;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

public class TokenRangeMergerTest
{
    @Test
    public void testNoMergingSingleDC()
    {
        IPartitioner partitioner = new Murmur3Partitioner();
        TokenRange tr1 = new TokenRange("100", "200", Lists.newArrayList("node0", "node1"));
        tr1.addToEndpoint_details(new EndpointDetails("node0", "dc0"));
        tr1.addToEndpoint_details(new EndpointDetails("node1", "dc0"));

        TokenRange tr2 = new TokenRange("200", "300", Lists.newArrayList("node1", "node2"));
        tr2.addToEndpoint_details(new EndpointDetails("node1", "dc0"));
        tr2.addToEndpoint_details(new EndpointDetails("node2", "dc0"));

        TokenRange tr3 = new TokenRange("300", "100", Lists.newArrayList("node2", "node0"));
        tr3.addToEndpoint_details(new EndpointDetails("node2", "dc0"));
        tr3.addToEndpoint_details(new EndpointDetails("node0", "dc0"));

        List<TokenRange> ranges = Lists.newArrayList(tr1, tr2, tr3);
        List<TokenRange> merged = new TokenRangeMerger(partitioner).mergeRanges(ranges, Sets.newHashSet("dc0"));
        assertEquals(3, merged.size());
    }

    @Test
    public void testMergingSingleDC()
    {
        IPartitioner partitioner = new Murmur3Partitioner();
        TokenRange tr1 = new TokenRange("100", "200", Lists.newArrayList("node0", "node1"));
        tr1.addToEndpoint_details(new EndpointDetails("node0", "dc0"));
        tr1.addToEndpoint_details(new EndpointDetails("node1", "dc0"));
        tr1.addToRpc_endpoints("node0");
        tr1.addToRpc_endpoints("node1");

        TokenRange tr2 = new TokenRange("200", "300", Lists.newArrayList("node0", "node1"));
        tr2.addToEndpoint_details(new EndpointDetails("node0", "dc0"));
        tr2.addToEndpoint_details(new EndpointDetails("node1", "dc0"));
        tr2.addToRpc_endpoints("node0");
        tr2.addToRpc_endpoints("node1");

        TokenRange tr3 = new TokenRange("300", "100", Lists.newArrayList("node2", "node0"));
        tr3.addToEndpoint_details(new EndpointDetails("node2", "dc0"));
        tr3.addToEndpoint_details(new EndpointDetails("node0", "dc0"));
        tr3.addToRpc_endpoints("node0");
        tr3.addToRpc_endpoints("node1");

        List<TokenRange> ranges = Lists.newArrayList(tr1, tr2, tr3);
        List<TokenRange> merged = new TokenRangeMerger(partitioner).mergeRanges(ranges, Sets.newHashSet("dc0"));
        assertEquals(2, merged.size());
        TokenRange merged1 = merged.get(0);
        assertEquals("100", merged1.start_token);
        assertEquals("300", merged1.end_token);

        TokenRange merged2 = merged.get(1);
        assertEquals("300", merged2.start_token);
        assertEquals("100", merged2.end_token);

    }

    @Test
    public void testMoreMergingSingleDC()
    {
        IPartitioner partitioner = new Murmur3Partitioner();
        TokenRange tr1 = new TokenRange("100", "200", Lists.newArrayList("node0", "node1"));
        tr1.addToEndpoint_details(new EndpointDetails("node0", "dc0"));
        tr1.addToEndpoint_details(new EndpointDetails("node1", "dc0"));
        tr1.addToRpc_endpoints("node0");
        tr1.addToRpc_endpoints("node1");

        TokenRange tr2 = new TokenRange("200", "300", Lists.newArrayList("node0", "node1"));
        tr2.addToEndpoint_details(new EndpointDetails("node0", "dc0"));
        tr2.addToEndpoint_details(new EndpointDetails("node1", "dc0"));
        tr2.addToRpc_endpoints("node0");
        tr2.addToRpc_endpoints("node1");

        TokenRange tr3 = new TokenRange("300", "100", Lists.newArrayList("node0", "node1"));
        tr3.addToEndpoint_details(new EndpointDetails("node0", "dc0"));
        tr3.addToEndpoint_details(new EndpointDetails("node1", "dc0"));
        tr3.addToRpc_endpoints("node0");
        tr3.addToRpc_endpoints("node1");

        List<TokenRange> ranges = Lists.newArrayList(tr1, tr2, tr3);
        List<TokenRange> merged = new TokenRangeMerger(partitioner).mergeRanges(ranges, Sets.newHashSet("dc0"));
        assertEquals(1, merged.size());

        TokenRange tokenRange = merged.get(0);
        assertEquals("100", tokenRange.start_token);
        assertEquals("100", tokenRange.end_token);
        assertNotNull(tokenRange.endpoint_details);
        assertNotNull(tokenRange.endpoints);
        assertNotNull(tokenRange.rpc_endpoints);
        assertEquals(2, tokenRange.endpoints.size());
        assertEquals(2, tokenRange.rpc_endpoints.size());
    }


    @Test
    public void testMergingMultipleDC()
    {
        IPartitioner partitioner = new Murmur3Partitioner();
        TokenRange tr1 = new TokenRange("100", "200", Lists.newArrayList("node0", "node1"));
        tr1.addToEndpoint_details(new EndpointDetails("node0", "dc0"));
        tr1.addToEndpoint_details(new EndpointDetails("node1", "dc1"));

        TokenRange tr2 = new TokenRange("200", "300", Lists.newArrayList("node0", "node2"));
        tr2.addToEndpoint_details(new EndpointDetails("node0", "dc0"));
        tr2.addToEndpoint_details(new EndpointDetails("node2", "dc1"));

        TokenRange tr3 = new TokenRange("300", "100", Lists.newArrayList("node0", "node3"));
        tr3.addToEndpoint_details(new EndpointDetails("node0", "dc0"));
        tr3.addToEndpoint_details(new EndpointDetails("node3", "dc1"));

        List<TokenRange> ranges = Lists.newArrayList(tr1, tr2, tr3);
        List<TokenRange> merged = new TokenRangeMerger(partitioner).mergeRanges(ranges, Sets.newHashSet("dc0"));

        assertEquals(1, merged.size());
        TokenRange tr = merged.get(0);
        assertEquals(1, tr.getEndpoints().size());
        assertEquals(1, tr.getEndpoint_details().size());
        assertEquals("100", tr.start_token);
        assertEquals("100", tr.end_token);
    }



}
