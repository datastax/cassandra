/*
 * Copyright DataStax, Inc.
 */
package org.apache.cassandra.dht;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import org.apache.cassandra.config.Schema;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.schema.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Contains a utest port of the range-streamer loging of the corresponding dtests in {@code bootstrap_test.py}.
 */
@RunWith(BMUnitRunner.class)
@BMRule(name = "Intercept RangeStreamer.handleSourceNotFound",
        targetClass = "RangeStreamer",
        targetMethod = "handleSourceNotFound",
        targetLocation = "AT INVOKE org.apache.cassandra.db.Keyspace.isInitialized",
        action = "org.apache.cassandra.dht.RangeStreamerBootstrapTest.hasInsufficientSources()")
public class RangeStreamerBootstrapTest
{

    @Test
    public void testConsistentRangeMovementTruewithReplicaDownShouldFailNovnode()
    {
        testConsistentRangeMovement(true, 2, false);
    }

    @Test
    public void testConsistentRangeMovementFalseWithReplicaDownShouldSucceedNovnode()
    {
        testConsistentRangeMovement(false, 2, false);
    }

    @Test
    public void testConsistentRangeMovementTrueWithRf1ShouldFailNovnode()
    {
        testConsistentRangeMovement(true, 1, false);
    }

    @Test
    public void testConsistentRangeMovementFalseWithRf1ShouldSucceedNovnode()
    {
        testConsistentRangeMovement(false, 1, false);
    }

    @Test
    public void testConsistentRangeMovementTruewithReplicaDownShouldFailVnode()
    {
        testConsistentRangeMovement(true, 2, true);
    }

    @Test
    public void testConsistentRangeMovementFalseWithReplicaDownShouldSucceedVnode()
    {
        testConsistentRangeMovement(false, 2, true);
    }

    @Test
    public void testConsistentRangeMovementTrueWithRf1ShouldFailVnode()
    {
        testConsistentRangeMovement(false, 1, true);
    }

    @Test
    public void testConsistentRangeMovementFalseWithRf1ShouldSucceedVnode()
    {
        testConsistentRangeMovement(false, 1, true);
    }

    static final List<Collection<Token>> tokensVnodes = new ArrayList<>(3);
    static final List<Collection<Token>> tokensNoVnodes = Arrays.asList(
        Collections.singleton(new Murmur3Partitioner.LongToken(-9223372036854775808L)),
        Collections.singleton(new Murmur3Partitioner.LongToken(3074457345618258602L)),
        Collections.singleton(new Murmur3Partitioner.LongToken(-3074457345618258603L))
    );
    static boolean insufficientSources;

    public static void hasInsufficientSources()
    {
        insufficientSources = true;
    }

    static
    {
        Random r = new Random();
        for (int node = 0; node < 3; node++)
        {
            Token[] tokens = new Token[256];
            for (int i = 0; i < tokens.length; i++)
                tokens[i] = new Murmur3Partitioner.LongToken(r.nextLong());
            tokensVnodes.add(Arrays.asList(tokens));
        }
    }

    private final IEndpointSnitch snitch = new AbstractNetworkTopologySnitch()
    {
        public String getRack(InetAddress endpoint)
        {
            return "Rack-" + endpoint.getAddress()[2];
        }

        public String getDatacenter(InetAddress endpoint)
        {
            return "DC-" + endpoint.getAddress()[1];
        }
    };

    private Collection<Token> tokens(int node, boolean vnodes)
    {
        return (vnodes ? tokensVnodes : tokensNoVnodes).get(node);
    }

    private static InetAddress addr(int node)
    {
        byte[] addr = new byte[]{ 1, (byte) 1, (byte) 1, (byte) node };
        try
        {
            return InetAddress.getByAddress(addr);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void setup()
    {
//        DatabaseDescriptor.daemonInitialization();
        Keyspace.setInitialized();
        Gossiper.instance.startForTest();
    }

    private void testConsistentRangeMovement(boolean consistentRangeMovement,
                                             int rf,
                                             boolean vnodes)
    {
        DatabaseDescriptor.setEndpointSnitch(snitch);

        TokenMetadata tokenMetadata = new TokenMetadata()
        {
            TokenMetadata.Topology topo = new TokenMetadata.Topology();

            public Topology getTopology()
            {
                return super.topology;
            }
        };

        Collection<Token> node3tokens = tokens(2, vnodes);
        InetAddress node3address = addr(3);

        tokenMetadata.updateHostId(UUID.randomUUID(), addr(1));
        tokenMetadata.updateHostId(UUID.randomUUID(), addr(2));
        tokenMetadata.updateNormalTokens(tokens(0, vnodes), addr(1));
        tokenMetadata.updateNormalTokens(tokens(1, vnodes), addr(2));

        tokenMetadata.updateHostId(UUID.randomUUID(), node3address);
        tokenMetadata.addBootstrapTokens(node3tokens, node3address);

        if (Schema.instance.getKeyspaceInstance("ks1") != null)
            Schema.instance.removeKeyspaceInstance("ks1");

        Keyspace ks = Keyspace.mockKS(KeyspaceMetadata.create("ks1", KeyspaceParams.simple(rf)));
        Schema.instance.storeKeyspaceInstance(ks);

        AbstractReplicationStrategy strategy = ks.getReplicationStrategy();
        Collection<Range<Token>> pendingRanges = strategy.getPendingAddressRanges(tokenMetadata, node3tokens, node3address);

        //

        StreamStateStore stateStore = new StreamStateStore();
        StreamingOptions options = StreamingOptions.forBootStrap(tokenMetadata);

        IFailureDetector failureDetector = new IFailureDetector()
        {
            public boolean isAlive(InetAddress ep)
            {
                // node 2 is down
                return !ep.equals(addr(2));
            }

            public void interpret(InetAddress ep) {}
            public void report(InetAddress ep) {}
            public void remove(InetAddress ep) {}
            public void forceConviction(InetAddress ep) {}
            public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener) {}
            public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener) {}
        };

        RangeStreamer streamer = new RangeStreamer(tokenMetadata,
                                                   tokens(2, vnodes),
                                                   addr(3),
                                                   "Bootstrap",
                                                   consistentRangeMovement,
                                                   RangeStreamer.StreamConsistency.ONE,
                                                   snitch,
                                                   stateStore,
                                                   true,
                                                   options.toSourceFilter(snitch,
                                                                          failureDetector));

        //

        boolean exception = false;
        boolean requiredNodeDOwn = false;
        String msg = "<no exception>";
        insufficientSources = false;

        try
        {
            streamer.addRanges("ks1", strategy.getPendingAddressRanges(tokenMetadata, node3tokens, node3address));
        }
        catch (RuntimeException e)
        {
            exception = true;
            msg = e.getMessage();
            requiredNodeDOwn = msg.startsWith("A node required to move the data consistently is down");
        }

        if (!consistentRangeMovement) // successful_bootstrap_expected
        {
            // with rf = 1 and cassandra.consistent.rangemovement = false, missing sources are ignored
            if (rf == 1)
                // TODO verify that this message has been logged
                assertTrue("Missing 'Unable to find sufficient sources for streaming range': " + msg, insufficientSources);
            assertFalse("No exception must have been thrown, but got " + msg, exception);
        }
        else
        {
            assertTrue("Missing 'A node required to move the data consistently is down': " + msg, requiredNodeDOwn);
        }
    }
}
