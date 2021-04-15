package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.UnmodifiableArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EverywhereStrategyTest
{
    private Random random = new Random();

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void allRingMembersAreReplicas() throws Throwable
    {
        TokenMetadata metadata = new TokenMetadata();

        populateTokenMetadata(3, 1, metadata);

        EverywhereStrategy strategy = createStrategy(metadata);

        assertAllNodesCoverFullRing(strategy, metadata);
    }

    @Test
    public void allRingMembersAreReplicasWithvnodes() throws Throwable
    {
        TokenMetadata metadata = new TokenMetadata();
        populateTokenMetadata(5, 8, metadata);

        EverywhereStrategy strategy = createStrategy(metadata);

        assertAllNodesCoverFullRing(strategy, metadata);
    }

    @Test
    public void bootstrappingNodesAreNotIncludedAsReplicas() throws Throwable
    {
        TokenMetadata metadata = new TokenMetadata();

        populateTokenMetadata(3, 1, metadata);

        metadata.addBootstrapTokens(UnmodifiableArrayList.of(getRandomToken()),
                                    InetAddress.getByName("127.0.0.4"));

        EverywhereStrategy strategy = createStrategy(metadata);

        assertAllNodesCoverFullRing(strategy, metadata);
    }

    @Test
    public void leavingNodesDoNotAddPendingRanges() throws Throwable
    {
        TokenMetadata metadata = new TokenMetadata();

        populateTokenMetadata(3, 1, metadata);
        InetAddress leavingEndpoint = metadata.getAllRingMembers().iterator().next();
        metadata.addLeavingEndpoint(leavingEndpoint);

        EverywhereStrategy strategy = createStrategy(metadata);

        metadata.calculatePendingRanges(strategy, strategy.keyspaceName);
        PendingRangeMaps pendingRanges = metadata.getPendingRanges(strategy.keyspaceName);

        assertFalse("pending ranges must be empty",
                    pendingRanges.iterator().hasNext());
    }

    @Test
    public void bootstrapNodesNeedFullRingOnPendingRangesCalculation() throws Throwable
    {
        TokenMetadata metadata = new TokenMetadata();

        populateTokenMetadata(3, 1, metadata);

        EverywhereStrategy strategy = createStrategy(metadata);

        InetAddress bootstrapNode = InetAddress.getByName("127.0.0.4");
        metadata.addBootstrapTokens(UnmodifiableArrayList.of(getRandomToken()), bootstrapNode);

        metadata.calculatePendingRanges(strategy, strategy.keyspaceName);
        PendingRangeMaps pendingRangeMaps = metadata.getPendingRanges(strategy.keyspaceName);

        List<Range<Token>> pendingRanges = new ArrayList<>();
        for (Map.Entry<Range<Token>, List<InetAddress>> pendingRangeEntry : pendingRangeMaps)
        {
            List<InetAddress> pendingNodes = pendingRangeEntry.getValue();
            // only the bootstrap node has pending ranges
            assertEquals(1, pendingNodes.size());
            assertTrue(pendingNodes.contains(bootstrapNode));
            pendingRanges.add(pendingRangeEntry.getKey());
        }

        List<Range<Token>> normalizedRanges = Range.normalize(pendingRanges);
        assertEquals(1, normalizedRanges.size());
        Range<Token> tokenRange = normalizedRanges.get(0);
        // it must cover all ranges
        assertEquals(tokenRange.left, tokenRange.right);
    }

    private EverywhereStrategy createStrategy(TokenMetadata tokenMetadata)
    {
        IEndpointSnitch snitch = new PropertyFileSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);

        return new EverywhereStrategy("keyspace", tokenMetadata, snitch, Collections.emptyMap());
    }

    private void populateTokenMetadata(int nodeCount, int tokens, TokenMetadata metadata) throws UnknownHostException
    {
        List<InetAddress> nodes = new ArrayList<>();
        for (int i = 1; i <= nodeCount; i++)
        {
            nodes.add(InetAddress.getByName(String.format("127.0.0.%d", i)));
        }

        for (int i = 0; i < tokens; i++)
        {
            for (InetAddress node : nodes)
            {
                Token randomToken = getRandomToken();
                metadata.updateNormalToken(randomToken, node);
            }
        }
    }

    private void assertAllNodesCoverFullRing(AbstractReplicationStrategy strategy, TokenMetadata metadata)
    {
        for (Token ringToken : metadata.sortedTokens())
        {
            List<InetAddress> replicas = strategy.calculateNaturalEndpoints(ringToken, metadata);
            assertEquals(metadata.getAllRingMembers().size(), replicas.size());
            assertEquals(Sets.newHashSet(metadata.getAllRingMembers()), Sets.newHashSet(replicas));
        }
    }

    private Token getRandomToken()
    {
        return Murmur3Partitioner.instance.getRandomToken(random);
    }
}
