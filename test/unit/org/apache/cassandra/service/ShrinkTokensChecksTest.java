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

package org.apache.cassandra.service;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cluster-wide preconditions of {@link StorageService#shrinkTokens} and the token count override record.
 */
public class ShrinkTokensChecksTest
{
    private static final VersionedValue.VersionedValueFactory FACTORY = new VersionedValue.VersionedValueFactory(Murmur3Partitioner.instance);
    private static final List<Token> TOKENS = Arrays.asList(Murmur3Partitioner.instance.getTokenFactory().fromString("-10"),
                                                            Murmur3Partitioner.instance.getTokenFactory().fromString("10"));

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void clearOverride()
    {
        TokenCountOverride.clear();
    }

    private static EndpointState state(VersionedValue status, boolean supported)
    {
        EndpointState state = new EndpointState(HeartBeatState.empty());
        if (status != null)
            state.addApplicationState(ApplicationState.STATUS_WITH_PORT, status);
        state.addApplicationState(ApplicationState.RELEASE_VERSION, FACTORY.releaseVersion("5.0.7.0"));
        if (supported)
            state.addApplicationState(ApplicationState.SHRINK_TOKENS_SUPPORTED, FACTORY.shrinkTokensSupported());
        return state;
    }

    private static List<String> blockers(EndpointState... states) throws UnknownHostException
    {
        return blockers(e -> true, states);
    }

    private static List<String> blockers(Predicate<InetAddressAndPort> isAlive, EndpointState... states) throws UnknownHostException
    {
        Map<InetAddressAndPort, EndpointState> map = new HashMap<>();
        for (int i = 0; i < states.length; i++)
            map.put(InetAddressAndPort.getByName("127.0.0." + (i + 1)), states[i]);
        return StorageService.shrinkBlockers(map, isAlive);
    }

    @Test
    public void testShrinkBlockers() throws UnknownHostException
    {
        assertThat(blockers(state(FACTORY.normal(TOKENS), true), state(FACTORY.normal(TOKENS), true))).isEmpty();

        // a node that doesn't advertise the capability, even if it is down (its gossip state is still known)
        assertThat(blockers(state(FACTORY.normal(TOKENS), true), state(FACTORY.normal(TOKENS), false)))
        .singleElement().asString().contains("does not support shrinking tokens").contains("5.0.7.0");

        // a node that is down or unreachable
        InetAddressAndPort down = InetAddressAndPort.getByName("127.0.0.2");
        assertThat(blockers(e -> !e.equals(down), state(FACTORY.normal(TOKENS), true), state(FACTORY.normal(TOKENS), true)))
        .singleElement().asString().contains("127.0.0.2").contains("down or unreachable");

        // nodes that left or were removed don't count, even without the capability and down
        assertThat(blockers(e -> e.toString().contains("127.0.0.1:"),
                            state(FACTORY.normal(TOKENS), true), state(FACTORY.left(TOKENS, 0), false),
                            state(FACTORY.removedNonlocal(UUID.randomUUID(), 0), false))).isEmpty();

        // range movements and replacements in progress
        for (VersionedValue status : new VersionedValue[]{ FACTORY.bootstrapping(TOKENS), FACTORY.bootReplacing(InetAddressAndPort.getByName("127.0.0.9").getAddress()),
                                                           FACTORY.hibernate(true), FACTORY.leaving(TOKENS), FACTORY.moving(TOKENS.get(0)),
                                                           FACTORY.shrinking(TOKENS.subList(0, 1)), FACTORY.removingNonlocal(UUID.randomUUID()) })
            assertThat(blockers(state(FACTORY.normal(TOKENS), true), state(status, true))).as(status.value)
            .singleElement().asString().contains("is in state");
    }

    @Test
    public void testShrinkingStatus()
    {
        VersionedValue status = FACTORY.shrinking(TOKENS);
        assertThat(status.value).isEqualTo("SHRINKING,-10,10");
    }

    private static List<Token> tokens(String... tokens)
    {
        List<Token> result = new java.util.ArrayList<>();
        for (String token : tokens)
            result.add(Murmur3Partitioner.instance.getTokenFactory().fromString(token));
        return result;
    }

    @Test
    public void testTokenCountOverride()
    {
        List<Token> t4 = tokens("1", "2", "3", "4");
        List<Token> t2 = tokens("1", "3");
        List<Token> t1 = tokens("3");
        assertThat(TokenCountOverride.exists()).isFalse();
        assertThat(TokenCountOverride.matches(t2, 4)).isFalse();

        // round 1: 4 -> 2 tokens with num_tokens 4
        TokenCountOverride.record(t4, t2, 4);
        assertThat(TokenCountOverride.exists()).isTrue();
        assertThat(TokenCountOverride.matches(t2, 4)).isTrue();
        // order doesn't matter
        assertThat(TokenCountOverride.matches(tokens("3", "1"), 4)).isTrue();
        // num_tokens changed to an unrelated value
        assertThat(TokenCountOverride.matches(t2, 3)).isFalse();
        // other tokens with the same count
        assertThat(TokenCountOverride.matches(tokens("1", "2"), 4)).isFalse();

        // round 2 (the yaml may have been updated to 2 in between, without a restart): 2 -> 1 tokens
        TokenCountOverride.record(t2, t1, 4);
        assertThat(TokenCountOverride.matches(t1, 4)).isTrue();
        assertThat(TokenCountOverride.matches(t1, 2)).isTrue();
        assertThat(TokenCountOverride.matches(t1, 3)).isFalse();
        // crash after the record but before system.local was updated: the saved tokens are still the previous ones
        assertThat(TokenCountOverride.matches(t2, 4)).isTrue();

        TokenCountOverride.clear();
        assertThat(TokenCountOverride.exists()).isFalse();
    }

    @Test
    public void testTokenCountOverrideRollbackOfFirstRound()
    {
        List<Token> t4 = tokens("1", "2", "3", "4");
        List<Token> t2 = tokens("1", "3");
        // num_tokens is 1 in the test configuration: a failed shrink from 1 token doesn't need the record any more
        TokenCountOverride.record(tokens("7"), t2, DatabaseDescriptor.getNumTokens());
        TokenCountOverride.rollback(tokens("7"), t2);
        assertThat(TokenCountOverride.exists()).isFalse();
        // a failed shrink from 4 tokens with num_tokens 1 keeps the record for the 4 tokens
        TokenCountOverride.record(t4, t2, 4);
        TokenCountOverride.rollback(t4, t2);
        assertThat(TokenCountOverride.matches(t2, 4)).isFalse();
        assertThat(TokenCountOverride.matches(t4, 4)).isTrue();
    }
}
