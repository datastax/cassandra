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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.cassandra.utils.CassandraVersion;

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

    private static EndpointState state(VersionedValue status, String version)
    {
        EndpointState state = new EndpointState(HeartBeatState.empty());
        if (status != null)
            state.addApplicationState(ApplicationState.STATUS_WITH_PORT, status);
        if (version != null)
            state.addApplicationState(ApplicationState.RELEASE_VERSION, FACTORY.releaseVersion(version));
        return state;
    }

    private static List<String> blockers(EndpointState... states) throws UnknownHostException
    {
        Map<InetAddressAndPort, EndpointState> map = new HashMap<>();
        for (int i = 0; i < states.length; i++)
            map.put(InetAddressAndPort.getByName("127.0.0." + (i + 1)), states[i]);
        return StorageService.shrinkBlockers(map);
    }

    @Test
    public void testSupportedVersions()
    {
        for (String version : new String[]{ "5.0.7.0", "5.0.7.0-SNAPSHOT", "5.0.7.1", "5.0.8.0", "5.1.0.0", "6.0.0.0", "5.0.7.0-dev-hotfix-X" })
            assertThat(StorageService.supportsShrinkTokens(new CassandraVersion(version))).as(version).isTrue();
        for (String version : new String[]{ "5.0.6.9", "5.0.6.0", "5.0.7", "4.0.11.0", "5.0.6.0-SNAPSHOT" })
            assertThat(StorageService.supportsShrinkTokens(new CassandraVersion(version))).as(version).isFalse();
    }

    @Test
    public void testShrinkBlockers() throws UnknownHostException
    {
        String version = "5.0.7.0-SNAPSHOT";
        assertThat(blockers(state(FACTORY.normal(TOKENS), version), state(FACTORY.normal(TOKENS), version))).isEmpty();

        // an old node, even if it is down (its gossip state is still known)
        assertThat(blockers(state(FACTORY.normal(TOKENS), version), state(FACTORY.normal(TOKENS), "5.0.6.0")))
        .singleElement().asString().contains("does not support shrinking tokens");
        // unknown or unparsable versions are refused
        assertThat(blockers(state(FACTORY.normal(TOKENS), null))).singleElement().asString().contains("is unknown");
        assertThat(blockers(state(FACTORY.normal(TOKENS), "garbage"))).singleElement().asString().contains("is unknown");

        // nodes that left or were removed don't count, even with an old version
        assertThat(blockers(state(FACTORY.normal(TOKENS), version), state(FACTORY.left(TOKENS, 0), "4.0.0.0"),
                            state(FACTORY.removedNonlocal(java.util.UUID.randomUUID(), 0), "4.0.0.0"))).isEmpty();

        // range movements and replacements in progress
        for (VersionedValue status : new VersionedValue[]{ FACTORY.bootstrapping(TOKENS), FACTORY.bootReplacing(InetAddressAndPort.getByName("127.0.0.9").getAddress()),
                                                           FACTORY.hibernate(true), FACTORY.leaving(TOKENS), FACTORY.moving(TOKENS.get(0)),
                                                           FACTORY.shrinking(TOKENS.subList(0, 1)), FACTORY.removingNonlocal(java.util.UUID.randomUUID()) })
            assertThat(blockers(state(FACTORY.normal(TOKENS), version), state(status, version))).as(status.value)
            .singleElement().asString().contains("is in state");
    }

    @Test
    public void testShrinkingStatus()
    {
        VersionedValue status = FACTORY.shrinking(TOKENS);
        assertThat(status.value).isEqualTo("SHRINKING,-10,10");
    }

    @Test
    public void testTokenCountOverride()
    {
        assertThat(TokenCountOverride.exists()).isFalse();
        assertThat(TokenCountOverride.matches(TOKENS, 4)).isFalse();

        TokenCountOverride.record(TOKENS, 4);
        assertThat(TokenCountOverride.exists()).isTrue();
        assertThat(TokenCountOverride.matches(TOKENS, 4)).isTrue();
        // num_tokens changed to yet another value
        assertThat(TokenCountOverride.matches(TOKENS, 3)).isFalse();
        // same count, different tokens
        assertThat(TokenCountOverride.matches(Arrays.asList(TOKENS.get(0), Murmur3Partitioner.instance.getTokenFactory().fromString("11")), 4)).isFalse();
        assertThat(TokenCountOverride.matches(Collections.singletonList(TOKENS.get(0)), 4)).isFalse();
        // order doesn't matter
        assertThat(TokenCountOverride.matches(Arrays.asList(TOKENS.get(1), TOKENS.get(0)), 4)).isTrue();

        // a new record replaces the previous one
        TokenCountOverride.record(TOKENS.subList(0, 1), 4);
        assertThat(TokenCountOverride.matches(TOKENS, 4)).isFalse();
        assertThat(TokenCountOverride.matches(TOKENS.subList(0, 1), 4)).isTrue();

        TokenCountOverride.clear();
        assertThat(TokenCountOverride.exists()).isFalse();
    }
}
