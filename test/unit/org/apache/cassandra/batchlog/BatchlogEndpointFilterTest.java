//TODO

///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.cassandra.batchlog;
//
//        import java.net.InetAddress;
//        import java.net.UnknownHostException;
//        import java.util.Arrays;
//        import java.util.Collection;
//        import java.util.Collections;
//        import java.util.HashSet;
//        import java.util.List;
//        import java.util.concurrent.ThreadLocalRandom;
//        import java.util.function.Consumer;
//        import java.util.function.Supplier;
//        import java.util.stream.Collectors;
//        import java.util.stream.Stream;
//
//        import com.google.common.collect.ArrayListMultimap;
//        import com.google.common.collect.ImmutableMultimap;
//        import com.google.common.collect.Multimap;
//        import com.google.common.collect.Multimaps;
//        import org.junit.BeforeClass;
//        import org.junit.Test;
//
//        import org.apache.cassandra.config.Config;
//        import org.apache.cassandra.config.DatabaseDescriptor;
//        import org.apache.cassandra.db.ConsistencyLevel;
//        import org.apache.cassandra.exceptions.UnavailableException;
//        import org.apache.cassandra.locator.DynamicEndpointSnitch;
//        import org.apache.cassandra.locator.GossipingPropertyFileSnitch;
//        import org.apache.cassandra.locator.IEndpointSnitch;
//        import org.apache.cassandra.net.Verbs;
//        import org.apache.cassandra.service.StorageService;
//        import org.apache.cassandra.utils.FBUtilities;
//        import org.hamcrest.CoreMatchers;
//
//        import static org.hamcrest.CoreMatchers.*;
//        import static org.junit.Assert.assertEquals;
//        import static org.junit.Assert.assertThat;
//        import static org.junit.Assert.fail;
//
//public class BatchlogEndpointFilterTest
//{
//    private static final String LOCAL = "local";
//
//    // Repeat all tests some more times since we're dealing with random stuff - i.e. increase the
//    // chance to hit issues.
//    private static final int repetitions = 100;
//    private static final InetAddress[] INET_ADDRESSES = new InetAddress[0];
//
//    private DynamicEndpointSnitch dsnitch;
//
//    @BeforeClass
//    public static void setup()
//    {
//        DatabaseDescriptor.daemonInitialization();
//        DatabaseDescriptor.setBroadcastAddress(endpointAddress(0, 0));
//    }
//
//    @Test
//    public void shouldSelectCorrectEndpointFilter()
//    {
//        Multimap<String, InetAddress> endpoints = configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                Arrays.asList(LOCAL, "1", "2"),
//                15, 15, 15);
//
//        // simple test - random_remote does always return a RandomEndpointFilter
//        reconfigure(Config.BatchlogEndpointStrategy.random_remote, true, endpoints);
//        assertThat(BatchlogManager.endpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints),
//                CoreMatchers.instanceOf(BatchlogManager.RandomEndpointFilter.class));
//
//        reconfigure(Config.BatchlogEndpointStrategy.dynamic_remote, true, endpoints);
//        assertThat(BatchlogManager.endpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints),
//                CoreMatchers.instanceOf(BatchlogManager.DynamicEndpointFilter.class));
//
//        reconfigure(Config.BatchlogEndpointStrategy.dynamic, true, endpoints);
//        assertThat(BatchlogManager.endpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints),
//                CoreMatchers.instanceOf(BatchlogManager.DynamicEndpointFilter.class));
//
//        // dynamic + dynamic_remote must return the RandomEndpointFilter, if there's no dynamic snitch
//        reconfigure(Config.BatchlogEndpointStrategy.dynamic_remote, false, endpoints);
//        assertThat(BatchlogManager.endpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints),
//                CoreMatchers.instanceOf(BatchlogManager.RandomEndpointFilter.class));
//
//        reconfigure(Config.BatchlogEndpointStrategy.dynamic, false, endpoints);
//        assertThat(BatchlogManager.endpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints),
//                CoreMatchers.instanceOf(BatchlogManager.RandomEndpointFilter.class));
//
//        // dynamic + dynamic_remote must return a DynamicEndpoint, if dynamic snitch is enabled again
//        reconfigure(Config.BatchlogEndpointStrategy.dynamic_remote, true, endpoints);
//        assertThat(BatchlogManager.endpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints),
//                CoreMatchers.instanceOf(BatchlogManager.DynamicEndpointFilter.class));
//
//        reconfigure(Config.BatchlogEndpointStrategy.dynamic, true, endpoints);
//        assertThat(BatchlogManager.endpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints),
//                CoreMatchers.instanceOf(BatchlogManager.DynamicEndpointFilter.class));
//    }
//
//    @Test
//    public void shouldUseCoordinatorForSingleNodeDC()
//    {
//        withConfigs(Stream.of(
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Collections.singletonList(LOCAL),
//                        1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Collections.singletonList(LOCAL),
//                        1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
//                        Collections.singletonList(LOCAL),
//                        1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
//                        Collections.singletonList(LOCAL),
//                        1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Collections.singletonList(LOCAL),
//                        1)
//        ), this::shouldUseCoordinatorForSingleNodeDC);
//    }
//
//    private void shouldUseCoordinatorForSingleNodeDC(Multimap<String, InetAddress> endpoints)
//    {
//        Collection<InetAddress> result = BatchlogManager.filterEndpoints(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints);
//        assertThat(result.size(), is(1));
//        assertThat(result, hasItem(endpointAddress(0, 0)));
//    }
//
//    @Test
//    public void shouldUseCoordinatorAndTheOtherForTwoNodesInOneRack()
//    {
//        withConfigs(Stream.of(
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Collections.singletonList(LOCAL),
//                        2),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Collections.singletonList(LOCAL),
//                        2),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
//                        Collections.singletonList(LOCAL),
//                        2),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
//                        Collections.singletonList(LOCAL),
//                        2),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Collections.singletonList(LOCAL),
//                        2)
//        ), this::shouldUseCoordinatorAndTheOtherForTwoNodesInOneRack);
//    }
//
//    private void shouldUseCoordinatorAndTheOtherForTwoNodesInOneRack(Multimap<String, InetAddress> endpoints)
//    {
//        Collection<InetAddress> result = createEndpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints).filter();
//        assertThat(result.size(), is(2));
//        assertThat(new HashSet<>(result).size(), is(2));
//        assertThat(result, hasItem(endpointAddress(0, 0)));
//        assertThat(result, hasItem(endpointAddress(0, 1)));
//    }
//
//    @Test
//    public void shouldUseCoordinatorAndTheOtherForTwoNodesInTwoRacks()
//    {
//        withConfigs(Stream.of(
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Arrays.asList(LOCAL, "r1"),
//                        1, 1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Arrays.asList(LOCAL, "r1"),
//                        1, 1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
//                        Arrays.asList(LOCAL, "r1"),
//                        1, 1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Arrays.asList(LOCAL, "r1"),
//                        1, 1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
//                        Arrays.asList(LOCAL, "r1"),
//                        1, 1)
//        ), this::shouldUseCoordinatorAndTheOtherForTwoNodesInTwoRacks);
//    }
//
//    private void shouldUseCoordinatorAndTheOtherForTwoNodesInTwoRacks(Multimap<String, InetAddress> endpoints)
//    {
//        Collection<InetAddress> result = createEndpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints).filter();
//        assertThat(result.size(), is(2));
//        assertThat(new HashSet<>(result).size(), is(2));
//        assertThat(result, hasItem(endpointAddress(0, 0)));
//        assertThat(result, hasItem(endpointAddress(1, 0)));
//    }
//
//    @Test
//    public void shouldSelectOneNodeFromLocalRackAndOneNodeFromTheOtherRack()
//    {
//        withConfigs(Stream.of(
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Arrays.asList(LOCAL, "1"),
//                        2, 1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Arrays.asList(LOCAL, "r1"),
//                        2, 1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
//                        Arrays.asList(LOCAL, "r1"),
//                        2, 1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Arrays.asList(LOCAL, "r1"),
//                        2, 1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
//                        Arrays.asList(LOCAL, "r1"),
//                        2, 1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Arrays.asList(LOCAL, "r1"),
//                        1, 1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
//                        Arrays.asList(LOCAL, "r1"),
//                        1, 1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Arrays.asList(LOCAL, "r1"),
//                        1, 1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
//                        Arrays.asList(LOCAL, "r1"),
//                        1, 1)
//        ), this::shouldSelectOneNodeFromLocalRackAndOneNodeFromTheOtherRack);
//    }
//
//    private void shouldSelectOneNodeFromLocalRackAndOneNodeFromTheOtherRack(Multimap<String, InetAddress> endpoints)
//    {
//        for (int i = 0; i < repetitions; i++)
//        {
//            Collection<InetAddress> result = createEndpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints).filter();
//            assertThat(result.size(), is(2));
//            assertThat(new HashSet<>(result).size(), is(2));
//            assertThat(result, hasItem(endpointAddress(1, 0)));
//            assertThat(result, either(hasItem(endpointAddress(0, 0)))
//                    .or(hasItem(endpointAddress(0, 1))));
//        }
//    }
//
//    @Test
//    public void shouldFailIfAllBatchlogEnpointsAreUnavailable()
//    {
//        withConfigs(Stream.of(
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Collections.singletonList(LOCAL),
//                        3),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Collections.singletonList(LOCAL),
//                        3),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Collections.singletonList(LOCAL),
//                        3),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
//                        Collections.singletonList(LOCAL),
//                        3),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
//                        Collections.singletonList(LOCAL),
//                        3),
//
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Collections.singletonList(LOCAL),
//                        15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Collections.singletonList(LOCAL),
//                        15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Collections.singletonList(LOCAL),
//                        15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
//                        Collections.singletonList(LOCAL),
//                        15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
//                        Collections.singletonList(LOCAL),
//                        15),
//
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Arrays.asList(LOCAL, "r1"),
//                        15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Arrays.asList(LOCAL, "r1"),
//                        15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Arrays.asList(LOCAL, "r1"),
//                        15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
//                        Arrays.asList(LOCAL, "r1"),
//                        15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
//                        Arrays.asList(LOCAL, "r1"),
//                        15, 15),
//
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Arrays.asList(LOCAL, "r1", "r2"),
//                        15, 15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Arrays.asList(LOCAL, "r1", "r2"),
//                        15, 15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Arrays.asList(LOCAL, "r1", "r2"),
//                        15, 15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
//                        Arrays.asList(LOCAL, "r1", "r2"),
//                        15, 15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
//                        Arrays.asList(LOCAL, "r1", "r2"),
//                        15, 15, 15)
//        ), this::shouldFailIfAllBatchlogEnpointsAreUnavailable);
//    }
//
//    private void shouldFailIfAllBatchlogEnpointsAreUnavailable(Multimap<String, InetAddress> endpoints)
//    {
//        for (int i = 0; i < repetitions; i++)
//        {
//            BatchlogManager.EndpointFilter filter = DatabaseDescriptor.getBatchlogEndpointStrategy().dynamicSnitch
//                    ? new TestDynamicEndpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints)
//            {
//                protected boolean isAlive(InetAddress input)
//                {
//                    return input.equals(endpointAddress(0, 0));
//                }
//            }
//                    : new TestRandomEndpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints)
//            {
//                protected boolean isAlive(InetAddress input)
//                {
//                    return input.equals(endpointAddress(0, 0));
//                }
//            };
//            try
//            {
//                filter.filter();
//                fail();
//            }
//            catch (UnavailableException ignore)
//            {
//                // ok!
//            }
//        }
//    }
//
//    @Test
//    public void shouldNotFailIfThereAreAtLeastTwoLiveNodesBesideCoordinator()
//    {
//        withConfigs(Stream.of(
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Collections.singletonList(LOCAL),
//                        3),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Collections.singletonList(LOCAL),
//                        3),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Collections.singletonList(LOCAL),
//                        3),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
//                        Collections.singletonList(LOCAL),
//                        3),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
//                        Collections.singletonList(LOCAL),
//                        3),
//
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Collections.singletonList(LOCAL),
//                        15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Collections.singletonList(LOCAL),
//                        15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Collections.singletonList(LOCAL),
//                        15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
//                        Collections.singletonList(LOCAL),
//                        15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
//                        Collections.singletonList(LOCAL),
//                        15),
//
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Arrays.asList(LOCAL, "r1"),
//                        15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Arrays.asList(LOCAL, "r1"),
//                        15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Arrays.asList(LOCAL, "r1"),
//                        15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
//                        Arrays.asList(LOCAL, "r1"),
//                        15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
//                        Arrays.asList(LOCAL, "r1"),
//                        15, 15),
//
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Arrays.asList(LOCAL, "r1", "r2"),
//                        15, 15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Arrays.asList(LOCAL, "r1", "r2"),
//                        15, 15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Arrays.asList(LOCAL, "r1", "r2"),
//                        15, 15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
//                        Arrays.asList(LOCAL, "r1", "r2"),
//                        15, 15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
//                        Arrays.asList(LOCAL, "r1", "r2"),
//                        15, 15, 15)
//        ), this::shouldNotFailIfThereAreAtLeastTwoLiveNodesBesideCoordinator);
//    }
//
//    private void shouldNotFailIfThereAreAtLeastTwoLiveNodesBesideCoordinator(Multimap<String, InetAddress> endpoints)
//    {
//        for (int i = 0; i < repetitions; i++)
//        {
//            BatchlogManager.EndpointFilter filter = DatabaseDescriptor.getBatchlogEndpointStrategy().dynamicSnitch
//                    ? new TestDynamicEndpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints)
//            {
//                protected boolean isAlive(InetAddress input)
//                {
//                    return nodeInRack(input) >= endpoints.get(LOCAL).size() - 2;
//                }
//            }
//                    : new TestRandomEndpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints)
//            {
//                protected boolean isAlive(InetAddress input)
//                {
//                    return nodeInRack(input) >= endpoints.get(LOCAL).size() - 2;
//                }
//            };
//            filter.filter();
//        }
//    }
//
//    @Test
//    public void shouldNotFailIfThereAreAtLeastTwoLiveNodesIncludingCoordinator()
//    {
//        withConfigs(Stream.of(
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Collections.singletonList(LOCAL),
//                        3),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Collections.singletonList(LOCAL),
//                        3),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Collections.singletonList(LOCAL),
//                        3),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
//                        Collections.singletonList(LOCAL),
//                        3),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
//                        Collections.singletonList(LOCAL),
//                        3),
//
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Collections.singletonList(LOCAL),
//                        15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Collections.singletonList(LOCAL),
//                        15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Collections.singletonList(LOCAL),
//                        15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
//                        Collections.singletonList(LOCAL),
//                        15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
//                        Collections.singletonList(LOCAL),
//                        15),
//
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Arrays.asList(LOCAL, "r1"),
//                        15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Arrays.asList(LOCAL, "r1"),
//                        15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Arrays.asList(LOCAL, "r1"),
//                        15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
//                        Arrays.asList(LOCAL, "r1"),
//                        15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
//                        Arrays.asList(LOCAL, "r1"),
//                        15, 15),
//
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Arrays.asList(LOCAL, "r1", "r2"),
//                        15, 15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Arrays.asList(LOCAL, "r1", "r2"),
//                        15, 15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Arrays.asList(LOCAL, "r1", "r2"),
//                        15, 15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
//                        Arrays.asList(LOCAL, "r1", "r2"),
//                        15, 15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
//                        Arrays.asList(LOCAL, "r1", "r2"),
//                        15, 15, 15)
//        ), this::shouldNotFailIfThereAreAtLeastTwoLiveNodesIncludingCoordinator);
//    }
//
//    private void shouldNotFailIfThereAreAtLeastTwoLiveNodesIncludingCoordinator(Multimap<String, InetAddress> endpoints)
//    {
//        for (int i = 0; i < repetitions; i++)
//        {
//            BatchlogManager.EndpointFilter filter = DatabaseDescriptor.getBatchlogEndpointStrategy().dynamicSnitch
//                    ? new TestDynamicEndpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints)
//            {
//                protected boolean isAlive(InetAddress input)
//                {
//                    return nodeInRack(input) <= 1;
//                }
//            }
//                    : new TestRandomEndpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints)
//            {
//                protected boolean isAlive(InetAddress input)
//                {
//                    return nodeInRack(input) <= 1;
//                }
//            };
//            filter.filter();
//        }
//    }
//
//    @Test
//    public void shouldSelectTwoHostsFromNonLocalRacks()
//    {
//        withConfigs(Stream.of(
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Arrays.asList(LOCAL, "1", "2"),
//                        15, 15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Arrays.asList(LOCAL, "1", "2"),
//                        15, 1, 1),
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Arrays.asList(LOCAL, "1"),
//                        15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
//                        Collections.singletonList(LOCAL),
//                        15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Arrays.asList(LOCAL, "1", "2"),
//                        15, 15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Arrays.asList(LOCAL, "1", "2"),
//                        15, 1, 1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Arrays.asList(LOCAL, "1"),
//                        15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                        Collections.singletonList(LOCAL),
//                        15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Arrays.asList(LOCAL, "1", "2"),
//                        15, 15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Arrays.asList(LOCAL, "1", "2"),
//                        15, 1, 1),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Arrays.asList(LOCAL, "1"),
//                        15, 15),
//                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                        Collections.singletonList(LOCAL),
//                        15)
//        ), this::assertTwoEndpointsWithoutCoordinator);
//    }
//
//    private void assertTwoEndpointsWithoutCoordinator(Multimap<String, InetAddress> endpoints)
//    {
//        for (int i = 0; i < repetitions; i++)
//        {
//            Collection<InetAddress> result = createEndpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints).filter();
//            // result should be the last two non-local replicas
//            // (Collections.shuffle has been replaced with Collections.reverse for testing)
//            assertThat(result.size(), is(2));
//            assertThat(new HashSet<>(result).size(), is(2));
//            assertThat(result, not(hasItems(endpoints.get(LOCAL).toArray(INET_ADDRESSES))));
//        }
//    }
//
//    /**
//     * Test with {@link Config.BatchlogEndpointStrategy#dynamic}.
//     */
//    @Test
//    public void shouldSelectTwoFastestHostsFromSingleLocalRackWithDynamicSnitch()
//    {
//        for (int i = 0; i < repetitions; i++)
//        {
//            InetAddress self = endpointAddress(0, 0);
//            InetAddress host1 = endpointAddress(0, 1);
//            InetAddress host2 = endpointAddress(0, 2);
//            InetAddress host3 = endpointAddress(0, 3);
//            List<InetAddress> hosts = Arrays.asList(host1, host2, host3);
//
//            Multimap<String, InetAddress> endpoints = configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                    Collections.singletonList(LOCAL),
//                    20);
//
//            BatchlogManager.EndpointFilter filter = new TestDynamicEndpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints);
//
//            // ascending
//            setScores(endpoints, hosts, 10, 12, 14);
//            List<InetAddress> order = Arrays.asList(host1, host2, host3);
//            assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3)));
//
//            Collection<InetAddress> result = filter.filter();
//            assertThat(result.size(), is(2));
//            assertThat(result, hasItem(host1));
//            assertThat(result, hasItem(host2));
//
//            // descending
//            setScores(endpoints, hosts, 50, 30, 1);
//            order = Arrays.asList(host3, host2, host1);
//            assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3)));
//            result = filter.filter();
//            assertThat(result.size(), is(2));
//            assertThat(result, hasItem(host2));
//            assertThat(result, hasItem(host3));
//        }
//    }
//
//    /**
//     * Test with {@link Config.BatchlogEndpointStrategy#dynamic}.
//     */
//    @Test
//    public void shouldSelectOneFastestHostsFromNonLocalRackWithDynamicSnitch()
//    {
//        for (int i = 0; i < repetitions; i++)
//        {
//            // for each rack, get last host (only in test), then sort all endpoints from each rack by scores
//            Multimap<String, InetAddress> endpoints = configure(Config.BatchlogEndpointStrategy.dynamic, true,
//                    Arrays.asList(LOCAL, "r1", "r2"),
//                    15, 15, 15);
//            InetAddress r0h1 = endpointAddress(0, 1);
//            InetAddress r1h1 = endpointAddress(1, 0);
//            InetAddress r1h2 = endpointAddress(1, 1);
//            InetAddress r2h1 = endpointAddress(2, 0);
//            InetAddress r2h2 = endpointAddress(2, 1);
//            List<InetAddress> hosts = Arrays.asList(r0h1, r1h1, r1h2, r2h1, r2h2);
//
//            BatchlogManager.EndpointFilter filter = new TestDynamicEndpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints);
//
//            // ascending
//            setScores(endpoints, hosts, 11, 6/* r1h1 */, 12, 5/* r2h1 */, 10);
//            Collection<InetAddress> result = filter.filter();
//            assertThat(result.size(), is(2));
//            assertThat(result, hasItem(r1h1));
//            assertThat(result, hasItem(r2h1));
//
//            // descending
//            setScores(endpoints, hosts, 5/* r0h1 */, 20, 5, 0/* r2h1 */, 15);
//            result = filter.filter();
//            assertThat(result.size(), is(2));
//            assertThat(result, hasItem(r0h1));
//            assertThat(result, hasItem(r2h1));
//        }
//    }
//
//    /**
//     * Test with {@link Config.BatchlogEndpointStrategy#dynamic_remote}.
//     */
//    @Test
//    public void shouldSelectTwoFastestHostsFromSingleLocalRackWithDynamicSnitchRemote()
//    {
//        for (int i = 0; i < repetitions; i++)
//        {
//            InetAddress self = endpointAddress(0, 0);
//            InetAddress host1 = endpointAddress(0, 1);
//            InetAddress host2 = endpointAddress(0, 2);
//            InetAddress host3 = endpointAddress(0, 3);
//            List<InetAddress> hosts = Arrays.asList(host1, host2, host3);
//
//            Multimap<String, InetAddress> endpoints = configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                    Collections.singletonList(LOCAL),
//                    20);
//
//            BatchlogManager.EndpointFilter filter = new TestDynamicEndpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints);
//
//            // ascending
//            setScores(endpoints, hosts,
//                    10, 12, 14);
//            List<InetAddress> order = Arrays.asList(host1, host2, host3);
//            assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3)));
//
//            Collection<InetAddress> result = filter.filter();
//            assertThat(result.size(), is(2));
//            assertThat(result, hasItem(host1));
//            assertThat(result, hasItem(host2));
//
//            // descending
//            setScores(endpoints, hosts,
//                    50, 30, 1);
//            order = Arrays.asList(host3, host2, host1);
//            assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3)));
//            result = filter.filter();
//            assertThat(result.size(), is(2));
//            assertThat(result, hasItem(host2));
//            assertThat(result, hasItem(host3));
//        }
//    }
//
//    /**
//     * Test with {@link Config.BatchlogEndpointStrategy#dynamic_remote}.
//     */
//    @Test
//    public void shouldSelectOneFastestHostsFromNonLocalRackWithDynamicSnitchRemote()
//    {
//        for (int i = 0; i < repetitions; i++)
//        {
//            // for each rack, get last host (only in test), then sort all endpoints from each rack by scores
//            Multimap<String, InetAddress> endpoints = configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
//                    Arrays.asList(LOCAL, "r1", "r2"),
//                    15, 15, 15);
//            InetAddress r0h1 = endpointAddress(0, 1);
//            InetAddress r1h1 = endpointAddress(1, 0);
//            InetAddress r1h2 = endpointAddress(1, 1);
//            InetAddress r2h1 = endpointAddress(2, 0);
//            InetAddress r2h2 = endpointAddress(2, 1);
//            List<InetAddress> hosts = Arrays.asList(r0h1, r1h1, r1h2, r2h1, r2h2);
//
//            BatchlogManager.EndpointFilter filter = new TestDynamicEndpointFilter(ConsistencyLevel.LOCAL_QUORUM, LOCAL, endpoints);
//
//            // ascending
//            setScores(endpoints, hosts,
//                    11,
//                    10, 12,
//                    5, 10);
//            Collection<InetAddress> result = filter.filter();
//            assertThat(result.size(), is(2));
//            assertThat(result, hasItem(r1h1));
//            assertThat(result, hasItem(r2h1));
//
//            // descending
//            setScores(endpoints, hosts,
//                    5, // rack 0
//                    20, 5, // rack 1
//                    0, 15); // rack 2
//            result = filter.filter();
//            assertThat(result.size(), is(2));
//            assertThat(result, hasItem(r1h2));
//            assertThat(result, hasItem(r2h1));
//        }
//    }
//
//    private void setScores(Multimap<String, InetAddress> endpoints,
//                           List<InetAddress> hosts,
//                           Integer... scores)
//    {
//        int maxScore = 0;
//
//        // set the requested scores for the requested hosts
//        for (int round = 0; round < 50; round++)
//        {
//            for (int i = 0; i < hosts.size(); i++)
//            {
//                dsnitch.receiveTiming(Verbs.READS.SINGLE_READ, hosts.get(i), scores[i]);
//                maxScore = Math.max(maxScore, scores[i]);
//            }
//        }
//
//        // set some random (higher) scores for unrequested hosts
//        for (InetAddress ep : endpoints.values())
//        {
//            if (hosts.contains(ep))
//                continue;
//            for (int r = 0; r < 1; r++)
//                dsnitch.receiveTiming(Verbs.READS.SINGLE_READ, ep, maxScore + ThreadLocalRandom.current().nextInt(100) + 1);
//        }
//
//        dsnitch.updateScores();
//    }
//
//    private static class TestRandomEndpointFilter extends BatchlogManager.RandomEndpointFilter
//    {
//        TestRandomEndpointFilter(ConsistencyLevel consistencyLevel, String localRack, Multimap<String, InetAddress> endpoints)
//        {
//            super(consistencyLevel, localRack, endpoints);
//        }
//
//        @Override
//        protected boolean isValid(InetAddress input)
//        {
//            return !getCoordinator().equals(input) && isAlive(input);
//        }
//
//        @Override
//        protected boolean isAlive(InetAddress input)
//        {
//            return true;
//        }
//    }
//
//    private static class TestDynamicEndpointFilter extends BatchlogManager.DynamicEndpointFilter
//    {
//        TestDynamicEndpointFilter(ConsistencyLevel consistencyLevel, String localRack, Multimap<String, InetAddress> endpoints)
//        {
//            super(consistencyLevel, localRack, endpoints);
//        }
//
//        @Override
//        protected boolean isValid(InetAddress input)
//        {
//            return !getCoordinator().equals(input) && isAlive(input);
//        }
//
//        @Override
//        protected boolean isAlive(InetAddress input)
//        {
//            return true;
//        }
//    }
//
//    private int nodeInRack(InetAddress input)
//    {
//        return input.getAddress()[3];
//    }
//
//    private static InetAddress endpointAddress(int rack, int nodeInRack)
//    {
//        if (rack == 0 && nodeInRack == 0)
//            return FBUtilities.getBroadcastAddress();
//
//        try
//        {
//            return InetAddress.getByAddress(new byte[]{ 0, 0, (byte) rack, (byte) nodeInRack });
//        }
//        catch (UnknownHostException e)
//        {
//            throw new RuntimeException(e);
//        }
//    }
//
//    private Multimap<String, InetAddress> configure(Config.BatchlogEndpointStrategy batchlogEndpointStrategy,
//                                                    boolean dynamicSnitch,
//                                                    List<String> racks,
//                                                    int... nodesPerRack)
//    {
//        StorageService.instance.unsafeInitialize();
//
//        // if any of the three assertions fires, your test is busted
//        assert !racks.isEmpty();
//        assert racks.size() <= 10;
//        assert racks.size() == nodesPerRack.length;
//
//        ImmutableMultimap.Builder<String, InetAddress> builder = ImmutableMultimap.builder();
//        for (int r = 0; r < racks.size(); r++)
//        {
//            String rack = racks.get(r);
//            for (int n = 0; n < nodesPerRack[r]; n++)
//                builder.put(rack, endpointAddress(r, n));
//        }
//
//        ImmutableMultimap<String, InetAddress> endpoints = builder.build();
//
//        reconfigure(batchlogEndpointStrategy, dynamicSnitch, endpoints);
//
//        return endpoints;
//    }
//
//    private void reconfigure(Config.BatchlogEndpointStrategy batchlogEndpointStrategy,
//                             boolean dynamicSnitch,
//                             Multimap<String, InetAddress> endpoints)
//    {
//        DatabaseDescriptor.setBatchlogEndpointStrategy(batchlogEndpointStrategy);
//
//        if (DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch)
//            ((DynamicEndpointSnitch) DatabaseDescriptor.getEndpointSnitch()).close();
//
//        Multimap<InetAddress, String> endpointRacks = Multimaps.invertFrom(endpoints, ArrayListMultimap.create());
//        GossipingPropertyFileSnitch gpfs = new GossipingPropertyFileSnitch()
//        {
//            @Override
//            public String getDatacenter(InetAddress endpoint)
//            {
//                return "dc1";
//            }
//
//            @Override
//            public String getRack(InetAddress endpoint)
//            {
//                return endpointRacks.get(endpoint).iterator().next();
//            }
//        };
//        IEndpointSnitch snitch;
//        if (dynamicSnitch)
//            snitch = dsnitch = new DynamicEndpointSnitch(gpfs, String.valueOf(gpfs.hashCode()), false);
//        else
//        {
//            dsnitch = null;
//            snitch = gpfs;
//        }
//
//        DatabaseDescriptor.setDynamicBadnessThreshold(0);
//        DatabaseDescriptor.setEndpointSnitch(snitch);
//
//        DatabaseDescriptor.setBatchlogEndpointStrategy(batchlogEndpointStrategy);
//    }
//
//    private void withConfigs(Stream<Supplier<Multimap<String, InetAddress>>> supplierStream,
//                             Consumer<Multimap<String, InetAddress>> testFunction)
//    {
//        supplierStream.map(Supplier::get)
//                .forEach(endpoints -> {
//                    try
//                    {
//                        testFunction.accept(endpoints);
//                    }
//                    catch (AssertionError e)
//                    {
//                        throw new AssertionError(configToString(endpoints), e);
//                    }
//                });
//    }
//
//    private String configToString(Multimap<String, InetAddress> endpoints)
//    {
//        return "strategy:" + DatabaseDescriptor.getBatchlogEndpointStrategy()
//                + " snitch:" + DatabaseDescriptor.getEndpointSnitch().getClass().getSimpleName()
//                + " nodes-per-rack: " + endpoints.asMap().entrySet().stream()
//                .map(e -> e.getKey() + '=' + e.getValue().size())
//                .collect(Collectors.joining());
//    }
//
//    private BatchlogManager.EndpointFilter createEndpointFilter(ConsistencyLevel consistencyLevel, String local, Multimap<String, InetAddress> endpoints)
//    {
//        return DatabaseDescriptor.getBatchlogEndpointStrategy().dynamicSnitch
//                ? new TestDynamicEndpointFilter(consistencyLevel, local, endpoints)
//                : new TestRandomEndpointFilter(consistencyLevel, local, endpoints);
//    }
//}