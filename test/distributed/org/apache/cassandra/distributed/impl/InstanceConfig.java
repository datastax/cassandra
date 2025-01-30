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

package org.apache.cassandra.distributed.impl;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Splitter;
import com.google.common.net.HostAndPort;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.upgrade.UpgradeTestBase;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SimpleSeedProvider;

import org.apache.commons.lang3.ObjectUtils;

public class InstanceConfig implements IInstanceConfig
{
    private static final Logger logger = LoggerFactory.getLogger(InstanceConfig.class);

    public final int num;
    private final int jmxPort;

    public int num() { return num; }

    private final NetworkTopology networkTopology;
    public NetworkTopology networkTopology() { return networkTopology; }

    private volatile UUID hostId;
    public void setHostId(UUID hostId) { this.hostId = hostId; }
    public UUID hostId() { return hostId; }
    private final Map<String, Object> params = new TreeMap<>();
    private final Map<String, Object> dtestParams = new TreeMap<>();

    private final EnumSet featureFlags;

    private volatile InetAddressAndPort broadcastAddressAndPort;

    private InstanceConfig(int num,
                           NetworkTopology networkTopology,
                           String broadcast_address,
                           String listen_address,
                           String broadcast_rpc_address,
                           String rpc_address,
                           String seedIp,
                           int seedPort,
                           String saved_caches_directory,
                           String[] data_file_directories,
                           String commitlog_directory,
                           String hints_directory,
                           String cdc_raw_directory,
                           String metadata_directory,
                           Collection<String> initial_token,
                           int storage_port,
                           int native_transport_port,
                           int jmx_port)
    {
        this.num = num;
        this.networkTopology = networkTopology;
        this.hostId = new UUID(0x4000L, (1L << 63) | num); // deterministic hostId for simulator
        //TODO move away from magic strings in favor of constants
        this    .set("num_tokens", initial_token.size())
                .set("initial_token", initial_token.stream().collect(Collectors.joining(",")))
                .set("broadcast_address", broadcast_address)
                .set("listen_address", listen_address)
                .set("broadcast_rpc_address", broadcast_rpc_address)
                .set("rpc_address", rpc_address)
                .set("saved_caches_directory", saved_caches_directory)
                .set("data_file_directories", data_file_directories)
                .set("commitlog_directory", commitlog_directory)
                .set("hints_directory", hints_directory)
                .set("cdc_raw_directory", cdc_raw_directory)
                .set("metadata_directory", metadata_directory)
                .set("partitioner", "org.apache.cassandra.dht.Murmur3Partitioner")
                .set("start_native_transport", true)
                .set("concurrent_writes", 2)
                .set("concurrent_counter_writes", 2)
                .set("concurrent_materialized_view_writes", 2)
                .set("concurrent_reads", 2)
                .set("memtable_flush_writers", 1)
                .set("concurrent_compactors", 1)
                .set("memtable_heap_space", "10MiB")
                .set("commitlog_sync", "periodic")
                .set("commitlog_sync_period_in_ms", 10000)
                .set("storage_port", storage_port)
                .set("native_transport_port", native_transport_port)
                .set("endpoint_snitch", DistributedTestSnitch.class.getName())
                .set("seed_provider", new ParameterizedClass(SimpleSeedProvider.class.getName(),
                        Collections.singletonMap("seeds", seedIp + ':' + seedPort)))
                // required settings for dtest functionality
                .set("diagnostic_events_enabled", true)
                .set("auto_bootstrap", false)
                // capacities that are based on `totalMemory` that should be fixed size
                .set("index_summary_capacity", "50MiB")
                .set("counter_cache_size", "50MiB")
                .set("key_cache_size", "50MiB")
                .set("commitlog_disk_access_mode", "legacy");
        if (CassandraRelevantProperties.DTEST_JVM_DTESTS_USE_LATEST.getBoolean())
        {
            // TODO: make this load latest_diff.yaml or cassandra_latest.yaml
            this.set("memtable", Map.of(
                "configurations", Map.of(
                    "default", Map.of(
                        "class_name", "TrieMemtable"))))
                .set("key_cache_size", "0MiB")

                .set("memtable_allocation_type", "offheap_objects")

                .set("commitlog_disk_access_mode", "auto")

                .set("trickle_fsync", "true")

                .set("sstable", Map.of(
                    "selected_format", "bti"))

                .set("column_index_size", "4KiB")

                .set("default_compaction", Map.of(
                    "class_name", "UnifiedCompactionStrategy",
                    "parameters", Map.of(
                        "scaling_parameters", "T4",
                        "max_sstables_to_compact", "64",
                        "target_sstable_size", "1GiB",
                        "sstable_growth","0.3333333333333333",
                        "min_sstable_size", "100MiB")))

                .set("concurrent_compactors", "8")

                .set("uuid_sstable_identifiers_enabled", "true")

                .set("stream_entire_sstables", "true")

                .set("default_secondary_index", "sai")
                .set("default_secondary_index_enabled", "true")

                .set("storage_compatibility_mode", "NONE");
        }
        this.featureFlags = EnumSet.noneOf(Feature.class);
        this.jmxPort = jmx_port;
    }

    private InstanceConfig(InstanceConfig copy)
    {
        this.num = copy.num;
        this.networkTopology = new NetworkTopology(copy.networkTopology);
        this.params.putAll(copy.params);
        this.dtestParams.putAll(copy.dtestParams);
        this.hostId = copy.hostId;
        this.featureFlags = copy.featureFlags;
        this.broadcastAddressAndPort = copy.broadcastAddressAndPort;
        this.jmxPort = copy.jmxPort;
    }

    @Override
    public InetSocketAddress broadcastAddress()
    {
        return DistributedTestSnitch.fromCassandraInetAddressAndPort(getBroadcastAddressAndPort());
    }

    public void unsetBroadcastAddressAndPort()
    {
        broadcastAddressAndPort = null;
    }

    protected InetAddressAndPort getBroadcastAddressAndPort()
    {
        if (broadcastAddressAndPort == null)
        {
            broadcastAddressAndPort = getAddressAndPortFromConfig("broadcast_address", "storage_port");
        }
        return broadcastAddressAndPort;
    }

    private InetAddressAndPort getAddressAndPortFromConfig(String addressProp, String portProp)
    {
        try
        {
            return InetAddressAndPort.getByNameOverrideDefaults(getString(addressProp), getInt(portProp));
        }
        catch (UnknownHostException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public String localRack()
    {
        return networkTopology().localRack(broadcastAddress());
    }

    public String localDatacenter()
    {
        return networkTopology().localDC(broadcastAddress());
    }

    @Override
    public int jmxPort()
    {
        return this.jmxPort;
    }

    public InstanceConfig with(Feature featureFlag)
    {
        featureFlags.add(featureFlag);
        return this;
    }

    public InstanceConfig with(Feature... flags)
    {
        for (Feature flag : flags)
            featureFlags.add(flag);
        return this;
    }

    public boolean has(Feature featureFlag)
    {
        return featureFlags.contains(featureFlag);
    }

    public InstanceConfig set(String fieldName, Object value)
    {
        getParams(fieldName).put(fieldName, value);
        return this;
    }

    public InstanceConfig remove(String fieldName)
    {
        getParams(fieldName).remove(fieldName);
        return this;
    }

    public InstanceConfig forceSet(String fieldName, Object value)
    {
        getParams(fieldName).put(fieldName, value);
        return this;
    }

    private Map<String, Object> getParams(String fieldName)
    {
        Map<String, Object> map = params;
        if (fieldName.startsWith("dtest"))
            map = dtestParams;
        return map;
    }

    public void propagate(Object writeToConfig, Map<Class<?>, Function<Object, Object>> mapping)
    {
        throw new IllegalStateException("In-JVM dtests no longer support propagate");
    }

    @Override
    public void validate()
    {
        // Previous logic would validate vnode was not used, but with vnode support added that validation isn't needed.
        // Rather than attempting validating the configs here, its best to leave that to the instance; this method
        // is no longer really needed, but can not be removed due to backwards compatability.
    }

    public Object get(String name)
    {
        return getParams(name).get(name);
    }

    public int getInt(String name)
    {
        return (Integer) get(name);
    }

    public String getString(String name)
    {
        return (String) get(name);
    }

    public Map<String, Object> getParams()
    {
        return params;
    }

    public static InstanceConfig generate(int nodeNum,
                                          INodeProvisionStrategy provisionStrategy,
                                          NetworkTopology networkTopology,
                                          Path root,
                                          Collection<String> tokens,
                                          int datadirCount)
    {
        return new InstanceConfig(nodeNum,
                                  networkTopology,
                                  provisionStrategy.ipAddress(nodeNum),
                                  provisionStrategy.ipAddress(nodeNum),
                                  provisionStrategy.ipAddress(nodeNum),
                                  provisionStrategy.ipAddress(nodeNum),
                                  provisionStrategy.seedIp(),
                                  provisionStrategy.seedPort(),
                                  String.format("%s/node%d/saved_caches", root, nodeNum),
                                  datadirs(datadirCount, root, nodeNum),
                                  String.format("%s/node%d/commitlog", root, nodeNum),
                                  String.format("%s/node%d/hints", root, nodeNum),
                                  String.format("%s/node%d/cdc", root, nodeNum),
                                  String.format("%s/node%d/metadata", root, nodeNum),
                                  tokens,
                                  provisionStrategy.storagePort(nodeNum),
                                  provisionStrategy.nativeTransportPort(nodeNum),
                                  provisionStrategy.jmxPort(nodeNum));
    }

    private static String[] datadirs(int datadirCount, Path root, int nodeNum)
    {
        String datadirFormat = String.format("%s/node%d/data%%d", root, nodeNum);
        String [] datadirs = new String[datadirCount];
        for (int i = 0; i < datadirs.length; i++)
            datadirs[i] = String.format(datadirFormat, i);
        return datadirs;
    }

    public InstanceConfig forVersion(Semver version)
    {
        ParameterizedClass seedProviderConfig = (ParameterizedClass) params.get("seed_provider");

        // Versions before 4.0 need to set 'seed_provider' without specifying the port
        // the extra comparison to strict version is due to a bug in Semver (see STAR-871)
        if (version.isGreaterThanOrEqualTo(UpgradeTestBase.v40) || version.isGreaterThanOrEqualTo(UpgradeTestBase.v40.toStrict()) || seedProviderConfig == null)
            return this;

        assert ObjectUtils.equals(seedProviderConfig.class_name, SimpleSeedProvider.class.getName());
        String seedsStr = seedProviderConfig.parameters.get("seeds");
        assert seedsStr != null;
        seedsStr = Splitter.on(',')
                           .omitEmptyStrings()
                           .trimResults()
                           .splitToList(seedsStr)
                           .stream()
                           .map(str -> HostAndPort.fromString(str).getHost())
                           .collect(Collectors.joining(","));

        seedProviderConfig = new ParameterizedClass(seedProviderConfig.class_name, Collections.singletonMap("seeds", seedsStr));

        logger.warn("Stripping ports from seed addresses because the version {} is < {}, new seeds list is: {}", version, UpgradeTestBase.v40, seedsStr);
        return new InstanceConfig(this).set("seed_provider", seedProviderConfig);
    }

    public String toString()
    {
        return params.toString();
    }
}
