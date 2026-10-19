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

package org.apache.cassandra.tools.nodetool;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.ObjectName;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.metrics.AbstractMetrics;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.JsonUtils;
import org.yaml.snakeyaml.Yaml;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_ALL_QUERIES_LATENCY_HISTOGRAM_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_HISTOGRAMS_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_TABLE_STATE_METRICS_ENABLED;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.within;

public class TableStatsTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "tablestats");
        tool.assertOnCleanExit();

        String help =   "NAME\n" +
                        "        nodetool tablestats - Print statistics on tables\n" + 
                        "\n" + 
                        "SYNOPSIS\n" + 
                        "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                        "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                        "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                        "                [(-u <username> | --username <username>)] tablestats\n" +
                        "                [(-F <format> | --format <format>)] [(-H | --human-readable)] [-i]\n" +
                        "                [(-l | --sstable-location-check)] [(-s <sort_key> | --sort <sort_key>)]\n" +
                        "                [(-t <top> | --top <top>)] [--] [<keyspace.table>...]\n" +
                        "\n" + 
                        "OPTIONS\n" +
                        "        -F <format>, --format <format>\n" +
                        "            Output format (json, yaml)\n" + 
                        "\n" + 
                        "        -h <host>, --host <host>\n" + 
                        "            Node hostname or ip address\n" + 
                        "\n" + 
                        "        -H, --human-readable\n" + 
                        "            Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB\n" + 
                        "\n" + 
                        "        -i\n" + 
                        "            Ignore the list of tables and display the remaining tables\n" +
                        "\n" +
                        "        -l, --sstable-location-check\n" +
                        "            Check whether or not the SSTables are in the correct location.\n" +
                        "\n" + 
                        "        -p <port>, --port <port>\n" + 
                        "            Remote jmx agent port number\n" + 
                        "\n" + 
                        "        -pp, --print-port\n" + 
                        "            Operate in 4.0 mode with hosts disambiguated by port number\n" + 
                        "\n" + 
                        "        -pw <password>, --password <password>\n" + 
                        "            Remote jmx agent password\n" + 
                        "\n" + 
                        "        -pwf <passwordFilePath>, --password-file <passwordFilePath>\n" + 
                        "            Path to the JMX password file\n" + 
                        "\n" + 
                        "        -s <sort_key>, --sort <sort_key>\n" + 
                        "            Sort tables by specified sort key\n" + 
                        "            (average_live_cells_per_slice_last_five_minutes,\n" + 
                        "            average_tombstones_per_slice_last_five_minutes,\n" + 
                        "            bloom_filter_false_positives, bloom_filter_false_ratio,\n" + 
                        "            bloom_filter_off_heap_memory_used, bloom_filter_space_used,\n" + 
                        "            compacted_partition_maximum_bytes, compacted_partition_mean_bytes,\n" + 
                        "            compacted_partition_minimum_bytes,\n" + 
                        "            compression_metadata_off_heap_memory_used, full_name,\n" +
                        "            index_summary_off_heap_memory_used, local_read_count,\n" +
                        "            local_read_latency_ms, local_write_latency_ms,\n" + 
                        "            maximum_live_cells_per_slice_last_five_minutes,\n" + 
                        "            maximum_tombstones_per_slice_last_five_minutes, memtable_cell_count,\n" + 
                        "            memtable_data_size, memtable_off_heap_memory_used,\n" + 
                        "            memtable_switch_count, number_of_partitions_estimate,\n" + 
                        "            off_heap_memory_used_total, pending_flushes, percent_repaired,\n" + 
                        "            read_latency, reads, space_used_by_snapshots_total, space_used_live,\n" + 
                        "            space_used_total, sstable_compression_ratio, sstable_count,\n" + 
                        "            table_name, write_latency, writes, max_sstable_size,\n" +
                        "            local_read_write_ratio, twcs_max_duration,\n" +
                        "            sai_local_query_latency_ms, sai_post_filtering_read_latency,\n" +
                        "            sai_disk_used_bytes, sai_sstable_indexes_hit,\n" +
                        "            sai_index_segments_hit, sai_rows_filtered, sai_total_query_timeouts,\n" +
                        "            sai_total_queryable_index_ratio)\n" +
                        "\n" + 
                        "        -t <top>, --top <top>\n" + 
                        "            Show only the top K tables for the sort key (specify the number K of\n" + 
                        "            tables to be shown\n" + 
                        "\n" + 
                        "        -u <username>, --username <username>\n" + 
                        "            Remote jmx agent username\n" + 
                        "\n" + 
                        "        --\n" + 
                        "            This option can be used to separate command-line options from the\n" + 
                        "            list of argument, (useful when arguments might be mistaken for\n" + 
                        "            command-line options\n" + 
                        "\n" + 
                        "        [<keyspace.table>...]\n" + 
                        "            List of tables (or keyspace) names\n" + 
                        "\n" + 
                        "\n";
        assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testTableStats()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Keyspace: system_schema");
        assertThat(StringUtils.countMatches(tool.getStdout(), "Table:")).isGreaterThan(1);

        tool = ToolRunner.invokeNodetool("tablestats", "system_distributed");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Keyspace: system_distributed");
        assertThat(tool.getStdout()).doesNotContain("Keyspace : system_schema");
        assertThat(StringUtils.countMatches(tool.getStdout(), "Table:")).isGreaterThan(1);
    }

    @Test
    public void testTableIgnoreArg()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-i", "system_schema.aggregates");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Keyspace: system_schema");
        assertThat(tool.getStdout()).doesNotContain("Table: system_schema.aggregates");
        assertThat(StringUtils.countMatches(tool.getStdout(), "Table:")).isGreaterThan(1);
    }

    @Test
    public void testHumanReadableArg()
    {
        Arrays.asList("-H", "--human-readable").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", arg);
            tool.assertOnCleanExit();
            assertThat(tool.getStdout()).contains(" KiB");
        });
    }

    @Test
    public void testSortArg()
    {
        Pattern regExp = Pattern.compile("((?m)Table: .*$)");

        Arrays.asList("-s", "--sort").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", arg, "table_name");
            Matcher m = regExp.matcher(tool.getStdout());
            ArrayList<String> orig = new ArrayList<>();
            while (m.find())
                orig.add(m.group(1));

            tool = ToolRunner.invokeNodetool("tablestats", arg, "sstable_count");
            tool.assertOnCleanExit();
            m = regExp.matcher(tool.getStdout());
            ArrayList<String> sorted = new ArrayList<>();
            while (m.find())
                sorted.add(m.group(1));

            assertThat(sorted).isNotEqualTo(orig);
            Collections.sort(orig);
            Collections.sort(sorted);
            assertThat(sorted).isEqualTo(orig);
        });

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-s", "wrongSort");
        assertThat(tool.getStdout()).contains("argument for sort must be one of");
        tool.assertCleanStdErr();
        assertThat(tool.getExitCode()).isEqualTo(1);
    }

    @Test
    public void testTopArg()
    {
        Arrays.asList("-t", "--top").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-s", "table_name", arg, "1");
            tool.assertOnCleanExit();
            assertThat(StringUtils.countMatches(tool.getStdout(), "Table:")).isEqualTo(1);
        });

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-s", "table_name", "-t", "-1");
        tool.assertCleanStdErr();
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("argument for top must be a positive integer");
    }

    @Test
    public void testSSTableLocationCheckArg()
    {
        Arrays.asList("-l", "--sstable-location-check").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", arg, "system.local");
            tool.assertOnCleanExit();
            assertThat(StringUtils.countMatches(tool.getStdout(), "SSTables in correct location: ")).isEqualTo(1);
        });

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", "system.local");
        tool.assertCleanStdErr();
        assertThat(tool.getStdout()).doesNotContain("SSTables in correct location: ");
    }

    @Test
    public void testFormatJson()
    {
        Arrays.asList("-F", "--format").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", arg, "json");
            tool.assertOnCleanExit();
            String json = tool.getStdout();
            assertThatCode(() -> JsonUtils.JSON_OBJECT_MAPPER.readTree(json)).doesNotThrowAnyException();
            assertThat(json).containsPattern("\"sstable_count\"\\s*:\\s*[0-9]+")
                            .containsPattern("\"old_sstable_count\"\\s*:\\s*[0-9]+");
        });
    }

    @Test
    public void testFormatYaml()
    {
        Arrays.asList("-F", "--format").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", arg, "yaml");
            tool.assertOnCleanExit();
            String yaml = tool.getStdout();
            assertThatCode(() -> new Yaml().load(yaml)).doesNotThrowAnyException();
            assertThat(yaml).containsPattern("sstable_count:\\s*[0-9]+")
                            .containsPattern("old_sstable_count:\\s*[0-9]+");
        });
    }

    @Test
    public void testTableStatsWithSAIIndex() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(SAI_HISTOGRAMS_ENABLED, true)
                                                             .set(SAI_TABLE_STATE_METRICS_ENABLED, true))
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        }
        // two sstables, holding 2 and 1 rows matching v = 1
        for (int i = 0; i < 4; i++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i % 2);
        flush();
        for (int i = 4; i < 6; i++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i % 2);
        flush();
        String table = KEYSPACE + '.' + currentTable();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore());

        // no SAI query has run yet: the mean of an empty timer is NaN, while an empty histogram reports 0
        String stdout = invokeTableStats(table);
        assertThat(saiMetric(stdout, "SAI local query latency \\(mean\\): (\\S+) ms")).isNaN();
        assertThat(saiMetric(stdout, "SAI post-filtering latency \\(mean\\): (\\S+) ms")).isNaN();
        assertThat(saiMetric(stdout, "SAI sstable indexes hit per query \\(mean\\): (\\S+)")).isZero();
        assertThat(saiMetric(stdout, "SAI index segments hit per query \\(mean\\): (\\S+)")).isZero();
        assertThat(saiMetric(stdout, "SAI rows filtered per query \\(mean\\): (\\S+)")).isZero();
        // the space used includes the per-column index components, not only the per-sstable ones
        assertThat(group.totalDiskUsage()).isGreaterThan(group.diskUsage());
        assertThat(stdout).contains("SAI space used (bytes): " + group.totalDiskUsage() + '\n')
                          .contains("SAI local query timeouts: 0")
                          .contains("SAI queryable/total indexes: 1/1");

        // the per query histograms count zeroes as ones, so the asserted means are all greater than 1
        for (int i = 0; i < 3; i++)
            assertThat(executeNet("SELECT * FROM %s WHERE v = 1").all()).hasSize(3);
        double queryLatencyMs = serverTimerMeanMs(currentTable(), "QueryLatency");
        double postFilteringReadLatencyMs = serverTimerMeanMs(currentTable(), "PostFilteringReadLatency");
        assertThat(queryLatencyMs).isPositive();
        assertThat(postFilteringReadLatencyMs).isPositive();

        stdout = invokeTableStats(table);
        assertThat(saiMetric(stdout, "SAI local query latency \\(mean\\): (\\S+) ms")).isCloseTo(queryLatencyMs, within(0.001));
        assertThat(saiMetric(stdout, "SAI post-filtering latency \\(mean\\): (\\S+) ms")).isCloseTo(postFilteringReadLatencyMs, within(0.001));
        assertThat(saiMetric(stdout, "SAI sstable indexes hit per query \\(mean\\): (\\S+)")).isEqualTo(2.0);
        assertThat(saiMetric(stdout, "SAI index segments hit per query \\(mean\\): (\\S+)")).isEqualTo(2.0);
        assertThat(saiMetric(stdout, "SAI rows filtered per query \\(mean\\): (\\S+)")).isEqualTo(3.0);
        assertThat(stdout).contains("SAI local query timeouts: 0")
                          .contains("SAI queryable/total indexes: 1/1");

        JsonNode stats = invokeTableStatsJson(table);
        assertThat(parseDouble(stats.get("sai_local_query_latency_ms").asText())).isCloseTo(queryLatencyMs, within(0.001));
        assertThat(parseDouble(stats.get("sai_post_filtering_read_latency").asText())).isCloseTo(postFilteringReadLatencyMs, within(0.001));
        assertThat(stats.get("sai_disk_used_bytes").asText()).isEqualTo(String.valueOf(group.totalDiskUsage()));
        assertThat(stats.get("sai_sstable_indexes_hit").asDouble()).isEqualTo(2.0);
        assertThat(stats.get("sai_index_segments_hit").asDouble()).isEqualTo(2.0);
        assertThat(stats.get("sai_rows_filtered").asDouble()).isEqualTo(3.0);
        assertThat(stats.get("sai_total_query_timeouts").asLong()).isZero();
        assertThat(stats.get("sai_total_queryable_index_ratio").asText()).isEqualTo("1/1");

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-F", "yaml", table);
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).containsPattern("sai_sstable_indexes_hit: 2\\.0\n")
                                    .containsPattern("sai_rows_filtered: 3\\.0\n")
                                    .containsPattern("sai_total_queryable_index_ratio: 1/1\n");
    }

    @Test
    public void testTableStatsWithSAIIndexWithoutQueryLatencyMetric() throws Throwable
    {
        // without SAI histograms the query latency timer is not registered at all
        try (WithProperties properties = new WithProperties().set(SAI_HISTOGRAMS_ENABLED, false)
                                                             .set(SAI_ALL_QUERIES_LATENCY_HISTOGRAM_ENABLED, false))
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        }
        for (int i = 0; i < 6; i++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i % 2);
        assertThat(executeNet("SELECT * FROM %s WHERE v = 1").all()).hasSize(3);

        String stdout = invokeTableStats(KEYSPACE + '.' + currentTable());
        assertThat(saiMetric(stdout, "SAI local query latency \\(mean\\): (\\S+) ms")).isNaN();
        assertThat(saiMetric(stdout, "SAI post-filtering latency \\(mean\\): (\\S+) ms")).isPositive();
        assertThat(saiMetric(stdout, "SAI rows filtered per query \\(mean\\): (\\S+)")).isEqualTo(3.0);
        assertThat(stdout).contains("SAI queryable/total indexes: 1/1");
    }

    @Test
    public void testTableStatsWithSAIIndexWithoutTableStateMetrics() throws Throwable
    {
        // without SAI table state metrics the indexes can't be counted, but the other SAI metrics are still there
        try (WithProperties properties = new WithProperties().set(SAI_TABLE_STATE_METRICS_ENABLED, false))
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        }
        for (int i = 0; i < 6; i++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i % 2);
        flush();
        assertThat(executeNet("SELECT * FROM %s WHERE v = 1").all()).hasSize(3);
        String table = KEYSPACE + '.' + currentTable();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore());

        // the space used falls back to the index group metrics, which only count the per-sstable components
        String stdout = invokeTableStats(table);
        assertThat(saiMetric(stdout, "SAI rows filtered per query \\(mean\\): (\\S+)")).isEqualTo(3.0);
        assertThat(stdout).contains("SAI space used (bytes): " + group.diskUsage() + '\n')
                          .contains("SAI queryable/total indexes: n/a");

        JsonNode stats = invokeTableStatsJson(table);
        assertThat(stats.get("sai_disk_used_bytes").asText()).isEqualTo(String.valueOf(group.diskUsage()));
        assertThat(stats.get("sai_rows_filtered").asDouble()).isEqualTo(3.0);
        assertThat(stats.get("sai_total_queryable_index_ratio").asText()).isEqualTo("n/a");
    }

    @Test
    public void testTableStatsWithoutSAIIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (0, 0)");
        flush();
        String table = KEYSPACE + '.' + currentTable();

        assertThat(invokeTableStats(table)).doesNotContain("SAI ");

        // the SAI section goes away with the last SAI index of the table
        String index = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        assertThat(invokeTableStats(table)).contains("SAI queryable/total indexes: 1/1");
        dropIndex("DROP INDEX %s." + index);
        assertThat(invokeTableStats(table)).doesNotContain("SAI ");
    }

    @Test
    public void testSAIMetricsOfOlderServers() throws Throwable
    {
        // older servers, like upstream 5.0, register the post-filtering read latency with the per table metrics, and
        // call the histogram of the rows fetched before post-filtering RowsFiltered
        String keyspace = "old_ks";
        String table = "old_table";
        CassandraMetricsRegistry.MetricName postFilteringReadLatency = saiMetricName(keyspace, table, TableQueryMetrics.PerTable.METRIC_TYPE, "PostFilteringReadLatency");
        CassandraMetricsRegistry.MetricName rowsFiltered = saiMetricName(keyspace, table, TableQueryMetrics.PerQuery.METRIC_TYPE, "RowsFiltered");
        CassandraMetricsRegistry.MetricName rowsFetched = saiMetricName(keyspace, table, TableQueryMetrics.PerQuery.METRIC_TYPE, "RowsFetched");
        try (NodeProbe probe = new NodeProbe(jmxHost, jmxPort))
        {
            Metrics.timer(postFilteringReadLatency).update(2, TimeUnit.MILLISECONDS);
            Metrics.histogram(rowsFiltered, false).update(3);

            CassandraMetricsRegistry.JmxTimerMBean timer = (CassandraMetricsRegistry.JmxTimerMBean) probe.getSaiMetric(keyspace, table, "PostFilteringReadLatency");
            assertThat(timer.getCount()).isEqualTo(1);
            // the mean of a timer is in microseconds, and its histogram buckets are within 20% of each other
            assertThat(timer.getMean()).isCloseTo(2000.0, within(400.0));
            assertThat(((CassandraMetricsRegistry.JmxHistogramMBean) probe.getSaiMetric(keyspace, table, "RowsFiltered")).getMean()).isEqualTo(3.0);
            assertThat(probe.getSaiMetric(keyspace, table, "QueryLatency")).isNull();
            assertThat(probe.getSaiMetric(keyspace, table, "SSTableIndexesHit")).isNull();

            // the current name is preferred
            Metrics.histogram(rowsFetched, false).update(5);
            assertThat(((CassandraMetricsRegistry.JmxHistogramMBean) probe.getSaiMetric(keyspace, table, "RowsFiltered")).getMean()).isEqualTo(5.0);
        }
        finally
        {
            Metrics.remove(postFilteringReadLatency);
            Metrics.remove(rowsFiltered);
            Metrics.remove(rowsFetched);
        }
    }

    private static CassandraMetricsRegistry.MetricName saiMetricName(String keyspace, String table, String scope, String name)
    {
        String mBeanName = String.format("%s:type=%s,keyspace=%s,table=%s,scope=%s,name=%s",
                                         DefaultNameFactory.GROUP_NAME, AbstractMetrics.TYPE, keyspace, table, scope, name);
        return new CassandraMetricsRegistry.MetricName(DefaultNameFactory.GROUP_NAME, AbstractMetrics.TYPE, name,
                                                       keyspace + '.' + table + '.' + scope, mBeanName);
    }

    private static double serverTimerMeanMs(String table, String name) throws Exception
    {
        ObjectName objectName = new ObjectName(String.format("%s:type=%s,keyspace=%s,table=%s,scope=%s,name=%s",
                                                             DefaultNameFactory.GROUP_NAME, AbstractMetrics.TYPE, KEYSPACE, table,
                                                             TableQueryMetrics.PerQuery.METRIC_TYPE, name));
        // JMX timers report their mean in microseconds
        return (double) ManagementFactory.getPlatformMBeanServer().getAttribute(objectName, "Mean") / 1000;
    }

    private static String invokeTableStats(String table)
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", table);
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Table: " + table.substring(table.indexOf('.') + 1));
        return tool.getStdout();
    }

    private static JsonNode invokeTableStatsJson(String table) throws IOException
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-F", "json", table);
        tool.assertOnCleanExit();
        String[] names = table.split("\\.");
        return JsonUtils.JSON_OBJECT_MAPPER.readTree(tool.getStdout()).get(names[0]).get("tables").get(names[1]);
    }

    private static double saiMetric(String stdout, String regex)
    {
        Matcher matcher = Pattern.compile(regex).matcher(stdout);
        assertThat(matcher.find()).as("%s not found in:%n%s", regex, stdout).isTrue();
        return parseDouble(matcher.group(1));
    }

    /**
     * Parses a double formatted by nodetool with {@code String.format}, which uses the decimal separator of the
     * default locale.
     */
    private static double parseDouble(String value)
    {
        return Double.parseDouble(value.replace(',', '.'));
    }
}
