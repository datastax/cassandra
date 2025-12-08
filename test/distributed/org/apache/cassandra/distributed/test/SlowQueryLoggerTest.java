/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.distributed.test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.monitoring.MonitoringTask;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.utils.Throwables;
import org.assertj.core.api.AbstractIterableAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;

import static net.bytebuddy.matcher.ElementMatchers.named;
import org.apache.cassandra.config.CassandraRelevantProperties;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

public class SlowQueryLoggerTest extends TestBaseImpl
{
    private static final String TABLE = "t";
    private static final int SLOW_QUERY_LOG_TIMEOUT_MS = 100;

    /**
     * Test that the slow query logger does not log sensitive data.
     */
    @Test
    public void testDoesNotLogSensitiveData() throws Throwable
    {
        // effectively disable the scheduled monitoring task so we control it manually for better test stability
        CassandraRelevantProperties.MONITORING_REPORT_INTERVAL_MS.setLong(TimeUnit.HOURS.toMillis(1));

        try (Cluster cluster = init(Cluster.build(2)
                                           .withInstanceInitializer(SlowQueryLoggerTest.BBHelper::install)
                                           .withConfig(config -> config.set("slow_query_log_timeout_in_ms", SLOW_QUERY_LOG_TIMEOUT_MS))
                                           .start()))
        {
            ICoordinator coordinator = cluster.coordinator(1);
            IInvokableInstance node = cluster.get(2);

            cluster.schemaChange(format("CREATE TABLE %s.%s (k text, c text, v text, PRIMARY KEY (k, c))"));
            coordinator.execute(format("INSERT INTO %s.%s (k, c, v) VALUES ('secret_k', 'secret_c', 'secret_v')"), ALL);

            long mark = node.logs().mark();
            coordinator.execute(format("SELECT * FROM %s.%s WHERE k = 'secret_k' AND c = 'secret_c' AND v = 'secret_v' ALLOW FILTERING"), ALL);
            node.runOnInstance(() -> MonitoringTask.instance.logOperations(approxTime.now()));

            assertLogsContain(mark, node, "Some operations were slow", format("<SELECT \\* FROM %s\\.%s WHERE k = \\? AND c = \\? AND v = \\? ALLOW FILTERING>"));
            assertLogsNotContain(mark, node, "secret_k", "secret_c", "secret_v");
        }
    }

    private static String format(String query)
    {
        return String.format(query, KEYSPACE, TABLE);
    }

    private static void assertLogsContain(long mark, IInvokableInstance node, String... lines)
    {
        assertLogs(mark, node, AbstractIterableAssert::isNotEmpty, lines);
    }

    private static void assertLogsNotContain(long mark, IInvokableInstance node, String... lines)
    {
        assertLogs(mark, node, AbstractIterableAssert::isEmpty, lines);
    }

    private static void assertLogs(long mark, IInvokableInstance node, Consumer<ListAssert<String>> listAssert, String... lines)
    {
        for (String line : lines)
        {
            List<String> matchingLines = node.logs().grep(mark, line).getResult();
            listAssert.accept(Assertions.assertThat(matchingLines));
        }
    }

    /**
     * Bytebuddy interceptor to slow down reads on node 2.
     */
    public static class BBHelper
    {
        @SuppressWarnings("resource")
        public static void install(ClassLoader classLoader, Integer node)
        {
            if (node == 2)
            {
                new ByteBuddy().rebase(ReadCommand.class)
                               .method(named("executeLocally"))
                               .intercept(MethodDelegation.to(SlowQueryLoggerTest.BBHelper.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController,
                                                                 @SuperCall Callable<UnfilteredPartitionIterator> zuperCall)
        {
            try
            {
                if (executionController.metadata().name.equals(TABLE))
                    Thread.sleep(SLOW_QUERY_LOG_TIMEOUT_MS * 2);

                return zuperCall.call();
            }
            catch (Exception e)
            {
                throw Throwables.unchecked(e);
            }
        }
    }
}
