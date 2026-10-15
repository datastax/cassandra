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

package org.apache.cassandra.tools.nodetool.stats;

import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;

import javax.management.InstanceNotFoundException;

import org.junit.Test;

import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxHistogramMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxTimerMBean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TableStatsHolderTest
{
    @Test
    public void testMetricMean()
    {
        assertThat(TableStatsHolder.getMetricMean(null)).isNaN();

        // JMX timers report their mean in microseconds, and tablestats prints the SAI latencies in milliseconds
        double timerMeanMicros = 2500.0;
        double expectedTimerMeanMillis = 2.5;
        assertThat(TableStatsHolder.getMetricMean(mbean(JmxTimerMBean.class, timerMeanMicros))).isEqualTo(expectedTimerMeanMillis);

        // histograms have no unit, their mean is only rounded to two decimals
        assertThat(TableStatsHolder.getMetricMean(mbean(JmxHistogramMBean.class, 1.0 / 3))).isEqualTo(0.33);
        // the mean of an empty histogram
        assertThat(TableStatsHolder.getMetricMean(mbean(JmxHistogramMBean.class, Double.NaN))).isZero();

        assertThatThrownBy(() -> TableStatsHolder.getMetricMean("not a metric")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testMetricMeanOfUnregisteredMetric()
    {
        // what a JMX proxy throws when its MBean has been unregistered
        UndeclaredThrowableException unregistered = new UndeclaredThrowableException(new InstanceNotFoundException());
        assertThat(TableStatsHolder.getMetricMean(mbean(JmxTimerMBean.class, unregistered))).isNaN();
        assertThat(TableStatsHolder.getMetricMean(mbean(JmxHistogramMBean.class, unregistered))).isNaN();

        UndeclaredThrowableException other = new UndeclaredThrowableException(new Exception());
        assertThatThrownBy(() -> TableStatsHolder.getMetricMean(mbean(JmxTimerMBean.class, other))).isSameAs(other);
    }

    /**
     * Returns an MBean whose {@code getMean()} returns {@code mean}, or throws it if it is an exception.
     */
    private static <T> T mbean(Class<T> mbeanClass, Object mean)
    {
        return mbeanClass.cast(Proxy.newProxyInstance(mbeanClass.getClassLoader(), new Class<?>[]{ mbeanClass }, (proxy, method, args) -> {
            assertThat(method.getName()).isEqualTo("getMean");
            if (mean instanceof RuntimeException)
                throw (RuntimeException) mean;
            return mean;
        }));
    }
}
