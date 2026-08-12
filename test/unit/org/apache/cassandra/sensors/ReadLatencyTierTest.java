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

package org.apache.cassandra.sensors;

import org.junit.Test;

import static org.apache.cassandra.sensors.ReadLatencyTier.Bounds.MILLIS_1;
import static org.apache.cassandra.sensors.ReadLatencyTier.Bounds.MILLIS_10;
import static org.apache.cassandra.sensors.ReadLatencyTier.Bounds.MILLIS_50;
import static org.apache.cassandra.sensors.ReadLatencyTier.Bounds.MILLIS_200;
import static org.assertj.core.api.Assertions.assertThat;

public class ReadLatencyTierTest
{
    @Test
    public void testTier1_zero()
    {
        assertThat(ReadLatencyTier.fromNanos(0)).isEqualTo(ReadLatencyTier.TIER_1);
    }

    @Test
    public void testTier1_justBelowUpperBound()
    {
        assertThat(ReadLatencyTier.fromNanos(MILLIS_1 - 1)).isEqualTo(ReadLatencyTier.TIER_1);
    }

    @Test
    public void testTier2_atLowerBound()
    {
        assertThat(ReadLatencyTier.fromNanos(MILLIS_1)).isEqualTo(ReadLatencyTier.TIER_2);
    }

    @Test
    public void testTier2_justBelowUpperBound()
    {
        assertThat(ReadLatencyTier.fromNanos(MILLIS_10 - 1)).isEqualTo(ReadLatencyTier.TIER_2);
    }

    @Test
    public void testTier3_atLowerBound()
    {
        assertThat(ReadLatencyTier.fromNanos(MILLIS_10)).isEqualTo(ReadLatencyTier.TIER_3);
    }

    @Test
    public void testTier3_justBelowUpperBound()
    {
        assertThat(ReadLatencyTier.fromNanos(MILLIS_50 - 1)).isEqualTo(ReadLatencyTier.TIER_3);
    }

    @Test
    public void testTier4_atLowerBound()
    {
        assertThat(ReadLatencyTier.fromNanos(MILLIS_50)).isEqualTo(ReadLatencyTier.TIER_4);
    }

    @Test
    public void testTier4_justBelowUpperBound()
    {
        assertThat(ReadLatencyTier.fromNanos(MILLIS_200 - 1)).isEqualTo(ReadLatencyTier.TIER_4);
    }

    @Test
    public void testTier5_atLowerBound()
    {
        assertThat(ReadLatencyTier.fromNanos(MILLIS_200)).isEqualTo(ReadLatencyTier.TIER_5);
    }

    @Test
    public void testTier5_largeValue()
    {
        assertThat(ReadLatencyTier.fromNanos(Long.MAX_VALUE - 1)).isEqualTo(ReadLatencyTier.TIER_5);
    }

    @Test
    public void testTierValues_areOrdered()
    {
        assertThat(ReadLatencyTier.TIER_1.value()).isLessThan(ReadLatencyTier.TIER_2.value());
        assertThat(ReadLatencyTier.TIER_2.value()).isLessThan(ReadLatencyTier.TIER_3.value());
        assertThat(ReadLatencyTier.TIER_3.value()).isLessThan(ReadLatencyTier.TIER_4.value());
        assertThat(ReadLatencyTier.TIER_4.value()).isLessThan(ReadLatencyTier.TIER_5.value());
    }

    @Test
    public void testTierValues_matchExpected()
    {
        assertThat(ReadLatencyTier.TIER_1.value()).isEqualTo(1.0);
        assertThat(ReadLatencyTier.TIER_2.value()).isEqualTo(2.0);
        assertThat(ReadLatencyTier.TIER_3.value()).isEqualTo(3.0);
        assertThat(ReadLatencyTier.TIER_4.value()).isEqualTo(4.0);
        assertThat(ReadLatencyTier.TIER_5.value()).isEqualTo(5.0);
    }
}
