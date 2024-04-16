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

package org.apache.cassandra.io.util;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SliceDescriptorTest
{
    @Test
    public void testEmptySliceDescriptor()
    {
        SliceDescriptor sliceDescriptor = SliceDescriptor.NONE;
        assertThat(sliceDescriptor.exists()).isFalse();
        assertThat(sliceDescriptor.dataStart).isEqualTo(0);
        assertThat(sliceDescriptor.dataEnd).isEqualTo(0);
        assertThat(sliceDescriptor.chunkSize).isEqualTo(0);
        assertThat(sliceDescriptor.sliceStart).isEqualTo(0);
        assertThat(sliceDescriptor.sliceEnd).isEqualTo(0);
        assertThat(sliceDescriptor.dataEndOr(1234)).isEqualTo(1234);
    }

    @Test
    public void testSliceDescriptor()
    {
        SliceDescriptor sliceDescriptor = new SliceDescriptor(37, 87, 16);
        assertThat(sliceDescriptor.exists()).isTrue();
        assertThat(sliceDescriptor.dataStart).isEqualTo(37);
        assertThat(sliceDescriptor.dataEnd).isEqualTo(87);
        assertThat(sliceDescriptor.chunkSize).isEqualTo(16);
        assertThat(sliceDescriptor.sliceStart).isEqualTo(32);
        assertThat(sliceDescriptor.sliceEnd).isEqualTo(96);
        assertThat(sliceDescriptor.dataEndOr(1234)).isEqualTo(87);

        sliceDescriptor = new SliceDescriptor(37, 96, 16);
        assertThat(sliceDescriptor.sliceEnd).isEqualTo(96);

        sliceDescriptor = new SliceDescriptor(37, 95, 16);
        assertThat(sliceDescriptor.sliceEnd).isEqualTo(96);

        sliceDescriptor = new SliceDescriptor(37, 97, 16);
        assertThat(sliceDescriptor.sliceEnd).isEqualTo(112);
    }
}