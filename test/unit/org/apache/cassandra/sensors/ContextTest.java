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

import static org.assertj.core.api.Assertions.assertThat;

public class ContextTest
{
    // -----------------------------------------------------------------------
    // Context.request(RequestSensors)
    // -----------------------------------------------------------------------

    @Test
    public void testRequestContextIsFromContext()
    {
        RequestSensors sensors = new ActiveRequestSensors();
        assertThat(Context.from(sensors).isRequestContext()).isTrue();
    }

    @Test
    public void testFromContextGettersReturnEmpty()
    {
        RequestSensors sensors = new ActiveRequestSensors();
        Context ctx = Context.from(sensors);
        assertThat(ctx.getKeyspace()).isEmpty();
        assertThat(ctx.getTable()).isEmpty();
        assertThat(ctx.getTableId()).isEmpty();
    }

    @Test
    public void testRequestContextFromOwnerIsPresent()
    {
        RequestSensors sensors = new ActiveRequestSensors();
        assertThat(Context.from(sensors).getRequestOwner())
                .hasValue(sensors.getRequestOwner());
    }

    @Test
    public void testFromContextToString()
    {
        RequestSensors sensors = new ActiveRequestSensors();
        assertThat(Context.from(sensors).toString())
                .isEqualTo("Context{requestOwner='" + sensors.getRequestOwner() + "'}");
    }

    @Test
    public void testFromContextEqualityWithSameOwner()
    {
        RequestSensors sensors = new ActiveRequestSensors();
        assertThat(Context.from(sensors)).isEqualTo(Context.from(sensors));
    }

    @Test
    public void testFromContextHashCodeIsStable()
    {
        RequestSensors sensors = new ActiveRequestSensors();
        assertThat(Context.from(sensors).hashCode())
                .isEqualTo(Context.from(sensors).hashCode());
    }

    // -----------------------------------------------------------------------
    // Table context
    // -----------------------------------------------------------------------

    @Test
    public void testTableContextIsNotFromContext()
    {
        Context ctx = new Context("ks", "tbl", "uuid-1");
        assertThat(ctx.isRequestContext()).isFalse();
    }

    @Test
    public void testTableContextGettersReturnValues()
    {
        Context ctx = new Context("ks", "tbl", "uuid-1");
        assertThat(ctx.getKeyspace()).hasValue("ks");
        assertThat(ctx.getTable()).hasValue("tbl");
        assertThat(ctx.getTableId()).hasValue("uuid-1");
    }

    @Test
    public void testTableContextToString()
    {
        Context ctx = new Context("ks", "tbl", "uuid-1");
        assertThat(ctx.toString()).contains("ks").contains("tbl").contains("uuid-1");
    }

    @Test
    public void testTableContextEqualityAndHashCode()
    {
        Context a = new Context("ks", "tbl", "uuid-1");
        Context b = new Context("ks", "tbl", "uuid-1");
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    public void testTableContextNotEqualToFromContext()
    {
        RequestSensors sensors = new ActiveRequestSensors();
        Context tableCtx = new Context("ks", "tbl", "uuid-1");
        assertThat(tableCtx).isNotEqualTo(Context.from(sensors));
        assertThat(Context.from(sensors)).isNotEqualTo(tableCtx);
    }
}
