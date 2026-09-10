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
    // Context.request()
    // -----------------------------------------------------------------------

    @Test
    public void testRequestContextIsSingleton()
    {
        assertThat(Context.request()).isSameAs(Context.request());
    }

    @Test
    public void testRequestContextIsRequestContext()
    {
        assertThat(Context.request().isRequestContext()).isTrue();
    }

    @Test
    public void testRequestContextGettersReturnEmpty()
    {
        assertThat(Context.request().getKeyspace()).isEmpty();
        assertThat(Context.request().getTable()).isEmpty();
        assertThat(Context.request().getTableId()).isEmpty();
    }

    @Test
    public void testRequestContextToString()
    {
        assertThat(Context.request().toString()).isEqualTo("Context{request}");
    }

    @Test
    public void testRequestContextEqualsItself()
    {
        assertThat(Context.request()).isEqualTo(Context.request());
    }

    @Test
    public void testRequestContextHashCodeIsStable()
    {
        assertThat(Context.request().hashCode()).isEqualTo(Context.request().hashCode());
    }

    // -----------------------------------------------------------------------
    // Table context
    // -----------------------------------------------------------------------

    @Test
    public void testTableContextIsNotRequestContext()
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
    public void testTableContextNotEqualToRequestContext()
    {
        Context tableCtx = new Context("ks", "tbl", "uuid-1");
        assertThat(tableCtx).isNotEqualTo(Context.request());
        assertThat(Context.request()).isNotEqualTo(tableCtx);
    }
}
