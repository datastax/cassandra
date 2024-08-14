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

package org.apache.cassandra.utils;

import org.junit.Assert;
import org.junit.Test;

public class ThrowablesTest
{
    @Test
    public void testGetCauseOfType()
    {
        // getCauseOfType with null throwable returns empty Optional
        Assert.assertTrue(Throwables.getCauseOfType(null, RuntimeException.class).isEmpty());
        // getCauseOfType with no cause returns empty Optional
        Assert.assertTrue(Throwables.getCauseOfType(new Exception(), RuntimeException.class).isEmpty());
        // getCauseOfType with no matching causing in chain returns empty Optional
        Assert.assertTrue(Throwables.getCauseOfType(new Exception(new Exception()), RuntimeException.class).isEmpty());
        // getCauseOfType with throwable of the specified type returns Optional with the throwable
        RuntimeException cause = new RuntimeException();
        Assert.assertEquals(cause, Throwables.getCauseOfType(cause, RuntimeException.class).get());
        // getCauseOfType with cause of the specified type returns Optional with the cause
        Exception exception = new Exception(cause);
        Assert.assertEquals(cause, Throwables.getCauseOfType(exception, RuntimeException.class).get());
        // with multiple causes in chain, return first one
        RuntimeException cause2 = new RuntimeException(cause);
        Exception exception2 = new Exception(cause2);
        Assert.assertEquals(cause2, Throwables.getCauseOfType(exception2, RuntimeException.class).get());
    }
}
