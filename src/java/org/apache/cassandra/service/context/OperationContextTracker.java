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

package org.apache.cassandra.service.context;

import org.apache.cassandra.concurrent.ExecutorLocals;

public class OperationContextTracker extends ExecutorLocals.Impl
{
    public static final OperationContextTracker instance = new OperationContextTracker();

    private OperationContextTracker()
    {}

    public OperationContext get()
    {
        return ExecutorLocals.current().operationContext;
    }

    public void set(OperationContext operationContext)
    {
        ExecutorLocals current = ExecutorLocals.current();
        ExecutorLocals.Impl.set(current.traceState, current.clientWarnState, current.sensors, operationContext);
    }

    public static void start(OperationContext context)
    {
        instance.set(context);
    }

    public static void endCurrent()
    {
        OperationContext ctx = instance.get();
        if (ctx != null)
        {
            ctx.close();
            instance.set(null);
        }
    }
}
