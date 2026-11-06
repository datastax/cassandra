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

package org.apache.cassandra.index.sai.utils;

import java.util.concurrent.Executor;

/**
 * An {@link InsertFanout} with no admission control: every unit is dispatched to the executor
 * immediately, so both the number of in-flight tasks and their memory are unbounded (limited only by
 * the executor and available heap). It still tracks completion (for {@link #awaitCompletion()}) and the
 * first error. Selected when the in-flight budget is configured to 0 (disabled) — an escape hatch that
 * restores the pre-admission behavior for comparison or when a bound is explicitly not wanted.
 */
public final class UnboundedFanout extends InsertFanout
{
    public UnboundedFanout(Executor executor)
    {
        super(executor);
    }

    @Override
    protected int admit(int weight)
    {
        return 0; // no admission control
    }

    @Override
    protected void release(int admitted)
    {
        // no-op
    }
}
