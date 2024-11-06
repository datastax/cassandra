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

package org.apache.cassandra.concurrent;

import org.apache.cassandra.utils.MBeanWrapper;

public class TaskExecutionCallback
{
    public static TaskExecutionCallback instance = new TaskExecutionCallback(new NoopTaskCompletedCallback(),
                                                                             new NoopTaskDequeueCallback());

    public interface OnDequeue
    {
        void onDequeue(Stage stage, long enqueuedDurationNanos);
    }

    public interface OnCompleted
    {
        void onCompleted(Stage stage, long executionDurationNanos);
    }

    /** onCompleted doesn't need to be volatile. We don't need any synchronization here
     * The stage code will pick up there's a new callback object sooner or later; we don't really care
     */
    private OnCompleted onCompleted;

    /** onDequeue doesn't need to be volatile. We don't need any synchronization here
     * The stage code will pick up there's a new callback object sooner or later; we don't really care
     */
    private OnDequeue onDequeue;

    private TaskExecutionCallback(OnCompleted onCompleted, OnDequeue onDequeue)
    {
        this.onCompleted = onCompleted;
        this.onDequeue = onDequeue;
    }

    public void onCompleted(Stage stage, long executionDurationNanos)
    {
        onCompleted.onCompleted(stage, executionDurationNanos);
    }
    public void onDequeue(Stage stage, long enqueuedDurationNanos)
    {
        onDequeue.onDequeue(stage, enqueuedDurationNanos);
    }

    public void setTaskDequeueCallback(OnDequeue onDequeue)
    {
        this.onDequeue = onDequeue;
    }

    public void setTaskCompletionCallback(OnCompleted onCompleted)
    {
        this.onCompleted = onCompleted;
    }

    public static class NoopTaskCompletedCallback implements OnCompleted
    {
        @Override
        public void onCompleted(Stage stage, long executionDurationNanos)
        {
        }
    }

    public static class NoopTaskDequeueCallback implements OnDequeue
    {
        @Override
        public void onDequeue(Stage stage, long enqueuedDurationNanos)
        {
        }
    }
}
