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

package org.apache.cassandra.concurrent;

/**
 * A task or callable that wraps a user-supplied task, and can name the class of that task so an executor reports the
 * class of the work rather than of its own wrapper.
 */
interface WrappedTask
{
    /**
     * The class of the user-supplied task, for diagnostics only. Tasks submitted as a {@code Callable} or through
     * {@code Stage.submit} report the callable's class, which may be a lambda.
     */
    Class<?> taskClass();

    /** The class of the work {@code task} runs: the wrapped task's class when it is a wrapper, else its own. */
    static Class<?> classOf(Object task)
    {
        // test TimedTask first, as the executor does on the submitting thread: testing the same class against two
        // interfaces from two threads thrashes its one-entry secondary supers cache (JDK-8180450)
        if (task instanceof TimedTask)
            return ((TimedTask) task).taskClass();
        return task instanceof WrappedTask ? ((WrappedTask) task).taskClass() : task.getClass();
    }
}
