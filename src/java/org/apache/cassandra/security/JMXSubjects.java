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

package org.apache.cassandra.security;

import java.lang.reflect.Method;
import java.security.AccessController;

import javax.security.auth.Subject;

/**
 * Resolves the {@link Subject} associated with the current JMX call in a JDK-version-safe way.
 * <p>
 * On JDK 11/17/21 the JMX layer associates the authenticated {@code Subject} with the current
 * {@link java.security.AccessControlContext}, and Cassandra reads it with the historical
 * {@code Subject.getSubject(AccessControlContext)}.
 * <p>
 * On JDK 24+ (JEP 486) {@code Subject.getSubject(AccessControlContext)} unconditionally throws
 * {@link UnsupportedOperationException}; instead the JMX layer associates the {@code Subject} via
 * {@code Subject.callAs} and it is read with {@code Subject.current()} (added in JDK 18). That method is
 * invoked reflectively because Cassandra is compiled with {@code source/target 11}, where it does not exist.
 */
public final class JMXSubjects
{
    private static final Method SUBJECT_CURRENT = subjectCurrentMethod();

    private JMXSubjects()
    {
    }

    private static Method subjectCurrentMethod()
    {
        try
        {
            return Subject.class.getMethod("current");
        }
        catch (NoSuchMethodException e)
        {
            return null; // JDK < 18
        }
    }

    /**
     * @return the {@link Subject} associated with the current JMX invocation, or {@code null} if there is none.
     */
    @SuppressWarnings({ "deprecation", "removal" })
    public static Subject current()
    {
        if (ThreadAwareSecurityManager.isSecurityManagerSupported())
            return Subject.getSubject(AccessController.getContext());

        if (SUBJECT_CURRENT == null)
            throw new IllegalStateException("Subject.current() is unavailable but a SecurityManager is not supported on this JVM");

        try
        {
            return (Subject) SUBJECT_CURRENT.invoke(null);
        }
        catch (ReflectiveOperationException e)
        {
            throw new RuntimeException("Failed to invoke Subject.current()", e);
        }
    }
}
