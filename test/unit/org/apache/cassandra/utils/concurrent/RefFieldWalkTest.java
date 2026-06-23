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

package org.apache.cassandra.utils.concurrent;

import java.lang.reflect.Field;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.exceptions.UnaccessibleFieldException;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeTrue;

import sun.misc.Unsafe;

/**
 * Regression test for the strong-reference leak detector's field walk (CASSANDRA-21171).
 *
 * On JDK 16+ a field that cannot be made reflectively accessible (module-protected) falls back to
 * {@code Unsafe.objectFieldOffset}, which throws {@code UnsupportedOperationException} for record components
 * (JDK 25 enforces this). {@code Ref.getFieldValue} surfaces that as an {@code UnaccessibleFieldException}.
 * Before the fix, {@code InProgressVisit.nextChild} did not catch it, so the first JDK record reachable from a
 * tracked graph (e.g. {@code java.security.SecureClassLoader$CodeSourceKey}) aborted the entire leak-detection
 * pass. The walk must instead skip the unreadable field and keep traversing.
 */
public class RefFieldWalkTest
{
    private static Unsafe unsafe()
    {
        try
        {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        }
        catch (Exception e)
        {
            return null;
        }
    }

    /**
     * Walking a module-protected JDK record must skip its unreadable component(s) and return normally
     * instead of throwing out of the field walk. Without the fix this throws {@link UnaccessibleFieldException}
     * on JDK 16+ and fails the test; with the fix the field is skipped and the visit completes.
     */
    @Test
    public void nextChildSkipsUnreadableRecordField() throws Exception
    {
        Unsafe unsafe = unsafe();
        assumeTrue("Unsafe unavailable", unsafe != null);

        // The exact record type from the original DriverBurnTest failure: a module-protected JDK record.
        Class<?> recordType;
        try
        {
            recordType = Class.forName("java.security.SecureClassLoader$CodeSourceKey");
        }
        catch (Throwable t)
        {
            assumeNoException("Record type not present on this JDK", t);
            return;
        }

        Object recordInstance = unsafe.allocateInstance(recordType);
        List<Field> fields = Ref.getFields(recordType);
        assumeTrue("Record has no reference component to walk", !fields.isEmpty());

        // Precondition: this field must be genuinely unreadable in this JVM (module-protected record component on
        // JDK 16+). If the running JDK/--add-opens makes it readable, the skip path is not exercised here -> skip.
        boolean unreadable;
        try
        {
            Ref.getFieldValue(recordInstance, fields.get(0));
            unreadable = false;
        }
        catch (UnaccessibleFieldException expected)
        {
            unreadable = true;
        }
        assumeTrue("Field is readable in this environment; the unreadable-field skip path is not exercised", unreadable);

        Ref.InProgressVisit visit = Ref.newInProgressVisit(recordInstance, fields, null, "record");

        // The regression: before the fix this propagates UnaccessibleFieldException and aborts the leak pass.
        // After the fix the unreadable component is skipped; with no other reference components, the visit yields
        // no children (null) rather than throwing.
        Pair<Object, Field> child = visit.nextChild();
        assertNull("Unreadable record component should be skipped, leaving no walkable children", child);
    }
}
