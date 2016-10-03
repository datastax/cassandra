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

package org.apache.cassandra.auth;

import java.util.Arrays;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class PermissionFactory
{
    // ALL permissions.  See note in Permission.  This would ideally be in the Permission interface, but
    // all interface static variables (only since Java 8!) are implicitly final.  Register should generally happen
    // early enough in the initialization that these variables are effectively final.
    public static ImmutableSet<Permission> ALL = ImmutableSet.of();
    public static final ImmutableSet<Permission> NONE = ImmutableSet.of();

    // Registered permissions: serialize XXPermission.Y as XX.Y (to make things look pretty in LIST PERMISSIONS more
    // than anything).  Unfortunately it does not seem possible to have multiple type bounds on a wildcard.
    private static ImmutableMap<String, Class<? extends Enum>> EXTENDED = ImmutableMap.of();

    static synchronized void register(Class<? extends Enum> clazz, Permission... values)
    {
        assert Permission.class.isAssignableFrom(clazz) : "Permission classes must implement Permission";
        String className = clazz.getSimpleName();
        assert clazz.getSimpleName().endsWith("Permission") : "Convention is that Permission class names end with Permission";
        String shortName = className.substring(0, className.length() - "Permission".length());
        assert !EXTENDED.containsKey(shortName) : "Map already contains this permission!";

        EXTENDED = ImmutableMap.<String, Class<? extends Enum>>builder()
                   .putAll(EXTENDED)
                   .put(shortName, clazz)
                   .build();

        ALL = ImmutableSet.<Permission>builder()
              .addAll(ALL)
              .addAll(Arrays.asList(values))
              .build();
    }

    public static Permission valueOf(String name)
    {
        int split = name.indexOf('.');

        if (split < 0)
        {
            // No class name -> Cassandra permission
            return CassandraPermission.valueOf(name);
        }
        else
        {
            Class<? extends Enum> permissionClass = EXTENDED.get(name.substring(0, split));

            if (permissionClass == null)
            {
                throw new RuntimeException("Class not registered for " + name);
            }

            // Cast should be safe based on the assertions in register()
            return (Permission)Enum.valueOf(permissionClass, name.substring(split+1));
        }
    }
}
