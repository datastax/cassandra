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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;

public class PermissionFactory
{
    // ALL permissions.  See note in Permission.  This would ideally be in the Permission interface, but
    // all interface static variables (only since Java 8!) are implicitly final.
    public static ImmutableSet<Permission> ALL = ImmutableSet.of();
    public static final ImmutableSet<Permission> NONE = ImmutableSet.of();

    static synchronized void register(Permission... values)
    {
        ALL = ImmutableSet.copyOf(Stream.concat(Arrays.stream(values), ALL.stream()).collect(Collectors.toSet()));
    }

    // unfortunately it does not seem possible to have multiple type bounds with a wildcard
    private ConcurrentHashMap<String, Class<? extends Enum>> extendedPermissions = new ConcurrentHashMap<>();

    public Permission valueOf(String name)
    {
        int split = name.indexOf('.');

        if (split < 0)
        {
            // No class name -> Cassandra permission
            return CassandraPermission.valueOf(name);
        }
        else
        {
            String className = "org.apache.cassandra.auth." + name.substring(0, split) + "Permission";
            String permissionName = name.substring(split+1);
            Class<? extends Enum> permissionClass = extendedPermissions.computeIfAbsent(className, classname -> {
                try
                {
                    Class loading = Class.forName(className);
                    assert Enum.class.isAssignableFrom(loading) : "Permission classes must extend enum";
                    assert Permission.class.isAssignableFrom(loading) : "Permission classes must implement Permission";
                    return loading;
                }
                catch (ClassNotFoundException e)
                {
                    throw new RuntimeException("Couldn't load permission class " + classname, e);
                }
            });


            // Cast should be safe based on the assertions above
            return (Permission)Enum.valueOf(permissionClass, permissionName);
        }
    }
}
