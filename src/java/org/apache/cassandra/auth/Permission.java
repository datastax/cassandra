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

/**
 * Permissions which a role can have over a resource.  This is an interface implemented by multiple enums for
 * extensibility.  This works pretty well: the == test inside the enum class will work fine with multiple inheriting
 * enums, serializing is just calling getName, and deserializing (in PermissionFactory) is just calculating the class
 * name from the qualified name and calling valueOf().It would be even a bit cleaner to extend enum directly,
 * but Java does not allow that.
 *
 * The primary difficulty is CassandraPermission.ALL.  With multiple enumerated types that could be dynamically registered by
 * extending classes, it's not so easy to figure out the set of all permissions.  The current solution is to have
 * everything that extends Permission register its values on load.  This is a bit clunky but it *should* work, because
 * if a resource contains a permission in its applicable set, then that class will be loaded first and all permissions
 * updated.  The Parser should also contain most if not all resource/permission classes and will be loaded pretty quickly.
 */
public interface Permission
{
    String name();         // Simple enum name, e.g. CREATE
    String getName();      // Qualified name.  For Cassandra these are the same, for extensions the class name will be merged (see PermissionFactory), for example XX.CanDoSomething.
}
