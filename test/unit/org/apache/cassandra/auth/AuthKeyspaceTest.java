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

import java.util.Set;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.assertj.core.api.Assertions.assertThat;

public class AuthKeyspaceTest
{
    private StorageCompatibilityMode originalMode;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        originalMode = DatabaseDescriptor.getStorageCompatibilityMode();
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(originalMode);
    }

    @Test
    public void testMetadataExcludesCidrAndIdentityTablesInCC4Mode()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.CASSANDRA_4);
        KeyspaceMetadata metadata = AuthKeyspace.metadata();
        Set<String> tableNames = metadata.tables.stream().map(t -> t.name).collect(Collectors.toSet());

        assertThat(tableNames).contains(AuthKeyspace.ROLES, AuthKeyspace.ROLE_MEMBERS,
                                        AuthKeyspace.ROLE_PERMISSIONS, AuthKeyspace.RESOURCE_ROLE_INDEX,
                                        AuthKeyspace.NETWORK_PERMISSIONS);
        assertThat(tableNames).doesNotContain(AuthKeyspace.CIDR_PERMISSIONS, AuthKeyspace.CIDR_GROUPS,
                                              AuthKeyspace.IDENTITY_TO_ROLES);
    }

    @Test
    public void testMetadataIncludesCidrAndIdentityTablesInNoneMode()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
        KeyspaceMetadata metadata = AuthKeyspace.metadata();
        Set<String> tableNames = metadata.tables.stream().map(t -> t.name).collect(Collectors.toSet());

        assertThat(tableNames).contains(AuthKeyspace.ROLES, AuthKeyspace.ROLE_MEMBERS,
                                        AuthKeyspace.ROLE_PERMISSIONS, AuthKeyspace.RESOURCE_ROLE_INDEX,
                                        AuthKeyspace.NETWORK_PERMISSIONS,
                                        AuthKeyspace.CIDR_PERMISSIONS, AuthKeyspace.CIDR_GROUPS,
                                        AuthKeyspace.IDENTITY_TO_ROLES);
    }
}
