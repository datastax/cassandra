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

package org.apache.cassandra.schema;

import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * This is the type of schema on Astra.
 * Astra handles both regular tables and Collections.
 * A Collection has a special internal schema that allows users to work without a schema.
 */
public enum SchemaType {

    /**
     * Regular table
     */
    TABLE,
    /**
     * This is a Collection, handled by Stargate.
     */
    COLLECTION;

    public static SchemaType fromString(String str)
    {
        if (str == null)
        {
            return TABLE;
        }
        try
        {
            return valueOf(str.toUpperCase());
        } catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(String.format("Invalid value %s for option '%s'", str, TableParams.Option.SCHEMA_TYPE));
        }
    }
}