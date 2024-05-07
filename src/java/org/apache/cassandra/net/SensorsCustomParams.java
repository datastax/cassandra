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

package org.apache.cassandra.net;

import java.nio.ByteBuffer;

import org.apache.cassandra.sensors.Sensor;

/**
 * A utility class that contains the definition of custom params added to the {@link Message} header to propagate {@link Sensor} values from
 * writer to coordinator and necessary methods to encode sensor values as appropriate for the internode message format.
 */
public final class SensorsCustomParams
{
    /**
     * The per-request read bytes value for a given keyspace and table.
     */
    public static final String READ_BYTES_REQUEST = "READ_BYTES_REQUEST";
    /**
     * The total read bytes value for a given keyspace and table, across all requests. This is a monotonically increasing value.
     */
    public static final String READ_BYTES_TABLE = "READ_BYTES_TABLE";
    /**
     * The per-request write bytes value for a given keyspace and table.
     * To support batch writes, table name is encoded in the following format: WRITE_BYTES_REQUEST.<table>
     */
    public static final String WRITE_BYTES_REQUEST_TEMPLATE = "WRITE_BYTES_REQUEST.%s";
    /**
     * The total write bytes value for a given keyspace and table, across all requests.
     * To support batch writes, table name is encoded in the following format: WRITE_BYTES_TABLE.<table>
     */
    public static final String WRITE_BYTES_TABLE_TEMPLATE = "WRITE_BYTES_TABLE.%s";
    /**
     * The per-request index read bytes value for a given keyspace and table.
     */
    public static final String INDEX_READ_BYTES_REQUEST = "INDEX_READ_BYTES_REQUEST";
    /**
     * The total index read bytes value for a given keyspace and table, across all requests. This is a monotonically increasing value.
     */
    public static final String INDEX_READ_BYTES_TABLE = "INDEX_READ_BYTES_TABLE";
    /**
     * The per-request index write bytes value for a given keyspace and table.
     * To support batch writes, table name is encoded in the following format: INDEX_WRITE_BYTES_REQUEST.<table>
     */
    public static final String INDEX_WRITE_BYTES_REQUEST_TEMPLATE = "INDEX_WRITE_BYTES_REQUEST.%s";
    /**
     * The total index write bytes value for a given keyspace and table, across all requests.
     * To support batch writes, table name is encoded in the following format: INDEX_WRITE_BYTES_TABLE.<table>
     */
    public static final String INDEX_WRITE_BYTES_TABLE_TEMPLATE = "INDEX_WRITE_BYTES_TABLE.%s";

    private SensorsCustomParams()
    {
    }

    /**
     * Utility method to encode sensor value as byte buffer in the big endian order.
     */
    public static byte[] sensorValueAsBytes(double value)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        buffer.putDouble(value);

        return buffer.array();
    }

    public static String encodeTableInWriteBytesRequestParam(String tableName)
    {
        return String.format(WRITE_BYTES_REQUEST_TEMPLATE, tableName);
    }

    public static String encodeTableInWriteBytesTableParam(String tableName)
    {
        return String.format(WRITE_BYTES_TABLE_TEMPLATE, tableName);
    }

    public static String encodeTableInIndexWriteBytesRequestParam(String tableName)
    {
        return String.format(INDEX_WRITE_BYTES_REQUEST_TEMPLATE, tableName);
    }

    public static String encodeTableInIndexWriteBytesTableParam(String tableName)
    {
        return String.format(INDEX_WRITE_BYTES_TABLE_TEMPLATE, tableName);
    }
}
