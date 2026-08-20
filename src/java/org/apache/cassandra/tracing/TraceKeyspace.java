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
package org.apache.cassandra.tracing;

import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;

import static java.lang.String.format;

public final class TraceKeyspace
{
    private TraceKeyspace()
    {
    }

    /**
     * Generation is used as a timestamp for automatic table creation on startup.
     * If you make any changes to the tables below, make sure to increment the
     * generation and document your change here.
     *
     * gen 1577836800000000: (3.0) maps to Jan 1 2020; an arbitrary cut-off date by which we assume no nodes older than 2.0.2
     *                       will ever start; see the note below for why this is necessary; actual change in 3.0:
     *                       removed default ttl, reduced bloom filter fp chance from 0.1 to 0.01.
     * gen 1577836800000001: (pre-)adds coordinator_port column to sessions and source_port column to events in 3.0, 3.11, 4.0
     * gen 1577836800000002: compression chunk length reduced to 16KiB, memtable_flush_period_in_ms now unset on all tables in 4.0
     *
     * * Until CASSANDRA-6016 (Oct 13, 2.0.2) and in all of 1.2, we used to create system_traces keyspace and
     *   tables in the same way that we created the purely local 'system' keyspace - using current time on node bounce
     *   (+1). For new definitions to take, we need to bump the generation further than that.
     */
    public static final long GENERATION = 1577836800000002L;

    public static final String SESSIONS = "sessions";
    public static final String EVENTS = "events";

    static final TableMetadata Sessions =
        parse(SESSIONS,
                "tracing sessions",
                "CREATE TABLE %s ("
                + "session_id uuid,"
                + "command text,"
                + "client inet,"
                + "coordinator inet,"
                + "coordinator_port int,"
                + "duration int,"
                + "parameters map<text, text>,"
                + "request text,"
                + "started_at timestamp,"
                + "context map<text, text>,"
                + "PRIMARY KEY ((session_id)))");

    static final TableMetadata Events =
        parse(EVENTS,
                "tracing events",
                "CREATE TABLE %s ("
                + "session_id uuid,"
                + "event_id timeuuid,"
                + "activity text,"
                + "source inet,"
                + "source_port int,"
                + "source_elapsed int,"
                + "thread text,"
                + "context map<text, text>,"
                + "PRIMARY KEY ((session_id), event_id))");

    public static TableMetadata parse(String table, String description, String cql)
    {
        return CreateTableStatement.parse(format(cql, table), SchemaConstants.TRACE_KEYSPACE_NAME)
                                   .id(TableId.forSystemTable(SchemaConstants.TRACE_KEYSPACE_NAME, table))
                                   .gcGraceSeconds(0)
                                   .comment(description)
                                   .build();
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(SchemaConstants.TRACE_KEYSPACE_NAME,
                                       KeyspaceParams.systemDistributed(2),
                                       Tables.of(Sessions, Events));
    }

    public static TraceStorage asStorage()
    {
        TableMetadata eventsTable = TraceKeyspace.metadata().tables.getNullable(EVENTS);
        TableMetadata sessionsTable = TraceKeyspace.metadata().tables.getNullable(SESSIONS);
        return new KeyspaceTraceStorage(eventsTable, sessionsTable);
    }
}
