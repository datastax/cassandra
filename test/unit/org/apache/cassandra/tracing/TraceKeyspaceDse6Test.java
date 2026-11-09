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

package org.apache.cassandra.tracing;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * In the DSE 6 compatibility mode the trace rows leave out the port columns ({@code coordinator_port},
 * {@code source_port}): {@code system_traces} keeps DSE 6's definition, which has neither, until the mode is dropped.
 * Without the mode they are written as before.
 */
public class TraceKeyspaceDse6Test
{
    private static final ByteBuffer SESSION = nextTimeUUID().toBytes();

    private InternodeCompatibilityMode modeBefore;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void saveMode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
    }

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    @Test
    public void portColumnsWrittenWithNone()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        // precondition: no node older than 4.0 is known, the other reason to leave the ports out
        assertThat(Gossiper.instance.hasMajorVersion3OrUnknownNodes()).isFalse();
        assertThat(columns(startSession())).contains("coordinator", "coordinator_port");
        assertThat(columns(event())).contains("source", "source_port");
    }

    @Test
    public void portColumnsLeftOutInDse6Mode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertThat(columns(startSession())).contains("coordinator").doesNotContain("coordinator_port");
        assertThat(columns(event())).contains("source").doesNotContain("source_port");
    }

    private static Mutation startSession()
    {
        return TraceKeyspace.makeStartSessionMutation(SESSION, InetAddress.getLoopbackAddress(), Collections.emptyMap(), "request",
                                                      FBUtilities.now().toEpochMilli(), "QUERY", 60);
    }

    private static Mutation event()
    {
        return TraceKeyspace.makeEventMutation(SESSION, "activity", 10, "thread", 60);
    }

    /** The columns a trace mutation writes: those of its row, which are those of its serialization header. */
    private static List<String> columns(Mutation mutation)
    {
        PartitionUpdate update = mutation.getPartitionUpdates().iterator().next();
        return update.columns().regulars.stream().map(c -> c.name.toString()).collect(Collectors.toList());
    }
}
