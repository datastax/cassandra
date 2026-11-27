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

package org.apache.cassandra.net;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.schema.Dse51Compressors;

import static org.assertj.core.api.Assertions.assertThat;

public class DseCompatRefusalsTest
{
    @Before
    @After
    public void reset()
    {
        DseCompatRefusals.reset();
    }

    @Test
    public void countsByReasonAndInTotal()
    {
        assertThat(DseCompatRefusals.count("repair")).isZero();
        assertThat(DseCompatRefusals.total()).isZero();
        assertThat(DseCompatRefusals.snapshot()).isEmpty();

        DseCompatRefusals.refused("repair");
        DseCompatRefusals.refused("repair");
        DseCompatRefusals.refused("decommission");

        assertThat(DseCompatRefusals.count("repair")).isEqualTo(2);
        assertThat(DseCompatRefusals.count("decommission")).isEqualTo(1);
        assertThat(DseCompatRefusals.count("move")).isZero();
        assertThat(DseCompatRefusals.total()).isEqualTo(3);
    }

    @Test
    public void snapshotIsSortedAndDetached()
    {
        DseCompatRefusals.refused("schema change");
        DseCompatRefusals.refused("decommission");
        Map<String, Long> snapshot = DseCompatRefusals.snapshot();
        assertThat(new ArrayList<>(snapshot.keySet())).containsExactly("decommission", "schema change");
        assertThat(snapshot).containsEntry("decommission", 1L).containsEntry("schema change", 1L);

        DseCompatRefusals.refused("decommission");
        assertThat(snapshot.get("decommission")).isEqualTo(1L);
        assertThat(DseCompatRefusals.snapshot().get("decommission")).isEqualTo(2L);
    }

    /**
     * The refusals of the DSE 5.1 compatibility mode, counted under reasons of their own, which {@code nodetool
     * compatstatus} lists with the others: the repair messages and the Thrift reads of DSE 5.1 peers, the schema
     * changes with a compressor DSE 5.1 does not read, and the verbs the dialect lacks, named with DSE 5.1.
     */
    @Test
    public void theReasonsOfTheDse51ModeAreCountedApart()
    {
        DatabaseDescriptor.daemonInitialization();
        InternodeCompatibilityMode modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        try
        {
            DseCompatRefusals.refused(DseCompat.REFUSED_DSE51_REPAIR_MESSAGE);
            DseCompatRefusals.refused(DseCompat.REFUSED_DSE51_REPAIR_MESSAGE);
            DseCompatRefusals.refused(DseCompat.REFUSED_DSE51_THRIFT_READ);
            DseCompatRefusals.refused(Dse51Compressors.REFUSED_SCHEMA_CHANGE);
            DseCompatRefusals.refused(DseCompat.refusedVerb(Verb.PAXOS2_PREPARE_REQ));
            assertThat(DseCompatRefusals.snapshot())
            .containsExactly(Map.entry("Thrift read from a DSE 5.1 peer", 1L),
                             Map.entry("repair message from a DSE 5.1 peer", 2L),
                             Map.entry("schema change with a compressor DSE 5.1 does not read", 1L),
                             Map.entry("verb PAXOS2_PREPARE_REQ to a DSE 5.1 peer", 1L));
            assertThat(DseCompatRefusals.total()).isEqualTo(5);
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        }
    }

    @Test
    public void concurrentIncrementsAreNotLost() throws InterruptedException
    {
        int threads = 8;
        int perThread = 10_000;
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++)
        {
            Thread worker = new Thread(() -> {
                for (int i = 0; i < perThread; i++)
                    DseCompatRefusals.refused("concurrent");
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers)
            worker.join();
        assertThat(DseCompatRefusals.count("concurrent")).isEqualTo((long) threads * perThread);
    }
}
