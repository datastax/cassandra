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
