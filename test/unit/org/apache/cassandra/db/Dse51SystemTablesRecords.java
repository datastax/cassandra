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

package org.apache.cassandra.db;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.cassandra.nodes.DseNodeWorkloads;

/**
 * The record of the values DSE 5.1 stored in {@code system.local} and {@code system.peers}
 * ({@link Dse51SystemTables#recordPath}), written by {@link Dse51SystemTables}'s own writer, for the tests of other
 * packages that need {@link Dse51SystemTables#stored} to hold given values once the record is loaded.
 */
public final class Dse51SystemTablesRecords
{
    private Dse51SystemTablesRecords()
    {
    }

    /**
     * Writes the record with the node's own row when {@code localDseVersion} is not null, then a row per entry of
     * {@code peers}, each with its {@code dse_version}, the {@code Cassandra} workload and no graph.
     */
    public static void writeDseVersions(@Nullable String localDseVersion, Map<InetAddress, String> peers) throws IOException
    {
        List<Dse51SystemTables.StoredColumns> rows = new ArrayList<>();
        if (localDseVersion != null)
            rows.add(row(null, localDseVersion));
        peers.forEach((peer, dseVersion) -> rows.add(row(peer, dseVersion)));
        Dse51SystemTables.write(Dse51SystemTables.recordPath(), rows);
    }

    private static Dse51SystemTables.StoredColumns row(@Nullable InetAddress peer, String dseVersion)
    {
        return new Dse51SystemTables.StoredColumns(peer, List.of(DseNodeWorkloads.CASSANDRA), DseNodeWorkloads.CASSANDRA,
                                                   false, dseVersion);
    }
}
