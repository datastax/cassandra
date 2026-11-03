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
package org.apache.cassandra.distributed.test.sai;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * SAI indexes on the fields of UDT and tuple columns (HCD-653) on a cluster of 3 nodes with RF 3: the field
 * restrictions ({@code RowFilter} expressions of kind FIELD) and the {@code CONTAINS ELEMENT} groups (kind
 * ELEMENT_MATCH) are serialized to the replicas, served by their indexes, and re-applied by the coordinator when it
 * reconciles the replicas' responses (replica filtering protection). Every query is run from each coordinator, on the
 * indexed table and with ALLOW FILTERING on a twin table without indexes. Asserted:
 * <ul>
 *     <li>with consistent replicas, the results at QUORUM and ALL;</li>
 *     <li>with newer values on one replica (node 3), the reconciled results at ALL, without and with paging (page
 *     size 1): a newer value that no longer matches removes a row the other replicas return, and a newer matching
 *     value adds a row the others don't return;</li>
 *     <li>with newer values on two replicas (nodes 2 and 3), the reconciled results at QUORUM, without and with
 *     paging: every quorum has a replica with the newer values, and the quorums with node 1 need the coordinator's
 *     re-filtering;</li>
 *     <li>a single-field update of a non-frozen UDT, and paging across the nodes with consistent replicas.</li>
 * </ul>
 * <p>
 * The mixed-version behaviour (a replica without this feature fails to deserialize the new kinds with "Unknown index
 * expression kind") can't be tested here: it needs a second code version in the cluster.
 */
public class UdtFieldIndexDistributedTest extends TestBaseImpl
{
    private static Cluster CLUSTER;

    /** The indexed table of the current test. */
    private String table;

    /** The twin table, without indexes, of the current test. */
    private String plain;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(3)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false).with(GOSSIP).with(NETWORK))
                              .start());
        CLUSTER.schemaChange(withKeyspace("CREATE TYPE %s.address (street text, city text, zip int)"));
        CLUSTER.schemaChange(withKeyspace("CREATE TYPE %s.person (name text, age int, home frozen<address>)"));
    }

    /**
     * Creates the indexed table with the given name, with indexes on fields, and its twin table without indexes.
     */
    private void createTables(String name)
    {
        table = name;
        plain = name + "_plain";
        for (String t : new String[]{ table, plain })
            CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + t + " (id int PRIMARY KEY, addr frozen<address>, " +
                                              "p person, addrs list<frozen<address>>, t tuple<text, int>) " +
                                              "WITH read_repair = 'NONE'"));
        for (String target : new String[]{ "addr.city", "addr.zip", "p.age", "p.home.city", "addrs.city", "addrs.zip", "t.field2" })
            CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + '(' + target + ") USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);
    }

    @AfterClass
    public static void closeCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    /**
     * Writes on all the replicas of both tables ({@code %t} is the table).
     */
    private void write(String query)
    {
        for (String t : new String[]{ table, plain })
            CLUSTER.coordinator(1).execute(withKeyspace(query.replace("%t", "%s." + t)), ALL);
    }

    /**
     * Writes on one replica only, on both tables ({@code %t} is the table).
     */
    private void writeOnNode(int node, String query)
    {
        for (String t : new String[]{ table, plain })
            CLUSTER.get(node).executeInternal(withKeyspace(query.replace("%t", "%s." + t)));
    }

    /**
     * Checks that a query returns the rows with the given ids, from every coordinator, on the indexed table and, with
     * ALLOW FILTERING, on the table without indexes.
     */
    private void assertIds(ConsistencyLevel cl, String where, Integer... ids)
    {
        for (int coordinator = 1; coordinator <= CLUSTER.size(); coordinator++)
        {
            assertThat(ids(CLUSTER.coordinator(coordinator).execute(withKeyspace("SELECT id FROM %s." + table + " WHERE " + where), cl)))
            .as("%s at %s from node %d", where, cl, coordinator)
            .containsExactlyInAnyOrder(ids);
            String filtered = withKeyspace("SELECT id FROM %s." + plain + " WHERE " + where) + (where.contains("ALLOW FILTERING") ? "" : " ALLOW FILTERING");
            assertThat(ids(CLUSTER.coordinator(coordinator).execute(filtered, cl)))
            .as("%s with ALLOW FILTERING at %s from node %d", where, cl, coordinator)
            .containsExactlyInAnyOrder(ids);
        }
    }

    /**
     * Checks a query with paging, from every coordinator, on both tables.
     */
    private void assertPagedIds(ConsistencyLevel cl, int pageSize, String where, Integer... ids)
    {
        for (int coordinator = 1; coordinator <= CLUSTER.size(); coordinator++)
        {
            for (String t : new String[]{ table, plain })
            {
                String query = withKeyspace("SELECT id FROM %s." + t + " WHERE " + where) + (t.equals(plain) ? " ALLOW FILTERING" : "");
                List<Integer> result = new ArrayList<>();
                CLUSTER.coordinator(coordinator).executeWithPaging(query, cl, pageSize).forEachRemaining(row -> result.add((Integer) row[0]));
                assertThat(result).as("%s at %s from node %d with page size %d", query, cl, coordinator, pageSize)
                                  .containsExactlyInAnyOrder(ids);
            }
        }
    }

    private static List<Integer> ids(Object[][] rows)
    {
        List<Integer> ids = new ArrayList<>();
        for (Object[] row : rows)
            ids.add((Integer) row[0]);
        return ids;
    }

    @Test
    public void testFieldRestrictions()
    {
        createTables("field_restrictions");
        write("INSERT INTO %t (id, addr, p, addrs, t) VALUES (1, {street: 'a', city: 'Rome', zip: 200}, " +
              "{name: 'Ann', age: 30, home: {city: 'Rome'}}, [{city: 'Rome', zip: 200}, {city: 'Milan', zip: 100}], ('foo', 1))");
        write("INSERT INTO %t (id, addr, p, addrs, t) VALUES (2, {street: 'b', city: 'Paris', zip: 100}, " +
              "{name: 'Bob', age: 40}, [{city: 'Rome', zip: 100}], ('bar', 2))");
        write("INSERT INTO %t (id) VALUES (3)");

        for (ConsistencyLevel cl : new ConsistencyLevel[]{ QUORUM, ALL })
        {
            // the fields of single UDTs and tuples
            assertIds(cl, "addr.city = 'Rome'", 1);
            assertIds(cl, "addr.zip >= 100 AND addr.zip < 200", 2);
            assertIds(cl, "addr.city IN ('Rome', 'Paris')", 1, 2);
            assertIds(cl, "addr.city IS NOT NULL", 1, 2);
            assertIds(cl, "p.home.city = 'Rome'", 1);
            assertIds(cl, "t.field2 = 2", 2);
            assertIds(cl, "addr.city = 'Rome' OR t.field2 = 2", 1, 2);
            // the fields inside collections: some element, the same element, no element
            assertIds(cl, "addrs.city = 'Rome'", 1, 2);
            assertIds(cl, "addrs.city = 'Rome' AND addrs.zip = 100", 1, 2);
            assertIds(cl, "addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)", 2);
            assertIds(cl, "addrs CONTAINS ELEMENT (city = 'Rome' AND zip != 100)", 1);
            assertIds(cl, "addrs CONTAINS ELEMENT (city = 'Milan' OR zip = 100)", 1, 2);
            assertIds(cl, "addrs NOT CONTAINS ELEMENT (city = 'Milan')", 2, 3);
            assertIds(cl, "addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 200) ALLOW FILTERING", 2, 3);
            assertIds(cl, "addrs.zip != 200", 2, 3);
        }
    }

    /**
     * A replica with a newer value of a field: the coordinator fetches the full rows from the replicas that didn't
     * return them and re-applies the field expressions and the element groups on the reconciled rows.
     */
    @Test
    public void testReplicaFilteringProtection()
    {
        createTables("replica_filtering");
        write("INSERT INTO %t (id, addr, p, addrs) VALUES (11, {city: 'Turin', zip: 1}, {name: 'Ann', age: 60}, [{city: 'Turin', zip: 1}]) USING TIMESTAMP 1");
        write("INSERT INTO %t (id, addr, p, addrs) VALUES (12, {city: 'Genoa', zip: 2}, {name: 'Bob', age: 61}, [{city: 'Genoa', zip: 2}]) USING TIMESTAMP 1");

        // node 3 has newer values: row 11 no longer matches the Turin restrictions, row 12 now matches them
        writeOnNode(3, "UPDATE %t USING TIMESTAMP 2 SET addr = {city: 'Naples', zip: 1}, addrs = [{city: 'Turin', zip: 3}], p.age = 70 WHERE id = 11");
        writeOnNode(3, "UPDATE %t USING TIMESTAMP 2 SET addr = {city: 'Turin', zip: 2}, addrs = [{city: 'Turin', zip: 1}], p.age = 60 WHERE id = 12");

        // at ALL, the reconciled rows; the replicas 1 and 2 alone return the old values
        assertIds(ALL, "addr.city = 'Turin'", 12);
        assertIds(ALL, "addr.city = 'Naples'", 11);
        assertIds(ALL, "addrs CONTAINS ELEMENT (city = 'Turin' AND zip = 1)", 12);
        assertIds(ALL, "addrs CONTAINS ELEMENT (city = 'Turin' AND zip = 3)", 11);
        assertIds(ALL, "addrs.city = 'Turin' AND addrs.zip = 1", 12);
        assertIds(ALL, "addrs.zip = 2", new Integer[0]);
        assertIds(ALL, "p.age = 60", 12);
        assertIds(ALL, "p.age = 70", 11);
        assertIds(ALL, "p.age >= 60 AND p.age < 65", 12);
        assertIds(ALL, "id IN (11, 12) AND addrs NOT CONTAINS ELEMENT (city = 'Turin' AND zip = 1) ALLOW FILTERING", 11);
        // with paging
        assertPagedIds(ALL, 1, "addr.city = 'Turin'", 12);
        assertPagedIds(ALL, 1, "addrs CONTAINS ELEMENT (city = 'Turin' AND zip = 1)", 12);
        assertPagedIds(ALL, 1, "addrs.city = 'Turin'", 11, 12);
        assertPagedIds(ALL, 2, "p.age >= 60", 11, 12);
    }

    /**
     * Newer values on two of the three replicas: every quorum reads one of them, so the results at QUORUM are the
     * reconciled rows whatever the replicas a coordinator picks; a quorum with node 1 (the old values) needs the
     * coordinator to fetch and re-filter the rows that only node 1 returns, or that only the other node returns.
     */
    @Test
    public void testReplicaFilteringProtectionAtQuorum()
    {
        createTables("replica_filtering_quorum");
        write("INSERT INTO %t (id, addr, p, addrs) VALUES (13, {city: 'Turin', zip: 1}, {name: 'Ann', age: 60}, [{city: 'Turin', zip: 1}]) USING TIMESTAMP 1");
        write("INSERT INTO %t (id, addr, p, addrs) VALUES (14, {city: 'Genoa', zip: 2}, {name: 'Bob', age: 61}, [{city: 'Genoa', zip: 2}]) USING TIMESTAMP 1");
        write("INSERT INTO %t (id, addr, p, addrs) VALUES (15, {city: 'Turin', zip: 5}, {name: 'Cid', age: 62}, [{city: 'Turin', zip: 5}]) USING TIMESTAMP 1");
        for (int node : new int[]{ 2, 3 })
        {
            writeOnNode(node, "UPDATE %t USING TIMESTAMP 2 SET addr = {city: 'Naples', zip: 1}, addrs = [{city: 'Turin', zip: 3}], p.age = 70 WHERE id = 13");
            writeOnNode(node, "UPDATE %t USING TIMESTAMP 2 SET addr = {city: 'Turin', zip: 2}, addrs = [{city: 'Turin', zip: 1}], p.age = 60 WHERE id = 14");
        }

        assertIds(QUORUM, "addr.city = 'Turin'", 14, 15);
        assertIds(QUORUM, "addr.city = 'Naples'", 13);
        assertIds(QUORUM, "addrs CONTAINS ELEMENT (city = 'Turin' AND zip = 1)", 14);
        assertIds(QUORUM, "addrs CONTAINS ELEMENT (city = 'Turin' AND zip = 3)", 13);
        assertIds(QUORUM, "addrs.city = 'Turin' AND addrs.zip = 1", 14);
        assertIds(QUORUM, "addrs.zip = 2", new Integer[0]);
        assertIds(QUORUM, "p.age = 60", 14);
        assertIds(QUORUM, "p.age = 70", 13);
        assertIds(QUORUM, "id IN (13, 14, 15) AND addrs NOT CONTAINS ELEMENT (city = 'Turin' AND zip = 1) ALLOW FILTERING", 13, 15);
        for (int pageSize : new int[]{ 1, 2 })
        {
            assertPagedIds(QUORUM, pageSize, "addr.city = 'Turin'", 14, 15);
            assertPagedIds(QUORUM, pageSize, "addrs CONTAINS ELEMENT (city = 'Turin' AND zip = 1)", 14);
            assertPagedIds(QUORUM, pageSize, "p.age >= 60 AND p.age < 65", 14, 15);
        }
    }

    /**
     * A single-field update of a non-frozen UDT, then a query on the field.
     */
    @Test
    public void testNonFrozenFieldUpdate()
    {
        createTables("field_update");
        write("INSERT INTO %t (id, p) VALUES (21, {name: 'Carl', age: 80})");
        assertIds(QUORUM, "p.age = 80", 21);
        for (String t : new String[]{ table, plain })
            CLUSTER.coordinator(2).execute(withKeyspace("UPDATE %s." + t + " SET p.age = 81 WHERE id = 21"), QUORUM);
        for (ConsistencyLevel cl : new ConsistencyLevel[]{ QUORUM, ALL })
        {
            assertIds(cl, "p.age = 81", 21);
            assertIds(cl, "p.age = 80", new Integer[0]);
        }
        CLUSTER.forEach(node -> node.flush(KEYSPACE));
        assertIds(ALL, "p.age = 81", 21);
    }

    /**
     * Paging through the rows of the three nodes, with a field restriction and with a group.
     */
    @Test
    public void testPaging()
    {
        createTables("paging");
        List<Integer> expected = IntStream.range(100, 140).boxed().collect(Collectors.toList());
        for (int id : expected)
            write("INSERT INTO %t (id, addr, addrs) VALUES (" + id + ", {city: 'Bari', zip: " + id + "}, [{city: 'Bari', zip: 1}, {city: 'Lecce', zip: " + id + "}])");
        CLUSTER.get(1).flush(KEYSPACE);

        for (String where : new String[]{ "addr.city = 'Bari'",
                                          "addrs CONTAINS ELEMENT (city = 'Lecce' AND zip >= 100)",
                                          "addr.city = 'Bari' AND addrs.zip >= 100" })
        {
            for (ConsistencyLevel cl : new ConsistencyLevel[]{ QUORUM, ALL })
            {
                for (int pageSize : new int[]{ 1, 7, 100 })
                {
                    for (String t : new String[]{ table, plain })
                    {
                        String query = withKeyspace("SELECT id FROM %s." + t + " WHERE " + where) + (t.equals(plain) ? " ALLOW FILTERING" : "");
                        Iterator<Object[]> rows = CLUSTER.coordinator(2).executeWithPaging(query, cl, pageSize);
                        List<Integer> ids = new ArrayList<>();
                        rows.forEachRemaining(row -> ids.add((Integer) row[0]));
                        assertThat(ids).as("%s at %s with page size %d", query, cl, pageSize).containsExactlyInAnyOrderElementsOf(expected);
                    }
                }
            }
        }
    }
}
