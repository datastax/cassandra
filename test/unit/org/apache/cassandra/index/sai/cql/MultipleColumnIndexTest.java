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
package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class MultipleColumnIndexTest extends SAITester
{
    // Note: Full testing of multiple map index types is done in the
    // types/collections/maps/MultiMap*Test tests
    // This is just testing that the indexes can be created
    @Test
    public void canCreateMultipleMapIndexesOnSameColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, value map<int,int>, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(KEYS(value)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(VALUES(value)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(value)) USING 'StorageAttachedIndex'");
    }

    @Test
    public void canHaveAnalyzedAndUnanalyzedIndexesOnSameColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, value text, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : true }");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : false, 'equals_behaviour_when_analyzed': 'unsupported' }");

        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 1, "a");
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 2, "A");
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE value = 'a'"),
                       row(1));
            assertRows(execute("SELECT pk FROM %s WHERE value : 'a'"),
                       row(1),
                       row(2));
        });
    }

    @Test
    public void cannotHaveMultipleAnalyzingIndexesOnSameColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, value text, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : false }");
        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex' WITH OPTIONS = { 'normalize' : true }"))
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void indexNamedAsColumnWillCoExistWithGeneratedIndexNames() throws Throwable
    {
        createTable("CREATE TABLE %s(id int PRIMARY KEY, text_map map<text, text>)");
        execute("INSERT INTO %s(id, text_map) values (1, {'k1':'v1', 'k2':'v2'})");
        execute("INSERT INTO %s(id, text_map) values (2, {'k1':'v1', 'k3':'v3'})");
        execute("INSERT INTO %s(id, text_map) values (3, {'k4':'v4', 'k5':'v5'})");

        flush();

        createIndex("CREATE CUSTOM INDEX text_map ON %s(keys(text_map)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(values(text_map)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(text_map)) USING 'StorageAttachedIndex'");

        beforeAndAfterFlush(() -> {
            assertEquals(1, execute("SELECT * FROM %s WHERE text_map['k1'] = 'v1' AND text_map['k2'] = 'v2'").size());
            assertEquals(2, execute("SELECT * FROM %s WHERE text_map CONTAINS 'v1'").size());
            assertEquals(2, execute("SELECT * FROM %s WHERE text_map CONTAINS 'v1' AND text_map NOT CONTAINS 'v5'").size());
            assertEquals(2, execute("SELECT * FROM %s WHERE text_map CONTAINS 'v1' AND text_map NOT CONTAINS KEY 'k5'").size());
            assertEquals(1, execute("SELECT * FROM %s WHERE text_map CONTAINS 'v1' AND text_map['k2'] != 'v2'").size());


            assertEquals(2, execute("SELECT * FROM %s WHERE text_map CONTAINS KEY 'k1'").size());
            assertEquals(1, execute("SELECT * FROM %s WHERE text_map CONTAINS KEY 'k1' AND text_map CONTAINS 'v2'").size());
            assertEquals(1, execute("SELECT * FROM %s WHERE text_map CONTAINS KEY 'k1' AND text_map CONTAINS KEY 'k2'").size());
            assertEquals(1, execute("SELECT * FROM %s WHERE text_map CONTAINS KEY 'k1' AND text_map NOT CONTAINS 'v3'").size());
            assertEquals(1, execute("SELECT * FROM %s WHERE text_map CONTAINS KEY 'k1' AND text_map NOT CONTAINS KEY 'k3'").size());
            assertEquals(1, execute("SELECT * FROM %s WHERE text_map CONTAINS KEY 'k1' AND text_map['k2'] != 'v2'").size());

            assertEquals(2, execute("SELECT * FROM %s WHERE text_map['k1'] = 'v1' AND text_map CONTAINS KEY 'k1' AND text_map CONTAINS 'v1'").size());
            assertEquals(2, execute("SELECT * FROM %s WHERE text_map['k1'] = 'v1' AND text_map CONTAINS KEY 'k1' AND text_map CONTAINS 'v1' AND text_map NOT CONTAINS 'v8'").size());
            assertEquals(1, execute("SELECT * FROM %s WHERE text_map['k1'] = 'v1' AND text_map CONTAINS KEY 'k1' AND text_map CONTAINS 'v1' AND text_map NOT CONTAINS 'v3'").size());
            assertEquals(1, execute("SELECT * FROM %s WHERE text_map['k1'] = 'v1' AND text_map CONTAINS KEY 'k1' AND text_map CONTAINS KEY 'k2' AND text_map CONTAINS 'v1'").size());
            assertEquals(1, execute("SELECT * FROM %s WHERE text_map['k1'] = 'v1' AND text_map CONTAINS KEY 'k1' AND text_map CONTAINS KEY 'k2' AND text_map NOT CONTAINS 'v3'").size());
            assertEquals(0, execute("SELECT * FROM %s WHERE text_map['k1'] = 'v1' AND text_map CONTAINS KEY 'k1' AND text_map CONTAINS KEY 'k4'").size());
            assertEquals(1, execute("SELECT * FROM %s WHERE text_map['k1'] = 'v1' AND text_map CONTAINS KEY 'k1' AND text_map NOT CONTAINS KEY 'k2'").size());
            assertEquals(0, execute("SELECT * FROM %s WHERE text_map['k1'] = 'v1' AND text_map CONTAINS KEY 'k1' AND text_map CONTAINS KEY 'k4' AND text_map NOT CONTAINS KEY 'k2'").size());

            assertEquals(1, execute("SELECT * FROM %s WHERE text_map['k1'] != 'v2' AND text_map['k2'] = 'v2'").size());
            assertEquals(2, execute("SELECT * FROM %s WHERE text_map['k1'] != 'v2' AND text_map['k2'] != 'v2'").size());
            assertEquals(2, execute("SELECT * FROM %s WHERE text_map['k1'] != 'v2' AND text_map NOT CONTAINS 'v3'").size());
            assertEquals(1, execute("SELECT * FROM %s WHERE text_map['k1'] != 'v2' AND text_map NOT CONTAINS 'v3' AND text_map NOT CONTAINS 'v5'").size());
            assertEquals(2, execute("SELECT * FROM %s WHERE text_map['k1'] != 'v2' AND text_map NOT CONTAINS KEY 'k3'").size());
            assertEquals(1, execute("SELECT * FROM %s WHERE text_map['k1'] != 'v2' AND text_map NOT CONTAINS KEY 'k3' AND text_map NOT CONTAINS KEY 'k5'").size());
        });
    }
    
    @Test
    public void testContainsWithAnalyzedAndNotAnalyzedOnList()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<text>)");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, list("Johann Strauss") };
        Object[] row2 = new Object[]{ 2, list("Richard Strauss", "Johann Sebastian Bach") };
        execute(insert, row1);
        execute(insert, row2);

        // Test with not analyzed index only
        String notAnalyzedIndex = createIndex("CREATE CUSTOM INDEX not_analyzed ON %s(v) USING 'StorageAttachedIndex'");
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann Strauss'", row1).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss'", row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss'").uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann'").uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard'").uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy'").uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann Strauss'", row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss'", row1).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss'", row1, row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann'", row1, row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard'", row1, row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy'", row1, row2).uses(notAnalyzedIndex).doesntWarn();

        // Test with both analyzed and not analyzed indexes, it should prefer the not analyzed index
        String analyzedIndex = createIndex("CREATE CUSTOM INDEX analyzed ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        String warning = String.format(SingleColumnRestriction.ContainsRestriction.MULTIPLE_INDEXES_WARNING, 'v', notAnalyzedIndex);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann Strauss'", row1).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss'", row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss'").uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann'").uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard'").uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy'").uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann Strauss'", row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss'", row1).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss'", row1, row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann'", row1, row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard'", row1, row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy'", row1, row2).uses(notAnalyzedIndex).warns(warning);

        // Test with analyzed index only
        dropIndex("DROP INDEX %s." + notAnalyzedIndex);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann Strauss'", row1).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss'", row2).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss'", row1, row2).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann'", row1, row2).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard'", row2).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann Strauss'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard'", row1).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy'", row1, row2).uses(analyzedIndex).doesntWarn();
    }

    @Test
    public void testContainsWithAnalyzedAndNotAnalyzedOnSet()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v set<text>)");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, set("Johann Strauss") };
        Object[] row2 = new Object[]{ 2, set("Richard Strauss", "Johann Sebastian Bach") };
        execute(insert, row1);
        execute(insert, row2);

        // Test with not analyzed index only
        String notAnalyzedIndex = createIndex("CREATE CUSTOM INDEX not_analyzed ON %s(v) USING 'StorageAttachedIndex'");
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann Strauss'", row1).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss'", row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss'").uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann'").uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard'").uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy'").uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann Strauss'", row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss'", row1).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss'", row1, row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann'", row1, row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard'", row1, row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy'", row1, row2).uses(notAnalyzedIndex).doesntWarn();

        // Test with both analyzed and not analyzed indexes, it should prefer the not analyzed index
        String analyzedIndex = createIndex("CREATE CUSTOM INDEX analyzed ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        String warning = String.format(SingleColumnRestriction.ContainsRestriction.MULTIPLE_INDEXES_WARNING, 'v', notAnalyzedIndex);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann Strauss'", row1).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss'", row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss'").uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann'").uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard'").uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy'").uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann Strauss'", row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss'", row1).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss'", row1, row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann'", row1, row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard'", row1, row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy'", row1, row2).uses(notAnalyzedIndex).warns(warning);

        // Test with analyzed index only
        dropIndex("DROP INDEX %s." + notAnalyzedIndex);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann Strauss'", row1).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss'", row2).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss'", row1, row2).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann'", row1, row2).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard'", row2).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann Strauss'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard'", row1).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy'", row1, row2).uses(analyzedIndex).doesntWarn();
    }

    @Test
    public void testContainsWithAnalyzedAndNotAnalyzedOnMap()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v map<int, text>)");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, map(0, "Johann Strauss") };
        Object[] row2 = new Object[]{ 2, map(0, "Richard Strauss", 1, "Johann Sebastian Bach") };
        execute(insert, row1);
        execute(insert, row2);

        // Test with not analyzed index only
        String notAnalyzedIndex = createIndex("CREATE CUSTOM INDEX not_analyzed ON %s(v) USING 'StorageAttachedIndex'");
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann Strauss'", row1).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss'", row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss'").uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann'").uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard'").uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy'").uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann Strauss'", row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss'", row1).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss'", row1, row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann'", row1, row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard'", row1, row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy'", row1, row2).uses(notAnalyzedIndex).doesntWarn();

        // Test with both analyzed and not analyzed indexes, it should prefer the not analyzed index
        String analyzedIndex = createIndex("CREATE CUSTOM INDEX analyzed ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        String warning = String.format(SingleColumnRestriction.ContainsRestriction.MULTIPLE_INDEXES_WARNING, 'v', notAnalyzedIndex);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann Strauss'", row1).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss'", row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss'").uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann'").uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard'").uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy'").uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann Strauss'", row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss'", row1).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss'", row1, row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann'", row1, row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard'", row1, row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy'", row1, row2).uses(notAnalyzedIndex).warns(warning);

        // Test with analyzed index only
        dropIndex("DROP INDEX %s." + notAnalyzedIndex);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann Strauss'", row1).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss'", row2).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss'", row1, row2).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann'", row1, row2).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard'", row2).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann Strauss'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard'", row1).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy'", row1, row2).uses(analyzedIndex).doesntWarn();
    }

    @Test
    public void testContainsKeyWithAnalyzedAndNotAnalyzedOnMap()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v map<text, int>)");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, map("Johann Strauss", 0) };
        Object[] row2 = new Object[]{ 2, map("Richard Strauss", 0, "Johann Sebastian Bach", 1) };
        execute(insert, row1);
        execute(insert, row2);

        // Test with not analyzed index only
        String notAnalyzedIndex = createIndex("CREATE CUSTOM INDEX not_analyzed ON %s(keys(v)) USING 'StorageAttachedIndex'");
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Johann Strauss'", row1).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Richard Strauss'", row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Strauss'").uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Johann'").uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Richard'").uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Debussy'").uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Johann Strauss'", row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Richard Strauss'", row1).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Strauss'", row1, row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Johann'", row1, row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Richard'", row1, row2).uses(notAnalyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Debussy'", row1, row2).uses(notAnalyzedIndex).doesntWarn();

        // Test with both analyzed and not analyzed indexes, it should prefer the not analyzed index
        String analyzedIndex = createIndex("CREATE CUSTOM INDEX analyzed ON %s(keys(v)) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        String warning = String.format(SingleColumnRestriction.ContainsRestriction.MULTIPLE_INDEXES_WARNING, 'v', notAnalyzedIndex);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Johann Strauss'", row1).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Richard Strauss'", row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Strauss'").uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Johann'").uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Richard'").uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Debussy'").uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Johann Strauss'", row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Richard Strauss'", row1).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Strauss'", row1, row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Johann'", row1, row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Richard'", row1, row2).uses(notAnalyzedIndex).warns(warning);
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Debussy'", row1, row2).uses(notAnalyzedIndex).warns(warning);

        // Test with analyzed index only
        dropIndex("DROP INDEX %s." + notAnalyzedIndex);
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Johann Strauss'", row1).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Richard Strauss'", row2).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Strauss'", row1, row2).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Johann'", row1, row2).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Richard'", row2).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v CONTAINS KEY 'Debussy'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Johann Strauss'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Richard Strauss'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Strauss'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Johann'").uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Richard'", row1).uses(analyzedIndex).doesntWarn();
        assertThatPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS KEY 'Debussy'", row1, row2).uses(analyzedIndex).doesntWarn();
    }

    @Test
    public void shouldPickIndexWithDisjunctionSupportToServeTheQuery()
    {
        // given schema with both SAI and table-based secondary indexes
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)");
        createIndex("CREATE CUSTOM INDEX ON %s (a) USING 'StorageAttachedIndex'");
        createIndex("CREATE INDEX ON %s (b)");
        // insert some sample data
        for (int i = 0; i < 5; i++)
        {
            execute(String.format("INSERT INTO %%s(k, a, b) values (%d, %d, %d)", i, i, i));
        }
        // when query using OR operator
        UntypedResultSet result = execute("SELECT * FROM %s WHERE a = 1 OR a = 2");
        // then
        // query should not fail and contain row 1 and 2
        assertRowsIgnoringOrder(result, row(1, 1, 1), row(2, 2, 2));
    }
}
