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

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the {@code '_'} wildcard and the {@code ESCAPE} clause of {@code LIKE} on SAI indexes, on memtables and
 * sstables, with literal and bind marker operands.
 */
public class LikeEscapeTest extends SAITester
{
    @Test
    public void testUnderscoreAndEscapes() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        String[] values = { "a_c", "abc", "a%c", "a\\c", "ab", "a_", "a__c", "日_本", "日x本", "a😀c", "a!c" };
        for (int pk = 0; pk < values.length; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, values[pk]);

        beforeAndAfterFlush(() -> {
            // '_' matches any single character, multibyte ones included
            assertRowsMatchedByLike("v LIKE 'a_c'", "a_c", "abc", "a%c", "a\\c", "a😀c", "a!c");
            assertRowsMatchedByLike("v LIKE '__'", "ab", "a_");
            assertRowsMatchedByLike("v LIKE '日_本'", "日_本", "日x本");
            assertRowsMatchedByLike("v LIKE 'a_%'", "a_c", "abc", "a%c", "a\\c", "ab", "a_", "a__c", "a😀c", "a!c");
            assertRowsMatchedByLike("v LIKE '%_c'", "a_c", "abc", "a%c", "a\\c", "a__c", "a😀c", "a!c");

            // escaped wildcards are literals: exact, prefix, suffix, contains and generic patterns
            assertRowsMatchedByLike("v LIKE 'a!_c' ESCAPE '!'", "a_c");
            assertRowsMatchedByLike("v LIKE 'a!_%' ESCAPE '!'", "a_c", "a_", "a__c");
            assertRowsMatchedByLike("v LIKE '%!%c' ESCAPE '!'", "a%c");
            assertRowsMatchedByLike("v LIKE '%!%%' ESCAPE '!'", "a%c");
            assertRowsMatchedByLike("v LIKE '%!_!_%' ESCAPE '!'", "a__c");
            assertRowsMatchedByLike("v LIKE '_!_%' ESCAPE '!'", "a_c", "a_", "a__c", "日_本");
            assertRowsMatchedByLike("v LIKE 'a!!c' ESCAPE '!'", "a!c");
            assertRowsMatchedByLike("v LIKE 'a\\\\c' ESCAPE '\\'", "a\\c");
            assertRowsMatchedByLike("v LIKE '日本_本本' ESCAPE '本'", "日_本");

            // without an ESCAPE clause, there is no escape character
            assertRowsMatchedByLike("v LIKE 'a\\c'", "a\\c");
            assertRowsMatchedByLike("v LIKE 'a!_c'");

            // bind markers
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE v LIKE ? ESCAPE ?", "a#_%", "#"),
                                    row("a_c"), row("a_"), row("a__c"));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE v LIKE 'a#_c' ESCAPE ?", "#"), row("a_c"));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE v LIKE :p ESCAPE :e", "a~%c", "~"), row("a%c"));

            // ordered by the column
            assertRows(execute("SELECT v FROM %s WHERE v LIKE 'a_c' ORDER BY v ASC LIMIT 3"),
                       row("a!c"), row("a%c"), row("a\\c"));
            assertRows(execute("SELECT v FROM %s WHERE v LIKE 'a!_%%' ESCAPE '!' ORDER BY v DESC LIMIT 2"),
                       row("a_c"), row("a__c"));
        });
    }

    @Test
    public void testEscapesOnNormalizedIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : false }");
        execute("INSERT INTO %s (pk, v) VALUES (1, 'A_C')");
        execute("INSERT INTO %s (pk, v) VALUES (2, 'ABC')");

        beforeAndAfterFlush(() -> {
            assertRowsMatchedByLike("v LIKE 'a_c'", "A_C", "ABC");
            assertRowsMatchedByLike("v LIKE 'a!_c' ESCAPE '!'", "A_C");
            assertRowsMatchedByLike("v LIKE 'a!_%' ESCAPE '!'", "A_C");
        });
    }

    @Test
    public void testFullWidthWildcardsOnAsciiFoldingIndex() throws Throwable
    {
        // ASCII folding turns the full-width '＿', '％' and '＼' into '_', '%' and '\': in a pattern, they stay
        // literals
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'ascii' : true }");
        String[] values = { "a＿c", "a_c", "abc", "a％c", "xy＼", "xy\\" };
        for (int pk = 0; pk < values.length; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, values[pk]);

        beforeAndAfterFlush(() -> {
            assertRowsMatchedByLike("v LIKE 'a＿c'", "a＿c", "a_c");
            assertRowsMatchedByLike("v LIKE 'a％c'", "a％c");
            assertRowsMatchedByLike("v LIKE 'a_c'", "a＿c", "a_c", "abc", "a％c");
            assertRowsMatchedByLike("v LIKE 'x_＼'", "xy＼", "xy\\");
            assertRowsMatchedByLike("v LIKE '%＼'", "xy＼", "xy\\");
        });
    }

    @Test
    public void testWildcardsAsEscapeCharacter() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        String[] values = { "a%c", "a_c", "abc", "a%" };
        for (int pk = 0; pk < values.length; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, values[pk]);

        beforeAndAfterFlush(() -> {
            // with '%' as escape character, '%%' is a literal '%' and '%' can't be a wildcard any more
            assertRowsMatchedByLike("v LIKE 'a%%_' ESCAPE '%'", "a%c");
            assertRowsMatchedByLike("v LIKE 'a%%' ESCAPE '%'", "a%");
            // with '_' as escape character, '__' is a literal '_'
            assertRowsMatchedByLike("v LIKE 'a__c' ESCAPE '_'", "a_c");
            assertRowsMatchedByLike("v LIKE 'a%' ESCAPE '_'", "a%c", "a_c", "abc", "a%");
        });
    }

    @Test
    public void testColumnNamedEscape() throws Throwable
    {
        // ESCAPE is not a reserved keyword
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, escape text)");
        createIndex("CREATE CUSTOM INDEX ON %s(escape) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, escape) VALUES (1, 'a_c')");
        execute("INSERT INTO %s (pk, escape) VALUES (2, 'abc')");

        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE escape LIKE 'a!_%%' ESCAPE '!'"), row(1));
        assertRowsIgnoringOrder(execute("SELECT escape FROM %s WHERE escape LIKE 'a_c'"), row("a_c"), row("abc"));
    }

    @Test
    public void testInvalidEscapes()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        assertInvalid("SELECT * FROM %s WHERE v LIKE 'a%%' ESCAPE '!!'", "must be a single character");
        assertInvalid("SELECT * FROM %s WHERE v LIKE 'a%%' ESCAPE ''", "must be a single character");
        assertInvalid("SELECT * FROM %s WHERE v LIKE 'a!b%%' ESCAPE '!'", "must be followed by '%', '_' or itself");
        assertInvalid("SELECT * FROM %s WHERE v LIKE 'a!' ESCAPE '!'", "can't end with its escape character");
        assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE v LIKE 'a%%' ESCAPE ?", (Object) null))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Invalid null or unset ESCAPE");
    }

    private void assertRowsMatchedByLike(String restriction, String... expected) throws Throwable
    {
        Object[][] rows = new Object[expected.length][];
        for (int i = 0; i < expected.length; i++)
            rows[i] = row(expected[i]);
        assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE " + restriction.replace("%", "%%")), rows);
    }

    private void assertInvalid(String query, String message)
    {
        assertThatThrownBy(() -> execute(query)).isInstanceOf(InvalidRequestException.class)
                                                .hasMessageContaining(message);
    }
}
