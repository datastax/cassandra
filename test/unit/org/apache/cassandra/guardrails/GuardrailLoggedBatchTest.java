/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.guardrails;


import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.config.DatabaseDescriptor;

public class GuardrailLoggedBatchTest extends GuardrailTester
{
    private static boolean loggedBatchEnabled;

    @BeforeClass
    public static void setup()
    {
        loggedBatchEnabled = DatabaseDescriptor.getGuardrailsConfig().logged_batch_enabled;
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.getGuardrailsConfig().logged_batch_enabled = loggedBatchEnabled;
    }

    @Before
    public void setupTest()
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");
    }

    private void setGuardrails(boolean logged_batch_enabled)
    {
        DatabaseDescriptor.getGuardrailsConfig().logged_batch_enabled = logged_batch_enabled;
    }

    private void insertBatch(boolean loggedBatchEnabled, boolean logged) throws Throwable
    {
        setGuardrails(loggedBatchEnabled);

        BatchStatement batch = new BatchStatement(logged ? BatchStatement.Type.LOGGED : BatchStatement.Type.UNLOGGED);
        batch.add(new SimpleStatement(String.format("INSERT INTO %s.%s (k, c, v) VALUES (1, 2, 'val')", keyspace(), currentTable())));
        batch.add(new SimpleStatement(String.format("INSERT INTO %s.%s (k, c, v) VALUES (3, 4, 'val')", keyspace(), currentTable())));

        assertValid(batch);
    }

    @Test
    public void testInsertUnloggedBatch() throws Throwable
    {
        insertBatch(false, false);
        insertBatch(true, false);
    }

    @Test(expected = InvalidQueryException.class)
    public void testDisabledLoggedBatch() throws Throwable
    {
        insertBatch(false, true);
    }

    @Test
    public void testEnabledLoggedBatch() throws Throwable
    {
        insertBatch(true, true);
    }
}
