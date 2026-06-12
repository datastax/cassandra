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
package org.apache.cassandra.cql3.functions;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.config.CassandraRelevantProperties.DISABLE_USER_DEFINED_FUNCTIONS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the cassandra.disable_user_defined_functions system property.
 */
public class UDFunctionDisableTest extends CQLTester
{
    private String originalValue;

    @Before
    public void setUp()
    {
        // Save original value
        originalValue = DISABLE_USER_DEFINED_FUNCTIONS.getString();
    }

    @After
    public void tearDown()
    {
        // Restore original value
        if (originalValue != null)
            DISABLE_USER_DEFINED_FUNCTIONS.setString(originalValue);
        else
            DISABLE_USER_DEFINED_FUNCTIONS.reset();
    }

    @Test
    public void testUDFDisabledViaSystemProperty()
    {
        // Disable UDFs
        DISABLE_USER_DEFINED_FUNCTIONS.setBoolean(true);

        try
        {
            UDFunction.assertUdfsEnabled("java");
            fail("Expected InvalidRequestException when UDFs are disabled via system property");
        }
        catch (InvalidRequestException e)
        {
            assertTrue("Error message should mention system property",
                       e.getMessage().contains(DISABLE_USER_DEFINED_FUNCTIONS.getKey()));
            assertTrue("Error message should indicate property is set to true",
                       e.getMessage().contains("set to true"));
        }
    }

    @Test
    public void testUDFDisabledPropertyExplicitlyFalse()
    {
        // Explicitly set to false (should not block if other conditions are met)
        DISABLE_USER_DEFINED_FUNCTIONS.setBoolean(false);

        // Note: It may still throw if enable_user_defined_functions is false in config,
        // but we're only testing the system property check here
        try
        {
            UDFunction.assertUdfsEnabled("java");
        }
        catch (InvalidRequestException e)
        {
            assertFalse("Error should not be about system property when set to false", e.getMessage().contains(DISABLE_USER_DEFINED_FUNCTIONS.getKey()));
        }
    }

    @Test
    public void testUDFDisabledPropertyNotSet()
    {
        DISABLE_USER_DEFINED_FUNCTIONS.reset();

        try
        {
            UDFunction.assertUdfsEnabled("java");
        }
        catch (InvalidRequestException e)
        {
            assertFalse("Error should not be about system property when not set", e.getMessage().contains(DISABLE_USER_DEFINED_FUNCTIONS.getKey()));
        }
    }

    @Test
    public void testUDFDisabledPropertyCaseInsensitive()
    {
        String[] trueValues = { "true", "TRUE", "True", "TrUe" };

        for (String value : trueValues)
        {
            DISABLE_USER_DEFINED_FUNCTIONS.setString(value);

            try
            {
                UDFunction.assertUdfsEnabled("java");
                fail("Expected InvalidRequestException for value: " + value);
            }
            catch (InvalidRequestException e)
            {
                assertTrue("Error message should mention system property for value: " + value,
                           e.getMessage().contains(DISABLE_USER_DEFINED_FUNCTIONS.getKey()));
            }
        }
    }

    @Test
    public void testUDFDisabledPropertyInvalidValues()
    {
        String[] falseValues = { "false", "FALSE", "yes", "no", "0", "1", "invalid", "" };

        for (String value : falseValues)
        {
            DISABLE_USER_DEFINED_FUNCTIONS.setString(value);

            try
            {
                UDFunction.assertUdfsEnabled("java");
            }
            catch (InvalidRequestException e)
            {
                assertFalse("Error should not be about system property for value: " + value, e.getMessage().contains(DISABLE_USER_DEFINED_FUNCTIONS.getKey()));
            }
        }
    }

    @Test
    public void testSecurityManagerNotInstalledWhenUDFsDisabled()
    {
        // Save current SecurityManager state
        SecurityManager originalSecurityManager = System.getSecurityManager();

        try
        {
            // Clear any existing SecurityManager
            System.setSecurityManager(null);

            // Enable UDF disable flag
            DISABLE_USER_DEFINED_FUNCTIONS.setBoolean(true);

            // Reset the installed flag for testing
            org.apache.cassandra.security.ThreadAwareSecurityManager.resetInstalledFlagForTests();

            // Attempt to install SecurityManager - should be skipped
            org.apache.cassandra.security.ThreadAwareSecurityManager.install();

            // Verify SecurityManager was NOT installed
            assertNull("SecurityManager should not be installed when UDFs are disabled",
                       System.getSecurityManager());
        }
        finally
        {
            // Restore original SecurityManager
            System.setSecurityManager(originalSecurityManager);
        }
    }
}
