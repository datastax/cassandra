/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.io.compress;


public class KeyAccessException extends Exception
{
    public KeyAccessException()
    {
    }

    public KeyAccessException(String message)
    {
        super(message);
    }

    public KeyAccessException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public KeyAccessException(Throwable cause)
    {
        super(cause);
    }
}
