/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.gms;

public class CustomFailureDetector
{
    public static IFailureDetector make(String customImpl)
    {
        try
        {
            return (IFailureDetector) Class.forName(customImpl).newInstance();
        }
        catch (Throwable ex)
        {
            throw new IllegalStateException("Unknown failure detector: " + customImpl);
        }
    }
}
