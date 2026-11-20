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

package org.apache.cassandra.locator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import com.google.common.base.Preconditions;
import org.mockito.stubbing.Answer;

import static org.apache.cassandra.locator.Ec2MultiRegionSnitch.PRIVATE_IP_QUERY;
import static org.apache.cassandra.locator.Ec2MultiRegionSnitch.PUBLIC_IP_QUERY;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Real EC2 snitches for tests outside this package, on an instance metadata service that answers with the given zone
 * (and, for {@link Ec2MultiRegionSnitch}, the given address) and the given {@code cassandra-rackdc.properties} keys.
 */
public final class Ec2Snitches
{
    private Ec2Snitches()
    {
    }

    /** An {@link Ec2Snitch} in {@code zone}, with the properties given as key, value, key, value... */
    public static Ec2Snitch ec2Snitch(String zone, String... properties) throws IOException
    {
        return new Ec2Snitch(connector(zone, "unused", properties));
    }

    /**
     * An {@link Ec2MultiRegionSnitch} in {@code zone} whose public and private address is {@code address}. Its constructor
     * sets the broadcast address and, when none is set, the broadcast RPC address of {@code DatabaseDescriptor}: the
     * caller restores them.
     */
    public static Ec2MultiRegionSnitch ec2MultiRegionSnitch(String zone, String address, String... properties) throws IOException
    {
        return new Ec2MultiRegionSnitch(connector(zone, address, properties));
    }

    private static Ec2MetadataServiceConnector connector(String zone, String address, String... properties) throws IOException
    {
        Preconditions.checkArgument(properties.length % 2 == 0, "properties come as key, value pairs: %s", Arrays.toString(properties));
        Properties values = new Properties();
        for (int i = 0; i + 1 < properties.length; i += 2)
            values.setProperty(properties[i], properties[i + 1]);
        Ec2MetadataServiceConnector connector = mock(Ec2MetadataServiceConnector.class);
        doReturn(new SnitchProperties(values)).when(connector).getProperties();
        doAnswer((Answer<String>) invocation -> {
            String query = invocation.getArgument(0);
            return PUBLIC_IP_QUERY.equals(query) || PRIVATE_IP_QUERY.equals(query) ? address : zone;
        }).when(connector).apiCall(anyString());
        return connector;
    }
}
