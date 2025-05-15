package org.apache.cassandra.distributed.test.sai.features;

import java.io.IOException;

import org.junit.BeforeClass;

import org.apache.cassandra.index.sai.disk.format.Version;

/**
 * {@link FeaturesVersionSupportTester} for {@link Version#ED}.
 */
public class FeaturesVersionSupportEDTest extends FeaturesVersionSupportTester
{
    @BeforeClass
    public static void setup() throws IOException
    {
        initCluster(Version.ED);
    }
}
