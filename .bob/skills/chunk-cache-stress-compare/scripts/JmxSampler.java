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

import java.lang.management.ManagementFactory;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * Prints one CSV line every 2 s with the chunk-cache BufferPool sizes and the ChunkCache pressure counters.
 * Counters that do not exist on a build (the "before" build has no PoolExhausted etc.) are printed empty.
 *
 * usage: java JmxSampler.java <jmx port> <pid>
 */
public class JmxSampler
{
    static final String M = "org.apache.cassandra.metrics:";
    static final String[][] COLUMNS = {
        { "bp_used", M + "type=BufferPool,scope=chunk-cache,name=UsedSize", "Value" },
        { "bp_overflow", M + "type=BufferPool,scope=chunk-cache,name=OverflowSize", "Value" },
        { "bp_size", M + "type=BufferPool,scope=chunk-cache,name=Size", "Value" },
        { "cc_size", M + "type=Cache,scope=ChunkCache,name=Size", "Value" },
        { "cc_misses", M + "type=Cache,scope=ChunkCache,name=Misses", "Count" },
        { "cc_sync_reclaims", M + "type=Cache,scope=ChunkCache,name=SyncReclaims", "Count" },
        { "cc_reclaim_retry_ok", M + "type=Cache,scope=ChunkCache,name=ReclaimRetrySuccesses", "Count" },
        { "cc_blocked", M + "type=Cache,scope=ChunkCache,name=BlockedOnPoolAllocation", "Count" },
        { "cc_wait_ok", M + "type=Cache,scope=ChunkCache,name=PoolWaitSuccesses", "Count" },
        { "cc_pool_exhausted", M + "type=Cache,scope=ChunkCache,name=PoolExhausted", "Count" },
        { "direct_used", "java.nio:type=BufferPool,name=direct", "MemoryUsed" },
        { "threads", "java.lang:type=Threading", "ThreadCount" },
    };

    public static void main(String[] args) throws Exception
    {
        String port = args[0];
        String pid = args[1];
        StringBuilder header = new StringBuilder("time,rss_kb");
        for (String[] c : COLUMNS)
            header.append(',').append(c[0]);
        System.out.println(header);
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("HH:mm:ss");
        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://127.0.0.1:" + port + "/jmxrmi");
        try (JMXConnector connector = JMXConnectorFactory.connect(url))
        {
            MBeanServerConnection mbs = connector.getMBeanServerConnection();
            while (true)
            {
                StringBuilder line = new StringBuilder(LocalTime.now().format(fmt)).append(',').append(rss(pid));
                for (String[] c : COLUMNS)
                {
                    line.append(',');
                    try
                    {
                        line.append(mbs.getAttribute(new ObjectName(c[1]), c[2]));
                    }
                    catch (Exception e)
                    {
                        // metric not present on this build
                    }
                }
                System.out.println(line);
                System.out.flush();
                Thread.sleep(2000);
            }
        }
    }

    static String rss(String pid)
    {
        try
        {
            for (String l : java.nio.file.Files.readAllLines(java.nio.file.Path.of("/proc/" + pid + "/status")))
                if (l.startsWith("VmRSS:"))
                    return l.replaceAll("[^0-9]", "");
        }
        catch (Exception e)
        {
            // process gone
        }
        return "";
    }
}
