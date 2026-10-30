/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.index.sai.disk.vector;

import org.apache.cassandra.utils.MBeanWrapper;

/**
 * JMX-registered delegate for {@link JVectorCompactionControlMBean}. Every method calls the same
 * {@link JVectorVersionUtil} setters/getters the {@code system_views.jvector} virtual table uses, so the
 * two control surfaces stay consistent. Registered via {@link MBeanWrapper} (the in-VM platform server),
 * so it adds no RMI connector; and because the virtual table calls the {@link JVectorVersionUtil} setters
 * directly, the CQL path is not serialized behind the JMX server.
 */
public final class JVectorCompactionControl implements JVectorCompactionControlMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.index.sai:type=JVectorCompactionControl";

    private static final JVectorCompactionControl instance = new JVectorCompactionControl();

    private JVectorCompactionControl()
    {
    }

    /** Register the MBean with the in-VM platform server. Safe to call once at daemon startup. */
    public static void registerMBean()
    {
        MBeanWrapper.instance.registerMBean(instance, MBEAN_NAME, MBeanWrapper.OnException.LOG);
    }

    @Override
    public boolean getGraphCompactionMergeEnabled()
    {
        return JVectorVersionUtil.isGraphCompactionMergeEnabled();
    }

    @Override
    public void setGraphCompactionMergeEnabled(boolean enabled)
    {
        JVectorVersionUtil.setGraphCompactionMergeEnabled(enabled);
    }

    @Override
    public int getMergeBytesPerOrdinal()
    {
        return JVectorVersionUtil.getMergeBytesPerOrdinal();
    }

    @Override
    public void setMergeBytesPerOrdinal(int bytes)
    {
        JVectorVersionUtil.setMergeBytesPerOrdinal(bytes);
    }

    @Override
    public int getCompactionBuildThreads()
    {
        return JVectorVersionUtil.getDesiredCompactionBuildThreads();
    }

    @Override
    public void setCompactionBuildThreads(int threads)
    {
        JVectorVersionUtil.setCompactionBuildThreads(threads);
    }

    @Override
    public int getInsertInflightMb()
    {
        return JVectorVersionUtil.getInsertInflightMb();
    }

    @Override
    public void setInsertInflightMb(int mb)
    {
        JVectorVersionUtil.setInsertInflightMb(mb);
    }

    @Override
    public boolean getAmortizePqEncoding()
    {
        return JVectorVersionUtil.isAmortizePqEncoding();
    }

    @Override
    public void setAmortizePqEncoding(boolean enabled)
    {
        JVectorVersionUtil.setAmortizePqEncoding(enabled);
    }

    @Override
    public boolean getSerializeFlushPq()
    {
        return JVectorVersionUtil.isSerializeFlushPq();
    }

    @Override
    public void setSerializeFlushPq(boolean enabled)
    {
        JVectorVersionUtil.setSerializeFlushPq(enabled);
    }
}
