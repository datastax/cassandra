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
package org.apache.cassandra.service;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.utils.MBeanWrapper;

/**
 * Sample server-side extension used by the nodetool extension tests: registers an MBean that the commands of
 * {@link org.apache.cassandra.tools.SampleNodeToolExtension} call.
 */
public class SampleNodeToolServerExtension implements NodeToolServerExtension
{
    public static final String MBEAN_NAME = "org.apache.cassandra.test:type=NodeToolExtensionSample";

    /** How many times {@link #setup()} ran in this JVM (or, in in-JVM dtests, in this instance). */
    public static final AtomicInteger setups = new AtomicInteger();

    public interface SampleMBean
    {
        String ping(String message);
    }

    public static class Sample implements SampleMBean
    {
        @Override
        public String ping(String message)
        {
            return "pong " + message;
        }
    }

    @Override
    public void setup()
    {
        setups.incrementAndGet();
        MBeanWrapper.instance.registerMBean(new Sample(), MBEAN_NAME);
    }
}
