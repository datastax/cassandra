/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.commitlog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.config.CassandraRelevantProperties.COMMITLOG_FACTORY;

public abstract class ICommitLogFactory
{
    private static final Logger loggr = LoggerFactory.getLogger(ICommitLogFactory.class);
    private static final ICommitLogFactory defaultFactory = new ICommitLogFactory()
    {
        public ICommitLog create()
        {
            loggr.info("Using default commitlog factory to create CommitLog");
            CommitLog log = new CommitLog(CommitLogArchiver.construct(), DatabaseDescriptor.getCommitLogSegmentMgrProvider());
            MBeanWrapper.instance.registerMBean(log, "org.apache.cassandra.db:type=Commitlog");
            return log;
        }
    };

    static final ICommitLogFactory instance;
    static {
        loggr.info("Initializing commitlog transactions factory with {}={}", COMMITLOG_FACTORY.getKey(), COMMITLOG_FACTORY.isPresent() ? COMMITLOG_FACTORY.getString() : "default");
        instance = !COMMITLOG_FACTORY.isPresent()
          ? defaultFactory
          : FBUtilities.construct(COMMITLOG_FACTORY.getString(), "commitlog transactions factory");
    }

    public abstract ICommitLog create();

}
