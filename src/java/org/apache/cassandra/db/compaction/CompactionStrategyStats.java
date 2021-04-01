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

package org.apache.cassandra.db.compaction;

import java.io.Serializable;

/** Implements serializable to allow structured info to be returned via JMX. */
public class CompactionStrategyStats implements Serializable
{
    private static final long serialVersionUID = 3695927592357744816L;

    private final String keyspace;
    private final String table;
    private final String strategy;
    private final CompactionLevelStats[] levels;

    public CompactionStrategyStats(String keyspace, String table, String strategy, CompactionLevelStats[] levels)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.strategy = strategy;
        this.levels = levels;
    }

    @Override
    public String toString()
    {
        StringBuilder ret = new StringBuilder(1024);
        ret.append(keyspace)
           .append('.')
           .append(table)
           .append('/')
           .append(strategy)
           .append('\n');

        for (int i = 0; i < levels.length; i++)
        {
            ret.append("Level ").append(i).append(":\n");
            levels[i].toString(ret);
            ret.append('\n');
        }

        return ret.toString();
    }
}
