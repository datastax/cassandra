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


import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;

/// SSTable expiration task.
///
/// This is used when compaction identifies fully-expired SSTables that can be safely deleted. Executing the task
/// simply commits the associated transaction which has the effect of deleting the source SSTables.
public class ExpirationTask extends AbstractCompactionTask
{
    protected ExpirationTask(CompactionRealm realm, ILifecycleTransaction transaction)
    {
        super(realm, transaction);
    }

    @Override
    protected void runMayThrow() throws Exception
    {
        transaction.obsoleteOriginals();
        transaction.prepareToCommit();
        transaction.commit();
        CompactionManager.instance.incrementDeleteOnlyCompactions();
    }
}
