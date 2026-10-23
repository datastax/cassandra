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
package org.apache.cassandra.db.commitlog;

import org.junit.BeforeClass;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.utils.StorageCompatibilityMode;

/// The commit log tests with no storage compatibility mode, so that mutations are logged at the current messaging
/// version. The default test mode keeps the storage version at VERSION_40, where trie partition updates have no
/// encoding of their own and the trie encoding of the log is never written or replayed.
public class CommitLogStorageModeNoneTest extends CommitLogTest
{
    public CommitLogStorageModeNoneTest(ParameterizedClass commitLogCompression, EncryptionContext encryptionContext, Config.DiskAccessMode diskAccessMode)
    {
        super(commitLogCompression, encryptionContext, diskAccessMode);
    }

    @BeforeClass
    public static void setCommitLogModeDetails()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.batch);
        beforeClass();
    }
}
