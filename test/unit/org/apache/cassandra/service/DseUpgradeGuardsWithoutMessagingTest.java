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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.net.MessagingService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Without the DSE 6 compatibility mode, the guards of the upgrade read the recorded messaging versions only when the
 * messaging service exists, and never create it: a process whose messaging never started (a tool, an embedded use)
 * stays without one. A class of its own: any other test of the JVM may create the messaging service.
 */
public class DseUpgradeGuardsWithoutMessagingTest
{
    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void theGuardsDoNotCreateTheMessagingServiceWithoutTheMode()
    {
        InternodeCompatibilityMode modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        try
        {
            assertThat(MessagingService.isInstantiated()).isFalse();
            assertThat(DseUpgradeGuards.upgradeInProgress()).isFalse();
            assertThat(DseUpgradeGuards.pre40Peers()).isEmpty();
            assertThat(DseUpgradeGuards.knownDsePeers()).isEmpty();
            DseUpgradeGuards.refuseWhileUpgrading("decommission");
            assertThat(MessagingService.isInstantiated()).isFalse();

            // the messaging service, once created, is reported
            assertThat(MessagingService.instance()).isNotNull();
            assertThat(MessagingService.isInstantiated()).isTrue();
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        }
    }
}
