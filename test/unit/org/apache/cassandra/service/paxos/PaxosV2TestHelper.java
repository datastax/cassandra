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

package org.apache.cassandra.service.paxos;

import java.util.Collections;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.service.paxos.Ballot.Flag.NONE;
import static org.apache.cassandra.service.paxos.BallotGenerator.Global.nextBallot;

/**
 * Test helper that constructs and dispatches CAS v2 messages from within the
 * {@code org.apache.cassandra.service.paxos} package, where the package-private constructors of
 * {@link PaxosPrepare.Request} and {@link PaxosPropose.Request} are accessible.
 * <p>
 * Each helper goes through the real production {@code doVerb} path so that all sensor-wiring
 * logic in the handler is exercised. The local node is registered in {@link org.apache.cassandra.locator.TokenMetadata}
 * with a Murmur3 token so that {@link Paxos#isInRangeAndShouldProcess} passes without requiring a full
 * server bootstrap via {@code StorageService.instance.initServer()}.
 */
public class PaxosV2TestHelper
{
    static
    {
        // Register the local broadcast address with a token so that isInRangeAndShouldProcess returns true
        // for any key in a single-node ring (RF=1). getRandomTokens uses the TokenMetadata's own partitioner,
        // which ensures token type compatibility regardless of which partitioner is active at class load time.
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.updateNormalTokens(BootStrapper.getRandomTokens(metadata, 1),
                                    FBUtilities.getBroadcastAddressAndPort());
    }

    private PaxosV2TestHelper() {}

    /**
     * Builds a v2 Prepare request and dispatches it via {@link PaxosPrepare#requestHandler}.
     */
    public static void dispatchV2Prepare(ColumnFamilyStore cfs)
    {
        PartitionUpdate update = new RowUpdateBuilder(cfs.metadata(), 0, "0")
                                 .add("val", "0")
                                 .buildUpdate();
        SinglePartitionReadCommand read = SinglePartitionReadCommand.fullPartitionRead(
                cfs.metadata(), FBUtilities.nowInSeconds(), update.partitionKey());
        PaxosPrepare.Request request = new PaxosPrepare.Request(nextBallot(NONE),
                                                                localElectorate(),
                                                                read,
                                                                true);
        PaxosPrepare.requestHandler.doVerb(Message.builder(Verb.PAXOS2_PREPARE_REQ, request).build());
    }

    /**
     * Builds a v2 Propose request and dispatches it via {@link PaxosPropose#requestHandler}.
     */
    public static void dispatchV2Propose(ColumnFamilyStore cfs)
    {
        PartitionUpdate update = new RowUpdateBuilder(cfs.metadata(), 0, "0")
                                 .add("val", "0")
                                 .buildUpdate();
        Commit.Proposal proposal = Commit.Proposal.of(nextBallot(NONE), update);
        PaxosPropose.Request request = new PaxosPropose.Request(proposal);
        PaxosPropose.requestHandler.doVerb(Message.builder(Verb.PAXOS2_PROPOSE_REQ, request).build());
    }

    /**
     * Builds a v2 Commit ({@link Commit.Agreed}) and dispatches it via {@link PaxosCommit#requestHandler}.
     */
    public static void dispatchV2Commit(ColumnFamilyStore cfs)
    {
        PartitionUpdate update = new RowUpdateBuilder(cfs.metadata(), 0, "0")
                                 .add("val", "0")
                                 .buildUpdate();
        Commit.Agreed agreed = new Commit.Agreed(nextBallot(NONE), update);
        PaxosCommit.requestHandler.doVerb(Message.builder(Verb.PAXOS_COMMIT_REQ, agreed).build());
    }

    /** Builds a trivial Electorate containing only localhost (RF=1 single-node tests). */
    public static Paxos.Electorate localElectorate()
    {
        return new Paxos.Electorate(
                Collections.singletonList(FBUtilities.getBroadcastAddressAndPort()),
                Collections.emptyList());
    }
}
