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

package org.apache.cassandra.hints;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A hint received in the DSE 5.1 storage mode for another host than this node (from a decommissioning node), which
 * this node stores again: when the mode's hints files cannot hold it (a deletion time past 2038), it is refused and
 * counted, and acknowledged all the same, since the sender could not send it better.
 */
public class Dse51HintVerbHandlerTest
{
    private static final String KEYSPACE = "dse51_hint_verb_handler_test";
    private static final String TABLE = "table";

    private StorageCompatibilityMode storageBefore;
    private InternodeCompatibilityMode internodeBefore;
    private final List<Message<?>> sent = new ArrayList<>();

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    @Before
    public void save()
    {
        storageBefore = DatabaseDescriptor.getStorageCompatibilityMode();
        internodeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DseCompatRefusals.reset();
        MessagingService.instance().outboundSink.add((message, to) -> {
            sent.add(message);
            return false;
        });
    }

    @After
    public void restore()
    {
        MessagingService.instance().outboundSink.clear();
        DatabaseDescriptor.setStorageCompatibilityMode(storageBefore);
        DatabaseDescriptor.setInternodeCompatibilityMode(internodeBefore);
        DseCompatRefusals.reset();
    }

    @Test
    public void aHintForAnotherHostThatTheModeRefusesIsAcknowledged() throws IOException
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        Mutation mutation = RowUpdateBuilder.deleteRowAt(metadata, FBUtilities.timestampMicros(),
                                                         CellData.MAX_DELETION_TIME_2038_LEGACY_CAP + 1000L,
                                                         "refused", "name");
        UUID otherHost = UUID.randomUUID();
        InetAddressAndPort sender = InetAddressAndPort.getByName("127.0.0.5");
        Message<HintMessage> received = Message.builder(Verb.HINT_REQ,
                                                        new HintMessage(otherHost,
                                                                        Hint.create(mutation,
                                                                                    FBUtilities.now().toEpochMilli())))
                                               .from(sender)
                                               .build();
        long hints = StorageMetrics.totalHints.getCount();
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.DSE_5_1);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);

        HintVerbHandler.instance.doVerb(received);

        assertThat(DseCompatRefusals.count(Dse51Hints.REFUSED_HINT)).isEqualTo(1);
        assertThat(StorageMetrics.totalHints.getCount()).isEqualTo(hints);
        assertThat(HintsService.instance.getTotalHintsSize(otherHost)).isZero();
        assertThat(sent).singleElement()
                        .satisfies(response -> {
                            assertThat(response.verb()).isEqualTo(Verb.HINT_RSP);
                            assertThat(response.id()).isEqualTo(received.id());
                        });
    }
}
