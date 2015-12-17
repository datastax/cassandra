package org.apache.cassandra.net;

import java.util.List;
import java.util.regex.Pattern;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MessagingServiceTest
{
    private final MessagingService messagingService = MessagingService.test();

    @Test
    public void testDroppedMessages()
    {
        MessagingService.Verb verb = MessagingService.Verb.READ;

        for (int i = 0; i < 5000; i++)
            messagingService.incrementDroppedMessages(verb, i, i % 2 == 0);

        List<String> logs = messagingService.getDroppedMessagesLogs();
        assertEquals(1, logs.size());
        Pattern p = Pattern.compile("READ messages were dropped in last 5000 ms: 2500 for internal timeout and 2500 for cross node timeout. Median internal dropped latency: 2[0-9][0-9][0-9] ms and Median cross-node dropped latency: 2[0-9][0-9][0-9] ms");
        assertTrue(p.matcher(logs.get(0)).matches());
        assertEquals(5000, (int)messagingService.getDroppedMessages().get(verb.toString()));
        assertEquals(5000, (int) messagingService.getRecentlyDroppedMessages().get(verb.toString()));

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals(0, logs.size());

        for (int i = 0; i < 2500; i++)
            messagingService.incrementDroppedMessages(verb, i, i % 2 == 0);

        logs = messagingService.getDroppedMessagesLogs();
        p = Pattern.compile("READ messages were dropped in last 5000 ms: 1250 for internal timeout and 1250 for cross node timeout. Median internal dropped latency: 1[0-9][0-9][0-9] ms and Median cross-node dropped latency: 1[0-9][0-9][0-9] ms");
        assertTrue(p.matcher(logs.get(0)).matches());
        assertEquals(7500, (int)messagingService.getDroppedMessages().get(verb.toString()));
        assertEquals(2500, (int) messagingService.getRecentlyDroppedMessages().get(verb.toString()));
    }

}
