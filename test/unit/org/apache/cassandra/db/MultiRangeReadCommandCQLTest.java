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
package org.apache.cassandra.db;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.Pair;
import org.assertj.core.api.Assertions;

public class MultiRangeReadCommandCQLTest extends ReadCommandCQLTester<MultiRangeReadCommand>
{
    private static final IPartitioner partitioner = DatabaseDescriptor.getPartitioner();

    @Test
    public void testToCQLString()
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");

        // prepare a token to query and the midpoints of the left and right ranges
        Token token = partitioner.getToken(Int32Type.instance.decompose(0));
        AbstractBounds<PartitionPosition> boundsRight = new Range<>(token.minKeyBound(), partitioner.getMaximumToken().minKeyBound());
        Token right = partitioner.midpoint(boundsRight.left.getToken(), boundsRight.right.getToken());
        AbstractBounds<PartitionPosition> boundsLeft = new Range<>(partitioner.getMinimumToken().minKeyBound(), token.maxKeyBound());
        Token left = partitioner.midpoint(boundsLeft.left.getToken(), boundsLeft.right.getToken());

        // error message for multi-range queries, which are sent to the replicas after splitting a command but are not
        // directly supported by CQL on the coordinators
        String multiRangeError = "Restriction on partition key column k must not be nested under OR operator";

        // test with a secondary index
        createIndex("CREATE INDEX ON %s(v)");
        assertToCQLString("SELECT * FROM %s WHERE v = 0",
                          "SELECT * FROM %s WHERE (token(k) <= -1 OR token(k) > -1) AND v = 0 ALLOW FILTERING",
                          multiRangeError);
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND v = 0",
                          "SELECT * FROM %s WHERE k = 0 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c = 0 AND v = 0 ",
                          "SELECT * FROM %s WHERE k = 0 AND c = 0 AND v = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE token(k) > token(0) AND v = 0",
                          "SELECT * FROM %s WHERE (token(k) > " + token + " AND token(k) <= " + right + " OR token(k) > " + right + ") AND v = 0 ALLOW FILTERING",
                          multiRangeError);
        assertToCQLString("SELECT * FROM %s WHERE token(k) >= token(0) AND v = 0",
                          "SELECT * FROM %s WHERE (token(k) >= " + token + " AND token(k) <= " + right + " OR token(k) > " + right + ") AND v = 0 ALLOW FILTERING",
                          multiRangeError);
        assertToCQLString("SELECT * FROM %s WHERE token(k) < token(0) AND v = 0",
                          "SELECT * FROM %s WHERE (token(k) <= " + left + " OR token(k) > " + left + " AND token(k) < " + token + ") AND v = 0 ALLOW FILTERING",
                          multiRangeError);
        assertToCQLString("SELECT * FROM %s WHERE token(k) <= token(0) AND v = 0",
                          "SELECT * FROM %s WHERE (token(k) <= " + left + " OR token(k) > " + left + " AND token(k) <= " + token + ") AND v = 0 ALLOW FILTERING",
                          multiRangeError);
        assertToCQLString("SELECT * FROM %s WHERE token(k) >= token(0) AND token(k) <= token(0) AND v = 0",
                          "SELECT * FROM %s WHERE token(k) >= " + token + " AND token(k) <= " + token + " AND v = 0 ALLOW FILTERING");

        // test generic index-based ORDER BY
        createTable("CREATE TABLE %s (k int, c int, n int, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        assertToCQLString("SELECT * FROM %s ORDER BY n LIMIT 10",
                          "SELECT * FROM %s WHERE (token(k) <= -1 OR token(k) > -1) ORDER BY n ASC LIMIT 10 ALLOW FILTERING",
                          multiRangeError);
        assertToCQLString("SELECT * FROM %s ORDER BY n DESC LIMIT 10",
                          "SELECT * FROM %s WHERE (token(k) <= -1 OR token(k) > -1) ORDER BY n DESC LIMIT 10 ALLOW FILTERING",
                          multiRangeError);
        assertToCQLString("SELECT * FROM %s WHERE k = 0 ORDER BY n LIMIT 10",
                          "SELECT * FROM %s WHERE k = 0 ORDER BY n ASC LIMIT 10 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE n = 0 ORDER BY n LIMIT 10",
                          "SELECT * FROM %s WHERE (token(k) <= -1 OR token(k) > -1) AND n = 0 ORDER BY n ASC LIMIT 10 ALLOW FILTERING",
                          multiRangeError);

        // test ANN index-based ORDER BY
        createTable("CREATE TABLE %s (k int, c int, n int, v vector<float, 2>, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        String truncationError = "no viable alternative at input '..'";
        assertToCQLString("SELECT * FROM %s ORDER BY v ANN OF [1, 2] LIMIT 10",
                          "SELECT * FROM %s WHERE (token(k) <= -1 OR token(k) > -1) ORDER BY v ANN OF [1.0, ... LIMIT 10 ALLOW FILTERING",
                          truncationError);
        assertToCQLString("SELECT * FROM %s WHERE k = 0 ORDER BY v ANN OF [1, 2] LIMIT 10",
                          "SELECT * FROM %s WHERE k = 0 ORDER BY v ANN OF [1.0, ... LIMIT 10 ALLOW FILTERING",
                          truncationError);
        assertToCQLString("SELECT * FROM %s WHERE n = 0 ORDER BY v ANN OF [1, 2] LIMIT 10",
                          "SELECT * FROM %s WHERE (token(k) <= -1 OR token(k) > -1) AND n = 0 ORDER BY v ANN OF [1.0, ... LIMIT 10 ALLOW FILTERING",
                          truncationError);
    }

    @Override
    protected MultiRangeReadCommand parseCommand(String query)
    {
        ReadCommand command = parseReadCommand(query);
        Assertions.assertThat(command).isInstanceOf(PartitionRangeReadCommand.class);
        PartitionRangeReadCommand rangeCommand = (PartitionRangeReadCommand) command;

        // split the range into two consecutive ranges if possible, so the command is actually multi-range
        AbstractBounds<PartitionPosition> bounds = rangeCommand.dataRange().keyRange();
        Token token = partitioner.midpoint(bounds.left.getToken(), bounds.right.getToken());
        List<AbstractBounds<PartitionPosition>> ranges;
        if (bounds.contains(token.maxKeyBound()))
        {
            Pair<AbstractBounds<PartitionPosition>, AbstractBounds<PartitionPosition>> split = bounds.split(token.maxKeyBound());
            ranges = Arrays.asList(split.left, split.right);
        }
        else
        {
            ranges = List.of(bounds);
        }

        return MultiRangeReadCommand.create(rangeCommand, ranges, true);
    }
}
