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

package org.apache.cassandra.index.sai.plan;

import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.ANNOptions;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Redaction;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.cql.VectorTester;
import org.assertj.core.api.Assertions;

public class OrdererTest extends VectorTester
{
    @Test
    public void toStringRedactionTest()
    {
        // ANN
        VectorType<Float> vectorType = VectorType.getInstance(FloatType.instance, 2);
        IndexContext context = SAITester.createIndexContext("col", vectorType);
        Orderer orderer = new Orderer(context, Operator.ANN, vectorType.decompose(vector(1, 2)), ANNOptions.NONE);
        assertToString(orderer, "col ANN OF ? DESC", "col ANN OF [1.0, 2.0] DESC");

        // BM25
        IndexContext textContext = SAITester.createIndexContext("col", UTF8Type.instance);
        orderer = new Orderer(textContext, Operator.BM25, UTF8Type.instance.decompose("sensitive"), ANNOptions.NONE);
        assertToString(orderer, "col BM25 OF ? DESC", "col BM25 OF 'sensitive' DESC");

        // Generic ORDER BY
        orderer = new Orderer(textContext, Operator.ORDER_BY_ASC, null, ANNOptions.NONE);
        assertToString(orderer, "col ASC", "col ASC");
        orderer = new Orderer(textContext, Operator.ORDER_BY_DESC, null, ANNOptions.NONE);
        assertToString(orderer, "col DESC", "col DESC");
    }

    private static void assertToString(Orderer orderer, String expectedRedacted, String expectedUnredacted)
    {
        Assertions.assertThat(orderer.toString(Redaction.REDACT))
                  .isEqualTo(expectedRedacted);
        Assertions.assertThat(orderer.toString(Redaction.NONE))
                  .isEqualTo(expectedUnredacted);
    }
}
