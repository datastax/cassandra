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

package org.apache.cassandra.index.sai.disk.vector;

import io.github.jbellis.jvector.graph.NodeQueue;
import org.apache.cassandra.index.sai.utils.RowIdWithScore;
import org.apache.cassandra.utils.AbstractIterator;

/**
 * An iterator over {@link RowIdWithScore} that lazily consumes a {@link NodeQueue}.
 */
public class NodeQueueRowIdIterator extends AbstractIterator<RowIdWithScore>
{
    private final NodeQueue scoreQueue;

    public NodeQueueRowIdIterator(NodeQueue scoreQueue)
    {
        this.scoreQueue = scoreQueue;
    }

    @Override
    protected RowIdWithScore computeNext()
    {
        if (scoreQueue.size() == 0)
            return endOfData();
        float score = scoreQueue.topScore();
        int rowId = scoreQueue.pop();
        return new RowIdWithScore(rowId, score);
    }
}
