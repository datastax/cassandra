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
