package org.apache.cassandra.db.commitlog;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitLogSegmentHandler
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogSegmentHandler.class);
    public void handleReplayedSegment(final File file)
    {
        // no-op by default
    };

    public void handleReplayedSegment(final File file, boolean hasInvalidAndNoFailedMutations, boolean hasFailedMutations)
    {
        if (!hasFailedMutations)
        {
            // (don't decrease managed size, since this was never a "live" segment)
            logger.trace("(Unopened) segment {} is no longer needed and will be deleted now", file);
            FileUtils.deleteWithConfirm(file);
        }
        else
        {
            logger.debug("File {} should not be deleted as it contains invalid or failed mutations", file.name());
        }
    }
}
