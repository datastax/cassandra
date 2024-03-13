/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;

import javax.annotation.Nullable;

import com.datastax.bdp.db.utils.leaks.detection.LeaksDetector;
import com.datastax.bdp.db.utils.leaks.detection.LeaksTracker;

import org.apache.cassandra.utils.UnsafeByteBufferAccess;

/**
 * A small utility class for buffers that are allocated directly,
 * in this case we need to mark them as allocated by a pool so that we can confidently
 * update the memory used when the buffer is returned.
 */
final class AttachmentMarker
{
    private final Object attachment;
    private final BufferPool bufferPool;
    @Nullable private final LeaksTracker leak;

    private AttachmentMarker(ByteBuffer buffer, BufferPool bufferPool, LeaksDetector<ByteBuffer> leaksDetector)
    {
        this.attachment =  UnsafeByteBufferAccess.getAttachment(buffer);
        this.bufferPool = bufferPool;
        this.leak = leaksDetector.trackForDebug(buffer);
    }

    Object attachment()
    {
        return attachment;
    }

    static void mark(ByteBuffer buffer, BufferPool bufferPool, LeaksDetector<ByteBuffer> leaksDetector)
    {
        UnsafeByteBufferAccess.setAttachment(buffer, new AttachmentMarker(buffer, bufferPool, leaksDetector));
    }

    static AttachmentMarker unmark(ByteBuffer buffer, BufferPool bufferPool)
    {
        Object attachment = UnsafeByteBufferAccess.getAttachment(buffer);
        if (!(attachment instanceof AttachmentMarker))
            return null;

        AttachmentMarker marker = ((AttachmentMarker) attachment);

        // restore the original attachment, which is either the slab or the cleaner for buffers that were allocated directly
        UnsafeByteBufferAccess.setAttachment(buffer, marker.attachment);

        if (marker.bufferPool == bufferPool)
        {
            // if the buffer is being tracked then close the tracking operation
            if (marker.leak != null)
                marker.leak.close(buffer);

            return marker;
        }
        else
        {
            // this is a serious programming error
            assert false : "Buffer was returned to the wrong pool!";
            return null;
        }
    }
}
