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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SyncUtil;

/**
 * The file that says "this node is part of an in-place upgrade from DataStax Enterprise 6 that has not
 * been completed": {@code <metadata_directory>/dse-upgrade-in-progress}.
 * <p>
 * The node writes it on its first start in {@code internode_compatibility_mode: DSE_6}. While it exists, a
 * start with {@code internode_compatibility_mode: NONE} is refused, because DSE peers may still be in the
 * cluster and a node without the mode cannot talk to them. The operator deletes it once every node of the
 * cluster runs this software, before dropping the mode. No release-version heuristics are used to detect
 * DSE peers at startup: the marker and the record of DSE 6 peers ({@link DsePeersRecord}) are the only
 * evidence the check of a start with {@code NONE} uses.
 */
public final class DseUpgradeMarker
{
    public static final String FILE_NAME = "dse-upgrade-in-progress";

    private DseUpgradeMarker()
    {
    }

    public static Path path()
    {
        return DatabaseDescriptor.getMetadataDirectory().toPath().resolve(FILE_NAME);
    }

    /**
     * Whether the marker is present. A marker whose existence cannot be determined (for example the metadata
     * directory is not readable) counts as present, so that a start without the mode is refused rather than let
     * through.
     */
    public static boolean exists()
    {
        return !Files.notExists(path());
    }

    /**
     * Creates the marker if it is absent and syncs it and its directory to disk; returns true if it was created by
     * this call. The creation is atomic ({@code CREATE_NEW}), so of two concurrent callers only one creates it. Only
     * the existence of the file matters: when its content cannot be built or written, the file is kept as it is
     * (empty or partial) and the failure is thrown.
     */
    public static boolean writeIfAbsent() throws IOException
    {
        // built only after CREATE_NEW succeeded: a failure to build it leaves the created file in place and is thrown,
        // like a failure to write it
        Supplier<String> content = () -> "DSE 6 in-place upgrade in progress on this node since " + FBUtilities.now() + " (" +
                                         FBUtilities.getBroadcastAddressAndPort() + "). Delete this file only after every node " +
                                         "of the cluster runs this software, before restarting with internode_compatibility_mode: NONE.\n";
        return writeIfAbsent(content, (channel, bytes) -> {
            while (bytes.hasRemaining())
                channel.write(bytes);
            SyncUtil.force(channel, true);
        });
    }

    /** Writes the content of a newly created marker and forces it to disk. */
    @VisibleForTesting
    interface ContentWriter
    {
        void write(FileChannel channel, ByteBuffer content) throws IOException;
    }

    @VisibleForTesting
    static boolean writeIfAbsent(Supplier<String> content, ContentWriter writer) throws IOException
    {
        Path path = path();
        Files.createDirectories(path.getParent());
        FileChannel channel;
        try
        {
            channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        }
        catch (FileAlreadyExistsException e)
        {
            return false;
        }

        // the file exists from here on: a failure to write its content keeps it, so the evidence of the upgrade is not lost
        try (FileChannel created = channel)
        {
            writer.write(created, ByteBuffer.wrap(content.get().getBytes(StandardCharsets.UTF_8)));
        }
        finally
        {
            SyncUtil.trySyncDir(new File(path.getParent()));
        }
        return true;
    }

    @VisibleForTesting
    public static boolean delete() throws IOException
    {
        return Files.deleteIfExists(path());
    }
}
