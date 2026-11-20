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

package org.apache.cassandra.nodes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SyncUtil;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * The import of DataStax Enterprise's node metadata files, {@code nodes/local} and {@code nodes/peers} of the metadata
 * directory, into the system tables. {@link Nodes} uses it in place of {@link CC4UpgradeNodesPersistence}, the import
 * of that directory as CC4 wrote it, when {@link #handlesNodesDirectory()} holds: in
 * {@code internode_compatibility_mode: DSE_6}, or, once the flag is dropped, while the directory still holds the marker
 * of an import made in {@code DSE_6}. Without the mode and without a marker the CC4 import runs, unchanged. Both read
 * the files with {@link CC4NodesFileReader}.
 *
 * In {@code internode_compatibility_mode: DSE_6} the directory holds DSE's own files, which a node rolled back to DSE
 * restarts from, so it stays in place and the import is recorded by the marker file {@code nodes/.imported-by-hcd},
 * which holds the size, modification time and CRC32 of {@code nodes/local} and {@code nodes/peers} as they were
 * imported ({@code absent} for a file that did not exist). A later start skips the import only if the two files are
 * unchanged; otherwise DSE ran after the import (a rollback), or the directory was replaced, and DSE's files are
 * authoritative: the import runs again, with a warning, peers absent from DSE's (existing) peers file are removed from
 * the system tables, and per table the later truncation record of the file and of the tables is kept. A
 * {@code nodes/local} that appeared since an import made without one does not match either. A valid marker whose
 * {@code nodes/local} names a host id other than the one the system tables hold (system keyspace lost, or the directory
 * of another node whose files happen to match the marker) also imports again rather than starting with a new identity.
 * When the flag is dropped, a start with a valid marker archives the directory to {@code nodes.migrated} (the marker
 * moves along); with a marker that no longer matches, it imports again as above, then archives.
 *
 * In {@code DSE_6} the system tables are flushed before the marker is written, so that the marker never certifies an
 * import that is not on disk. {@link #loadLocal()} and {@link #loadPeers()} run while {@link Nodes} is being
 * initialised, and a flush needs {@code Nodes.local()} (the flush writer reads the local host id), so a blocking flush
 * there would deadlock the start. This completion is therefore scheduled by {@link #loadPeers()} on a thread of its own
 * and never waited for by the loads that schedule it; its flush proceeds once {@link Nodes} is initialised, since the
 * {@code Nodes.local()} of the flush writer waits for it. The flush runs concurrently with the start up to
 * {@code Nodes.reload()}, the commit log replay included, which is safe: the system keyspace was normally scrubbed by
 * the {@code checkSystemKeyspaceState} startup check, right before the {@code SystemKeyspace.checkHealth()} that
 * initialises {@link Nodes}, and flushes concurrent with writes are ordinary. The next {@link #loadLocal()} or
 * {@link #loadPeers()} waits for a pending completion first and rethrows its failure: the {@code Nodes.reload()} of the
 * start, which runs after {@link Nodes} is initialised, thus sees the marker, or fails the start when the completion
 * scheduled at the initialisation failed. A completion scheduled by that reload (only when the files changed in
 * between) is not waited for: a failure is logged and the next start imports again. A crash before the marker is
 * written leaves no marker matching the files (none, {@link ImportState#NOT_IMPORTED}, or the one of an earlier import,
 * {@link ImportState#STALE}): the next start imports DSE's files again, which is idempotent, the rows written being the
 * same, and safe, DSE's files being authoritative for what the crashed start may have learnt in between. With the flag
 * dropped nothing is flushed and the directory is archived inline, as the CC4 import does.
 */
public class Dse6UpgradeNodesPersistence extends NodesPersistence
{
    private static final Logger logger = LoggerFactory.getLogger(Dse6UpgradeNodesPersistence.class);

    public static final String IMPORT_MARKER_NAME = ".imported-by-hcd";
    private static final String MARKER_TEMP_NAME = ".hcd-import-marker.tmp";
    /** The files of the nodes directory the marker fingerprints. */
    private static final String[] COVERED_FILES = { "local", "peers" };
    /** The backups DSE leaves after an interrupted write of a covered file. */
    private static final String[] RECOVERY_FILES = { "local.old", "peers.old" };

    /**
     * The completion scheduled by the last {@link #loadPeers()} that imported in {@code DSE_6}, until it is waited
     * for successfully; see the class javadoc.
     */
    private static final AtomicReference<Future<?>> pendingCompletion = new AtomicReference<>();

    enum ImportState
    {
        /** no marker (no regular file of its name): import (first start, or a crash before the marker was written) */
        NOT_IMPORTED,
        /** a marker for these very files: the system tables are authoritative */
        DONE,
        /** a marker exists but does not match the files: DSE's files are authoritative again */
        STALE
    }

    /**
     * Set by {@link #loadLocal()} when it found the import stale (the marker does not match the files, or the system
     * tables do not hold the host id of {@code nodes/local}: they lost the imported identity, or the directory is
     * another node's): {@link #loadPeers()} then imports again too, even when the recovery of an interrupted DSE write,
     * done by the local import, made the files match the marker again.
     */
    private boolean localStale;

    /** Set by {@link #loadLocal()} when it wrote the local row of an import: the completion flushes it. */
    private boolean localImported;

    @Override
    public LocalInfo loadLocal()
    {
        // a reload after an import: the marker of that import must be on disk before the state is read
        if (DatabaseDescriptor.isDse6CompatibilityMode())
            awaitPendingCompletion();
        ImportState state = importState(true);
        localStale = false;
        localImported = false;
        if (state == ImportState.DONE)
        {
            LocalInfo fromTables = super.loadLocal();
            UUID fileHostId = CC4NodesFileReader.readLocalHostId();
            if (fileHostId == null || (fromTables != null && fileHostId.equals(fromTables.getHostId())))
            {
                logger.info("DSE node metadata already imported (marker matches nodes/local and nodes/peers), loading from the system tables");
                return fromTables;
            }
            // e.g. the system keyspace was wiped or restored while metadata_directory survived, or nodes/ was replaced
            // by another node's copy whose marker matches its files: starting from the tables would generate a new node
            // identity next to DSE's nodes/local, or keep this node's next to another node's files
            logger.warn("DSE node metadata import marker present but the system tables hold host id {} instead of {} " +
                        "from nodes/local: importing DSE's files again", fromTables == null ? null : fromTables.getHostId(), fileHostId);
            state = ImportState.STALE;
        }
        localStale = state == ImportState.STALE;
        LocalInfo migrated = CC4NodesFileReader.tryReadLocalInfo();
        if (migrated != null)
        {
            logger.info("Migrating local node metadata from DSE6 file-based store (host_id={})", migrated.getHostId());
            if (state == ImportState.STALE)
                keepLaterTruncationRecords(migrated, super.loadLocal());
            saveLocal(migrated);
            // with the flag dropped never flushed (as by the CC4 import); in DSE_6 flushed by the completion
            // scheduled by loadPeers, never here (see the class javadoc)
            localImported = DatabaseDescriptor.isDse6CompatibilityMode();
            return migrated;
        }
        logger.debug("No DSE6 local metadata found to migrate, falling back to system tables");
        return super.loadLocal();
    }

    @Override
    public Stream<PeerInfo> loadPeers()
    {
        if (DatabaseDescriptor.isDse6CompatibilityMode())
            awaitPendingCompletion();
        ImportState state = localStale ? ImportState.STALE : importState(false);
        boolean flushLocal = localImported;
        localStale = false;
        localImported = false;
        if (state == ImportState.DONE)
        {
            // the flag was dropped after an import done under DSE_6: archive now, as a CC4 import would have
            if (!DatabaseDescriptor.isDse6CompatibilityMode())
                CC4NodesFileReader.archiveCC4NodesDirectory();
            return super.loadPeers();
        }

        List<PeerInfo> migrated = CC4NodesFileReader.tryReadPeers().collect(Collectors.toList());
        // DSE always keeps a peers file once it wrote one (an empty map without peers): an absent file was
        // removed by someone else and says nothing about which peers are gone
        boolean written = state == ImportState.STALE
                          && Files.exists(markerDirectory().resolve("peers"))
                          && deletePeersMissingFrom(migrated);
        if (!migrated.isEmpty())
        {
            logger.info("Migrating {} peer entries from DSE6 file-based store", migrated.size());
            for (PeerInfo peer : migrated)
                savePeer(peer);
            written = true;
        }
        if (DatabaseDescriptor.isDse6CompatibilityMode())
            scheduleMarker(flushLocal, written);
        else
            CC4NodesFileReader.archiveCC4NodesDirectory();

        if (migrated.isEmpty())
        {
            if (DatabaseDescriptor.isDse6CompatibilityMode())
                // a single DSE 6.9 node writes an empty peers map: the normal case, not a warning
                logger.info("No DSE peer metadata found to import, falling back to system tables");
            else
                logger.warn("No DSE6 peer metadata found to migrate, falling back to system tables");
            return super.loadPeers();
        }

        return migrated.stream();
    }

    /**
     * Whether {@link Nodes} imports the {@code nodes} directory of the metadata directory with this class rather than
     * with {@link CC4UpgradeNodesPersistence}: in {@code internode_compatibility_mode: DSE_6}, or when the directory
     * holds the import marker, the flag having been dropped after an import made in {@code DSE_6} (see the class
     * javadoc).
     *
     * @throws RuntimeException if the directory cannot be read, or the attributes of the marker cannot be read
     */
    static boolean handlesNodesDirectory()
    {
        if (DatabaseDescriptor.isDse6CompatibilityMode())
            return true;
        Path nodes = markerDirectory();
        return Files.isDirectory(nodes) && hasMarker(nodes);
    }

    /**
     * Schedules the flush of the imported rows followed by the marker; its content is taken now, from the files just
     * imported. The thread of the completion ends with it.
     */
    private void scheduleMarker(boolean flushLocal, boolean flushPeers)
    {
        Path nodes = markerDirectory();
        Path marker = nodes.resolve(IMPORT_MARKER_NAME);
        String content = markerContent(nodes);
        ExecutorPlus executor = executorFactory().sequential("DseNodesImportMarker");
        try
        {
            pendingCompletion.set(executor.submit(() -> {
                try
                {
                    flushImported(flushLocal, flushPeers);
                    writeImportMarker(nodes, marker, content);
                }
                catch (RuntimeException e)
                {
                    // an Error passes through unwrapped on purpose: JVMStabilityInspector must see it as it is
                    throw new RuntimeException("Failed to complete the node metadata import: flushing the system tables, " +
                                               "then writing the import marker " + marker, e);
                }
            }));
        }
        finally
        {
            executor.shutdown();
        }
    }

    /**
     * Flushes the imported tables; unlike {@link #syncLocal()} and {@link #syncPeers()}, a flush that cannot run
     * (flush executor shut down) fails, so that no marker certifies rows that are not on disk (except with
     * {@code cassandra.unsafesystem}, where the system tables are never flushed).
     */
    @VisibleForTesting
    protected void flushImported(boolean local, boolean peers)
    {
        if (local)
            SystemKeyspace.forceBlockingFlush(SystemKeyspace.LOCAL);
        if (peers)
            SystemKeyspace.forceBlockingFlush(SystemKeyspace.LEGACY_PEERS, SystemKeyspace.PEERS_V2);
    }

    /**
     * Waits for the completion of the last import (flush, then marker) and rethrows its failure; after a success
     * nothing is pending any more. Called by {@link #loadLocal()} and {@link #loadPeers()}, where it never blocks
     * while {@link Nodes} is being initialised: {@link Nodes} creates the only instance of this class of a process,
     * so its first load has no completion pending. At the {@code Nodes.reload()} of the start it can wait for the
     * flush, inside the update of the local node metadata that the reload is: other updates of the local node metadata
     * wait too.
     */
    @VisibleForTesting
    static void awaitPendingCompletion()
    {
        Future<?> pending = pendingCompletion.get();
        if (pending == null)
            return;
        if (!pending.isDone())
            logger.info("Waiting for the DSE node metadata import to be flushed and its marker written");
        FBUtilities.waitOnFuture(pending);
        pendingCompletion.compareAndSet(pending, null);
    }

    /**
     * The release version in {@code nodes/local} of the metadata directory: in {@code DSE_6} the file DSE wrote, which
     * stays in place after the import, so it still names the DSE release this node ran once the system tables hold
     * the release of this software. Read without recovering an interrupted DSE write.
     *
     * @return the release version, or null if there is no {@code nodes/local} or it has no release version
     * @throws RuntimeException if the file exists but cannot be read or parsed
     */
    public static CassandraVersion localReleaseVersionInNodesDirectory()
    {
        return CC4NodesFileReader.readLocalReleaseVersion();
    }

    /**
     * The DSE workload fields of {@code nodes/local} in the metadata directory: in {@code DSE_6} the file DSE wrote,
     * which stays in place after the import. Read without recovering an interrupted DSE write.
     *
     * @return the fields, or null if there is no {@code nodes/local}
     * @throws RuntimeException if the file exists but cannot be read or parsed
     */
    public static DseNodeWorkloads localWorkloadsInNodesDirectory()
    {
        return CC4NodesFileReader.readLocalWorkloads();
    }

    /**
     * The DSE workload fields of every entry of {@code nodes/peers} in the metadata directory, as DSE last wrote them:
     * this software never writes that file, except for restoring DSE's own backup after an interrupted DSE write, as
     * DSE does. Read without recovering an interrupted DSE write.
     *
     * @return the fields of every entry, empty if there is no {@code nodes/peers} or it holds no peer
     * @throws RuntimeException if the file exists but cannot be read or parsed
     */
    public static List<DseNodeWorkloads> peerWorkloadsInNodesDirectory()
    {
        return CC4NodesFileReader.readPeerWorkloads();
    }

    /** Forgets a pending completion, failed or not, without waiting for it. */
    @VisibleForTesting
    static void resetPendingCompletion()
    {
        pendingCompletion.set(null);
    }

    /**
     * On a re-import, a table truncated on this software after the previous import has a record DSE's file does
     * not know: keep, per table, the later of the two records, so that a commit log replay never resurrects
     * truncated rows. Records of another host id (a system keyspace restored from another node) are ignored:
     * their commit log positions refer to that node's segments.
     */
    private static void keepLaterTruncationRecords(LocalInfo migrated, LocalInfo fromTables)
    {
        if (fromTables == null || fromTables.getTruncationRecords().isEmpty())
            return;
        if (migrated.getHostId() == null || !migrated.getHostId().equals(fromTables.getHostId()))
            return;
        Map<UUID, TruncationRecord> merged = new HashMap<>(migrated.getTruncationRecords());
        for (Map.Entry<UUID, TruncationRecord> entry : fromTables.getTruncationRecords().entrySet())
        {
            TruncationRecord fromFile = merged.get(entry.getKey());
            if (fromFile == null || fromFile.truncatedAt < entry.getValue().truncatedAt)
                merged.put(entry.getKey(), entry.getValue());
        }
        migrated.setTruncationRecords(merged);
    }

    /**
     * On a re-import, the system tables still hold the peers of the previous import and of what this node learnt
     * afterwards; DSE's file is authoritative, so the peers it does not list are removed.
     *
     * @return true if a peer was removed
     */
    private boolean deletePeersMissingFrom(List<PeerInfo> migrated)
    {
        Set<InetAddressAndPort> kept = migrated.stream().map(PeerInfo::getPeerAddressAndPort).collect(Collectors.toSet());
        List<InetAddressAndPort> removed = super.loadPeers().map(PeerInfo::getPeerAddressAndPort)
                                                .filter(peer -> !kept.contains(peer))
                                                .collect(Collectors.toList());
        for (InetAddressAndPort peer : removed)
        {
            logger.info("Removing peer {} from the system tables: DSE's peers file does not list it", peer);
            deletePeer(peer);
        }
        return !removed.isEmpty();
    }

    private static Path markerDirectory()
    {
        return DatabaseDescriptor.getMetadataDirectory().toPath().resolve("nodes");
    }

    @VisibleForTesting
    static ImportState importState()
    {
        return importState(false);
    }

    private static ImportState importState(boolean logStale)
    {
        Path nodes = markerDirectory();
        if (!Files.isDirectory(nodes) || !hasMarker(nodes))
            return ImportState.NOT_IMPORTED;
        Path marker = nodes.resolve(IMPORT_MARKER_NAME);

        // checked before nodes/local is read: an interrupted write may have left it damaged, and the import recovers
        // it from the backup; a marker never lists a backup (it is written after the import recovered them)
        for (String name : RECOVERY_FILES)
        {
            Path backup = nodes.resolve(name);
            if (Files.exists(backup))
            {
                if (logStale)
                    logger.warn("The DSE node metadata import marker {} does not match the files in {}: DSE was interrupted " +
                                "while writing them ({} present); recovering and importing DSE's files again",
                                marker.getFileName(), nodes, backup.getFileName());
                return ImportState.STALE;
            }
        }

        String recorded = readFingerprint(marker);
        String current = fingerprint(nodes);
        if (!current.equals(recorded))
        {
            if (logStale)
                logger.warn("The DSE node metadata files in {} changed since the import recorded by {} (DSE ran after it, " +
                            "e.g. a rollback; or the directory was replaced): importing DSE's files again, they are " +
                            "authoritative. Recorded: [{}], now: [{}]",
                            nodes, marker.getFileName(), recorded.replace('\n', ' ').trim(), current.replace('\n', ' ').trim());
            return ImportState.STALE;
        }
        return ImportState.DONE;
    }

    /**
     * Whether {@code nodes} holds the import marker. A directory or other non-file entry of that name is no marker: the
     * import is then not done, and the marker write fails on it (writeImportMarker) rather than replacing it. Fails
     * closed: a nodes directory that cannot be read is no state to start from, and a marker whose attributes cannot
     * be read is not taken for an absent one.
     *
     * @throws RuntimeException if the directory cannot be read, or the attributes of the marker cannot be read
     */
    private static boolean hasMarker(Path nodes)
    {
        if (!Files.isReadable(nodes))
            throw new RuntimeException("Failed to read " + nodes + ": whether it holds the import marker cannot be told");
        Path marker = nodes.resolve(IMPORT_MARKER_NAME);
        try
        {
            return Files.readAttributes(marker, BasicFileAttributes.class).isRegularFile();
        }
        catch (NoSuchFileException e)
        {
            return false;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to read the attributes of " + marker, e);
        }
    }

    /**
     * One line per covered file: {@code name=size:modification time in ms:CRC32 of the content in hex}, or
     * {@code name=absent}. The CRC32 also tells apart a rewrite that keeps the size within the resolution of the
     * file-system clock; the files are a few kilobytes. The {@code .old} files DSE leaves after an interrupted write
     * are not covered: {@link #importState} finds the import stale while one exists, and the marker is written after
     * the import recovered them.
     */
    @VisibleForTesting
    static String fingerprint(Path nodes)
    {
        StringBuilder sb = new StringBuilder();
        for (String name : COVERED_FILES)
            sb.append(name).append('=').append(describe(nodes.resolve(name))).append('\n');
        return sb.toString();
    }

    private static String describe(Path file)
    {
        try
        {
            long size = Files.size(file);
            long modified = Files.getLastModifiedTime(file).toMillis();
            CRC32 crc = new CRC32();
            crc.update(Files.readAllBytes(file));
            return size + ":" + modified + ':' + Long.toHexString(crc.getValue());
        }
        catch (NoSuchFileException e)
        {
            return "absent";
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to read the size, modification time and content of " + file, e);
        }
    }

    private static String readFingerprint(Path marker)
    {
        try
        {
            // decoded leniently: a damaged marker does not match, it does not fail the start
            return new String(Files.readAllBytes(marker), StandardCharsets.UTF_8).lines()
                        .filter(line -> !line.startsWith("#") && !line.isEmpty())
                        .map(line -> line + '\n')
                        .collect(Collectors.joining());
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to read the import marker " + marker, e);
        }
    }

    private static String markerContent(Path nodes)
    {
        return "# DSE node metadata imported into the system tables at " + FBUtilities.now() + '\n' +
               "# the import is skipped while nodes/local and nodes/peers keep this size:modification time:CRC32\n" +
               fingerprint(nodes);
    }

    /**
     * Writes the marker durably (temporary file, fsync, atomic rename, best-effort fsync of the directory) after the
     * system tables were flushed, replacing the marker of an earlier import. A failure to write it fails the start (at
     * the reload that waits for the completion, see the class javadoc) instead of warning like a failed archive:
     * without the marker the next start would import DSE's files again over what this node learnt since, whereas a
     * failed archive only repeats an idempotent import.
     */
    private static void writeImportMarker(Path nodes, Path marker, String content)
    {
        Path temp = nodes.resolve(MARKER_TEMP_NAME);
        try
        {
            if (Files.exists(marker) && !Files.isRegularFile(marker))
                throw new IOException(marker + " is not a regular file, so it cannot be the import marker: remove it");
            try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))
            {
                ByteBuffer buffer = StandardCharsets.UTF_8.encode(content);
                while (buffer.hasRemaining())
                    channel.write(buffer);
                SyncUtil.force(channel, true);
            }
            Files.move(temp, marker, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            SyncUtil.trySyncDir(new File(nodes));
            logger.info("Wrote the DSE node metadata import marker {}; the nodes/ files stay in place for a rollback", marker);
        }
        catch (IOException e)
        {
            RuntimeException failure = new RuntimeException("Failed to write the import marker " + marker, e);
            try
            {
                Files.deleteIfExists(temp);
            }
            catch (IOException | RuntimeException cleanup)
            {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }
}
