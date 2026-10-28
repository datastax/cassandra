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
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import com.google.common.net.InetAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.SyncUtil;

/**
 * The peers this node knows to speak the pre-4.0 dialect (DSE 6 peers), kept in memory and persisted to
 * {@code <metadata_directory>/dse-peers-seen}, one address with port per line.
 * <p>
 * The startup checks run before gossip and before any connection, so the messaging versions the node knows at
 * that time are only those it persisted itself: this file is what lets a start with
 * {@code internode_compatibility_mode: NONE} be refused while a DSE 6 peer was still in the cluster the last
 * time this node ran (the second source of {@code StartupChecks.checkDsePeersWithoutFlag}, next to the upgrade
 * marker). No release-version heuristics are involved. A peer enters the record when the node's own messaging
 * version map ({@code MessagingService.versions}) puts it at a pre-4.0 version; a raw DSE messaging version (256 or
 * more) is read as the dialect, through {@link DseCompat#messagingVersionFromGossip}. The copies of that map handed to
 * outbound connections do not feed the record. A peer leaves the record only when it is seen at 4.0 or later (it was
 * upgraded) or its version is forgotten (it left the ring). At a {@code DSE_6} start the persisted record is loaded first
 * ({@code StartupChecks.loadDsePeersRecord}), so a DSE peer that stays down during a whole run of this node is not
 * forgotten, and a peer upgraded while this node was down leaves the record when it is seen.
 * <p>
 * The file is rewritten off the calling thread (the version is recorded on event loops and the gossip stage),
 * atomically and synced. Only maintained while the DSE 6 compatibility mode is on; with {@code NONE} nothing
 * is recorded and nothing is written.
 */
public final class DsePeersRecord
{
    private static final Logger logger = LoggerFactory.getLogger(DsePeersRecord.class);

    public static final String FILE_NAME = "dse-peers-seen";
    private static final String TEMPORARY_FILE_NAME = FILE_NAME + ".tmp";
    private static final String HEADER = "# peers known to speak the pre-4.0 dialect (DSE 6); maintained by the node, one address with port per line";

    private static final Set<InetAddressAndPort> PRE40_PEERS = ConcurrentHashMap.newKeySet();
    /** Lines of the persisted file that did not parse as an address: kept as they are, never silently dropped. */
    private static final Set<String> UNPARSED_LINES = ConcurrentHashMap.newKeySet();
    private static final AtomicBoolean LOADED = new AtomicBoolean();
    private static volatile Future<?> lastPersist;

    private DsePeersRecord()
    {
    }

    public static Path path()
    {
        return DatabaseDescriptor.getMetadataDirectory().toPath().resolve(FILE_NAME);
    }

    /**
     * Loads the persisted record into memory, once per process; called at a {@code DSE_6} start before any
     * connection. A no-op with {@code NONE}. A load that fails can be made again.
     */
    public static void loadAtStartup() throws IOException
    {
        if (!DseCompat.enabled() || !LOADED.compareAndSet(false, true))
            return;
        List<String> persisted;
        try
        {
            persisted = persistedPeers();
        }
        catch (IOException | RuntimeException e)
        {
            LOADED.set(false);
            throw e;
        }
        for (String line : persisted)
        {
            InetAddressAndPort peer = parseEntry(line);
            if (peer == null)
            {
                logger.error("Entry '{}' of the record of DSE 6 peers {} is not an address with port (<ip>:<port>); it is kept in the file", line, path());
                UNPARSED_LINES.add(line);
            }
            else
            {
                PRE40_PEERS.add(peer);
            }
        }
        if (!PRE40_PEERS.isEmpty())
            logger.info("DSE 6 peers on record from the previous run: {}", peers());
    }

    /**
     * The address of an entry of the record, an IP address with port as this node writes them, or null when the entry
     * is not one. A host name is not an entry: it is not resolved, so that reading the record never waits on DNS.
     */
    public static InetAddressAndPort parseEntry(String entry)
    {
        try
        {
            HostAndPort hostAndPort = HostAndPort.fromString(entry);
            if (!hostAndPort.hasPort() || !InetAddresses.isInetAddress(hostAndPort.getHost()))
                return null;
            return InetAddressAndPort.getByName(entry);
        }
        catch (UnknownHostException | RuntimeException e)
        {
            return null;
        }
    }

    /**
     * Called whenever a peer's messaging version is recorded, with the raw value; a no-op unless the DSE 6
     * compatibility mode is on. Persists the record when the peer enters or leaves it.
     */
    public static void versionRecorded(InetAddressAndPort peer, int version)
    {
        if (!DseCompat.enabled())
            return;
        boolean pre40 = DseCompat.messagingVersionFromGossip(version) < MessagingService.VERSION_40;
        boolean changed = pre40 ? PRE40_PEERS.add(peer) : PRE40_PEERS.remove(peer);
        if (changed)
            persistLater();
    }

    /** Called when a peer's messaging version is forgotten (it left the ring); a no-op unless the DSE 6 compatibility mode is on. */
    public static void versionForgotten(InetAddressAndPort peer)
    {
        if (!DseCompat.enabled())
            return;
        if (PRE40_PEERS.remove(peer))
            persistLater();
    }

    /** The peers currently on record, sorted. */
    public static Set<InetAddressAndPort> peers()
    {
        return Collections.unmodifiableSet(new TreeSet<>(PRE40_PEERS));
    }

    /**
     * The lines of the persisted record that are not addresses with port, trimmed and sorted: kept in the file, they
     * keep a start with {@code NONE} refused until they are removed by hand.
     */
    public static Set<String> unparsedEntries()
    {
        return Collections.unmodifiableSet(new TreeSet<>(UNPARSED_LINES));
    }

    /**
     * The addresses persisted by the last run of this node, sorted; empty when the file is absent. Read by
     * the startup checks, before any version is known.
     */
    public static List<String> persistedPeers() throws IOException
    {
        Path path = path();
        // Files.exists is false when the check itself fails: only a file known to be absent is an empty record, anything
        // else is read, and a failure to read it is reported (the NONE check then refuses the start: fail closed)
        if (Files.notExists(path))
            return Collections.emptyList();
        List<String> peers = new ArrayList<>();
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8))
        {
            String trimmed = line.trim();
            if (!trimmed.isEmpty() && !trimmed.startsWith("#"))
                peers.add(trimmed);
        }
        Collections.sort(peers);
        return peers;
    }

    /**
     * Writes the record off the calling thread, on the non-periodic tasks executor. {@code StorageService.drain} (a
     * {@code nodetool drain} or the shutdown of the node) stops messaging and gossip, which record the versions, then
     * flushes the tables and only at its end shuts that executor down, dropping the writes still queued and
     * interrupting a running one: a change recorded in the last moments before messaging stopped may then not reach
     * the file, and the peer enters the record again when it is next seen. Once the executor is shut down, or when it
     * rejects the write, the write is made in place, on the calling thread, which may hold the lock of the node's
     * version map ({@code EndpointMessagingVersions}); messaging and gossip have stopped by then, so no other update
     * waits on that lock.
     */
    private static void persistLater()
    {
        lastPersist = persistOn(ScheduledExecutors.nonPeriodicTasks);
    }

    /** Writes the record on {@code executor}, or in place when it is shut down or rejects the write; returns the pending write, if any. */
    @VisibleForTesting
    static Future<?> persistOn(ExecutorService executor)
    {
        if (!executor.isShutdown())
        {
            try
            {
                return executor.submit(DsePeersRecord::persist);
            }
            catch (RejectedExecutionException e)
            {
                // shut down meanwhile: write in place below
            }
        }
        persist();
        return null;
    }

    /**
     * Writes the current record, replacing the file atomically and syncing it, or deletes the file when the record
     * is empty. The record is snapshotted at write time, so the last write always reflects the latest state. A failure,
     * including the unchecked ones of the file utilities, is logged at ERROR and does not reach the executor's failure
     * handling (the disk failure policy is not meant for this metadata file): a start with {@code NONE} would then not
     * know about the peers.
     */
    private static synchronized void persist()
    {
        Path path = path();
        try
        {
            Set<String> lines = new TreeSet<>(UNPARSED_LINES);
            for (InetAddressAndPort peer : PRE40_PEERS)
                lines.add(peer.getHostAddressAndPort());
            if (lines.isEmpty())
            {
                Files.deleteIfExists(path);
                Files.deleteIfExists(path.resolveSibling(TEMPORARY_FILE_NAME));
                SyncUtil.trySyncDir(new File(path.getParent()));
                return;
            }
            Files.createDirectories(path.getParent());
            StringBuilder content = new StringBuilder(HEADER).append('\n');
            for (String line : lines)
                content.append(line).append('\n');
            File temp = new File(path.resolveSibling(TEMPORARY_FILE_NAME));
            try (FileOutputStreamPlus out = temp.newOutputStream(File.WriteMode.OVERWRITE))
            {
                out.write(content.toString().getBytes(StandardCharsets.UTF_8));
                out.sync();
            }
            temp.move(new File(path)); // atomic where the file system allows it (PathUtils.atomicMoveWithFallback)
            SyncUtil.trySyncDir(new File(path.getParent()));
        }
        catch (IOException | RuntimeException | FSError e)
        {
            logger.error("Unable to persist the record of DSE 6 peers to {}; a later start with internode_compatibility_mode: NONE " +
                         "will not know about them", path, e);
        }
    }

    /** Waits for the pending write, if any. */
    @VisibleForTesting
    public static void awaitPersisted() throws ExecutionException, InterruptedException
    {
        Future<?> pending = lastPersist;
        if (pending != null)
            pending.get();
    }

    /** Forgets every peer, allows a new load and deletes the file. */
    @VisibleForTesting
    public static void clear() throws IOException
    {
        try
        {
            awaitPersisted();
        }
        catch (ExecutionException | InterruptedException e)
        {
            throw new IOException(e);
        }
        synchronized (DsePeersRecord.class)
        {
            PRE40_PEERS.clear();
            UNPARSED_LINES.clear();
            LOADED.set(false);
            Files.deleteIfExists(path());
            Files.deleteIfExists(path().resolveSibling(TEMPORARY_FILE_NAME));
        }
    }
}
