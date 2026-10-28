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

package org.apache.cassandra.hints;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.exceptions.UnrecoverableIllegalStateException;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.SyncUtil;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * Converts the hints files DataStax Enterprise 6 left in the hints directory of a node upgraded in place into hints
 * files of this software, so that their hints are dispatched like any other: the writes the node's peers missed while
 * it was DSE (and the ones an upgraded peer missed while DSE held them back, since DSE dispatches hints only to peers
 * of its own schema version) reach their targets within minutes of the switch instead of at the repair after the
 * compatibility mode is dropped. Runs only in {@code internode_compatibility_mode: DSE_6}, when hints dispatch starts
 * (after the schema and the saved peers are loaded; on a node switched from DSE, the peers imported from DSE's node
 * metadata files), and at every such start while DSE files remain; with {@code NONE} the files are left alone. The
 * conversion runs on the thread that starts the dispatch, during the start of the node, which it delays by the time it
 * takes to read and write the DSE files.
 * <p>
 * A DSE hints file holds the hints for one host: past DSE's descriptor (the 3.0 layout with version -1, see
 * {@link HintsDescriptor#deserialize}), each record is framed as this software frames its own (size, CRC32 of the size,
 * hint, CRC32 of the size and the hint), and each hint decodes with the layout of the DSE 6.8 messaging version. The
 * hints of a file are dropped and counted when the host is not a peer in the ring, when the hint's table is unknown
 * here, when the hint names a column the table does not have here (a DSE-only column; the record passed both its
 * checksums, so it is whole as DSE wrote it), or when the hint is past its TTL (computed as {@link Hint#isLive()}
 * does); a WARN names the host not in the ring and the unknown columns. The others are written, with their creation
 * time and gc grace seconds, into one new hints file of this software for the same host, named with the timestamp of
 * the DSE file (after a restart the catalog sorts it before newer files; at the conversion it is queued after the files
 * of the host already loaded, which only changes the delivery order: the write timestamps decide the outcome). A
 * dropped hint is a write the repair after the compatibility mode is dropped brings to its replicas. The new file is
 * fsynced (its directory too, best effort), the DSE file deleted (the commit point), then its companions
 * ({@code .crc32}, {@code -Statistics.hints}, best effort; companions whose DSE file is gone are deleted at the next
 * start), and the new file is handed to the host's hints store. An empty DSE file (created and never written) is
 * deleted with its companions.
 * <p>
 * Any failure before the commit point deletes the partial new file and keeps the DSE file and its companions whole: all
 * or nothing per file, a cut last record included. That is a choice: a node is drained before the switch, so a cut file
 * is not expected, and it is kept whole, as DSE left it, rather than converted in part. A file whose content cannot be
 * converted (a checksum mismatch, a cut record, a descriptor that is not DSE's, a compressed or encrypted file, a hint
 * that does not decode for another reason) fails the same way at every start, and its WARN says so; after any other
 * failure (a write or delete error) the file is tried again at the next start. Either way the node logs a WARN and
 * starts, disk errors included. Only an error the stability inspector deems fatal (out of memory, an unrecoverable
 * state, an exhaustion of file handles) stops it.
 * <p>
 * A crash of the node can deliver hints twice; it does not lose them, as long as the directory sync before the commit
 * point, which is best effort, holds: a crash before the commit point is durable (before the deletion of the DSE file
 * reaches the disk, or while the new file is written) leaves the new file, whole or partial, beside the DSE file; at
 * the next start the catalog loads the new file, whose dispatch delivers the hints it holds (a damaged end is handled
 * as in any hints file), and the DSE file is converted again, into a file with the next free timestamp. A hint
 * delivered twice applies the same write twice, with its own timestamp, which changes nothing. Likewise a
 * {@code nodetool truncatehints} run while a file is converted does not reach the new file, which is handed to the
 * store after it: its hints are delivered.
 */
final class DseHintsConverter
{
    private static final Logger logger = LoggerFactory.getLogger(DseHintsConverter.class);

    /** The messaging version whose layout the hints of DSE 6.8 and 6.9 files decode with. */
    static final int DSE_HINTS_MESSAGING_VERSION = MessagingService.VERSION_DSE_68;

    /** The version field of the descriptor of a DSE 6.8 or 6.9 hints file; no Cassandra hints version is negative. */
    static final int DSE_HINTS_DESCRIPTOR_VERSION = -1;

    /** The suffix of a DSE hints file name; its companions replace it with {@code .crc32} and {@code -Statistics.hints}. */
    private static final String HINTS_SUFFIX = ".hints";

    /** The name of a companion file of a DSE hints file; group 1 is the name of the hints file without its suffix. */
    private static final Pattern DSE_COMPANION_FILE_NAME =
        Pattern.compile("^(" + HintsDescriptor.HOST_ID_REGEX + "\\-\\d+\\-v\\d+)(?:\\.crc32|\\-Statistics\\.hints)$");

    /** Lists the DSE hints files of a directory: {@link HintsCatalog#dseHintsFiles}, or a failing stand-in in tests. */
    @VisibleForTesting
    interface DseHintsFileLister
    {
        List<Path> list(File hintsDirectory) throws IOException;
    }

    /** Opens the new hints file of a conversion: {@link HintsWriter#create}, or a failing stand-in in tests. */
    @VisibleForTesting
    interface WriterOpener
    {
        HintsWriter open(File hintsDirectory, HintsDescriptor descriptor) throws IOException;
    }

    /**
     * Deletes a DSE hints file at the commit point of its conversion, or an empty DSE file: {@link Files#delete}, or a
     * failing stand-in in tests.
     */
    @VisibleForTesting
    interface DseFileDeleter
    {
        void delete(Path dseFile) throws IOException;
    }

    /**
     * The content of a DSE hints file cannot be converted (a damaged or cut descriptor or record, a descriptor that is
     * not DSE's, a compressed or encrypted file, a hint that does not decode for another reason than an unknown table
     * or column): the same failure at every try, unlike a write error. Its message is the reason given to the operator.
     */
    private static final class UnconvertibleFileException extends IOException
    {
        UnconvertibleFileException(String message)
        {
            super(message);
        }

        UnconvertibleFileException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }

    /** What the conversion of one DSE hints file, or of all of them, did. */
    static final class Counts
    {
        int converted;
        int unknownHost;
        int unknownTable;
        int unknownColumn;
        int expired;
        /**
         * The DSE file was gone before its conversion started: nothing was read. Set on the result of one file only;
         * the totals of a run ({@link Summary#hints}) count such files in {@link Summary#filesGone} instead.
         */
        boolean gone;

        int dropped()
        {
            return unknownHost + unknownTable + unknownColumn + expired;
        }

        void add(Counts other)
        {
            converted += other.converted;
            unknownHost += other.unknownHost;
            unknownTable += other.unknownTable;
            unknownColumn += other.unknownColumn;
            expired += other.expired;
        }
    }

    /**
     * What a conversion run did: the files found, converted (and deleted), gone before their conversion and kept after
     * a failure, and the hints.
     */
    static final class Summary
    {
        final Counts hints = new Counts();
        int filesFound;
        int filesConverted;
        int filesGone;
        int filesKept;
    }

    private final File hintsDirectory;
    private final ImmutableMap<String, Object> writerParams;
    private final Predicate<UUID> isPeerInRing;
    private final Function<UUID, InetAddressAndPort> endpointForHost;
    private final Consumer<HintsDescriptor> hintsStore;
    private final WriterOpener writerOpener;
    private final DseFileDeleter dseFileDeleter;

    @VisibleForTesting
    DseHintsConverter(File hintsDirectory,
                      ImmutableMap<String, Object> writerParams,
                      Predicate<UUID> isPeerInRing,
                      Function<UUID, InetAddressAndPort> endpointForHost,
                      Consumer<HintsDescriptor> hintsStore)
    {
        this(hintsDirectory, writerParams, isPeerInRing, endpointForHost, hintsStore, HintsWriter::create, Files::delete);
    }

    /**
     * As the other constructor, opening the new file and deleting the DSE file at the commit point with the given
     * functions, failing ones in tests.
     */
    @VisibleForTesting
    DseHintsConverter(File hintsDirectory,
                      ImmutableMap<String, Object> writerParams,
                      Predicate<UUID> isPeerInRing,
                      Function<UUID, InetAddressAndPort> endpointForHost,
                      Consumer<HintsDescriptor> hintsStore,
                      WriterOpener writerOpener,
                      DseFileDeleter dseFileDeleter)
    {
        this.hintsDirectory = hintsDirectory;
        this.writerParams = writerParams;
        this.isPeerInRing = isPeerInRing;
        this.endpointForHost = endpointForHost;
        this.hintsStore = hintsStore;
        this.writerOpener = writerOpener;
        this.dseFileDeleter = dseFileDeleter;
    }

    /**
     * In {@code internode_compatibility_mode: DSE_6}, converts the DSE hints files of the catalog's directory and hands
     * the new files to the catalog's stores; with any other mode does nothing.
     *
     * @return what was done, or {@code null} when the mode is not {@code DSE_6}
     */
    static Summary convertIfDse6(HintsCatalog catalog)
    {
        // checked here too, before the clock is read; the other overload, entered directly by tests, checks it again
        if (!DatabaseDescriptor.isDse6CompatibilityMode())
            return null;
        return convertIfDse6(catalog, currentTimeMillis());
    }

    /** {@link #convertIfDse6(HintsCatalog)} checking the TTL of the hints against the given time, in ms. */
    @VisibleForTesting
    static Summary convertIfDse6(HintsCatalog catalog, long now)
    {
        if (!DatabaseDescriptor.isDse6CompatibilityMode())
            return null;
        DseHintsConverter converter = new DseHintsConverter(catalog.getHintsDirectory(),
                                                            catalog.getWriterParams(),
                                                            DseHintsConverter::isPeerInRing,
                                                            HintsEndpointProvider.instance::endpointForHost,
                                                            descriptor -> catalog.get(descriptor.hostId).offerLast(descriptor));
        return converter.convertAll(HintsCatalog::dseHintsFiles, now);
    }

    /**
     * Whether a host id is the one of a peer in the ring: known to the token metadata, or, before gossip has filled it
     * (with {@code -Dcassandra.load_ring_state=false}), saved in {@code system.peers}. This node's own host id is not a
     * peer: no hint is ever meant for the node that holds it.
     */
    private static boolean isPeerInRing(UUID hostId)
    {
        if (hostId.equals(SystemKeyspace.getLocalHostId()))
            return false;
        return StorageService.instance.getEndpointForHostId(hostId) != null
               || SystemKeyspace.loadHostIds().containsValue(hostId);
    }

    /**
     * Converts every DSE hints file the lister finds, one at a time, logging, when there is any, one line before it
     * starts, one line per file and a summary line; a listing failure is logged and converts nothing. After a
     * successful listing, and before the conversions, the orphan companions are deleted
     * ({@link #deleteOrphanCompanions}, which logs each), even when no DSE file is found.
     *
     * @param now the time, in ms, the TTL of the hints is checked against
     */
    @VisibleForTesting
    Summary convertAll(DseHintsFileLister lister, long now)
    {
        Summary summary = new Summary();
        List<Path> files;
        try
        {
            files = lister.list(hintsDirectory);
        }
        catch (IOException | UncheckedIOException e)
        {
            logger.warn("DSE 6 compatibility mode: unable to look for hints files written by DataStax Enterprise 6 in {}: {}",
                        hintsDirectory, e.toString());
            logger.debug("DSE 6 compatibility mode: failure while listing {}", hintsDirectory, e);
            return summary;
        }
        deleteOrphanCompanions();
        if (files.isEmpty())
            return summary;

        summary.filesFound = files.size();
        long bytes = 0;
        for (Path file : files)
        {
            try
            {
                bytes += Files.size(file);
            }
            catch (IOException | UncheckedIOException e)
            {
                // only for the line below: the conversion of the file reports the failure
            }
        }
        logger.info("DSE 6 compatibility mode: converting {} hints file(s) written by DataStax Enterprise 6 in {} ({} bytes) " +
                    "before the hints dispatch starts", files.size(), hintsDirectory, bytes);
        for (Path file : files)
        {
            Counts counts = convert(file, now);
            if (counts == null)
            {
                summary.filesKept++;
                continue;
            }
            if (counts.gone)
            {
                summary.filesGone++;
                continue;
            }
            summary.filesConverted++;
            summary.hints.add(counts);
        }
        logger.info("DSE 6 compatibility mode: {} hints file(s) written by DataStax Enterprise 6 found in {}, {} converted and deleted, " +
                    "{} gone before their conversion, {} left in place after a failure; {} hint(s) converted, {} dropped " +
                    "({} for a host not in the ring, {} for an unknown table, {} for an unknown column, {} expired)",
                    summary.filesFound, hintsDirectory, summary.filesConverted, summary.filesGone, summary.filesKept, summary.hints.converted,
                    summary.hints.dropped(), summary.hints.unknownHost, summary.hints.unknownTable, summary.hints.unknownColumn,
                    summary.hints.expired);
        return summary;
    }

    /**
     * Converts one DSE hints file; see the class comment. A file gone or empty before it is opened is handled outside
     * the per-file failure handling: an Error there (only its log lines or the deletion of its companions can raise
     * one) goes on to the guard of the dispatch start, and the files not converted yet wait for the next start.
     *
     * @return what was converted and dropped, with {@link Counts#gone} set when the DSE file was gone before its
     *         conversion, or {@code null} when the DSE file was kept after a failure
     */
    @VisibleForTesting
    Counts convert(Path dseFile, long now)
    {
        HintsDescriptor descriptor = null;
        HintsDescriptor converted = null;
        HintsWriter writer = null;
        HintsWriter.Session session = null;
        ByteBuffer buffer = null;
        boolean sessionClosed = false;
        boolean dseFileDeleted = false;
        boolean handing = false;
        boolean dseFileVanished = false;
        Counts counts = new Counts();
        Set<String> unknownColumns = new TreeSet<>();
        // a file gone or empty is dealt with before any input is opened
        long size = -1;
        try
        {
            size = Files.size(dseFile);
        }
        catch (NoSuchFileException e)
        {
            return gone(dseFile, counts);
        }
        catch (IOException | RuntimeException e)
        {
            // the opening below reports the failure
        }
        if (size == 0)
            return deleteEmpty(dseFile, counts);
        try (ChecksummedDataInput input = openUnlessGone(dseFile))
        {
            // gone since its size was read: nothing to close (only an Error of the log line could make the catch below
            // report this file as kept)
            if (input == null)
                return gone(dseFile, counts);
            try
            {
                descriptor = HintsDescriptor.deserialize(input);
            }
            catch (IOException | RuntimeException e)
            {
                // a disk error is an FSError, neither of these
                throw new UnconvertibleFileException("its descriptor does not read", e);
            }
            if (descriptor.version != DSE_HINTS_DESCRIPTOR_VERSION)
                throw new UnconvertibleFileException("its descriptor has version " + descriptor.version + ", not DSE's " + DSE_HINTS_DESCRIPTOR_VERSION);
            if (!descriptor.parameters.isEmpty())
                throw new UnconvertibleFileException("its descriptor has parameters " + descriptor.parameters + " (compression or encryption), which the conversion does not support");

            boolean peer = isPeerInRing.test(descriptor.hostId);
            long fileSize = input.getChannel().size();
            while (!input.isEOF())
            {
                long recordStart = input.getPosition();
                byte[] record = readRecord(input, fileSize, recordStart);
                if (!peer)
                {
                    counts.unknownHost++;
                    continue;
                }

                Hint hint;
                try (DataInputBuffer in = new DataInputBuffer(record))
                {
                    hint = Hint.serializer.deserialize(in, DSE_HINTS_MESSAGING_VERSION);
                    if (in.available() > 0)
                        throw new UnconvertibleFileException("the hint at offset " + recordStart + " has " + in.available() + " byte(s) left after its mutation");
                }
                catch (UnknownTableException e)
                {
                    counts.unknownTable++;
                    continue;
                }
                catch (UnconvertibleFileException e)
                {
                    throw e;
                }
                catch (IOException | RuntimeException e)
                {
                    // the record passed both its checksums: a column unknown here is DSE's, not damage
                    if (e instanceof RuntimeException && isUnknownColumn((RuntimeException) e))
                    {
                        counts.unknownColumn++;
                        unknownColumns.add(unknownColumn(e.getMessage()));
                        continue;
                    }
                    throw new UnconvertibleFileException("the hint at offset " + recordStart + " does not decode", e);
                }

                if (!Hint.isLive(hint.creationTime, now, hint.ttl()))
                {
                    counts.expired++;
                    continue;
                }

                if (writer == null)
                {
                    HintsDescriptor candidate = freeDescriptor(descriptor.hostId, descriptor.timestamp);
                    try
                    {
                        // the file is ours from here, even when the writing of its descriptor fails below
                        converted = candidate;
                        writer = writerOpener.open(hintsDirectory, candidate);
                    }
                    catch (FileAlreadyExistsException e)
                    {
                        // taken since freeDescriptor looked: not ours, not to be deleted
                        converted = null;
                        throw e;
                    }
                    buffer = ByteBuffer.allocateDirect(HintsWriteExecutor.WRITE_BUFFER_SIZE);
                    session = writer.newSession(buffer);
                }
                session.append(hint);
                counts.converted++;
            }

            if (writer != null)
            {
                // on a failure of either, the catch below closes the channel only
                session.close();
                sessionClosed = true;
                writer.close();
                writer = null;
                // the new file is fsynced (its checksum and statistics files are not: the dispatch reads neither); the
                // directory entries too, best effort, before the DSE file goes
                SyncUtil.trySyncDir(hintsDirectory);
            }

            // the commit point: from here the new file is the only copy of the hints
            try
            {
                dseFileDeleter.delete(dseFile);
            }
            catch (NoSuchFileException e)
            {
                // deleted by someone else since it was read: the new file holds its hints all the same
                dseFileVanished = true;
                if (converted == null)
                    logger.warn("DSE 6 compatibility mode: the hints file {} written by DataStax Enterprise 6 disappeared while it was " +
                                "converted; it had no hint to keep", dseFile.getFileName());
                else
                    logger.warn("DSE 6 compatibility mode: the hints file {} written by DataStax Enterprise 6 disappeared while it was " +
                                "converted; its hints are kept in {}", dseFile.getFileName(), converted.fileName());
            }
            dseFileDeleted = true;
            SyncUtil.trySyncDir(hintsDirectory);
            boolean companionsDeleted = deleteCompanions(dseFile);
            // before the hand-over, which may fail: the hints are dropped for good from here
            warnDropped(dseFile, descriptor.hostId, counts, unknownColumns);
            if (converted != null)
            {
                handing = true;
                hintsStore.accept(converted);
                handing = false;
            }
            String endpoint = endpointOf(descriptor.hostId);

            logger.info("DSE 6 compatibility mode: converted the hints file {} written by DataStax Enterprise 6 for host {} ({}): " +
                        "{} hint(s) written to {}, {} dropped ({} for a host not in the ring, {} for an unknown table, " +
                        "{} for an unknown column, {} expired); {}{}",
                        dseFile.getFileName(), descriptor.hostId, endpoint, counts.converted,
                        converted == null ? "no file" : converted.fileName(), counts.dropped(), counts.unknownHost,
                        counts.unknownTable, counts.unknownColumn, counts.expired, dseFileVanished ? "found it gone" : "deleted it",
                        companionsDeleted ? (dseFileVanished ? ", deleted its companion files" : " and its companion files") : "");
            return counts;
        }
        catch (Throwable t)
        {
            if (writer != null)
            {
                // only the channel: the file is deleted below, its checksum and statistics would be written for nothing
                try
                {
                    writer.channel.close();
                }
                catch (Throwable closeFailure)
                {
                    t.addSuppressed(closeFailure);
                }
            }
            if (!dseFileDeleted)
            {
                if (converted != null)
                    deleteConverted(converted, sessionClosed);
                if (t instanceof UnconvertibleFileException)
                    logger.warn("DSE 6 compatibility mode: unable to convert the hints file {} written by DataStax Enterprise 6{}, left in place " +
                                "with its companion files: {}. Its content cannot be converted: the conversion fails the same way at " +
                                "every start of this node, which starts all the same. A repair of the affected tables once " +
                                "internode_compatibility_mode is dropped restores its writes; once it has, move the file and its " +
                                "companion files out of the hints directory.",
                                dseFile.getFileName(), descriptor == null ? "" : " for host " + descriptor.hostId, reason(t));
                else
                    logger.warn("DSE 6 compatibility mode: unable to convert the hints file {} written by DataStax Enterprise 6{}, left in place " +
                                "with its companion files: {}. The conversion is tried again at the next start of this node; until then " +
                                "its hints are delivered only if this node is rolled back to DSE, and otherwise a repair of the affected " +
                                "tables once internode_compatibility_mode is dropped restores them.",
                                dseFile.getFileName(), descriptor == null ? "" : " for host " + descriptor.hostId, reason(t));
            }
            else if (handing)
            {
                // the DSE file is gone and the new file is complete on disk: keep it, the catalog loads it at the next start
                logger.warn("DSE 6 compatibility mode: converted the hints file {} written by DataStax Enterprise 6 for host {} into {} " +
                            "and deleted it, but could not hand the new file to the hints dispatch: {}. It is dispatched after " +
                            "the next start of this node.", dseFile.getFileName(), descriptor.hostId, converted.fileName(), reason(t));
            }
            else
            {
                // a failure past the commit point outside the hand-over: closing the input after the INFO line, or an
                // Error from the directory sync, the companion deletion, the endpoint lookup or the log lines
                logger.warn("DSE 6 compatibility mode: converted the hints file {} written by DataStax Enterprise 6{}{} and " +
                            "deleted it, then failed: {}", dseFile.getFileName(), descriptor == null ? "" : " for host " + descriptor.hostId,
                            converted == null ? " (no hint to write)" : " into " + converted.fileName(), reason(t));
            }
            logger.debug("DSE 6 compatibility mode: failure while converting the hints file {}", dseFile, t);
            // out of memory goes on to the guard of the dispatch start, which inspects it once
            if (find(t, OutOfMemoryError.class) != null)
                throw Throwables.throwAsUncheckedException(t);
            inspectUnlessDiskError(t);
            // past the commit point the file is converted: at worst its dispatch waits for the next start
            return dseFileDeleted ? counts : null;
        }
        finally
        {
            if (buffer != null)
                FileUtils.clean(buffer);
        }
    }

    /**
     * {@link JVMStabilityInspector#inspectThrowable} unless the failure is a disk error, anywhere among its causes and
     * suppressed exceptions: the disk failure policy would stop a node that has not finished starting, while the
     * conversion is simply tried again at the next start. An unrecoverable state or an exhaustion of file handles in
     * the chain, fatal anyway, is inspected by itself (with what it carries) rather than from the top of the chain.
     * Out of memory is handled by the callers before.
     */
    private static void inspectUnlessDiskError(Throwable t)
    {
        Throwable fatal = find(t, UnrecoverableIllegalStateException.class);
        if (fatal == null)
            fatal = find(t, DseHintsConverter::isFileHandleExhaustion);
        if (fatal != null)
            JVMStabilityInspector.inspectThrowable(fatal);
        else if (find(t, FSError.class) == null)
            JVMStabilityInspector.inspectThrowable(t);
    }

    /**
     * What {@link JVMStabilityInspector} takes for an exhaustion of file handles, which it deems fatal: a copy of the
     * check of its private defaultGlobalErrorHandler, to be kept in step with it.
     */
    private static boolean isFileHandleExhaustion(Throwable t)
    {
        return (t instanceof FileNotFoundException || t instanceof FileSystemException || t instanceof SocketException)
               && t.getMessage() != null && t.getMessage().contains("Too many open files");
    }

    /** The failure, one of its causes or one of their suppressed exceptions of the given class, or {@code null}. */
    private static <T extends Throwable> T find(Throwable t, Class<T> type)
    {
        return type.cast(find(t, type::isInstance));
    }

    private static Throwable find(Throwable t, Predicate<Throwable> matches)
    {
        return find(t, matches, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private static Throwable find(Throwable t, Predicate<Throwable> matches, Set<Throwable> seen)
    {
        if (t == null || !seen.add(t))
            return null;
        if (matches.test(t))
            return t;
        for (Throwable suppressed : t.getSuppressed())
        {
            Throwable found = find(suppressed, matches, seen);
            if (found != null)
                return found;
        }
        return find(t.getCause(), matches, seen);
    }

    /**
     * {@link #convertIfDse6(HintsCatalog)} for the dispatch start: a failure outside the conversion of a single file
     * (which reports its own, but for an Error while a gone or empty file is handled, see {@link #convert}) is logged
     * and does not stop the start, unless the stability inspector deems it fatal, a disk error excepted; an out of
     * memory error, wherever it was raised, is inspected here, once.
     */
    static void convertAtDispatchStart(HintsCatalog catalog)
    {
        convertAtDispatchStart(() -> convertIfDse6(catalog));
    }

    @VisibleForTesting
    static void convertAtDispatchStart(Supplier<Summary> conversion)
    {
        try
        {
            conversion.get();
        }
        catch (Throwable t)
        {
            OutOfMemoryError oom = find(t, OutOfMemoryError.class);
            if (oom != null)
            {
                // the error alone, so that the disk failure policy cannot act first on a disk error of the same chain;
                // the default handler throws it, a replaced one may not
                JVMStabilityInspector.inspectThrowable(oom);
                throw oom;
            }
            logger.warn("DSE 6 compatibility mode: the conversion of the hints files written by DataStax Enterprise 6 failed: {}. " +
                        "The files not converted yet are left in place and converted at the next start of this node.", reason(t));
            logger.debug("DSE 6 compatibility mode: failure while converting the hints files written by DataStax Enterprise 6", t);
            inspectUnlessDiskError(t);
        }
    }

    /**
     * The address of a host for the log, never failing on an exception or an assertion of the provider: a custom
     * endpoint provider must not undo a conversion.
     */
    private String endpointOf(UUID hostId)
    {
        try
        {
            InetAddressAndPort endpoint = endpointForHost.apply(hostId);
            return endpoint == null ? "address unknown" : endpoint.toString();
        }
        catch (RuntimeException | AssertionError e)
        {
            return "address unknown: " + e;
        }
    }

    /** The failure, its suppressed exceptions and its causes: a disk error names only the file, its cause tells why. */
    private static String reason(Throwable t)
    {
        StringBuilder reason = new StringBuilder();
        appendReason(reason, t, Collections.newSetFromMap(new IdentityHashMap<>()));
        return reason.toString();
    }

    private static void appendReason(StringBuilder reason, Throwable t, Set<Throwable> seen)
    {
        if (!seen.add(t))
            return;
        // an unconvertible file is told by its message alone; its cause follows
        reason.append(t instanceof UnconvertibleFileException ? t.getMessage() : t.toString());
        for (Throwable suppressed : t.getSuppressed())
        {
            if (seen.contains(suppressed))
                continue;
            reason.append(", suppressed: [");
            appendReason(reason, suppressed, seen);
            reason.append(']');
        }
        if (t.getCause() != null && !seen.contains(t.getCause()))
        {
            reason.append(", caused by ");
            appendReason(reason, t.getCause(), seen);
        }
    }

    /**
     * The WARNs of the hints of a converted file dropped for good for a reason the operator acts on: a host not in the
     * ring, a column unknown here (named, with its table).
     */
    private static void warnDropped(Path dseFile, UUID hostId, Counts counts, Set<String> unknownColumns)
    {
        if (counts.unknownHost > 0)
            logger.warn("DSE 6 compatibility mode: dropped the {} hint(s) of the hints file {} written by DataStax Enterprise 6 " +
                        "for host {}, which is not a peer of this node in the ring (neither in its token metadata nor in " +
                        "system.peers). If that host is part of the cluster, a repair of the affected tables once " +
                        "internode_compatibility_mode is dropped brings those writes to it.",
                        counts.unknownHost, dseFile.getFileName(), hostId);
        if (counts.unknownColumn > 0)
            logger.warn("DSE 6 compatibility mode: dropped {} hint(s) of the hints file {} written by DataStax Enterprise 6 for " +
                        "host {} that name a column this node does not know ({}). A repair of those tables once " +
                        "internode_compatibility_mode is dropped brings their writes to the host.",
                        counts.unknownColumn, dseFile.getFileName(), hostId, String.join(", ", unknownColumns));
    }

    /** "column {@code c} of {@code ks.t}" from the message {@link #isUnknownColumn} accepts. */
    private static String unknownColumn(String message)
    {
        String rest = message.substring("Unknown column ".length());
        int of = rest.lastIndexOf(" during deserialization of ");
        return "column " + rest.substring(0, of) + " of " + rest.substring(of + " during deserialization of ".length());
    }

    /**
     * Whether a failure to decode a hint is a column of the hint's table unknown here, neither a column nor a dropped
     * column of the table, as {@link org.apache.cassandra.db.Columns.Serializer} reports it.
     */
    private static boolean isUnknownColumn(RuntimeException e)
    {
        String message = e.getMessage();
        return e.getClass() == RuntimeException.class && message != null
               && message.startsWith("Unknown column ") && message.contains(" during deserialization of ");
    }

    /**
     * Reads the next record of a DSE hints file, checking both its CRC32s; a cut, corrupt or unreadable record fails
     * with its offset, as damaged content (a disk error is an FSError, not an IOException).
     */
    private static byte[] readRecord(ChecksummedDataInput input, long fileSize, long recordStart) throws IOException
    {
        try
        {
            input.resetCrc();
            if (fileSize - recordStart < 2 * Integer.BYTES)
                throw new UnconvertibleFileException("the record at offset " + recordStart + " is cut in its size");
            int size = input.readInt();
            if (!input.checkCrc())
                throw new UnconvertibleFileException("the record at offset " + recordStart + " fails the checksum of its size");
            if (size < 0 || size > fileSize - input.getPosition() - Integer.BYTES)
                throw new UnconvertibleFileException("the record at offset " + recordStart + " is cut in its hint (size " + size + ')');
            byte[] record = new byte[size];
            input.readFully(record);
            if (!input.checkCrc())
                throw new UnconvertibleFileException("the record at offset " + recordStart + " fails the checksum of its hint");
            return record;
        }
        catch (UnconvertibleFileException e)
        {
            throw e;
        }
        catch (IOException e)
        {
            throw new UnconvertibleFileException("the record at offset " + recordStart + " does not read", e);
        }
    }

    /**
     * A descriptor of this software for the host, with the given timestamp or, when a file of that name or one of its
     * companions exists (left by a conversion interrupted before the DSE file was deleted), the next free one.
     */
    private HintsDescriptor freeDescriptor(UUID hostId, long timestamp)
    {
        while (true)
        {
            HintsDescriptor candidate = new HintsDescriptor(hostId, timestamp, writerParams);
            if (!candidate.file(hintsDirectory).exists()
                && !candidate.checksumFile(hintsDirectory).exists()
                && !new File(hintsDirectory, candidate.statisticsFileName()).exists())
                return candidate;
            timestamp++;
        }
    }

    /**
     * The input of a DSE hints file, or {@code null} when the file is gone (deleted since it was listed).
     */
    private static ChecksummedDataInput openUnlessGone(Path dseFile)
    {
        try
        {
            return ChecksummedDataInput.open(new File(dseFile));
        }
        catch (UncheckedIOException e)
        {
            if (e.getCause() instanceof NoSuchFileException)
                return null;
            throw e;
        }
    }

    /**
     * Deletes the companion files of DSE hints files that are gone, left by a crash between the commit point of a
     * conversion and the deletion of the companions, by a failed deletion of them, or by a DSE hints file deleted by
     * someone else; they hold no hint. A failure is logged, and the companions are tried again at every start. The
     * companions of a DSE hints file still in the directory stay, those of one set aside as {@code .corrupt.hints} too.
     */
    private void deleteOrphanCompanions()
    {
        List<Path> orphans;
        try (Stream<Path> list = Files.list(hintsDirectory.toPath()))
        {
            orphans = list.filter(p -> {
                              Matcher m = DSE_COMPANION_FILE_NAME.matcher(p.getFileName().toString());
                              return m.matches()
                                     && !Files.exists(p.resolveSibling(m.group(1) + HINTS_SUFFIX))
                                     && !Files.exists(p.resolveSibling(m.group(1) + ".corrupt" + HINTS_SUFFIX));
                          })
                          .sorted()
                          .collect(Collectors.toList());
        }
        catch (IOException | UncheckedIOException e)
        {
            logger.warn("DSE 6 compatibility mode: unable to look in {} for companion files of DataStax Enterprise 6 hints files " +
                        "that are gone: {}", hintsDirectory, e.toString());
            logger.debug("DSE 6 compatibility mode: failure while listing {}", hintsDirectory, e);
            return;
        }
        for (Path orphan : orphans)
        {
            try
            {
                if (Files.deleteIfExists(orphan))
                    logger.info("DSE 6 compatibility mode: deleted {}, a companion file of a DataStax Enterprise 6 hints file " +
                                "that is gone; it held no hint", orphan.getFileName());
            }
            catch (IOException | RuntimeException e)
            {
                logger.warn("DSE 6 compatibility mode: unable to delete {}, a companion file of a DataStax Enterprise 6 hints file " +
                            "that is gone: {}. It is tried again at the next start; it holds no hint.", orphan, e.toString());
                logger.debug("DSE 6 compatibility mode: failure while deleting {}", orphan, e);
            }
        }
    }

    /**
     * Deletes the companions of a DSE hints file once the file itself is gone, logging those it cannot delete; a missing
     * companion is not an error.
     *
     * @return whether it deleted at least one companion and none is left
     */
    private static boolean deleteCompanions(Path dseFile)
    {
        String name = dseFile.getFileName().toString();
        String base = name.substring(0, name.length() - HINTS_SUFFIX.length());
        boolean failed = false;
        boolean deletedAny = false;
        for (Path companion : new Path[]{ dseFile.resolveSibling(base + ".crc32"), dseFile.resolveSibling(base + "-Statistics" + HINTS_SUFFIX) })
        {
            try
            {
                deletedAny |= Files.deleteIfExists(companion);
            }
            catch (IOException | RuntimeException e)
            {
                failed = true;
                logger.warn("DSE 6 compatibility mode: unable to delete {}, a companion file of the DataStax Enterprise 6 hints file " +
                            "{}, which is gone: {}. It is tried again at the next start; it holds no hint.", companion, dseFile.getFileName(), e.toString());
            }
        }
        return deletedAny && !failed;
    }

    /**
     * Deletes an empty DSE hints file (no descriptor and no hint: a file DSE created and never wrote, for example after a
     * crash) and its companions.
     *
     * @return no hint, or {@code null} when the file could not be deleted and is kept
     */
    private Counts deleteEmpty(Path dseFile, Counts counts)
    {
        try
        {
            dseFileDeleter.delete(dseFile);
        }
        catch (NoSuchFileException e)
        {
            return gone(dseFile, counts);
        }
        catch (IOException | RuntimeException e)
        {
            logger.warn("DSE 6 compatibility mode: the hints file {} written by DataStax Enterprise 6 is empty, with no hint " +
                        "to convert, and could not be deleted: {}. It is tried again at the next start of this node.",
                        dseFile.getFileName(), e.toString());
            logger.debug("DSE 6 compatibility mode: failure while deleting the empty hints file {}", dseFile, e);
            inspectUnlessDiskError(e);
            return null;
        }
        boolean companionsDeleted = deleteCompanions(dseFile);
        logger.info("DSE 6 compatibility mode: the hints file {} written by DataStax Enterprise 6 is empty, with no hint to " +
                    "convert; deleted it{}", dseFile.getFileName(), companionsDeleted ? " and its companion files" : "");
        return counts;
    }

    /** The result of the conversion of a DSE file gone since it was listed, deleting its companions (best effort). */
    private static Counts gone(Path dseFile, Counts counts)
    {
        counts.gone = true;
        boolean companionsDeleted = deleteCompanions(dseFile);
        logger.info("DSE 6 compatibility mode: the hints file {} written by DataStax Enterprise 6 disappeared before it was " +
                    "converted; nothing to convert{}", dseFile.getFileName(), companionsDeleted ? ", deleted its companion files" : "");
        return counts;
    }

    /**
     * Deletes a new hints file of this software and its companions after a failed conversion, logging what it cannot.
     *
     * @param sessionClosed whether the writing session was closed, which counted the file's hints on disk
     */
    private void deleteConverted(HintsDescriptor converted, boolean sessionClosed)
    {
        File hintsFile = converted.file(hintsDirectory);
        if (hintsFile.exists())
        {
            if (hintsFile.tryDelete())
            {
                if (sessionClosed)
                    HintsServiceMetrics.hintsOnDisk.dec(converted.statistics().totalCount());
            }
            else
            {
                logger.warn("DSE 6 compatibility mode: unable to delete {}, written by a failed conversion of a DataStax Enterprise 6 " +
                            "hints file; after the next start of this node its hints are delivered, and delivered again when the " +
                            "DSE file is converted", hintsFile);
            }
        }
        for (File companion : new File[]{ converted.checksumFile(hintsDirectory), new File(hintsDirectory, converted.statisticsFileName()) })
        {
            if (companion.exists() && !companion.tryDelete())
                logger.warn("DSE 6 compatibility mode: unable to delete {}, written by a failed conversion of a DataStax Enterprise 6 " +
                            "hints file. Delete it by hand; it holds no hint.", companion);
        }
    }
}
