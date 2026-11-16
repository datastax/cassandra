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

package org.apache.cassandra.index.sai.disk.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.TermsReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsMeta;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.service.Dse6SaiVersionPin;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IndexInput;

import static org.apache.cassandra.config.InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION;
import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The SAI on-disk formats of DSE 6.8 ({@code aa}) and DSE 6.9 ({@code ca}), which a node upgraded from DSE writes
 * under the pin of {@code cassandra.sai.latest.version} ({@link Dse6SaiVersionPin}), against the index files DSE
 * wrote: the {@code <version>-t} fixtures of {@link DseFixtures#sstableDirectory}, the collection table
 * {@code test_ks.t} and its index {@code t_v_sai} on {@code v text}.
 * <p>
 * Reading: the fixture sstable is placed in the table directory and opened the way a node opens the sstables it
 * finds at start, then the index is registered the way the table registers it at start; the index must serve DSE's
 * files as they are, without building anything, with the pin and without it.
 * <p>
 * Writing: with the pin, this tree writes the rows of the fixture and flushes; its index files must carry the
 * components, file names, headers, footers and decoded metadata of DSE's files of the same version, and answer the
 * same queries. Their postings differ from DSE's in two known ways, asserted as such: an {@code aa} flush writes one
 * posting per partition ({@link #assertAaPostings}), and DSE's {@code ca} files keep a posting for a cell deleted
 * after it was indexed ({@link #assertCaPostings}).
 * <p>
 * The node is not run in DSE 6 compatibility mode: the pin is set as {@link Dse6SaiVersionPin} derives it from the
 * release of the fixture ({@link SAIUtil#setCurrentVersion}), the trie version {@code bb} that the mode sets is set by
 * {@link #TRIE_VERSION}, and the sstables flushed here keep the identifiers of the test configuration, not the numeric
 * generations of the mode; SAI file names are compared without the sstable's own prefix. No SAI code reads the mode.
 * <p>
 * Whether DSE reads the files this tree writes needs a DSE binary and is not tested here.
 */
public class DseSaiFormatCompatibilityTest extends CQLTester
{
    private static final String KEYSPACE_NAME = "test_ks";
    private static final String TABLE_NAME = "t";
    private static final String INDEX_NAME = "t_v_sai";

    /**
     * The trie index version a node in DSE 6 compatibility mode writes, {@code bb} as DSE 6 does, set before the
     * sstable formats load in {@link #setUpClass()}: the primary keys of a {@code ca} index are encoded at the
     * byte-comparable version of their sstable.
     */
    private static final WithProperties TRIE_VERSION =
        new WithProperties().set(CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION,
                                 DSE_6_TRIE_INDEX_FORMAT_VERSION);

    /** Counts the index builds submitted to the compaction manager, initial builds and rebuilds alike. */
    private static final Injections.Counter INDEX_BUILDS =
        Injections.newCounter("DseSaiIndexBuilds")
                  .add(newInvokePoint().onClass(CompactionManager.class).onMethod("submitIndexBuild"))
                  .build();

    /** The SAI version a node writes without the pin: the default of {@code cassandra.sai.latest.version}. */
    private static final Version UNPINNED =
        Version.parse(CassandraRelevantProperties.SAI_CURRENT_VERSION.getDefaultValue());

    /** The header SAI writes at the start of each component file but the completion markers: magic and version. */
    private static final int HEADER_LENGTH = SAICodecUtils.headerSize();
    /** The footer SAI writes at the end of the same files: magic, algorithm id, then an 8-byte checksum. */
    private static final int FOOTER_LENGTH = CodecUtil.footerLength();
    /** The checksum ends the footer, after the magic and the algorithm id, two ints. */
    private static final int FOOTER_CHECKSUM_LENGTH = FOOTER_LENGTH - 2 * Integer.BYTES;

    /** The prefix of the file names of the fixture sstables. */
    private static final String FIXTURE_PREFIX = "bb-1-bti-";
    /** The columns of the fixture table. */
    private static final List<String> COLUMNS = List.of("id", "ck", "fu", "l", "m", "n", "s", "tp", "u", "v");
    /** The highest {@code k} of the values {@code 'val<k>'} of either fixture. */
    private static final int MAX_VALUE = 202;

    /**
     * The facts that follow from the postings, compared through them instead: the root, offset and length of the
     * terms and postings in their files, and, for {@code aa}, the row count and the row id bounds, and so the max key:
     * this tree's last posting is the one kept at the static row of partition 3, the last partition in token order
     * ({@link #assertAaPostings}).
     */
    private static final Set<String> CA_RELAXED = Set.of(".componentLayout");
    private static final Set<String> AA_RELAXED = Set.of(".componentLayout", ".numRows", ".minSSTableRowId",
                                                         ".maxSSTableRowId", ".maxKey");

    /** The SAI components a flush of this tree writes byte for byte as DSE did, by version. */
    private static final Map<Version, SortedSet<String>> IDENTICAL =
        Map.of(Version.AA, new TreeSet<>(List.of("SAI_GroupComplete.db", "SAI_GroupMeta.db", "SAI_TokenValues.db",
                                                 "SAI_t_v_sai_ColumnComplete.db")),
               Version.CA, new TreeSet<>(List.of("SAI+ca+GroupComplete.db", "SAI+ca+GroupMeta.db",
                                                 "SAI+ca+PrimaryKeyBlockOffsets.db", "SAI+ca+PrimaryKeyBlocks.db",
                                                 "SAI+ca+PrimaryKeyTrie.db", "SAI+ca+TokenValues.db",
                                                 "SAI+ca+t_v_sai+ColumnComplete.db")));

    private static final String EQ = "SELECT id, ck FROM test_ks.t WHERE v = ?";
    private static final String IN = "SELECT id, ck FROM test_ks.t WHERE v IN ('val4', 'val50', 'val3', 'val6', " +
                                     "'val200')";
    private static final String OR = "SELECT id, ck FROM test_ks.t WHERE v = 'val9' OR v = 'val8' OR v = 'val1'";

    /**
     * Hides {@link CQLTester#setUpClass()}, so that the trie version that {@link #TRIE_VERSION} sets when this class is
     * initialised is checked before the server starts, and the version the server writes after.
     */
    @BeforeClass
    public static void setUpClass()
    {
        assertThat(CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION.getString())
        .isEqualTo(DSE_6_TRIE_INDEX_FORMAT_VERSION);
        CQLTester.setUpClass();
        assertThat(BtiFormat.getInstance().getLatestVersion().version).isEqualTo(DSE_6_TRIE_INDEX_FORMAT_VERSION);
    }

    @AfterClass
    public static void restoreTrieVersion()
    {
        TRIE_VERSION.close();
    }

    @Before
    public void countIndexBuilds() throws Throwable
    {
        Injections.inject(INDEX_BUILDS);
        INDEX_BUILDS.reset();
    }

    @After
    public void removeInjections()
    {
        Injections.deleteAll();
    }

    @After
    public void dropFixtureKeyspace()
    {
        schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE_NAME);
        SAIUtil.resetCurrentVersion();
    }

    /**
     * With the pin, DSE's files are served as they are, without a build; then a rebuild of the index, which the
     * observables of the first part must see, writes it at DSE's version again
     * ({@link #rebuildWritesOnePostingPerRow}).
     */
    @Test
    public void readsDse68IndexWithoutRebuildThenRebuilds() throws Throwable
    {
        readsWithThePinThenRebuilds(DseVersion.DSE_6_8_65);
    }

    @Test
    public void readsDse69IndexWithoutRebuildThenRebuilds() throws Throwable
    {
        readsWithThePinThenRebuilds(DseVersion.DSE_6_9_26);
    }

    private void readsWithThePinThenRebuilds(DseVersion fixture) throws Throwable
    {
        ColumnFamilyStore cfs = readsDseIndexWithoutRebuild(fixture, pinOf(fixture));
        SSTableReader dseSSTable = Iterables.getOnlyElement(cfs.getLiveSSTables());
        IndexFiles dse = describe(cfs, dseSSTable);
        rebuildWritesOnePostingPerRow(cfs, fixture, dseSSTable, dse);
    }

    /**
     * Without the pin the node writes {@link #UNPINNED}, but it reads each sstable's index at the version its file
     * names carry: DSE's files are served as they are, and a flush adds an sstable with an index at the unpinned
     * version. A rebuild of the index of DSE's sstable writes it at DSE's version again.
     */
    @Test
    public void readsDse68IndexWithThePinOff() throws Throwable
    {
        readsDseIndexWithThePinOff(DseVersion.DSE_6_8_65);
    }

    @Test
    public void readsDse69IndexWithThePinOff() throws Throwable
    {
        readsDseIndexWithThePinOff(DseVersion.DSE_6_9_26);
    }

    /**
     * The start validates the checksums of the per-sstable components of DSE's files (of the per-index ones, only the
     * header and footer): a corrupt one is not served, and the index of the sstable is built again, at DSE's version.
     */
    @Test
    public void rebuildsCorruptDse68IndexAtStart() throws Throwable
    {
        rebuildsCorruptDseIndexAtStart(DseVersion.DSE_6_8_65);
    }

    @Test
    public void rebuildsCorruptDse69IndexAtStart() throws Throwable
    {
        rebuildsCorruptDseIndexAtStart(DseVersion.DSE_6_9_26);
    }

    @Test
    public void writesDse68IndexFormat() throws Throwable
    {
        writesDseIndexFormat(DseVersion.DSE_6_8_65);
    }

    @Test
    public void writesDse69IndexFormat() throws Throwable
    {
        writesDseIndexFormat(DseVersion.DSE_6_9_26);
    }

    private void readsDseIndexWithThePinOff(DseVersion fixture) throws Throwable
    {
        Version dse = pinOf(fixture);
        assertThat(UNPINNED).isNotEqualTo(dse);
        ColumnFamilyStore cfs = readsDseIndexWithoutRebuild(fixture, UNPINNED);

        SSTableReader dseSSTable = Iterables.getOnlyElement(cfs.getLiveSSTables());
        execute("INSERT INTO test_ks.t (id, ck, v) VALUES (101, 'ck1', 'val300')");
        flush(KEYSPACE_NAME, TABLE_NAME);
        SSTableReader flushed = Iterables.getOnlyElement(cfs.getLiveSSTables()
                                                            .stream()
                                                            .filter(s -> !s.descriptor.equals(dseSSTable.descriptor))
                                                            .collect(Collectors.toList()));
        IndexContext context = index(cfs).getIndexContext();
        assertThat(context.getView().getSSTableIndex(dseSSTable.descriptor).getVersion()).isEqualTo(dse);
        assertThat(context.getView().getSSTableIndex(flushed.descriptor).getVersion()).isEqualTo(UNPINNED);
        assertThat(IndexDescriptor.load(flushed, Set.of(context)).perSSTableComponents().version()).isEqualTo(UNPINNED);
        assertThat(saiFileNames(flushed.descriptor))
        .contains(UNPINNED.fileNameFormatter().format(IndexComponentType.GROUP_COMPLETION_MARKER, (String) null, 0));
        assertThat(saiFileNames(dseSSTable.descriptor))
        .isEqualTo(saiFileNames(DseFixtures.sstableDirectory(fixture), FIXTURE_PREFIX));
        assertThat(rows("SELECT id, ck FROM test_ks.t WHERE v = 'val50'"))
        .isEqualTo(rowsOf(documentedRows(fixture), "val50"));
        assertThat(rows("SELECT id, ck FROM test_ks.t WHERE v = 'val300'")).containsExactly(List.of(101, "ck1"));
        // DSE's files, as they were
        assertThat(digests(dseSSTable.descriptor)).isEqualTo(fixtureSaiDigests(fixture));

        rebuildWritesOnePostingPerRow(cfs, fixture, dseSSTable, describe(cfs, dseSSTable));
        assertThat(context.getView().getSSTableIndex(flushed.descriptor).getVersion()).isEqualTo(UNPINNED);
    }

    private void rebuildsCorruptDseIndexAtStart(DseVersion fixture) throws Throwable
    {
        Version dse = pinOf(fixture);
        SAIUtil.setCurrentVersion(dse);
        String tokens = dse.fileNameFormatter().format(IndexComponentType.TOKEN_VALUES, (String) null, 0);
        List<String> builds = new ArrayList<>();
        Map<String, String> corrupt = new TreeMap<>();
        ColumnFamilyStore cfs = startWithFixture(fixture, builds, directory -> {
            // one byte of the tokens, between the header and the footer, so that only the checksum tells
            Path file = directory.resolve(FIXTURE_PREFIX + tokens);
            byte[] bytes = Files.readAllBytes(file);
            assertThat(bytes.length / 2).isGreaterThanOrEqualTo(HEADER_LENGTH)
                                        .isLessThan(bytes.length - FOOTER_LENGTH);
            bytes[bytes.length / 2] ^= 0x01;
            Files.write(file, bytes);
            corrupt.putAll(digests(directory));
        });
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());

        assertThat(INDEX_BUILDS.get()).as("index builds submitted").isPositive();
        assertThat(builds).as("index builds logged")
                          .anyMatch(m -> m.contains("Submitting") && m.contains("initial index builds"));
        assertThat(index(cfs).getIndexContext().getView().getSSTableIndex(sstable.descriptor).getVersion())
        .isEqualTo(dse);
        Map<String, String> fixtureDigests = fixtureSaiDigests(fixture);
        assertThat(corrupt.get(FIXTURE_PREFIX + tokens)).isNotNull().isNotEqualTo(fixtureDigests.get(tokens));
        assertThat(digests(sstable.descriptor).get(tokens)).isNotNull()
                                                          .isNotEqualTo(corrupt.get(FIXTURE_PREFIX + tokens));
        assertBuiltFromTheData(cfs, fixture, sstable);
        assertFixtureRows(fixture);
    }

    /**
     * Places the fixture of {@code fixture} in an empty table, starts its index as a node start does with
     * {@code current} as the version the node writes, and checks that the index serves DSE's files without building
     * and answers the queries with the rows of the fixture.
     */
    private ColumnFamilyStore readsDseIndexWithoutRebuild(DseVersion fixture, Version current) throws Throwable
    {
        SAIUtil.setCurrentVersion(current);
        Version dse = pinOf(fixture);
        Map<String, String> before = digests(DseFixtures.sstableDirectory(fixture));

        List<String> builds = new ArrayList<>();
        ColumnFamilyStore cfs = startWithFixture(fixture, builds);
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());

        // no index build ran: none was submitted, none was logged
        assertThat(INDEX_BUILDS.get()).as("index builds submitted").isZero();
        assertThat(builds).as("index builds logged").isEmpty();
        // the index serves DSE's files, at DSE's version
        StorageAttachedIndex index = index(cfs);
        assertThat(cfs.indexManager.isIndexQueryable(index)).isTrue();
        SSTableIndex sstableIndex = index.getIndexContext().getView().getSSTableIndex(sstable.descriptor);
        assertThat(sstableIndex).as("index of " + sstable.descriptor).isNotNull();
        assertThat(sstableIndex.getVersion()).isEqualTo(dse);
        assertThat(sstableIndex.usedPerIndexComponents().version()).isEqualTo(dse);

        assertFixtureRows(fixture);

        // the files are DSE's, unchanged, and no other file was written
        assertThat(digests(cfs.getDirectories().getDirectoryForNewSSTables().toPath())).isEqualTo(before);
        return cfs;
    }

    /**
     * Rebuilds the index of the table, whose sstables include {@code dseSSTable}, the one of {@code fixture}, whose
     * index files DSE wrote, decoded as {@code dse}: the observables of the tests above see a build; the index of
     * DSE's sstable is built from its data ({@link #assertBuiltFromTheData}): the postings of DSE's files, by key, but
     * the stale one of (2, ck1), whose value was deleted after it was indexed (DSE keeps it in {@code ca},
     * {@link #assertCaPostings}, and in {@code aa}, where the flush of this tree keeps it too), and the per-sstable
     * keys of DSE's flush and partition 1.
     */
    private void rebuildWritesOnePostingPerRow(ColumnFamilyStore cfs,
                                               DseVersion fixture,
                                               SSTableReader dseSSTable,
                                               IndexFiles dse) throws Throwable
    {
        long buildsBefore = INDEX_BUILDS.get();
        try (LoggedBuilds rebuild = new LoggedBuilds())
        {
            cfs.indexManager.rebuildIndexesBlocking(Set.of(INDEX_NAME));
            assertThat(rebuild.messages()).as("index builds logged by a rebuild")
                                          .anyMatch(m -> m.contains("Starting index build on"));
        }
        assertThat(INDEX_BUILDS.get()).as("index builds submitted by a rebuild").isGreaterThan(buildsBefore);

        IndexFiles rebuilt = assertBuiltFromTheData(cfs, fixture, dseSSTable);
        Version version = pinOf(fixture);

        // the per-sstable keys of DSE's flush, and partition 1, so the per-sstable components are not DSE's bytes
        assertThat(dse.keys).doesNotContain("1");
        List<String> keys = new ArrayList<>(rebuilt.keys);
        assertThat(keys.remove("1")).isTrue();
        assertThat(keys).isEqualTo(dse.keys);

        // DSE's postings, by key, but the stale one
        Map<String, List<String>> dsePostings = keysOfPostings(dse);
        assertThat(dsePostings.remove("val3")).isEqualTo(List.of("2" + (version == Version.AA ? "" : ":ck1")));
        assertThat(keysOfPostings(rebuilt)).isEqualTo(dsePostings);
    }

    /**
     * The index of {@code sstable}, the one of {@code fixture}, was built from its data: under DSE's file names, at
     * DSE's version, whatever the version the node writes for new sstables, with one posting for each live row
     * holding the value (unlike a flush of {@code aa}, {@link #assertAaPostings}), and a per-sstable key for
     * partition 1, which only its deletion holds: on a table with static columns, {@code StorageAttachedIndexBuilder}
     * passes the static row of every partition to the writer, empty or not, while a flush skips an empty static row.
     */
    private static IndexFiles assertBuiltFromTheData(ColumnFamilyStore cfs, DseVersion fixture, SSTableReader sstable)
    throws IOException
    {
        Version version = pinOf(fixture);
        assertThat(index(cfs).getIndexContext().getView().getSSTableIndex(sstable.descriptor).getVersion())
        .isEqualTo(version);
        assertThat(saiFileNames(sstable.descriptor)).isEqualTo(fixtureSaiDigests(fixture).keySet());
        IndexFiles built = describe(cfs, sstable);
        assertThat(built.facts.get("version")).isEqualTo(version.toString());
        assertThat(built.keys).contains("1");

        // one posting for each live row holding the value
        Map<String, List<String>> live = new TreeMap<>();
        documentedRows(fixture).forEach((row, v) -> {
            if (v != null)
            {
                String key = version == Version.AA ? row.get(0).toString() : row.get(0) + ":" + row.get(1);
                live.computeIfAbsent(v, t -> new ArrayList<>()).add(key);
            }
        });
        live.values().forEach(rows -> rows.sort(null));
        assertThat(keysOfPostings(built)).as("the keys of the postings of each term, after a build").isEqualTo(live);
        return built;
    }

    /** The keys of the postings of each term of {@code files}, sorted. */
    private static Map<String, List<String>> keysOfPostings(IndexFiles files)
    {
        Map<String, List<String>> keys = new TreeMap<>();
        files.postings.forEach((term, list) -> keys.put(term, list.stream()
                                                                  .map(posting -> posting.key)
                                                                  .sorted()
                                                                  .collect(Collectors.toList())));
        return keys;
    }

    /**
     * Serves DSE's files of {@code fixture} and reads its rows; then writes the same rows with the pin of the
     * fixture, flushes, checks that both index files answer the same and compares them.
     */
    private void writesDseIndexFormat(DseVersion fixture) throws Throwable
    {
        Version pin = pinOf(fixture);
        SAIUtil.setCurrentVersion(pin);
        List<String> builds = new ArrayList<>();
        ColumnFamilyStore dseCfs = startWithFixture(fixture, builds);
        SSTableReader dseSSTable = Iterables.getOnlyElement(dseCfs.getLiveSSTables());
        // DSE's files, served without a build
        assertThat(INDEX_BUILDS.get()).as("index builds submitted").isZero();
        assertThat(builds).as("index builds logged").isEmpty();
        assertThat(digests(dseSSTable.descriptor)).isEqualTo(fixtureSaiDigests(fixture));
        assertFixtureRows(fixture);
        IndexFiles dse = describe(dseCfs, dseSSTable);
        Map<String, String> dseDigests = digests(dseSSTable.descriptor);
        Map<String, List<List<Object>>> dseAnswers = answers();
        List<ByteBuffer[]> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : execute("SELECT " + String.join(", ", COLUMNS) + " FROM test_ks.t"))
        {
            rows.add(COLUMNS.stream()
                            .map(c -> row.has(c) ? row.getBytes(c) : ByteBufferUtil.UNSET_BYTE_BUFFER)
                            .toArray(ByteBuffer[]::new));
        }

        dropFixtureKeyspace();
        SAIUtil.setCurrentVersion(pin);
        ColumnFamilyStore cfs = createFixtureSchema();
        writeFixtureRows(rows);
        flush(KEYSPACE_NAME, TABLE_NAME);
        SSTableReader written = Iterables.getOnlyElement(cfs.getLiveSSTables());
        assertThat(written.descriptor.version.format.name()).isEqualTo(BtiFormat.NAME);
        assertThat(written.descriptor.version.version).isEqualTo(dseSSTable.descriptor.version.version);
        assertThat(index(cfs).getIndexContext().getView().getSSTableIndex(written.descriptor).getVersion())
        .isEqualTo(pin);
        IndexFiles ours = describe(cfs, written);
        Map<String, String> ourDigests = digests(written.descriptor);

        // the components whose bytes are identical to DSE's: the per-sstable ones except the aa partition offsets,
        // which point into the data file, plus the column's completion marker
        SortedSet<String> identical = new TreeSet<>();
        ourDigests.forEach((name, digest) -> {
            if (digest.equals(dseDigests.get(name)))
                identical.add(name);
        });
        assertThat(identical).as("SAI components byte for byte identical to DSE's").isEqualTo(IDENTICAL.get(pin));

        // our files answer as DSE's do, with the data of the fixture
        assertThat(answers()).as("answers of our index against DSE's").isEqualTo(dseAnswers);
        assertFixtureRows(fixture);

        // everything else is equal, but for what the postings decide (see assertAaPostings and assertCaPostings)
        Set<String> relaxed = pin == Version.AA ? AA_RELAXED : CA_RELAXED;
        assertThat(ours.facts.get("version")).isEqualTo(pin.toString());
        assertThat(differences(strict(ours.facts, relaxed), strict(dse.facts, relaxed)))
        .as("index files written with the pin " + pin + " against DSE's")
        .isEmpty();
        assertBoundsFollowPostings(ours);
        assertBoundsFollowPostings(dse);
        if (pin == Version.AA)
            assertAaPostings(ours, dse, documentedRows(fixture));
        else
            assertCaPostings(ours, dse, documentedRows(fixture));
    }

    /**
     * The {@code aa} format resolves a posting to a partition only, which is read and filtered: the postings of our
     * files and of DSE's must lead to the same partitions. The flush of {@code aa} does not write the postings DSE
     * writes on a table with clustering columns: {@code MemtableIndexWriter} maps the keys of the memtable index to
     * row ids with a {@code RowMapping} keyed by partition-only primary keys that keeps the last row id of each key,
     * so each term has one posting per partition, at the row id of the last row of the partition (its static row
     * when it has no other), and {@code numRows} counts those row ids; the values of rows that a deletion removed
     * from a partition that survives keep a posting there. DSE writes one posting per row holding the value, and so
     * does an index build of this tree. Queries are not affected: such a posting leads to the partition, whose rows
     * are read and filtered. The shape is asserted, so that a change is noticed.
     */
    private static void assertAaPostings(IndexFiles ours, IndexFiles dse, Map<List<Object>, String> live)
    {
        // DSE: one posting per row holding the value, counted by numRows; ours: one per partition, at its last row
        assertThat(dse.numRows()).isEqualTo(dse.postings.values().stream().mapToLong(List::size).sum())
                                 // the 195 live rows holding a value and the stale posting of (2, ck1)
                                 .isEqualTo(196);
        assertThat(dse.postings.values().stream().flatMap(List::stream).filter(p -> !dse.isLastRowOfPartition(p)))
        .as("DSE's postings to rows other than the last of their partition")
        .isNotEmpty();
        for (Map.Entry<String, List<Posting>> term : ours.postings.entrySet())
        {
            assertThat(term.getValue()).as(term.getKey()).allMatch(ours::isLastRowOfPartition);
            assertThat(term.getValue().stream().map(Posting::partition).distinct().count())
            .as(term.getKey()).isEqualTo(term.getValue().size());
        }
        assertThat(ours.numRows()).isEqualTo(ours.postings.values()
                                                          .stream()
                                                          .flatMap(List::stream)
                                                          .map(p -> p.rowId)
                                                          .distinct()
                                                          .count())
                                 // the 98 partitions holding a value (2, and 4 to 100), and partition 3's static row
                                 .isEqualTo(99);

        // the same partitions for each term, but those where a deletion removed every row holding the value
        Map<String, Set<Integer>> kept = new TreeMap<>();
        Set<String> terms = new TreeSet<>(ours.postings.keySet());
        terms.addAll(dse.postings.keySet());
        for (String term : terms)
        {
            Set<Integer> ourPartitions = ours.partitionsOf(term);
            Set<Integer> dsePartitions = dse.partitionsOf(term);
            assertThat(ourPartitions).as("partitions of " + term).containsAll(dsePartitions);
            Set<Integer> extra = new TreeSet<>(ourPartitions);
            extra.removeAll(dsePartitions);
            for (int partition : extra)
            {
                assertThat(live.entrySet())
                .as("live rows of partition " + partition + " holding " + term)
                .noneMatch(e -> e.getKey().get(0).equals(partition) && term.equals(e.getValue()));
            }
            if (!extra.isEmpty())
                kept.put(term, extra);
        }
        // the values of the rows of partition 3 that the range deletion removed, kept at its static row
        assertThat(kept).isEqualTo(Map.of("val5", Set.of(3), "val6", Set.of(3)));
    }

    /**
     * The postings of our {@code ca} files are a subset of DSE's: DSE keeps a posting for the value of a cell deleted
     * after it was indexed, (2, ck1) with {@code 'val3'}, while this tree removes it from the memtable index when the
     * cell is deleted. Queries are not affected: the row read through that posting holds no such value.
     */
    private static void assertCaPostings(IndexFiles ours, IndexFiles dse, Map<List<Object>, String> live)
    {
        Map<String, List<String>> missing = new TreeMap<>();
        Set<String> terms = new TreeSet<>(ours.postings.keySet());
        terms.addAll(dse.postings.keySet());
        for (String term : terms)
        {
            List<Posting> ourPostings = ours.postings.getOrDefault(term, List.of());
            List<Posting> dsePostings = dse.postings.getOrDefault(term, List.of());
            assertThat(dsePostings).as("postings of " + term).containsAll(ourPostings);
            for (Posting posting : dsePostings)
            {
                if (ourPostings.contains(posting))
                    continue;
                List<Object> row = Arrays.asList(posting.partition(), posting.clustering());
                assertThat(live).as("row of the posting " + posting + " of " + term).containsKey(row);
                assertThat(live.get(row)).as("v of the row of the posting " + posting + " of " + term).isNull();
                missing.computeIfAbsent(term, t -> new ArrayList<>()).add(posting.toString());
            }
        }
        assertThat(missing).isEqualTo(Map.of("val3", List.of("97=2:ck1")));
    }

    /** The row id bounds of each segment are those of its postings, and the max key that of the last one. */
    private static void assertBoundsFollowPostings(IndexFiles files)
    {
        List<Posting> postings = files.postings.values().stream().flatMap(List::stream).collect(Collectors.toList());
        assertThat(postings).isNotEmpty();
        Posting min = postings.stream().min(Comparator.comparingLong(p -> p.rowId)).orElseThrow();
        Posting max = postings.stream().max(Comparator.comparingLong(p -> p.rowId)).orElseThrow();
        assertThat(files.facts.get("segment.0.minSSTableRowId")).isEqualTo(Long.toString(min.rowId));
        assertThat(files.facts.get("segment.0.maxSSTableRowId")).isEqualTo(Long.toString(max.rowId));
        assertThat(files.facts.get("segment.0.maxKey")).isEqualTo(max.key);
    }

    /** {@code facts} without those whose name ends with one of {@code relaxed}. */
    private static SortedMap<String, String> strict(SortedMap<String, String> facts, Set<String> relaxed)
    {
        SortedMap<String, String> strict = new TreeMap<>(facts);
        strict.keySet().removeIf(name -> relaxed.stream().anyMatch(name::endsWith));
        return strict;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // the fixture table

    private ColumnFamilyStore createFixtureSchema()
    {
        schemaChange("CREATE KEYSPACE " + KEYSPACE_NAME +
                     " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        schemaChange("CREATE TYPE test_ks.addr (street text, zip int)");
        schemaChange("CREATE TABLE test_ks.t (id int, ck text, fu frozen<addr>, l list<int>, m map<text, int>, " +
                     "n bigint, s int static, tp frozen<tuple<int, text>>, u addr, v text, PRIMARY KEY (id, ck)) " +
                     "WITH CLUSTERING ORDER BY (ck ASC)");
        schemaChange("CREATE CUSTOM INDEX " + INDEX_NAME + " ON test_ks.t (v) USING 'StorageAttachedIndex'");
        waitForIndexQueryable(KEYSPACE_NAME, INDEX_NAME);
        ColumnFamilyStore cfs = table();
        cfs.disableAutoCompaction();
        return cfs;
    }

    /**
     * Writes {@code rows}, the rows read from a fixture (in the order of {@link #COLUMNS}, unset where the fixture
     * holds no value), with the statements of {@code test/data/dse-compat/README.md}: partitions 1 and 3 inserted as
     * listed there, the static value of partition 3 and the other rows inserted with the values of the fixture,
     * {@code v = 'val3'} for (2, ck1), then the three deletions.
     */
    private void writeFixtureRows(List<ByteBuffer[]> rows) throws Throwable
    {
        for (int i : new int[]{ 1, 3 })
        {
            for (int j = 1; j <= 2; j++)
            {
                int k = 2 * (i - 1) + j;
                execute(String.format("INSERT INTO test_ks.t (id, ck, fu, l, m, n, s, tp, u, v) VALUES (%d, 'ck%d', " +
                                      "{street: 'Street%d', zip: %d}, [%d, %d, %d], {'m': %d}, %d, %d, " +
                                      "(%d, 'tuple%d'), {street: 'Avenue%d', zip: %d}, 'val%d')",
                                      i, j, i, 100000 + i, i, i + 1, i + 2, i, 100 * k, 10 * i, i, i, i, 200000 + i,
                                      k));
            }
        }
        String insert = "INSERT INTO test_ks.t (" + String.join(", ", COLUMNS) + ") VALUES (" +
                        COLUMNS.stream().map(c -> "?").collect(Collectors.joining(", ")) + ')';
        int ck = COLUMNS.indexOf("ck");
        int v = COLUMNS.indexOf("v");
        int written = 0;
        for (ByteBuffer[] row : rows)
        {
            // the static-only row of partition 3: its rows are inserted above, its static value as the fixture has it
            if (row[ck] == ByteBufferUtil.UNSET_BYTE_BUFFER)
            {
                assertThat(Int32Type.instance.compose(row[0])).isEqualTo(3);
                assertThat(Int32Type.instance.compose(row[COLUMNS.indexOf("s")])).isEqualTo(30);
                execute("INSERT INTO test_ks.t (id, s) VALUES (?, ?)", row[0], row[COLUMNS.indexOf("s")]);
                continue;
            }
            // the row whose v the cell deletion below removes is inserted with the v listed in the README
            if (row[v] == ByteBufferUtil.UNSET_BYTE_BUFFER)
            {
                assertThat(List.of(Int32Type.instance.compose(row[0]), UTF8Type.instance.compose(row[ck])))
                .isEqualTo(List.of(2, "ck1"));
                row = row.clone();
                row[v] = UTF8Type.instance.decompose("val3");
            }
            execute(insert, (Object[]) row);
            written++;
        }
        assertThat(written).isEqualTo(DseFixtures.COLLECTION_TABLE_LIVE_REGULAR_ROWS);
        execute("DELETE FROM test_ks.t WHERE id = 1");
        execute("DELETE v FROM test_ks.t WHERE id = 2 AND ck = 'ck1'");
        execute("DELETE FROM test_ks.t WHERE id = 3 AND ck >= 'ck1' AND ck <= 'ck2'");
    }

    /**
     * The live regular rows of the fixture, {@code [id, ck]} to {@code v}, as {@code test/data/dse-compat/README.md}
     * lists them: partitions 1 and 3 deleted, {@code v} of (2, ck1) deleted, and {@code v = 'val<k>'} for the others,
     * with {@code k = 2 * (id - 1) + j} in the 6.8.65 set and in partition 2 of the 6.9.26 set, and
     * {@code k = 2 * id + j} in the other partitions of the 6.9.26 set ({@code j} is 1 for {@code ck1}, 2 for
     * {@code ck2}).
     */
    private static Map<List<Object>, String> documentedRows(DseVersion fixture)
    {
        Map<List<Object>, String> rows = new LinkedHashMap<>();
        for (int i = 1; i <= 100; i++)
        {
            if (i == 1 || i == 3)
                continue;
            for (int j = 1; j <= 2; j++)
            {
                int k = fixture == DseVersion.DSE_6_8_65 || i == 2 ? 2 * (i - 1) + j : 2 * i + j;
                rows.put(List.of(i, "ck" + j), i == 2 && j == 1 ? null : "val" + k);
            }
        }
        assertThat(rows).hasSize(DseFixtures.COLLECTION_TABLE_LIVE_REGULAR_ROWS);
        return rows;
    }

    /** The rows of {@code rows} whose {@code v} is one of {@code values}, as {@code [id, ck]}. */
    private static List<List<Object>> rowsOf(Map<List<Object>, String> rows, String... values)
    {
        Set<String> wanted = Set.of(values);
        return rows.entrySet()
                   .stream()
                   .filter(e -> e.getValue() != null && wanted.contains(e.getValue()))
                   .map(Map.Entry::getKey)
                   .collect(Collectors.toList());
    }

    /** What the table answers without the index and through it is the data of {@code fixture}. */
    private void assertFixtureRows(DseVersion fixture) throws Throwable
    {
        Map<List<Object>, String> documented = documentedRows(fixture);

        // no index: every row of the table
        Map<List<Object>, String> scanned = new LinkedHashMap<>();
        for (UntypedResultSet.Row row : execute("SELECT id, ck, v FROM test_ks.t"))
        {
            if (row.has("ck"))
                scanned.put(List.of(row.getInt("id"), row.getString("ck")), row.has("v") ? row.getString("v") : null);
        }
        assertThat(scanned).containsExactlyInAnyOrderEntriesOf(documented);
        assertThat(execute("SELECT count(*) FROM test_ks.t").one().getLong("count"))
        .isEqualTo(DseFixtures.COLLECTION_TABLE_COUNT_STAR);

        // through the index: every value written, the deleted ones included, and some never written
        for (int k = 1; k <= MAX_VALUE + 1; k++)
        {
            String value = "val" + k;
            assertThat(rows(EQ, value)).as(value).isEqualTo(rowsOf(documented, value));
        }
        assertThat(rows(IN))
        .containsExactlyInAnyOrderElementsOf(rowsOf(documented, "val4", "val50", "val3", "val6", "val200"));
        assertThat(rows(OR)).isNotEmpty()
                            .containsExactlyInAnyOrderElementsOf(rowsOf(documented, "val9", "val8", "val1"));
        // with a filter on a column the index does not cover (n = 100 * k in both sets)
        List<Object> val50 = Iterables.getOnlyElement(rowsOf(documented, "val50"));
        assertThat(rows("SELECT id, ck FROM test_ks.t WHERE v = 'val50' AND n = 5000 ALLOW FILTERING"))
        .containsExactly(val50);
        assertThat(rows("SELECT id, ck FROM test_ks.t WHERE v = 'val50' AND n = 5100 ALLOW FILTERING")).isEmpty();
        // a range on text is not an operation of the index: refused, or done by filtering
        assertThatThrownBy(() -> execute("SELECT id, ck FROM test_ks.t WHERE v > 'val98'"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Column 'v' has an index but does not support the operators specified in the query");
        List<List<Object>> above = documented.entrySet()
                                             .stream()
                                             .filter(e -> e.getValue() != null && e.getValue().compareTo("val98") > 0)
                                             .map(Map.Entry::getKey)
                                             .collect(Collectors.toList());
        assertThat(rows("SELECT id, ck FROM test_ks.t WHERE v > 'val98' ALLOW FILTERING"))
        .isNotEmpty()
        .containsExactlyInAnyOrderElementsOf(above);
        // within one partition
        assertThat(rows("SELECT id, ck FROM test_ks.t WHERE id = ? AND v = 'val50'", val50.get(0)))
        .containsExactly(val50);
        assertThat(rows("SELECT id, ck FROM test_ks.t WHERE id = ? AND v = 'val50'", (int) val50.get(0) + 1))
        .isEmpty();
    }

    /** The answers of the queries through the index that {@link #assertFixtureRows} runs. */
    private Map<String, List<List<Object>>> answers() throws Throwable
    {
        Map<String, List<List<Object>>> answers = new LinkedHashMap<>();
        for (int k = 1; k <= MAX_VALUE + 1; k++)
            answers.put("val" + k, rows(EQ, "val" + k));
        answers.put("in", rows(IN));
        answers.put("or", rows(OR));
        return answers;
    }

    private List<List<Object>> rows(String query, Object... values) throws Throwable
    {
        List<List<Object>> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : execute(query, values))
            rows.add(List.of(row.getInt("id"), row.getString("ck")));
        return rows;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // a node start over DSE's files

    /**
     * Creates the fixture table and its index, then, with the table empty, does what a node start does to a table
     * whose directory holds the sstable of {@code fixture}: the directory is cleaned as the startup checks do
     * ({@code ColumnFamilyStore.scrubDataDirectories}), the sstables found in it are opened and added to the table
     * before any index exists (its storage handler's {@code loadInitialSSTables}), then the index is
     * registered as a new table registers it ({@code addIndex(..., true)} in the constructor of
     * {@code ColumnFamilyStore}), and its initialization task runs while the node is {@code STARTING}, so it validates
     * the files it finds, the checksums of the per-sstable ones ({@link #rebuildsCorruptDseIndexAtStart}), and the
     * pre-join task of the index runs. The index builds logged on the way are added to {@code builds}.
     */
    private ColumnFamilyStore startWithFixture(DseVersion fixture, List<String> builds) throws Exception
    {
        return startWithFixture(fixture, builds, directory -> {});
    }

    /** What is done to the copied files of the fixture before the start. */
    private interface FileChange
    {
        void apply(Path directory) throws IOException;
    }

    private ColumnFamilyStore startWithFixture(DseVersion fixture, List<String> builds, FileChange change)
    throws Exception
    {
        ColumnFamilyStore cfs = createFixtureSchema();
        assertThat(cfs.getLiveSSTables()).isEmpty();
        IndexMetadata indexMetadata = cfs.metadata().indexes.get(INDEX_NAME).orElseThrow();
        // the index the schema holds is registered again below, as at start; until then the table has none
        cfs.indexManager.removeIndex(INDEX_NAME);

        Path directory = cfs.getDirectories().getDirectoryForNewSSTables().toPath();
        for (Path file : DseFixtures.sstableComponents(fixture))
            Files.copy(file, directory.resolve(file.getFileName()));
        change.apply(directory);

        // the startup checks clean each table directory before the tables open
        ColumnFamilyStore.scrubDataDirectories(cfs.metadata());
        Collection<SSTableReader> sstables = cfs.getStorageHandler().loadInitialSSTables();
        assertThat(sstables).hasSize(1);

        boolean wasStarting = StorageService.instance.isStarting();
        assertThat(wasStarting || StorageService.instance.isNormal()).as(StorageService.instance.getOperationMode())
                                                                     .isTrue();
        // only the builds of this start
        INDEX_BUILDS.reset();
        try (LoggedBuilds logged = new LoggedBuilds())
        {
            StorageService.instance.setStartingModeUnsafe();
            try
            {
                cfs.indexManager.addIndex(indexMetadata, true).get();
                // then, before the node joins, the pre-join task of the index
                cfs.indexManager.executePreJoinTasksBlocking(false);
            }
            finally
            {
                if (wasStarting)
                    StorageService.instance.setStartingModeUnsafe();
                else
                    StorageService.instance.setNormalModeUnsafe();
            }
            builds.addAll(logged.messages());
        }
        waitForIndexQueryable(KEYSPACE_NAME, INDEX_NAME);
        return cfs;
    }

    /** Collects what the index initialization and the index builder log when they build an sstable index. */
    private static final class LoggedBuilds implements AutoCloseable
    {
        private final Logger initialization = (Logger) LoggerFactory.getLogger(StorageAttachedIndex.class);
        private final Logger builder = (Logger) LoggerFactory.getLogger(StorageAttachedIndexBuilder.class);
        private final Level builderLevel = builder.getLevel();
        private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
        private final List<String> messages = new ArrayList<>();

        LoggedBuilds()
        {
            appender.start();
            initialization.addAppender(appender);
            builder.addAppender(appender);
            builder.setLevel(Level.DEBUG);
        }

        /** The index builds logged so far. */
        List<String> messages()
        {
            List<ILoggingEvent> events;
            // the threads of the builds append under the lock of the appender (AppenderBase.doAppend)
            synchronized (appender)
            {
                events = new ArrayList<>(appender.list);
                appender.list.clear();
            }
            for (ILoggingEvent event : events)
            {
                String message = event.getFormattedMessage();
                if ((message.contains("Submitting") && message.contains("index builds")) ||
                    message.contains("Starting index build on"))
                    messages.add(message);
            }
            return messages;
        }

        @Override
        public void close()
        {
            initialization.detachAppender(appender);
            builder.detachAppender(appender);
            builder.setLevel(builderLevel);
            appender.stop();
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // the index files, decoded with the readers of this tree

    /**
     * What the index files of {@code sstable} hold that does not depend on when they were written, nor on the bytes
     * of the data file they index: the version; the component file names, without the sstable's own prefix, and
     * those its TOC lists; the byte order, the header (magic and version) and the footer (magic and algorithm id) of
     * each component, but not its checksum, which is validated instead; the group metadata decoded (value counts,
     * block sizes and offsets) but not the partition offsets themselves, which point into the data file; every row
     * id of the per-sstable primary key map with its key; the column metadata of each segment decoded (row counts,
     * row id bounds, key and term bounds, the components with their attribute names, and apart the root, offset and
     * length of each, which follow from the postings); and, apart from the facts, every term of the index with the
     * row ids and keys of its posting list.
     */
    private static IndexFiles describe(ColumnFamilyStore cfs, SSTableReader sstable) throws IOException
    {
        IndexFiles files = new IndexFiles();
        IndexContext context = index(cfs).getIndexContext();
        IndexDescriptor descriptor = IndexDescriptor.load(sstable, Set.of(context));
        IndexComponents.ForRead perSSTable = descriptor.perSSTableComponents();
        IndexComponents.ForRead perIndex = descriptor.perIndexComponents(context);
        assertThat(perSSTable.isComplete()).isTrue();
        assertThat(perIndex.isComplete()).isTrue();
        assertThat(perSSTable.isValid(true)).as("per-sstable components of " + sstable.descriptor).isTrue();
        assertThat(perIndex.isValid(true)).as("per-index components of " + sstable.descriptor).isTrue();
        assertThat(perIndex.version()).isEqualTo(perSSTable.version());

        SortedMap<String, String> facts = files.facts;
        facts.put("version", perSSTable.version().toString());
        facts.put("files", saiFileNames(sstable.descriptor).toString());
        facts.put("toc", tocSaiEntries(sstable.descriptor).toString());
        for (IndexComponents.ForRead components : List.of(perSSTable, perIndex))
        {
            facts.put(prefix(components) + "components", components.all()
                                                                   .stream()
                                                                   .map(c -> c.componentType().name())
                                                                   .sorted()
                                                                   .collect(Collectors.toList())
                                                                   .toString());
            for (IndexComponent.ForRead component : components.all())
                describeEnvelope(component, facts);
        }

        MetadataSource groupMeta = MetadataSource.loadMetadata(perSSTable);
        for (IndexComponent.ForRead component : perSSTable.all())
        {
            String name = "groupMeta." + component.componentType().name() + '.';
            switch (component.componentType())
            {
                case TOKEN_VALUES:
                case OFFSETS_VALUES:
                case PRIMARY_KEY_BLOCK_OFFSETS:
                    NumericValuesMeta values = new NumericValuesMeta(groupMeta.get(component));
                    facts.put(name + "valueCount", Long.toString(values.valueCount));
                    facts.put(name + "blockSize", Integer.toString(values.blockSize));
                    facts.put(name + "blockMetaOffset", Long.toString(values.blockMetaOffset));
                    break;
                case PRIMARY_KEY_BLOCKS:
                    SortedTermsMeta terms = new SortedTermsMeta(groupMeta.get(component));
                    facts.put(name + "count", Long.toString(terms.count));
                    facts.put(name + "maxTermLength", Integer.toString(terms.maxTermLength));
                    facts.put(name + "trieFP", Long.toString(terms.trieFP));
                    break;
                default:
                    // the completion marker, the metadata itself and the key trie have no entry of their own
            }
        }

        try (SSTableContext sstableContext = SSTableContext.create(sstable, perSSTable);
             PrimaryKeyMap primaryKeys = sstableContext.primaryKeyMapFactory().newPerSSTablePrimaryKeyMap())
        {
            long count = primaryKeys.count();
            facts.put("primaryKeys.count", Long.toString(count));
            List<String> keys = new ArrayList<>();
            for (long rowId = 0; rowId < count; rowId++)
            {
                files.keys.add(key(primaryKeys.primaryKeyFromRowId(rowId)));
                keys.add(rowId + "=" + files.keys.get((int) rowId));
            }
            facts.put("primaryKeys", keys.toString());

            List<SegmentMetadata> segments = SegmentMetadata.load(MetadataSource.loadMetadata(perIndex),
                                                                  context,
                                                                  sstableContext);
            facts.put("segments", Integer.toString(segments.size()));
            for (int i = 0; i < segments.size(); i++)
            {
                SegmentMetadata segment = segments.get(i);
                String name = "segment." + i + '.';
                facts.put(name + "version", segment.version.toString());
                facts.put(name + "segmentRowIdOffset", Long.toString(segment.segmentRowIdOffset));
                facts.put(name + "numRows", Long.toString(segment.numRows));
                facts.put(name + "minSSTableRowId", Long.toString(segment.minSSTableRowId));
                facts.put(name + "maxSSTableRowId", Long.toString(segment.maxSSTableRowId));
                facts.put(name + "totalTermCount", Long.toString(segment.totalTermCount));
                facts.put(name + "termsDistribution", String.valueOf(segment.termsDistribution));
                facts.put(name + "minKey", key(segment.minKey));
                facts.put(name + "maxKey", key(segment.maxKey));
                facts.put(name + "minTerm", UTF8Type.instance.compose(segment.minTerm));
                facts.put(name + "maxTerm", UTF8Type.instance.compose(segment.maxTerm));
                Map<String, Map<String, String>> components = new TreeMap<>(segment.componentMetadatas.asMap());
                facts.put(name + "components", components.entrySet()
                                                         .stream()
                                                         .map(e -> e.getKey() + new TreeSet<>(e.getValue().keySet()))
                                                         .collect(Collectors.toList())
                                                         .toString());
                facts.put(name + "componentLayout", components.toString());
                files.postings.putAll(postings(context, perIndex, segment, primaryKeys));
            }
            assertThat(segments).hasSize(1);
        }
        return files;
    }

    /** What {@link #describe} finds in the index files of an sstable. */
    private static final class IndexFiles
    {
        /** The facts compared one by one, by name. */
        private final SortedMap<String, String> facts = new TreeMap<>();
        /** The key of each row id of the per-sstable map. */
        private final List<String> keys = new ArrayList<>();
        /** Each term with its postings. */
        private final Map<String, List<Posting>> postings = new LinkedHashMap<>();

        private long numRows()
        {
            return Long.parseLong(facts.get("segment.0.numRows"));
        }

        private Set<Integer> partitionsOf(String term)
        {
            return postings.getOrDefault(term, List.of())
                           .stream()
                           .map(Posting::partition)
                           .collect(Collectors.toCollection(TreeSet::new));
        }

        /** Whether the row id of {@code posting} is the last of its partition in the per-sstable map. */
        private boolean isLastRowOfPartition(Posting posting)
        {
            int next = (int) posting.rowId + 1;
            return next == keys.size() || partitionOf(keys.get(next)) != posting.partition();
        }
    }

    /** The partition of a key as {@link #key} writes it. */
    private static int partitionOf(String key)
    {
        return Integer.parseInt(key.split(":")[0]);
    }

    /** A posting: an sstable row id and the key the per-sstable map gives it, {@code <id>[:<ck>]}. */
    private static final class Posting
    {
        private final long rowId;
        private final String key;

        private Posting(long rowId, String key)
        {
            this.rowId = rowId;
            this.key = key;
        }

        private int partition()
        {
            return partitionOf(key);
        }

        private String clustering()
        {
            String[] parts = key.split(":");
            return parts.length > 1 ? parts[1] : null;
        }

        @Override
        public boolean equals(Object o)
        {
            return o instanceof Posting && ((Posting) o).rowId == rowId && ((Posting) o).key.equals(key);
        }

        @Override
        public int hashCode()
        {
            return Long.hashCode(rowId) * 31 + key.hashCode();
        }

        @Override
        public String toString()
        {
            return rowId + "=" + key;
        }
    }

    /** The facts that differ between {@code ours} and {@code dse}, or that only one of them has, one line each. */
    private static List<String> differences(SortedMap<String, String> ours, SortedMap<String, String> dse)
    {
        SortedSet<String> names = new TreeSet<>(ours.keySet());
        names.addAll(dse.keySet());
        return names.stream()
                    .filter(name -> ours.containsKey(name) != dse.containsKey(name)
                                    || !Objects.equals(ours.get(name), dse.get(name)))
                    .map(name -> name + ": ours " + ours.get(name) + ", DSE's " + dse.get(name))
                    .collect(Collectors.toList());
    }

    /** {@code perSSTable.} or {@code perIndex.}, to name the facts of one group of components. */
    private static String prefix(IndexComponents components)
    {
        return components.isPerIndexGroup() ? "perIndex." : "perSSTable.";
    }

    /** The byte order, header and footer of a component file, and its length if it is a completion marker. */
    private static void describeEnvelope(IndexComponent.ForRead component, Map<String, String> facts)
    throws IOException
    {
        String name = prefix(component.parent()) + component.componentType().name() + '.';
        byte[] bytes = Files.readAllBytes(component.file().toPath());
        facts.put(name + "byteOrder", component.byteOrder().toString());
        if (component.isCompletionMarker())
        {
            facts.put(name + "length", Integer.toString(bytes.length));
            return;
        }
        assertThat(bytes.length).as(component.file().toString()).isGreaterThanOrEqualTo(HEADER_LENGTH + FOOTER_LENGTH);
        facts.put(name + "header", Hex.bytesToHex(Arrays.copyOfRange(bytes, 0, HEADER_LENGTH)));
        facts.put(name + "footer", Hex.bytesToHex(Arrays.copyOfRange(bytes,
                                                                      bytes.length - FOOTER_LENGTH,
                                                                      bytes.length - FOOTER_CHECKSUM_LENGTH)));
        try (IndexInput input = component.openInput())
        {
            facts.put(name + "headerVersion", SAICodecUtils.checkHeader(input).toString());
            SAICodecUtils.validateChecksum(input, component.parent().version());
        }
    }

    /** Each term of the segment, in the order of the index, with its postings. */
    private static Map<String, List<Posting>> postings(IndexContext context,
                                                       IndexComponents.ForRead perIndex,
                                                       SegmentMetadata segment,
                                                       PrimaryKeyMap primaryKeys) throws IOException
    {
        SegmentMetadata.ComponentMetadata termsData = segment.componentMetadatas.get(IndexComponentType.TERMS_DATA);
        String footerPointer = termsData.attributes.get(SAICodecUtils.FOOTER_POINTER);
        ByteComparable.Version encoding = perIndex.byteComparableVersionFor(IndexComponentType.TERMS_DATA);
        Map<String, List<Posting>> terms = new LinkedHashMap<>();
        FileHandle termsFile = perIndex.get(IndexComponentType.TERMS_DATA).createFileHandle();
        FileHandle postingsFile = null;
        TermsReader termsReader;
        try
        {
            postingsFile = perIndex.get(IndexComponentType.POSTING_LISTS).createFileHandle();
            termsReader = new TermsReader(context, termsFile, encoding, postingsFile, termsData.root,
                                          footerPointer == null ? -1 : Long.parseLong(footerPointer),
                                          perIndex.version());
        }
        catch (Throwable t)
        {
            // the reader owns the handles once built, and closes them
            FileUtils.closeQuietly(termsFile, postingsFile);
            throw t;
        }
        try (TermsReader reader = termsReader;
             TermsIterator iterator = reader.allTerms())
        {
            while (iterator.hasNext())
            {
                byte[] term = ByteSourceInverse.readBytes(iterator.next().asComparableBytes(encoding));
                List<Posting> postings = new ArrayList<>();
                try (PostingList list = iterator.postings())
                {
                    int posting;
                    while ((posting = list.nextPosting()) != PostingList.END_OF_STREAM)
                    {
                        long rowId = segment.segmentRowIdOffset + posting;
                        postings.add(new Posting(rowId, key(primaryKeys.primaryKeyFromRowId(rowId))));
                    }
                }
                terms.put(new String(term, StandardCharsets.UTF_8), postings);
            }
        }
        return terms;
    }

    /** A primary key as {@code <id>:<ck>}, {@code <id>} alone for a key without clustering. */
    private static String key(PrimaryKey key)
    {
        PrimaryKey loaded = key.loadDeferred();
        String id = Integer.toString(Int32Type.instance.compose(loaded.partitionKey().getKey()));
        if (loaded.clustering() == null || loaded.clustering().size() == 0)
            return id;
        return id + ':' + UTF8Type.instance.compose(loaded.clustering().bufferAt(0));
    }

    /** The names of the SAI files of {@code descriptor}, without the sstable's own prefix. */
    private static SortedSet<String> saiFileNames(Descriptor descriptor) throws IOException
    {
        return saiFileNames(descriptor.directory.toPath(), descriptor.filenamePart() + Descriptor.FILENAME_SEPARATOR);
    }

    /** The names of the SAI files under {@code directory} whose name starts with {@code prefix}, without it. */
    private static SortedSet<String> saiFileNames(Path directory, String prefix) throws IOException
    {
        try (Stream<Path> files = Files.list(directory))
        {
            return files.map(p -> p.getFileName().toString())
                        .filter(name -> name.startsWith(prefix))
                        .map(name -> name.substring(prefix.length()))
                        .filter(name -> name.startsWith("SAI"))
                        .collect(Collectors.toCollection(TreeSet::new));
        }
    }

    /** The SAI components the TOC of {@code descriptor} lists. */
    private static SortedSet<String> tocSaiEntries(Descriptor descriptor) throws IOException
    {
        return Files.readAllLines(descriptor.fileFor(Component.parse("TOC.txt", descriptor.version.format)).toPath())
                    .stream()
                    .filter(entry -> entry.startsWith("SAI"))
                    .collect(Collectors.toCollection(TreeSet::new));
    }

    /** A digest of every file directly under {@code directory} (subdirectories left out), by file name. */
    private static Map<String, String> digests(Path directory) throws IOException
    {
        Map<String, String> digests = new TreeMap<>();
        try (Stream<Path> files = Files.list(directory))
        {
            for (Path file : files.filter(Files::isRegularFile).collect(Collectors.toList()))
            {
                byte[] digest = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file));
                digests.put(file.getFileName().toString(), Hex.bytesToHex(digest));
            }
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new AssertionError(e);
        }
        return digests;
    }

    /** A digest of each SAI file of the fixture of {@code fixture}, by its name without the sstable's own prefix. */
    private static Map<String, String> fixtureSaiDigests(DseVersion fixture) throws IOException
    {
        Map<String, String> digests = new TreeMap<>();
        digests(DseFixtures.sstableDirectory(fixture)).forEach((name, digest) -> {
            if (name.startsWith(FIXTURE_PREFIX + "SAI"))
                digests.put(name.substring(FIXTURE_PREFIX.length()), digest);
        });
        return digests;
    }

    /** A digest of each SAI file of {@code descriptor}, by its name without the sstable's own prefix. */
    private static Map<String, String> digests(Descriptor descriptor) throws IOException
    {
        String prefix = descriptor.filenamePart() + Descriptor.FILENAME_SEPARATOR;
        Map<String, String> digests = new TreeMap<>();
        digests(descriptor.directory.toPath()).forEach((name, digest) -> {
            if (name.startsWith(prefix + "SAI"))
                digests.put(name.substring(prefix.length()), digest);
        });
        return digests;
    }

    // ---------------------------------------------------------------------------------------------------------------

    /** The SAI version {@link Dse6SaiVersionPin} pins a node upgraded from the DSE release of {@code fixture} to. */
    private static Version pinOf(DseVersion fixture)
    {
        return Version.parse(Dse6SaiVersionPin.saiVersionOf(new CassandraVersion(fixture.releaseVersion)));
    }

    private static ColumnFamilyStore table()
    {
        return Keyspace.open(KEYSPACE_NAME).getColumnFamilyStore(TABLE_NAME);
    }

    private static StorageAttachedIndex index(ColumnFamilyStore cfs)
    {
        return (StorageAttachedIndex) cfs.indexManager.getIndexByName(INDEX_NAME);
    }
}
