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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.agrona.collections.Int2IntHashMap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.LikePatterns;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.kdtree.KDTreeIndexBuilder;
import org.apache.cassandra.index.sai.disk.v1.trie.InvertedIndexWriter;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.AutomatonQueries;
import org.apache.cassandra.index.sai.utils.AutomatonTermsExceededException;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import static org.apache.cassandra.index.sai.metrics.QueryEventListeners.NO_OP_TRIE_LISTENER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Disk-level tests for the automaton term-matching engine serving the non-prefix LIKE variants ({@code TermsReader#intersect} and
 * {@code InvertedIndexSearcher#searchAutomaton}): builds real per-segment inverted indexes from synthetic
 * term/rowid data and compares the engine results with a brute-force reference that runs the same automaton
 * over every term.
 */
public class AutomatonIntersectionTest extends SaiRandomizedTest
{
    private static final int NO_CAP = Integer.MAX_VALUE;

    private final Version version;

    @ParametersFactory()
    public static Collection<Object[]> data()
    {
        // Required because it configures SEGMENT_BUILD_MEMORY_LIMIT, which is needed for Version.AA
        if (DatabaseDescriptor.getRawConfig() == null)
            DatabaseDescriptor.setConfig(DatabaseDescriptor.loadConfig());
        return Version.ALL.stream().map(v -> new Object[]{v}).collect(Collectors.toList());
    }

    @BeforeClass
    public static void setupPartitioner()
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Before
    public void setCurrentSAIVersion()
    {
        SAIUtil.setCurrentVersion(version);
    }

    public AutomatonIntersectionTest(Version version)
    {
        this.version = version;
    }

    @Test
    public void testLikePatterns() throws IOException
    {
        Map<String, TreeSet<Integer>> dictionary = dictionary("abc", "abcabc", "xabcx", "a%b", "a_c", "ab", "", "zzz");
        withIndex(dictionary, fixture ->
        {
            for (String pattern : new String[]{ "abc%", "%abc", "%abc%", "a%c", "%", "abc", "a_c", "a%%c" })
                assertEngineMatchesBruteForce(fixture, dictionary, LikePatterns.fromLikePattern(pattern), "LIKE " + pattern);
        });
    }

    @Test
    public void testEmptyPatternEdgeCases() throws IOException
    {
        Map<String, TreeSet<Integer>> dictionary = dictionary("a", "b", "ab");
        withIndex(dictionary, fixture ->
        {
            // an empty LIKE pattern accepts only the empty term, which is not in the dictionary
            assertEngineMatchesBruteForce(fixture, dictionary, LikePatterns.fromLikePattern(""), "empty LIKE");
            // empty suffix/contains automatons accept every term
            assertEngineMatchesBruteForce(fixture, dictionary, LikePatterns.suffixAutomaton(""), "empty suffix");
            assertEngineMatchesBruteForce(fixture, dictionary, LikePatterns.containsAutomaton(""), "empty contains");
            // the empty language matches nothing at all
            assertEngineMatchesBruteForce(fixture, dictionary, LikePatterns.compile(Automata.makeEmpty()), "empty language");
        });
    }

    @Test
    public void testCommonPrefixBoundingCorrectness() throws IOException
    {
        // Terms straddling the [enc("ab"), enc("ac")) scan bounds of a LIKE 'ab%'-style automaton: "aa<U+FFFF>" and "ab"
        // sit just below/at the lower bound, "ab<U+FFFF>..." just below the upper bound, "ac" exactly at the
        // (exclusive) upper bound and "ac\0-like" terms just above it.
        Map<String, TreeSet<Integer>> dictionary = dictionary("aa", "aa\uFFFF", "ab", "ab\0", "ab\uFFFF\uFFFF",
                                                              "abz", "ac", "aca", "b", "");
        withIndex(dictionary, fixture ->
        {
            CompiledAutomaton automaton = LikePatterns.fromLikePattern("ab%");
            assertEngineMatchesBruteForce(fixture, dictionary, automaton, "prefix bounds ab%");

            var results = intersect(fixture, automaton, NO_CAP);
            var expected = expectedRowIds(dictionary, automaton);
            // guard against a vacuous comparison: exactly the four "ab"-prefixed terms must have matched
            assertEquals(union(dictionary, "ab", "ab\0", "ab\uFFFF\uFFFF", "abz"), expected);
            assertEquals(expected, results);
        });
    }

    @Test
    public void testVisitedTermCapTriggering() throws IOException
    {
        Map<String, TreeSet<Integer>> dictionary = new TreeMap<>();
        int rowId = 0;
        for (int i = 0; i < 100; i++)
            dictionary.computeIfAbsent(String.format("a%03d", i), k -> new TreeSet<>()).add(rowId++);
        dictionary.computeIfAbsent("b", k -> new TreeSet<>()).add(rowId++);

        withIndex(dictionary, fixture ->
        {
            CompiledAutomaton broad = LikePatterns.fromLikePattern("a%");
            // just enough budget: 100 terms visited in the bounded range
            assertEquals(expectedRowIds(dictionary, broad), intersect(fixture, broad, 100));
            // one short of the required budget
            assertThrows(AutomatonTermsExceededException.class, () -> intersect(fixture, broad, 99));

            // the cap applies to *visited* terms, so even an automaton matching nothing in a broad scan range trips it
            CompiledAutomaton selective = LikePatterns.fromLikePattern("a%zzz");
            assertEquals(expectedRowIds(dictionary, selective), intersect(fixture, selective, NO_CAP));
            assertThrows(AutomatonTermsExceededException.class, () -> intersect(fixture, selective, 10));

            // a match-all automaton scans the whole dictionary
            CompiledAutomaton all = LikePatterns.fromLikePattern("%");
            assertEquals(CompiledAutomaton.AUTOMATON_TYPE.ALL, all.type);
            assertEquals(expectedRowIds(dictionary, all), intersect(fixture, all, 101));
            assertThrows(AutomatonTermsExceededException.class, () -> intersect(fixture, all, 100));
        });
    }

    /**
     * Pins the seek-skipping optimization, not just its correctness: over a dictionary whose scanned range is
     * much larger than the set of viable candidates, the automaton-guided intersection must visit materially fewer
     * terms than the range contains. The bounded scan this replaced visited every term of the range (26 * 400 here).
     */
    @Test
    public void testSeekSkippingVisitsMateriallyFewerTerms() throws IOException
    {
        // 26 letter blocks x 400 numbered terms; the <any-char>0100<any-string> automaton accepts exactly one term
        // per block, and the automaton
        // has no common prefix, so the scanned range is the whole 10_400-term dictionary
        Map<String, TreeSet<Integer>> dictionary = new TreeMap<>();
        int rowId = 0;
        for (char letter = 'a'; letter <= 'z'; letter++)
            for (int i = 0; i < 400; i++)
                dictionary.computeIfAbsent(letter + String.format("%04d", i), k -> new TreeSet<>()).add(rowId++);

        withIndex(dictionary, fixture ->
        {
            // any single character, then "0100", then anything — the same shape as Lucene's "?0100*" wildcard,
            // composed directly since the LIKE surface has no single-character wildcard
            CompiledAutomaton automaton = LikePatterns.compile(
                Operations.concatenate(List.of(Automata.makeAnyChar(), Automata.makeString("0100"), Automata.makeAnyString())));
            assertEquals(CompiledAutomaton.AUTOMATON_TYPE.NORMAL, automaton.type);

            AutomatonQueries.ExpansionsBudget budget = new AutomatonQueries.ExpansionsBudget(NO_CAP);
            TreeSet<Integer> results = intersect(fixture, automaton, budget);

            // correctness first (and a guard against a vacuous comparison: exactly one match per letter block)
            assertEquals(expectedRowIds(dictionary, automaton), results);
            assertEquals(26, results.size());

            // the pin: the guided walk must visit far fewer terms than the scanned range size
            int visited = budget.visitedTerms();
            assertTrue("expected at least the 26 accepted terms to be visited, but visited only " + visited,
                       visited >= 26);
            assertTrue("seek-skipping regressed: visited " + visited + " of " + dictionary.size() +
                       " terms in the scanned range (expected at most " + dictionary.size() / 20 + ')',
                       visited <= dictionary.size() / 20);
        });
    }

    /**
     * The seek-skipping walk must degrade to (budget-guarded) sequential scanning for automatons that cannot
     * skip, and its visited-term accounting must equal the bounded-scan behaviour there: a suffix pattern
     * ({@code LIKE '%zzz'}) puts every term of an {@code a}-block dictionary inside a linear (loop) stretch,
     * so every term is visited exactly once.
     */
    @Test
    public void testNonSkippablePatternStillVisitsEveryTerm() throws IOException
    {
        Map<String, TreeSet<Integer>> dictionary = new TreeMap<>();
        int rowId = 0;
        for (int i = 0; i < 200; i++)
            dictionary.computeIfAbsent(String.format("a%03d", i), k -> new TreeSet<>()).add(rowId++);
        dictionary.computeIfAbsent("a042zzz", k -> new TreeSet<>()).add(rowId++);

        withIndex(dictionary, fixture ->
        {
            CompiledAutomaton suffix = LikePatterns.suffixAutomaton("zzz");
            assertEquals(CompiledAutomaton.AUTOMATON_TYPE.NORMAL, suffix.type);

            AutomatonQueries.ExpansionsBudget budget = new AutomatonQueries.ExpansionsBudget(NO_CAP);
            assertEquals(expectedRowIds(dictionary, suffix), intersect(fixture, suffix, budget));
            assertEquals(dictionary.size(), budget.visitedTerms());
        });
    }

    @Test
    public void testSharedBudgetSpansMultipleIntersections() throws IOException
    {
        Map<String, TreeSet<Integer>> dictionary = new TreeMap<>();
        int rowId = 0;
        for (int i = 0; i < 10; i++)
            dictionary.computeIfAbsent(String.format("a%02d", i), k -> new TreeSet<>()).add(rowId++);

        withIndex(dictionary, fixture ->
        {
            CompiledAutomaton broad = LikePatterns.fromLikePattern("a%");

            // The query path shares one budget per query (QueryContext#automatonExpansionsBudget) across every
            // segment intersection: a budget covering one 10-term scan but not two is exhausted by the second
            // scan (as it would be by the second segment of the same query), and the failure carries the
            // budget's total limit, not the per-scan count.
            AutomatonQueries.ExpansionsBudget budget = new AutomatonQueries.ExpansionsBudget(15);
            assertEquals(expectedRowIds(dictionary, broad), intersect(fixture, broad, budget));
            try
            {
                intersect(fixture, broad, budget);
                fail("the second intersection must exhaust the shared budget");
            }
            catch (AutomatonTermsExceededException e)
            {
                assertEquals(15, e.maxVisitedTerms());
            }

            // fresh per-call budgets (the engine-level int-cap overloads) are unaffected by earlier scans
            assertEquals(expectedRowIds(dictionary, broad), intersect(fixture, broad, 10));
            assertEquals(expectedRowIds(dictionary, broad), intersect(fixture, broad, 10));
        });
    }

    /**
     * If the second postings input open fails, the first one must be closed on the way out instead of leaking
     * (TermsReader used to open both before the try block whose catch closes them). Exercised for all the
     * two-open scan paths: the automaton intersection ({@code scanFilterAndMergePostings}), the exact term
     * lookup ({@code TermQuery}'s constructor) and the range scan merge ({@code readAndMergePostings}).
     */
    @Test
    public void testFirstPostingsInputClosedWhenSecondOpenFails() throws IOException
    {
        Map<String, TreeSet<Integer>> dictionary = dictionary("alpha", "alps", "beta", "gamma");
        withIndex(dictionary, fixture ->
        {
            CompiledAutomaton automaton = LikePatterns.fromLikePattern("al%");
            assertEquals(CompiledAutomaton.AUTOMATON_TYPE.NORMAL, automaton.type);

            // automaton intersection: scanFilterAndMergePostings
            expectInjectedOpenFailure(() -> intersect(fixture, automaton, NO_CAP));

            // exact match: the two opens in TermQuery's constructor
            ByteComparable term = version.onDiskFormat().encodeForTrie(UTF8Type.instance.decompose("alpha"), UTF8Type.instance);
            expectInjectedOpenFailure(() -> fixture.reader.exactMatch(term,
                                                                      (QueryEventListener.TrieIndexEventListener) NO_OP_TRIE_LISTENER,
                                                                      new QueryContext()));

            // unfiltered range scan: readAndMergePostings
            expectInjectedOpenFailure(() -> fixture.reader.rangeMatch(null, null, null,
                                                                      (QueryEventListener.TrieIndexEventListener) NO_OP_TRIE_LISTENER,
                                                                      new QueryContext()));
        });
    }

    /**
     * Runs the given scan with an {@link IndexFileUtils} override failing the second {@code openInput} and
     * asserts that the scan fails with the injected error and that the input opened before the failure is closed.
     */
    private static void expectInjectedOpenFailure(FailingScan scan) throws IOException
    {
        FailingIndexFileUtils tracking = new FailingIndexFileUtils(2);
        IndexFileUtils.setOverrideInstance(tracking);
        try
        {
            scan.run();
            fail("expected the injected openInput failure to propagate");
        }
        catch (RuntimeException e)
        {
            assertEquals("injected openInput failure", e.getMessage());
        }
        finally
        {
            IndexFileUtils.setOverrideInstance(null);
        }
        assertEquals("exactly one input must have been opened before the injected failure", 1, tracking.closedFlags.size());
        assertTrue("the input opened before the injected failure must be closed, not leaked", tracking.closedFlags.get(0).get());
    }

    private interface FailingScan
    {
        Object run() throws IOException;
    }

    /**
     * An {@link IndexFileUtils} override that tracks the close state of every input it opens and fails the n-th
     * open (mirroring the fault-injection seam of {@code AggregationPushdownTest}).
     */
    private static class FailingIndexFileUtils extends IndexFileUtils
    {
        final List<AtomicBoolean> closedFlags = new ArrayList<>();
        private final int failOnNthOpen;
        private int opens = 0;

        FailingIndexFileUtils(int failOnNthOpen)
        {
            super(IndexFileUtils.DEFAULT_WRITER_OPTION);
            this.failOnNthOpen = failOnNthOpen;
        }

        @Override
        public IndexInputReader openInput(FileHandle handle)
        {
            if (++opens == failOnNthOpen)
                throw new RuntimeException("injected openInput failure");
            RandomAccessReader reader = handle.createReader();
            AtomicBoolean closed = new AtomicBoolean();
            closedFlags.add(closed);
            return IndexInputReader.create(reader, () -> closed.set(true));
        }
    }

    @Test
    public void testUnionAcrossManyMatchingTerms() throws IOException
    {
        Map<String, TreeSet<Integer>> dictionary = new TreeMap<>();
        // 512 matching terms with interleaved and partially overlapping posting lists
        for (int i = 0; i < 512; i++)
        {
            TreeSet<Integer> postings = new TreeSet<>();
            postings.add(i);
            postings.add(1000 + (i % 7));
            postings.add(2000 + i * 3);
            dictionary.put(String.format("match%05d", i), postings);
        }
        for (int i = 0; i < 100; i++)
            dictionary.put(String.format("other%05d", i), new TreeSet<>(List.of(5000 + i)));

        withIndex(dictionary, fixture ->
        {
            CompiledAutomaton automaton = LikePatterns.fromLikePattern("match%");
            var results = intersect(fixture, automaton, NO_CAP);
            var expected = expectedRowIds(dictionary, automaton);
            assertEquals(512 + 7 + 512, expected.size());
            assertEquals(expected, results);
        });
    }

    @Test
    public void testRandomizedTenThousandTermsVsBruteForce() throws IOException
    {
        Map<String, TreeSet<Integer>> dictionary = new TreeMap<>();
        int rowId = 0;
        while (dictionary.size() < 10_000)
        {
            String term = randomTerm();
            TreeSet<Integer> postings = dictionary.computeIfAbsent(term, k -> new TreeSet<>());
            for (int p = randomIntBetween(1, 3); p > 0; p--)
                postings.add(rowId++);
        }
        List<String> terms = new ArrayList<>(dictionary.keySet());

        withIndex(dictionary, fixture ->
        {
            for (int i = 0; i < 30; i++)
            {
                String seed = terms.get(nextInt(terms.size()));
                CompiledAutomaton automaton = randomAutomaton(seed);
                assertEngineMatchesBruteForce(fixture, dictionary, automaton, "randomized " + i);
            }
        });
    }

    @Test
    public void testSearcherEntryPoint() throws Exception
    {
        Map<String, TreeSet<Integer>> dictionary = dictionary("alpha", "alps", "beta", "alcove", "gamma");
        // build a full searcher fixture (not just the reader) to exercise the additive entry point
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final IndexContext indexContext = SAITester.createIndexContext(newIndex(), UTF8Type.instance);

        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        SegmentMetadataBuilder metadataBuilder = new SegmentMetadataBuilder(0, components);
        metadataBuilder.setRowIdRange(0, Long.MAX_VALUE);
        metadataBuilder.setKeyRange(SAITester.TEST_FACTORY.createTokenOnly(DatabaseDescriptor.getPartitioner().getMinimumToken()),
                                    SAITester.TEST_FACTORY.createTokenOnly(DatabaseDescriptor.getPartitioner().getMaximumToken()));
        var entries = toTermEntries(dictionary);
        metadataBuilder.setTermRange(UTF8Type.instance.fromString("alcove"), UTF8Type.instance.fromString("gamma"));

        try (InvertedIndexWriter writer = new InvertedIndexWriter(components))
        {
            MemtableTermsIterator termsIterator = new MemtableTermsIterator(null, null, entries.iterator());
            SegmentMetadata.ComponentMetadataMap indexMetas = writer.writeAll(metadataBuilder.intercept(termsIterator), docLengths(dictionary));
            metadataBuilder.setComponentsMetadata(indexMetas);
            metadataBuilder.setNumRows(dictionary.values().stream().mapToInt(TreeSet::size).sum());
        }
        final SegmentMetadata segmentMetadata = metadataBuilder.build();

        try (PerIndexFiles indexFiles = new PerIndexFiles(components))
        {
            var sstableContext = mock(org.apache.cassandra.index.sai.SSTableContext.class);
            when(sstableContext.primaryKeyMapFactory()).thenReturn(KDTreeIndexBuilder.TEST_PRIMARY_KEY_MAP_FACTORY);
            when(sstableContext.usedPerSSTableComponents()).thenReturn(indexDescriptor.perSSTableComponents());
            try (IndexSearcher searcher = version.onDiskFormat().newIndexSearcher(sstableContext, indexContext, indexFiles, segmentMetadata))
            {
                assertTrue(searcher instanceof InvertedIndexSearcher);
                CompiledAutomaton automaton = LikePatterns.fromLikePattern("al%");
                TreeSet<Integer> expected = expectedRowIds(dictionary, automaton);
                assertEquals(union(dictionary, "alpha", "alps", "alcove"), expected);

                TreeSet<Integer> actual = new TreeSet<>();
                try (KeyRangeIterator results = ((InvertedIndexSearcher) searcher).searchAutomaton(automaton, new QueryContext()))
                {
                    // tokens are equal to their corresponding row ids with the test primary key map factory
                    while (results.hasNext())
                        actual.add((int) results.next().token().getLongValue());
                }
                assertEquals(expected, actual);
            }
        }
    }

    /**
     * A fixture holding an open {@link TermsReader} over an index built from a synthetic dictionary.
     */
    private static class Fixture
    {
        final TermsReader reader;

        Fixture(TermsReader reader)
        {
            this.reader = reader;
        }
    }

    private interface FixtureConsumer
    {
        void accept(Fixture fixture) throws IOException;
    }

    /**
     * Builds a real per-segment inverted index from the dictionary and runs the given assertion block against an
     * open {@link TermsReader}.
     */
    private void withIndex(Map<String, TreeSet<Integer>> dictionary, FixtureConsumer test) throws IOException
    {
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final IndexContext indexContext = SAITester.createIndexContext(newIndex(), UTF8Type.instance);
        var entries = toTermEntries(dictionary);

        SegmentMetadata.ComponentMetadataMap indexMetas;
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (InvertedIndexWriter writer = new InvertedIndexWriter(components))
        {
            indexMetas = writer.writeAll(new MemtableTermsIterator(null, null, entries.iterator()), docLengths(dictionary));
        }

        FileHandle termsData = components.get(IndexComponentType.TERMS_DATA).createFileHandle();
        FileHandle postingLists = components.get(IndexComponentType.POSTING_LISTS).createFileHandle();
        long termsFooterPointer = Long.parseLong(indexMetas.get(IndexComponentType.TERMS_DATA).attributes.get(SAICodecUtils.FOOTER_POINTER));

        try (TermsReader reader = new TermsReader(indexContext,
                                                  termsData,
                                                  components.byteComparableVersionFor(IndexComponentType.TERMS_DATA),
                                                  postingLists,
                                                  indexMetas.get(IndexComponentType.TERMS_DATA).root,
                                                  termsFooterPointer,
                                                  version))
        {
            test.accept(new Fixture(reader));
        }
    }

    /**
     * Converts the dictionary into trie-writer entries, sorted by the raw unsigned UTF-8 bytes of the terms:
     * that is the order the trie writer requires and the order the terms dictionary iterates in. Note that this
     * differs from {@link String} order for terms containing surrogate pairs (astral plane characters).
     */
    private List<Pair<ByteComparable.Preencoded, List<RowMapping.RowIdWithFrequency>>> toTermEntries(Map<String, TreeSet<Integer>> dictionary)
    {
        return dictionary.entrySet()
                         .stream()
                         .map(e -> Pair.create(utf8Bytes(e.getKey()), e.getValue()))
                         .sorted((a, b) -> compareUnsigned(a.left, b.left))
                         .map(e -> Pair.create(version.onDiskFormat()
                                                      .encodeForTrie(ByteBuffer.wrap(e.left), UTF8Type.instance)
                                                      .preencode(TypeUtil.BYTE_COMPARABLE_VERSION),
                                               e.right.stream().map(r -> new RowMapping.RowIdWithFrequency(r, 1)).collect(Collectors.toList())))
                         .collect(Collectors.toList());
    }

    private static Int2IntHashMap docLengths(Map<String, TreeSet<Integer>> dictionary)
    {
        Int2IntHashMap docLengths = new Int2IntHashMap(Integer.MIN_VALUE);
        for (TreeSet<Integer> postings : dictionary.values())
            for (int rowId : postings)
                docLengths.put(rowId, 1);
        return docLengths;
    }

    /** Runs the engine and collects the resulting row ids. */
    private TreeSet<Integer> intersect(Fixture fixture, CompiledAutomaton automaton, int maxVisitedTerms) throws IOException
    {
        return intersect(fixture, automaton, new AutomatonQueries.ExpansionsBudget(maxVisitedTerms));
    }

    /** Same as {@link #intersect(Fixture, CompiledAutomaton, int)}, consuming the given (possibly shared) budget. */
    private TreeSet<Integer> intersect(Fixture fixture, CompiledAutomaton automaton, AutomatonQueries.ExpansionsBudget budget) throws IOException
    {
        TreeSet<Integer> results = new TreeSet<>();
        try (PostingList postings = fixture.reader.intersect(automaton,
                                                             budget,
                                                             (QueryEventListener.TrieIndexEventListener) NO_OP_TRIE_LISTENER,
                                                             new QueryContext()))
        {
            assertNotNull(postings);
            for (long rowId = postings.nextPosting(); rowId != PostingList.END_OF_STREAM; rowId = postings.nextPosting())
                results.add((int) rowId);
        }
        return results;
    }

    /** Brute-force reference: run the automaton over the UTF-8 bytes of every dictionary term. */
    private static TreeSet<Integer> expectedRowIds(Map<String, TreeSet<Integer>> dictionary, CompiledAutomaton automaton)
    {
        TreeSet<Integer> expected = new TreeSet<>();
        for (Map.Entry<String, TreeSet<Integer>> entry : dictionary.entrySet())
            if (LikePatterns.accepts(automaton, utf8Bytes(entry.getKey())))
                expected.addAll(entry.getValue());
        return expected;
    }

    private void assertEngineMatchesBruteForce(Fixture fixture,
                                               Map<String, TreeSet<Integer>> dictionary,
                                               CompiledAutomaton automaton,
                                               String description) throws IOException
    {
        assertEquals("engine and brute-force results differ for " + description,
                     expectedRowIds(dictionary, automaton),
                     intersect(fixture, automaton, NO_CAP));
    }

    /** Builds a dictionary mapping each distinct term to a singleton posting of its position. */
    private static Map<String, TreeSet<Integer>> dictionary(String... terms)
    {
        Map<String, TreeSet<Integer>> dictionary = new TreeMap<>();
        for (int i = 0; i < terms.length; i++)
            dictionary.computeIfAbsent(terms[i], k -> new TreeSet<>()).add(i);
        return dictionary;
    }

    private static TreeSet<Integer> union(Map<String, TreeSet<Integer>> dictionary, String... terms)
    {
        TreeSet<Integer> union = new TreeSet<>();
        for (String term : terms)
            union.addAll(dictionary.get(term));
        return union;
    }

    private static byte[] utf8Bytes(String term)
    {
        return term.getBytes(StandardCharsets.UTF_8);
    }

    private static int compareUnsigned(byte[] a, byte[] b)
    {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++)
        {
            int cmp = Integer.compare(a[i] & 0xFF, b[i] & 0xFF);
            if (cmp != 0)
                return cmp;
        }
        return Integer.compare(a.length, b.length);
    }

    /** Random term mixing plain ascii with accented, CJK and astral-plane characters. */
    private String randomTerm()
    {
        StringBuilder sb = new StringBuilder();
        int length = randomIntBetween(1, 12);
        for (int i = 0; i < length; i++)
        {
            switch (nextInt(10))
            {
                case 0:
                    sb.append("àéîöü".charAt(nextInt(5)));
                    break;
                case 1:
                    sb.append("日本語中文".charAt(nextInt(5)));
                    break;
                case 2:
                    sb.appendCodePoint(0x1F600 + nextInt(16)); // astral plane emoji
                    break;
                default:
                    sb.append((char) ('a' + nextInt(6))); // deliberately small alphabet to force common prefixes
            }
        }
        return sb.toString();
    }

    /** Random automaton derived from an existing dictionary term so that queries frequently match. */
    private CompiledAutomaton randomAutomaton(String seed)
    {
        switch (nextInt(5))
        {
            case 0: // prefix wildcard
                return LikePatterns.fromLikePattern(seed.substring(0, seed.offsetByCodePoints(0, randomIntBetween(1, Math.max(1, seed.codePointCount(0, seed.length()) - 1)))) + '%');
            case 1: // contains
                return LikePatterns.fromLikePattern('%' + randomSubstring(seed) + '%');
            case 2: // suffix
                return LikePatterns.fromLikePattern('%' + randomSubstring(seed));
            case 3: // suffix (trimmed-value factory, embedded '%' would be literal)
                return LikePatterns.suffixAutomaton(randomSubstring(seed));
            default: // generic two-segment pattern, replacing a random code point with an any-string wildcard
            {
                int cpCount = seed.codePointCount(0, seed.length());
                int cpIndex = nextInt(cpCount);
                int start = seed.offsetByCodePoints(0, cpIndex);
                int end = seed.offsetByCodePoints(start, 1);
                return LikePatterns.fromLikePattern(seed.substring(0, start) + '%' + seed.substring(end));
            }
        }
    }

    private String randomSubstring(String seed)
    {
        int cpCount = seed.codePointCount(0, seed.length());
        int fromCp = nextInt(cpCount);
        int toCp = randomIntBetween(fromCp + 1, cpCount);
        return seed.substring(seed.offsetByCodePoints(0, fromCp), seed.offsetByCodePoints(0, toCp));
    }
}
