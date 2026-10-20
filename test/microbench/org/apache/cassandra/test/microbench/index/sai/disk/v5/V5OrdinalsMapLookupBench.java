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

package org.apache.cassandra.test.microbench.index.sai.disk.v5;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.v5.V5OnDiskOrdinalsMap;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.vector.OrdinalsView;
import org.apache.cassandra.index.sai.disk.vector.RowIdsView;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Microbenchmark for the query-time ordinal↔rowId mapping in {@link V5OnDiskOrdinalsMap}.
 *
 * <p>Covers both {@link V5VectorPostingsWriter.Structure#ZERO_OR_ONE_TO_MANY} (on-disk seeks)
 * and {@link V5VectorPostingsWriter.Structure#ONE_TO_MANY} (in-memory lookup after open).
 * Trials combining {@code ONE_TO_MANY} with {@code emptyRowRatio > 0} are skipped because
 * that structure requires every row to have a vector.
 *
 * <p>Each benchmark performs {@value #LOOKUPS_PER_INVOCATION} random probes per invocation.
 *
 * <p>Example invocations:
 * <pre>
 *   # ZERO_OR_ONE_TO_MANY (on-disk seeks)
 *   ant microbench-test -Dbenchmark.name=V5OrdinalsMapLookupBench \
 *       "-Djmh.args=-p structure=ZERO_OR_ONE_TO_MANY -p emptyRowRatio=0.001,0.1"
 *
 *   # ONE_TO_MANY (in-memory; emptyRowRatio must be 0)
 *   ant microbench-test -Dbenchmark.name=V5OrdinalsMapLookupBench \
 *       "-Djmh.args=-p structure=ONE_TO_MANY -p emptyRowRatio=0.0"
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class V5OrdinalsMapLookupBench
{
    static
    {
        DatabaseDescriptor.toolInitialization();
    }

    static final int LOOKUPS_PER_INVOCATION = 10_000;

    /**
     * Encoding structure under test.
     * {@code ONE_TO_MANY} trials with {@code emptyRowRatio > 0} are skipped because
     * that structure requires every row to have a vector.
     */
    @Param({ "ZERO_OR_ONE_TO_MANY", "ONE_TO_MANY" })
    public String structure;

    /** Total number of rows in the segment; also the size of the ordinal slot space. */
    @Param({ "100000", "1000000" })
    public int numRows;

    /**
     * Fraction of ordinal slots (out of {@link #numRows}) that are empty — i.e. have no
     * row assigned to them.  Matches the "hole density" metric used by
     * {@link V5VectorPostingsWriter#tooManyOrdinalMappingHoles}.
     * The non-empty ordinal slots absorb all vector-bearing rows, so the average fanout
     * per non-empty ordinal is {@code 1 / (1 - ordinalHoleRatio)}:
     * <ul>
     *   <li>{@code 0.0}  – no holes; every ordinal maps to exactly one row.</li>
     *   <li>{@code 0.01} – 1 % of ordinals are empty; ≈ 1.01 rows/ordinal.</li>     
     *   <li>{@code 0.9}  – 90 % of ordinals are empty; ≈ 10 rows/ordinal on average.</li>
     * </ul>
     */
    @Param({ "0.0", "0.01", "0.99" })
    public double ordinalHoleRatio;

    /**
     * Fraction of rows (out of {@link #numRows}) that have no vector at all (deleted rows,
     * or rows from a non-indexed column).  These rowIds never appear in any ordinal's list.
     * <ul>
     *   <li>{@code 0.001} – minimal; 0.1 % of rows have no vector.</li>
     *   <li>{@code 0.1}   – 10 % of rows have no vector (heavy-delete scenario).</li>
     * </ul>
     */
    @Param({ "0.0", "0.01", "0.99" })
    public double emptyRowRatio;

    // ---- trial-level state ----

    private File postingsFile;
    private FileHandle fileHandle;
    private V5OnDiskOrdinalsMap ordinalsMap;

    /** Pre-computed random ordinals for ordinalToRowIds probes. */
    private int[] ordinalProbes;
    /** Pre-computed random rowIds for rowIdToOrdinal probes. */
    private int[] rowIdProbes;

    private final Random random = new Random(42);

    @Setup(Level.Trial)
    public void setup() throws IOException
    {
        V5VectorPostingsWriter.Structure enc = V5VectorPostingsWriter.Structure.valueOf(structure);
        if (enc == V5VectorPostingsWriter.Structure.ONE_TO_MANY && emptyRowRatio > 0)
            throw new IllegalStateException(
                    "ONE_TO_MANY requires every row to have a vector; skipping emptyRowRatio=" + emptyRowRatio);

        postingsFile = FileUtils.createTempFile("V5OrdinalsMapLookupBench", ".postings");

        // ---- compute segment dimensions ----
        //
        // 1. A random subset of the row slots become vector ordinals; ordinal o has primary rowId o.
        // 2. Every other row is attached to a random existing ordinal, except for emptyRowRatio of them,
        //    which are left without a vector.
        // 3. ZERO_OR_ONE_TO_MANY only: renumber the ordinals so they are dense.
        //
        // maxRowId = numRows - 1

        final int maxRowId = numRows - 1;

        final int[] sparseOrdinals = pickOrdinals();
        final IntArrayList[] sparseRowIds = mapRowsToOrdinals(sparseOrdinals);
        final boolean dense = enc == V5VectorPostingsWriter.Structure.ZERO_OR_ONE_TO_MANY;
        final IntArrayList[] rowIdsPerOrdinal = dense ? densify(sparseRowIds) : sparseRowIds;
        final int[] ordinals = dense ? IntStream.range(0, rowIdsPerOrdinal.length).toArray() : sparseOrdinals;
        final int maxVectorOrdinal = ordinals[ordinals.length - 1];

        // ---- write to disk ----
        long start;
        long end;
        try (SequentialWriter writer = new SequentialWriter(
                postingsFile,
                SequentialWriterOption.newBuilder().finishOnClose(true).build()))
        {
            start = writer.position();
            V5VectorPostingsWriter pw = V5VectorPostingsWriter
                    .create(maxVectorOrdinal, i -> rowIdsPerOrdinal[i])
                    .structure(enc)
                    .maxRowId(maxRowId)
                    .skipRowIdSorting();
            end = pw.writeTo(writer);
        }

        // ---- open for reading ----
        fileHandle = new FileHandle.Builder(postingsFile)
                .withCompressionMetadata(null)
                .complete();
        ordinalsMap = new V5OnDiskOrdinalsMap(fileHandle, start, end - start);

        // ---- pre-compute random probe arrays ----
        ThreadLocalRandom rng = ThreadLocalRandom.current();

        ordinalProbes = new int[LOOKUPS_PER_INVOCATION];
        for (int i = 0; i < LOOKUPS_PER_INVOCATION; i++)
            ordinalProbes[i] = ordinals[rng.nextInt(ordinals.length)];

        // Probe across the full rowId space (vector rows + empty rows)
        rowIdProbes = new int[LOOKUPS_PER_INVOCATION];
        for (int i = 0; i < LOOKUPS_PER_INVOCATION; i++)
            rowIdProbes[i] = rng.nextInt(maxRowId + 1);
    }

    /**
     * Picks {@code numRows * (1 - ordinalHoleRatio)} distinct ordinals from [0, numRows), ascending. Row 0 is
     * always included so that every other row has a smaller ordinal to attach to
     * (ONE_TO_MANY requires min(rowIds) == ordinal).
     */
    private int[] pickOrdinals()
    {
        int numVectorOrdinals = Math.max(1, (int) Math.round(numRows * (1.0 - ordinalHoleRatio)));
        int[] rows = IntStream.range(1, numRows).toArray();
        for (int i = 0; i < numVectorOrdinals - 1; i++) // partial Fisher-Yates
        {
            int j = i + random.nextInt(rows.length - i);
            int tmp = rows[i];
            rows[i] = rows[j];
            rows[j] = tmp;
        }
        int[] ordinals = new int[numVectorOrdinals];
        System.arraycopy(rows, 0, ordinals, 1, numVectorOrdinals - 1); // ordinals[0] == 0
        Arrays.sort(ordinals);
        return ordinals;
    }

    /**
     * Returns an array of size {@code numRows} indexed by ordinal: ordinal o maps to {o, ...extra rowIds}, and
     * slots that are not ordinals are null (holes). Each non-primary row is attached to a random smaller ordinal,
     * or left unmapped with probability {@code emptyRowRatio}. Rows are visited ascending, so lists stay sorted.
     */
    private IntArrayList[] mapRowsToOrdinals(int[] ordinals)
    {
        IntArrayList[] rowIdsPerOrdinal = new IntArrayList[numRows];
        int ordinalsBelowRow = 0;
        for (int row = 0; row < numRows; row++)
        {
            if (ordinalsBelowRow < ordinals.length && ordinals[ordinalsBelowRow] == row)
            {
                rowIdsPerOrdinal[row] = new IntArrayList(1, IntArrayList.DEFAULT_NULL_VALUE);
                rowIdsPerOrdinal[row].add(row);
                ordinalsBelowRow++;
            }
            else if (random.nextDouble() >= emptyRowRatio)
            {
                rowIdsPerOrdinal[ordinals[random.nextInt(ordinalsBelowRow)]].add(row);
            }
        }
        return rowIdsPerOrdinal;
    }

    /** Removes the holes, renumbering the ordinals to [0, n) while preserving their order. */
    private static IntArrayList[] densify(IntArrayList[] sparse)
    {
        return Arrays.stream(sparse).filter(Objects::nonNull).toArray(IntArrayList[]::new);
    }

    @TearDown(Level.Trial)
    public void teardown()
    {
        if (ordinalsMap != null)
            ordinalsMap.close();
        if (fileHandle != null)
            fileHandle.close();
        if (postingsFile != null && postingsFile.exists())
            postingsFile.delete();
    }

    /**
     * Benchmark the ordinal→rowId(s) direction: given a vector ordinal produced by the
     * ANN graph search, retrieve the matching SSTable rowId(s).
     */
    @Benchmark
    @OperationsPerInvocation(LOOKUPS_PER_INVOCATION)
    public void ordinalToRowIds(Blackhole bh) throws IOException
    {
        try (RowIdsView view = ordinalsMap.getRowIdsView())
        {
            for (int i = 0; i < LOOKUPS_PER_INVOCATION; i++)
            {
                PrimitiveIterator.OfInt it = view.getSegmentRowIdsMatching(ordinalProbes[i]);
                while (it.hasNext())
                    bh.consume(it.nextInt());
            }
        }
    }

    /**
     * Benchmark the rowId→ordinal direction: given a rowId from the SSTable, retrieve the
     * associated vector ordinal. This is the hot path when building the allowed-ordinals
     * Bits mask for a filtered ANN search.
     */
    @Benchmark
    @OperationsPerInvocation(LOOKUPS_PER_INVOCATION)
    public void rowIdToOrdinal(Blackhole bh) throws IOException
    {
        try (OrdinalsView view = ordinalsMap.getOrdinalsView())
        {
            for (int i = 0; i < LOOKUPS_PER_INVOCATION; i++)
                bh.consume(view.getOrdinalForRowId(rowIdProbes[i]));
        }
    }
}
