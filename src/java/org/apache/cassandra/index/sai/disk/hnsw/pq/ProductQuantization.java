package org.apache.cassandra.index.sai.disk.hnsw.pq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.util.VectorUtil;

import static org.apache.cassandra.index.sai.disk.hnsw.pq.SimdOps.dot64;
import static org.apache.cassandra.index.sai.disk.hnsw.pq.SimdOps.simdAddInPlace;
import static org.apache.cassandra.index.sai.disk.hnsw.pq.SimdOps.simdSub;
import static org.apache.cassandra.index.sai.disk.hnsw.pq.SimdOps.simdSum;


public class ProductQuantization {
    private static final int CLUSTERS = 256; // number of clusters per subspace = one byte's worth
    private static final int K_MEANS_ITERATIONS = 15; // VSTODO try 20 as well

    private final List<List<float[]>> codebooks;
    private final int M;
    private final int originalDimension;
    private final float[] globalCentroid;
    private final int[] subvectorSizes;

    // so that decodedDotProduct doesn't have to allocate a new temporary array every call
    private final ThreadLocal<float[]> dotScratch;

    /**
     * Initializes the codebooks by clustering the input data using Product Quantization.
     *
     * @param vectors the points to quantize
     * @param M number of subspaces
     * @param globallyCenter whether to center the vectors globally before quantization
     *                       (not recommended when using the quantization for dot product)
     */
    public ProductQuantization(List<float[]> vectors, int M, boolean globallyCenter) {
        this.M = M;
        originalDimension = vectors.get(0).length;
        subvectorSizes = getSubvectorSizes(originalDimension, M);
        if (globallyCenter) {
            globalCentroid = KMeansPlusPlusClusterer.centroidOf(vectors);
            // subtract the centroid from each vector
            vectors = vectors.stream().parallel().map(v -> simdSub(v, globalCentroid)).toList();
        } else {
            globalCentroid = null;
        }
        codebooks = createCodebooks(vectors, M, subvectorSizes);
        dotScratch = ThreadLocal.withInitial(() -> new float[this.M]);
    }

    public ProductQuantization(List<List<float[]>> codebooks, float[] globalCentroid)
    {
        this.codebooks = codebooks;
        this.globalCentroid = globalCentroid;
        this.M = codebooks.size();
        this.subvectorSizes = new int[M];
        for (int i = 0; i < M; i++) {
            this.subvectorSizes[i] = codebooks.get(i).get(0).length;
        }
        this.originalDimension = Arrays.stream(subvectorSizes).sum();
        this.dotScratch = ThreadLocal.withInitial(() -> new float[this.M]);
    }

    /**
     * Encodes the given vectors using the PQ codebooks.
     */
    public List<byte[]> encodeAll(List<float[]> vectors) {
        return vectors.stream().parallel().map(this::encode).toList();
    }

    /**
     * Encodes the input vector using the PQ codebooks.
     *
     * @return one byte per subspace
     */
    public byte[] encode(float[] vector) {
        if (globalCentroid != null) {
            vector = simdSub(vector, globalCentroid);
        }

        float[] finalVector = vector;
        List<Integer> indices = IntStream.range(0, M)
                .mapToObj(m -> {
                    // the closest centroid in the corresponding codebook to each subvector
                    return closetCentroidIndex(getSubVector(finalVector, m, subvectorSizes), codebooks.get(m));
                })
                .toList();

        return toBytes(indices, M);
    }

    /**
     * Computes the dot product of the (approximate) original decoded vector with
     * another vector.
     *
     * If the PQ does not require centering, this method can compute the dot
     * product without materializing the decoded vector as a new float[], and will be
     * roughly 2x as fast as decode() + dot().
     */
    public float decodedDotProduct(byte[] encoded, float[] other) {
        if (globalCentroid != null) {
            float[] target = new float[originalDimension];
            decode(encoded, target);
            return VectorUtil.dotProduct(target, other);
        }

        var a = dotScratch.get();
        int offset = 0; // starting position in the target array for the current subvector
        int i = 0;
        for (int m = 0; m < M; m++) {
            int centroidIndex = Byte.toUnsignedInt(encoded[m]);
            float[] centroidSubvector = codebooks.get(m).get(centroidIndex);
            if (centroidSubvector.length == 2) {
                a[i++] = dot64(centroidSubvector, 0, other, offset);
            } else if (centroidSubvector.length == 3) {
                var b = centroidSubvector;
                var c = other;
                a[i++] = b[0] * c[offset] + b[1] * c[offset + 1] + b[2] * c[offset + 2];
            } else {
                // TODO support other M / subvectorSizes
                throw new UnsupportedOperationException("Only 2- and 3-dimensional subvectors are currently supported by decodedDotProduct");
            }
            offset += subvectorSizes[m];
        }

        return simdSum(a);
    }

    /**
     * Decodes the quantized representation (byte array) to its approximate original vector.
     */
    public float[] decode(byte[] encoded, float[] target) {
        int offset = 0; // starting position in the target array for the current subvector
        for (int m = 0; m < M; m++) {
            int centroidIndex = Byte.toUnsignedInt(encoded[m]);
            float[] centroidSubvector = codebooks.get(m).get(centroidIndex);
            System.arraycopy(centroidSubvector, 0, target, offset, subvectorSizes[m]);
            offset += subvectorSizes[m];
        }

        if (globalCentroid != null) {
            // Add back the global centroid to get the approximate original vector.
            simdAddInPlace(target, globalCentroid);
        }
        return target;
    }

    /**
     * The dimension of the vectors being quantized.
     */
    public int vectorDimension() {
        return originalDimension;
    }

    // for testing
    static void printCodebooks(List<List<float[]>> codebooks) {
        List<List<String>> strings = codebooks.stream()
                .map(L -> L.stream()
                        .map(ProductQuantization::arraySummary)
                        .collect(Collectors.toList()))
                .toList();
        System.out.printf("Codebooks: [%s]%n", String.join("\n ", strings.stream()
                .map(L -> "[" + String.join(", ", L) + "]")
                .toList()));
    }
    private static String arraySummary(float[] a) {
        List<String> b = new ArrayList<>();
        for (int i = 0; i < Math.min(4, a.length); i++) {
            b.add(String.valueOf(a[i]));
        }
        if (a.length > 4) {
            b.set(3, "... (" + a.length + ")");
        }
        return "[" + String.join(", ", b) + "]";
    }

    static List<List<float[]>> createCodebooks(List<float[]> vectors, int M, int[] subvectorSizes) {
        return IntStream.range(0, M).parallel()
                .mapToObj(m -> {
                    List<float[]> subvectors = vectors.stream().parallel()
                            .map(vector -> getSubVector(vector, m, subvectorSizes))
                            .toList();
                    var clusterer = new KMeansPlusPlusClusterer(subvectors, CLUSTERS, VectorUtil::squareDistance);
                    return clusterer.cluster(K_MEANS_ITERATIONS);
                })
                .toList();
    }
    
    static int closetCentroidIndex(float[] subvector, List<float[]> codebook) {
        return IntStream.range(0, codebook.size())
                .mapToObj(i -> new AbstractMap.SimpleEntry<>(i, VectorUtil.squareDistance(subvector, codebook.get(i))))
                .min(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .get();
    }

    static byte[] toBytes(List<Integer> indices, int M) {
        byte[] q = new byte[M];
        for (int m = 0; m < M; m++) {
            q[m] = (byte) (int) indices.get(m);
        }
        return q;
    }

    /**
     * Extracts the m-th subvector from a single vector.
     */
    static float[] getSubVector(float[] vector, int m, int[] subvectorSizes) {
        float[] subvector = new float[subvectorSizes[m]];
        int offset = Arrays.stream(subvectorSizes, 0, m).sum();
        System.arraycopy(vector, offset, subvector, 0, subvectorSizes[m]);
        return subvector;
    }

    /**
     * Splits the vector dimension into M subvectors of roughly equal size.
     */
    static int[] getSubvectorSizes(int dimensions, int M) {
        int[] sizes = new int[M];
        int baseSize = dimensions / M;
        int remainder = dimensions % M;
        // distribute the remainder among the subvectors
        for (int i = 0; i < M; i++) {
            sizes[i] = baseSize + (i < remainder ? 1 : 0);
        }
        return sizes;
    }

    public void save(OutputStream raw) throws IOException
    {
        // we don't close `out` because caller is responsible for closing `raw`
        var out = new DataOutputStream(raw);

        if (globalCentroid == null) {
            out.writeInt(0);
        } else {
            out.writeInt(globalCentroid.length);
            writeFloats(out, globalCentroid);
        }

        out.writeInt(M);
        assert Arrays.stream(subvectorSizes).sum() == originalDimension;
        assert M == subvectorSizes.length;
        for (var a : subvectorSizes) {
            out.writeInt(a);
        }

        assert codebooks.size() == M;
        assert codebooks.get(0).size() == CLUSTERS;
        out.writeInt(codebooks.get(0).size());
        for (var codebook : codebooks) {
            for (var centroid : codebook) {
                writeFloats(out, centroid);
            }
        }
    }

    private void writeFloats(DataOutputStream out, float[] v) throws IOException
    {
        for (var a : v) {
            out.writeFloat(a);
        }
    }

    public static ProductQuantization load(InputStream raw) throws IOException {
        // we don't close `in` because caller is responsible for closing `raw`
        var in = new DataInputStream(raw);

        int globalCentroidLength = in.readInt();
        float[] globalCentroid = null;
        if (globalCentroidLength > 0) {
            globalCentroid = readFloats(in, globalCentroidLength);
        }

        int M = in.readInt();
        int[] subvectorSizes = new int[M];
        for (int i = 0; i < M; i++) {
            subvectorSizes[i] = in.readInt();
        }

        int clusters = in.readInt();
        List<List<float[]>> codebooks = new ArrayList<>();
        for (int m = 0; m < M; m++) {
            List<float[]> codebook = new ArrayList<>();
            for (int i = 0; i < clusters; i++) {
                int n = subvectorSizes[m];
                float[] centroid = readFloats(in, n);
                codebook.add(centroid);
            }
            codebooks.add(codebook);
        }

        return new ProductQuantization(codebooks, globalCentroid);
    }

    private static float[] readFloats(DataInputStream in, int size) throws IOException
    {
        var v = new float[size];
        for (int i = 0; i < size; i++) {
            v[i] = in.readFloat();
        }
        return v;
    }
}
