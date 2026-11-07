package structures;

import utils.VectorUtil;

/** Combines tuple and utility indexes with cached dot products for streaming selection. */
public class DualTree {

    public KdTree tIdx;
    public ConeTree uIdx;

    int data_size, init_size, sample_size;

    double[][] data, samples;
    private final boolean cacheSampleDots;
    private double[][] sampleDotCache;
    private double[][] rawDotCache;
    private int originalDimension;

    public DualTree(int dim, int k, double eps, int data_size, int init_size, int sample_size, double[][] data,
                    double[][] samples) {
        this(dim, k, eps, data_size, init_size, sample_size, data, samples, true);
    }

    public DualTree(int dim, int k, double eps, int data_size, int init_size, int sample_size, double[][] data,
                    double[][] samples, boolean buildIndex) {
        this(dim, k, eps, data_size, init_size, sample_size, data, samples, buildIndex, false);
    }

    /** Streaming batches are immutable during index construction and selection. */
    DualTree(int dim, int k, double eps, int data_size, int init_size, int sample_size, double[][] data,
                    double[][] samples, boolean buildIndex, boolean cacheSampleDots) {
        double tau = 0.99;
        int leaf_size = Math.max(5, 2 * k);

        this.data = data;
        this.samples = samples;

        this.data_size = data_size;
        this.init_size = init_size;
        this.sample_size = sample_size;
        this.cacheSampleDots = cacheSampleDots;
        originalDimension = dim;
        resetSampleDotCache();

        if (buildIndex) {
            this.tIdx = new KdTree(dim + 1, leaf_size, this);
            this.uIdx = new ConeTree(dim + 1, k, eps, tau, this, cacheSampleDots);
        }
    }

    void resetSampleDotCache() {
        sampleDotCache = cacheSampleDots ? new double[sample_size][data_size] : null;
        rawDotCache = cacheSampleDots ? new double[sample_size][data_size] : null;
        if (sampleDotCache != null) {
            // Streaming scoring consumes every pair, even those a tree query prunes.
            // Fill once in contiguous direction rows; retain each dot's accumulation order.
            for (int u = 0; u < sample_size; u++) {
                double[] row = sampleDotCache[u];
                double[] direction = samples[u];
                double[] rawRow = rawDotCache[u];
                double[] positive = new double[originalDimension];
                boolean sharePrefix = direction.length == originalDimension + 1;
                for (int d = 0; d < originalDimension; d++) {
                    positive[d] = Math.max(0.0, direction[d]);
                    if (Double.doubleToRawLongBits(positive[d]) != Double.doubleToRawLongBits(direction[d]))
                        sharePrefix = false;
                }
                for (int point = 0; point < data_size; point++) {
                    double[] values = data[point];
                    double raw = 0.0;
                    if (sharePrefix && values.length == originalDimension + 1) {
                        // Preserve the exact prefix accumulator; no subtraction or reassociation.
                        for (int d = 0; d < originalDimension; d++) raw += values[d] * direction[d];
                        rawRow[point] = raw;
                        row[point] = raw + values[originalDimension] * direction[originalDimension];
                    } else {
                        row[point] = VectorUtil.inner_product(values, direction);
                        for (int d = 0; d < originalDimension; d++) raw += values[d] * positive[d];
                        rawRow[point] = raw;
                    }
                }
            }
        }
    }

    double sampleRawDot(int pointIdx, int sampleIdx) {
        if (rawDotCache != null) return rawDotCache[sampleIdx][pointIdx];
        double raw = 0.0;
        for (int d = 0; d < originalDimension; d++)
            raw += data[pointIdx][d] * Math.max(0.0, samples[sampleIdx][d]);
        return raw;
    }

    double[] sampleDotCache(int sampleIdx) {
        return sampleDotCache == null ? null : sampleDotCache[sampleIdx];
    }

    double sampleDot(int pointIdx, int sampleIdx) {
        return sampleDotCache == null
            ? VectorUtil.inner_product(data[pointIdx], samples[sampleIdx])
            : sampleDotCache[sampleIdx][pointIdx];
    }

    /** Streaming selection reads the dot table; populate query lists before exposing the index. */
    void materializeSearchResults() {
        if (uIdx != null) uIdx.materializeSearchResults();
    }

    public Operations insert(int t_idx) {
        materializeSearchResults();
        Operations opr = new Operations(t_idx, 1);
        if (tIdx == null || uIdx == null) {
            return opr;
        }
        boolean is_inserted = tIdx.insert(t_idx);
        if (!is_inserted) {
            System.err.println("Insert " + t_idx + " failed");
        }
        uIdx.insert(t_idx, opr);

        return opr;
    }

    public Operations delete(int t_idx) {
        materializeSearchResults();
        Operations opr = new Operations(t_idx, -1);
        if (tIdx == null || uIdx == null) {
            return opr;
        }
        boolean is_deleted = tIdx.delete(t_idx);
        if (!is_deleted) {
            System.err.println("Delete " + t_idx + " failed");
        }
        uIdx.delete(t_idx, opr);

        return opr;
    }
}
