package structures;

import utils.VectorUtil;
import java.util.*;

/** Maintains a bounded representative set from the previous set and the current input batch. */
public class StreamingRMS {

    private static final boolean RAW_COVERAGE = true;
    private static final boolean QUALITY_MAX = true;

    private static final boolean VERIFY_LAZY = Boolean.getBoolean("srms.verifyLazy");
    private static long lazyEvaluations, scanEquivalentEvaluations;
    public static String lazyStats() { return lazyEvaluations + "," + scanEquivalentEvaluations; }

    /** Orders candidates by marginal gain, then score, then stable candidate index. */
    private static boolean gainBetter(int a, int b, double[] gains, double[] quality) {
        return gains[a] > gains[b] || (gains[a] == gains[b] &&
            (quality[a] > quality[b] || (quality[a] == quality[b] && a < b)));
    }
    private static void siftGainDown(int[] heap, int size, int pos, double[] gains, double[] quality) {
        int value = heap[pos];
        while (2 * pos + 1 < size) {
            int child = 2 * pos + 1;
            if (child + 1 < size && gainBetter(heap[child + 1], heap[child], gains, quality)) child++;
            if (!gainBetter(heap[child], value, gains, quality)) break;
            heap[pos] = heap[child]; pos = child;
        }
        heap[pos] = value;
    }

    private final int dim;
    @SuppressWarnings("unused")
    private final int k;
    @SuppressWarnings("unused")
    private final double eps;

    private final double alpha;

    public StreamingState state;
    private DualTree dualTree;

    private int[] selectedPointIndices;

    private double[] dataMagnitudes;

    public double samplingTime = 0.0;
    public double indexTime = 0.0;
    public double scoringTime = 0.0;
    public double selectionTime = 0.0;

    public double samplingMemMB = 0.0;
    public double indexMemMB = 0.0;
    public double scoringMemMB = 0.0;
    public double selectionMemMB = 0.0;

    private boolean incrementalUsed = false;

    /** Directions, source identities and weights used together throughout the current batch. */
    private CurrentUtilityPool currentPool;

    /** Deduplicates directions while retaining coordinate-axis provenance. */
    private static final class SampleBuffer {
        final List<double[]> vectors = new ArrayList<>();
        final List<CurrentUtilityPool.Source> sources = new ArrayList<>();
        private final Set<DirectionKey> seen = new HashSet<>();

        boolean add(double[] vector, CurrentUtilityPool.Source source) {
            if (!seen.add(new DirectionKey(vector))) {
                return false;
            }
            vectors.add(vector);
            sources.add(source);
            return true;
        }

        int size() {
            return vectors.size();
        }

        boolean isEmpty() {
            return vectors.isEmpty();
        }
    }

    private static double usedMemMB() {
        Runtime rt = Runtime.getRuntime();
        long used = rt.totalMemory() - rt.freeMemory();
        return used / (1024.0 * 1024.0);
    }

    /** Merges candidates, constructs utility samples and prepares the scoring index. */
    public StreamingRMS(int dim, int k, double eps, double alpha, StreamingState state, double[][] batch_data) {
        this.dim = dim;
        this.k = k;
        this.eps = eps;
        this.alpha = StreamingState.normalizeAlpha(alpha);
        this.state = state;

        long t0 = System.nanoTime();

        double[][] merged_data = mergeQAndBatch(batch_data);

        double[][] samples = generateStreamingSamples(merged_data, batch_data.length);

        long t1 = System.nanoTime();
        this.samplingTime = (t1 - t0) / 1e6;
        this.samplingMemMB = usedMemMB();

        int total_size = merged_data.length;
        buildOrUpdateIndex(merged_data, samples, total_size);

        long t2 = System.nanoTime();
        this.indexTime = (t2 - t1) / 1e6;
        this.indexMemMB = usedMemMB();
    }

    public StreamingRMS(int dim, int k, double eps, StreamingState state, double[][] batch_data) {
        this(dim, k, eps, state != null ? state.alpha : StreamingState.DEFAULT_ALPHA, state, batch_data);
    }

    /** Builds the current index or updates a compatible prior index. */
    private void buildOrUpdateIndex(double[][] merged_data, double[][] samples, int total_size) {
        boolean buildIndex = state == null
            || !StreamingState.INDEX_MODE_NONE.equals(StreamingState.normalizeIndexMode(state.indexMode));
        if (!buildIndex) {
            this.incrementalUsed = false;
            this.dualTree = new DualTree(dim, k, eps, total_size, total_size,
                                         samples.length, merged_data, samples, false, true);
            return;
        }

        Set<Integer> current_Q_indices = new HashSet<>(Math.max(16, state.Q.size() * 2));
        for (int i = 0; i < state.Q.size(); i++) {
            current_Q_indices.add(state.Q.get(i).original_idx);
        }

        int overlap = 0;
        for (int idx : current_Q_indices) {
            if (state.prev_Q_indices.contains(idx)) {
                overlap++;
            }
        }

        double overlap_ratio = state.prev_Q_indices.isEmpty() ? 0.0 :
                              (double) overlap / state.prev_Q_indices.size();

        boolean use_incremental = state.enable_incremental &&
                                 overlap_ratio > 0.7 &&
                                 state.current_batch > 0;

        if (use_incremental && this.dualTree != null) {

            this.incrementalUsed = true;

            this.dualTree.data = merged_data;
            this.dualTree.samples = samples;
            this.dualTree.data_size = total_size;
            this.dualTree.sample_size = samples.length;
            this.dualTree.resetSampleDotCache();

            Set<Integer> to_delete = new HashSet<>(state.prev_Q_indices);
            to_delete.removeAll(current_Q_indices);
            if (!to_delete.isEmpty()) {

            }

            int Q_size = state.Q.size();
            for (int i = Q_size; i < total_size; i++) {
                this.dualTree.insert(i);
            }

            this.dualTree.uIdx = new ConeTree(dim + 1, k, eps, 0.99, this.dualTree);

        } else {

            this.dualTree = new DualTree(dim, k, eps, total_size, total_size,
                                         samples.length, merged_data, samples, true, true);
        }
    }

    /** Forms the ordered candidate set: previous representatives followed by the current batch. */
    private double[][] mergeQAndBatch(double[][] batch_data) {
        int Q_size = state.Q.size();
        int B_size = batch_data.length;
        int total_size = Q_size + B_size;

        double[][] merged = new double[total_size][dim + 1];
        dataMagnitudes = new double[total_size];

        for (int i = 0; i < Q_size; i++) {
            dataMagnitudes[i] = computeOriginalNorm(state.Q.get(i).vector);
            System.arraycopy(state.Q.get(i).vector, 0, merged[i], 0, dim + 1);
        }

        for (int i = 0; i < B_size; i++) {
            dataMagnitudes[Q_size + i] = computeOriginalNorm(batch_data[i]);
            System.arraycopy(batch_data[i], 0, merged[Q_size + i], 0, dim + 1);
        }

        return merged;
    }

    private double computeOriginalNorm(double[] vector) {
        double sum = 0.0;
        for (int d = 0; d < dim; d++) {
            sum += vector[d] * vector[d];
        }
        return Math.sqrt(sum);
    }

    private double[][] generateStreamingSamples(double[][] data, int batch_size) {
        String samplingMode = StreamingState.normalizeSamplingMode(state.samplingMode);
        if (StreamingState.SAMPLING_FIXED.equals(samplingMode)) {
            return generateFixedSamples(data);
        }
        if (StreamingState.SAMPLING_UNIFORM.equals(samplingMode)) {
            return generateUniformSamples(data);
        }
        return generateAdaptiveSamples(data, batch_size);
    }

    /** Combines retained directions, perturbed representative directions and new batch directions. */
    private double[][] generateAdaptiveSamples(double[][] data, int batch_size) {
        int s = state.sample_size;
        SampleBuffer samples = new SampleBuffer();
        Random rand = new Random(state.samplingSeed);

        int n_hist = (int)(s * state.ratio_hist);
        if (!state.U_hist.isEmpty()) {
            List<StreamingState.WeightedUtility> sorted_hist = new ArrayList<>(state.U_hist);
            sorted_hist.sort((a, b) -> Double.compare(b.weight, a.weight));

            for (int i = 0; i < Math.min(n_hist, sorted_hist.size()); i++) {
                StreamingState.WeightedUtility wu = sorted_hist.get(i);

                samples.add(wu.vector.clone(),
                            wu.axisProvenance ? CurrentUtilityPool.Source.AXIS
                                              : CurrentUtilityPool.Source.HIST);
            }
        }

        int n_seed = (int)(s * state.ratio_seed);
        if (!state.Q.isEmpty()) {
            List<StreamingState.Representative> sorted_Q = new ArrayList<>(state.Q);
            sorted_Q.sort((a, b) -> Double.compare(b.score, a.score));

            int n_top = (int)(n_seed * 0.8);
            int n_rand = n_seed - n_top;

            for (int i = 0; i < Math.min(n_top, sorted_Q.size()); i++) {
                double[] perturbed = perturbVector(sorted_Q.get(i).vector, 0.1, rand);
                samples.add(perturbed, CurrentUtilityPool.Source.SEED_TOP);
            }

            for (int i = 0; i < n_rand && sorted_Q.size() > 0; i++) {
                int idx = rand.nextInt(sorted_Q.size());
                double[] perturbed = perturbVector(sorted_Q.get(idx).vector, 0.15, rand);
                samples.add(perturbed, CurrentUtilityPool.Source.SEED_RANDOM);
            }
        }

        int n_new = s - samples.size();
        int Q_size = state.Q.size();

        for (int i = 0; i < n_new && i < batch_size; i++) {
            int idx = Q_size + (batch_size > n_new ?
                               rand.nextInt(batch_size) : i);
            double[] p = data[idx];

            double max_cos = -1;
            double[] u_max = null;
            if (!samples.isEmpty()) {
                for (double[] u : samples.vectors) {
                    double cos = VectorUtil.cosine_unit(p, u);
                    if (cos > max_cos) {
                        max_cos = cos;
                        u_max = u;
                    }
                }
            }

            double[] u_new;
            if (u_max == null || max_cos < state.tau) {
                u_new = p.clone();
            } else {
                u_new = new double[dim + 1];
                for (int d = 0; d < dim + 1; d++) {
                    u_new[d] = state.beta * u_max[d] + (1 - state.beta) * p[d];
                }
            }
            VectorUtil.to_unit(u_new);
            samples.add(u_new, CurrentUtilityPool.Source.NEW_DIRECTION);
        }

        if (state.axisSamplesEnabled) {
            appendAxisSamples(samples);
        }
        return finalizeSamples(samples);
    }

    /** Reuses the initial stratified samples; reinitializes if the required sample count changes. */
    private double[][] generateFixedSamples(double[][] data) {
        int targetSize = Math.min(state.sample_size, data.length);
        boolean needInit = state.fixedSamples == null || state.fixedSamples.size() != targetSize;
        if (needInit) {
            List<double[]> fixed = sampleUniformCore(data, targetSize);
            state.fixedSamples = new ArrayList<>();
            for (double[] sample : fixed) {
                state.fixedSamples.add(sample.clone());
            }
        }

        SampleBuffer samples = new SampleBuffer();
        for (double[] sample : state.fixedSamples) {
            samples.add(sample.clone(), CurrentUtilityPool.Source.FIXED);
        }
        if (state.axisSamplesEnabled) {
            appendAxisSamples(samples);
        }
        return finalizeSamples(samples);
    }

    /** Regenerates stratified samples from the current ordered candidate set. */
    private double[][] generateUniformSamples(double[][] data) {
        int targetSize = Math.min(state.sample_size, data.length);
        SampleBuffer samples = new SampleBuffer();
        for (double[] sample : sampleUniformCore(data, targetSize)) {
            samples.add(sample, CurrentUtilityPool.Source.UNIFORM);
        }
        if (state.axisSamplesEnabled) {
            appendAxisSamples(samples);
        }
        return finalizeSamples(samples);
    }

    /** Selects the midpoint index of each equal-size stratum; this is not uniform sampling on a sphere. */
    private List<double[]> sampleUniformCore(double[][] data, int targetSize) {
        List<double[]> samples = new ArrayList<>();
        if (data == null || data.length == 0 || targetSize <= 0) {
            return samples;
        }
        if (targetSize >= data.length) {
            for (double[] point : data) {
                samples.add(point.clone());
            }
            return samples;
        }

        for (int i = 0; i < targetSize; i++) {
            double position = (i + 0.5) * data.length / (double) targetSize;
            int idx = Math.min(data.length - 1, Math.max(0, (int)Math.floor(position)));
            samples.add(data[idx].clone());
        }
        return samples;
    }

    /** Adds one coordinate-axis direction per original attribute. */
    private void appendAxisSamples(SampleBuffer samples) {
        for (int d = 0; d < dim; d++) {
            double[] axis = new double[dim + 1];
            axis[d] = 1.0;
            axis[dim] = 0.0;
            samples.add(axis, CurrentUtilityPool.Source.AXIS);
        }
    }

    /** Normalizes the samples and binds each direction to its current-pool entry. */
    private double[][] finalizeSamples(SampleBuffer samples) {
        double[][] result = new double[samples.size()][dim + 1];
        CurrentUtilityPool pool = new CurrentUtilityPool();
        for (int i = 0; i < samples.size(); i++) {
            result[i] = samples.vectors.get(i);
            VectorUtil.to_unit(result[i]);
            pool.add(result[i], samples.sources.get(i));
        }
        this.currentPool = pool;
        return result;
    }

    /** Perturbs a direction and renormalizes it for utility sampling. */
    private double[] perturbVector(double[] v, double noise, Random rand) {
        double[] result = new double[v.length];
        for (int i = 0; i < v.length; i++) {
            result[i] = v[i] + (rand.nextDouble() - 0.5) * 2 * noise;
        }
        VectorUtil.to_unit(result);
        return result;
    }

    /** Uses prior-set coverage gaps for non-axis weights and the configured weight for axis directions.
     * Returns equal weights when feedback or objective weighting is disabled, or no prior set exists. */
    private double[] computeHistoricalSupportWeights() {
        int n = currentPool.size();
        double[] weights = new double[n];
        Arrays.fill(weights, 1.0);
        if (!state.utilityFeedbackEnabled || !state.objectiveWeightingEnabled || state.Q.isEmpty()) {
            return weights;
        }
        for (int i = 0; i < n; i++) {
            if (currentPool.get(i).source == CurrentUtilityPool.Source.AXIS) {
                weights[i] = state.axisObjectiveWeight;
                continue;
            }
            double maxCos = 0.0;
            for (int point = 0; point < state.Q.size(); point++) {
                double cos = dualTree.sampleDot(point, i);
                if (cos > maxCos) {
                    maxCos = cos;
                }
            }
            weights[i] = coverageGapWeight(maxCos, dim);
        }
        return weights;
    }

    /** Computes sqrt(dim) times the uncovered cosine fraction; the input dot product is scaled by sqrt(dim). */
    public static double coverageGapWeight(double maxCosScaled, int dim) {
        double repNorm = Math.sqrt(dim);
        double trueCos = Math.min(1.0, Math.max(0.0, maxCosScaled / repNorm));
        return repNorm * (1.0 - trueCos);
    }

    private boolean useKRMSObjective() {
        return state != null && StreamingState.normalizeRmsK(state.rmsK) > 1;
    }

    private double magnitudeFactorOf(int pointIdx, double maxMagnitude, double minMagnitude) {
        if (maxMagnitude > minMagnitude) {
            return 0.5 + (dataMagnitudes[pointIdx] - minMagnitude)
                / (maxMagnitude - minMagnitude);
        }
        return 1.0;
    }

    private double[] computeKThresholds(Phase2Scores rawScores, int totalSize, int utilitySize) {
        double[] thresholds = new double[utilitySize];
        if (totalSize == 0 || utilitySize == 0) {
            return thresholds;
        }

        int heapCapacity = Math.min(StreamingState.normalizeRmsK(state.rmsK), totalSize);
        for (int u = 0; u < utilitySize; u++) {
            PriorityQueue<Double> topK = new PriorityQueue<>(heapCapacity);
            for (int i = 0; i < totalSize; i++) {
                double score = rawScores.get(i, u);
                if (topK.size() < heapCapacity) {
                    topK.offer(score);
                } else if (score > topK.peek()) {
                    topK.poll();
                    topK.offer(score);
                }
            }
            thresholds[u] = topK.isEmpty() ? 0.0 : topK.peek();
        }
        return thresholds;
    }

    private double applyKTruncation(double rawScore, double threshold) {
        if (threshold <= 1e-12) {
            return 0.0;
        }
        double ratio = Math.max(0.0, rawScore) / threshold;
        String strategy = StreamingState.normalizeKTruncationStrategy(state.kTruncationStrategy);
        if (StreamingState.K_TRUNCATION_SOFT.equals(strategy) && ratio > 1.0) {
            double beta = StreamingState.normalizeKSoftBeta(state.kSoftBeta);
            double gamma = StreamingState.normalizeKSoftGamma(state.kSoftGamma);
            double eta = StreamingState.normalizeKSoftEta(state.kSoftEta);
            return 1.0 + eta * Math.log1p(beta * gamma * (ratio - 1.0)) / beta;
        }
        return Math.min(ratio, 1.0);
    }

    private double marginalPart(double[] scoreRow, double[] currentMax,
                                double[] utilityWeights, double[] uDirectionMax,
                                int[] activeDirections, int activeCount) {
        double total = 0.0;
        for (int a = 0; a < activeCount; a++) {
            int u = activeDirections[a];
            double marginalGain = scoreRow[u] - currentMax[u];
            if (marginalGain > 0.0) {
                total += utilityWeights[u] * marginalGain / uDirectionMax[u];
            }
        }
        return total;
    }

    /** Selects representatives, updates utility history and advances the stream state. */
    public void updateRepresentatives() {
        int t = state.current_batch;
        int Q_size = state.Q.size();
        int total_size = dualTree.data_size;

        long t0 = System.nanoTime();

        currentPool.applyWeights(computeHistoricalSupportWeights());

        StageDebug.logUtilityPool(t, currentPool);
        StageDebug.logWeightBinding(t, currentPool, state.U_hist);

        double[] scores = computeWeightedScores(Q_size, t);

        long t1 = System.nanoTime();
        this.scoringTime = (t1 - t0) / 1e6;
        this.scoringMemMB = usedMemMB();

        List<StreamingState.Representative> new_Q = selectDiverseTopK(scores, Q_size, t);

        updateUtilityHistory(new_Q);

        long t2 = System.nanoTime();
        this.selectionTime = (t2 - t1) / 1e6;
        this.selectionMemMB = usedMemMB();

        state.Q = new_Q;

        state.prev_Q_indices.clear();
        for (StreamingState.Representative rep : new_Q) {
            state.prev_Q_indices.add(rep.original_idx);
        }

        state.current_batch++;
        state.total_points_seen += (total_size - Q_size);
    }

    /** Computes per-point scores for ranking and tie-breaking, including historical retention. */
    private double[] computeWeightedScores(int Q_size, int t) {
        double[] scores = new double[dualTree.data_size];

        if (state.globalMaxVals == null) {
            state.globalMaxVals = new double[dim];
            Arrays.fill(state.globalMaxVals, -Double.MAX_VALUE);
        }

        double maxMagnitude = 0.0;
        double minMagnitude = Double.MAX_VALUE;
        for (int i = 0; i < dualTree.data_size; i++) {
            maxMagnitude = Math.max(maxMagnitude, dataMagnitudes[i]);
            minMagnitude = Math.min(minMagnitude, dataMagnitudes[i]);
        }

        final int dataSize = dualTree.data_size;
        final int sampleSize = dualTree.sample_size;
        final double[][] data = dualTree.data;
        double[] rawWeights = new double[sampleSize];
        for (int u_idx = 0; u_idx < sampleSize; u_idx++) {
            rawWeights[u_idx] = currentPool.get(u_idx).rawWeight;
        }

        for (int i = 0; i < dataSize; i++) {
            double score = 0.0;
            double[] point = data[i];

            double magnitude_factor = 1.0;
            if (maxMagnitude > minMagnitude) {

                magnitude_factor = 0.5 + (dataMagnitudes[i] - minMagnitude) / (maxMagnitude - minMagnitude);
            }

            boolean isWorldRecord = false;
            for (int d = 0; d < dim; d++) {
                double val = point[d];
                if (val > state.globalMaxVals[d]) {
                    state.globalMaxVals[d] = val;
                    isWorldRecord = true;
                }
            }

            double time_factor = 1.0;
            if (i < Q_size) {
                time_factor = StreamingState.timeFactor(
                    t, state.Q.get(i).t_join, state.lambda);
            }

            for (int u_idx = 0; u_idx < sampleSize; u_idx++) {

                double cos = dualTree.sampleDot(i, u_idx);

                double w_u = rawWeights[u_idx];

                score += w_u * cos * magnitude_factor * time_factor;
            }

            if (isWorldRecord) {
                score += state.worldRecordBonus;
            }

            scores[i] = score / sampleSize;
        }

        return scores;
    }

    /** Selects from the complete candidate set using weighted coverage and quality gains.
     * Uses lazy recomputation with stable tie-breaking; non-finite scores fall back to a full scan. */
    private List<StreamingState.Representative> selectDiverseTopK(
            double[] scores, int Q_size, int t) {
        String phase3Selection = StreamingState.normalizePhase3SelectionStrategy(
            state.phase3SelectionStrategy
        );
        if (StreamingState.PHASE3_SELECTION_ABSOLUTE_TOPR.equals(phase3Selection)) {
            return selectAbsoluteTopR(scores, Q_size, t);
        }

        int total_size = dualTree.data_size;

        int[] candidateIndex = new int[total_size];
        for (int i = 0; i < total_size; i++) {
            candidateIndex[i] = i;
        }

        List<double[]> utilityVectors = new ArrayList<>();
        List<Double> utilityWeights = new ArrayList<>();
        for (CurrentUtilityPool.Entry e : currentPool.entries()) {
            utilityVectors.add(e.vector);
            utilityWeights.add(e.normalizedWeight);
        }
        StageDebug.logPhase3Utilities(
            t, "current_pool", currentPool, utilityVectors, utilityWeights);

        int U = utilityVectors.size();

        double[] utilityWeightArray = new double[U];
        for (int u = 0; u < U; u++) {
            utilityWeightArray[u] = utilityWeights.get(u);
        }
        double[] currentMax = new double[U];
        Arrays.fill(currentMax, 0.0);

        double[] uDirectionMax = new double[U];

        double maxMagnitude = 0.0;
        double minMagnitude = Double.MAX_VALUE;
        for (int i = 0; i < total_size; i++) {
            maxMagnitude = Math.max(maxMagnitude, dataMagnitudes[i]);
            minMagnitude = Math.min(minMagnitude, dataMagnitudes[i]);
        }

        int candidateCount = candidateIndex.length;
        if (useKRMSObjective()) throw new IllegalStateException("Scoring experiment requires k=1");
        Phase2Scores phase2 = new Phase2Scores(candidateIndex, U);
        Phase2Scores coverage = RAW_COVERAGE ? new Phase2Scores(candidateIndex, U) : phase2;
        double[] coverageDen = RAW_COVERAGE ? new double[U] : uDirectionMax;
        double[] qualityDen = new double[U];
        double[] qualityCurrent = new double[U];

        for (int c = 0; c < candidateCount; c++) {
            int pointIdx = phase2.pointOf(c);

            double magnitudeFactor = magnitudeFactorOf(pointIdx, maxMagnitude, minMagnitude);
            double[] row = phase2.row(c);
            double[] rawRow = coverage.row(c), point = dualTree.data[pointIdx];
            for (int u = 0; u < U; u++) {
                double base = dualTree.sampleDot(pointIdx, u);
                if (base > uDirectionMax[u]) {
                    uDirectionMax[u] = base;
                }
                row[u] = Math.max(0.0, base * magnitudeFactor);
                qualityDen[u] = Math.max(qualityDen[u], row[u]);
                if (RAW_COVERAGE) {
                    double raw = 0.0;
                    raw = dualTree.sampleRawDot(pointIdx, u);
                    rawRow[u] = Math.max(0.0, raw);
                    coverageDen[u] = Math.max(coverageDen[u], rawRow[u]);
                }
            }
        }
        for (int u = 0; u < U; u++) {
            if (!(uDirectionMax[u] > 1e-8)) {
                uDirectionMax[u] = 1.0;
            }
        }

        double[] maxEffPerDirection = coverageDen.clone();

        for (int u = 0; u < U; u++) {
            if (RAW_COVERAGE && !(coverageDen[u] > 0.0)) coverageDen[u] = 1.0;
        }

        if (useKRMSObjective()) {
            double[] kThresholds = computeKThresholds(phase2, candidateCount, U);
            for (int c = 0; c < candidateCount; c++) {
                double[] row = phase2.row(c);
                for (int u = 0; u < U; u++) {
                    row[u] = applyKTruncation(row[u], kThresholds[u]);
                }
            }
        }

        int[] activeDirections = new int[U];
        int activeCount = 0;
        for (int u = 0; u < U; u++) {
            activeDirections[activeCount++] = u;
        }

        int[] qualityActive = new int[U];
        int qualityCount = 0;
        if (QUALITY_MAX) for (int u = 0; u < U; u++)
            if (qualityDen[u] > 0.0) qualityActive[qualityCount++] = u;

        List<StreamingState.Representative> new_Q = new ArrayList<>();

        boolean[] usedInPool = new boolean[total_size];

        int targetSize = Math.min(state.r, candidateCount);
        selectedPointIndices = new int[targetSize];
        int[] gainHeap = new int[candidateCount], gainRound = new int[candidateCount];
        double[] cachedGain = new double[candidateCount];
        int heapSize = candidateCount;
        boolean finiteGains = true;
        for (int c = 0; c < candidateCount; c++) {
            gainHeap[c] = c;
            double gain = 0.0 + (1.0 - alpha) * marginalPart(
                coverage.row(c), currentMax, utilityWeightArray, coverageDen, activeDirections, activeCount);
            gain += alpha * marginalPart(phase2.row(c), qualityCurrent, utilityWeightArray,
                qualityDen, qualityActive, qualityCount);
            cachedGain[c] = gain;
            if (!Double.isFinite(gain) || !Double.isFinite(scores[c])) finiteGains = false;
        }
        if (VERIFY_LAZY) lazyEvaluations += candidateCount;
        if (finiteGains) for (int i = heapSize / 2 - 1; i >= 0; i--)
            siftGainDown(gainHeap, heapSize, i, cachedGain, scores);
        while (new_Q.size() < targetSize) {
            int bestIdx = -1;
            int bestCandidatePos = -1;
            double bestTotalGain = 0.0;
            double bestQuality = 0.0;

            if (finiteGains) {
                int round = new_Q.size();
                while (gainRound[gainHeap[0]] != round && cachedGain[gainHeap[0]] != 0.0) {
                    int c = gainHeap[0];
                    double gain = 0.0 + (1.0 - alpha) * marginalPart(
                        coverage.row(c), currentMax, utilityWeightArray, coverageDen,
                        activeDirections, activeCount);
                    gain += alpha * marginalPart(phase2.row(c), qualityCurrent, utilityWeightArray,
                        qualityDen, qualityActive, qualityCount);
                    if (VERIFY_LAZY) {
                        lazyEvaluations++;
                        if (!(gain <= cachedGain[c])) throw new AssertionError("Lazy bound increased");
                    }
                    cachedGain[c] = gain; gainRound[c] = round;
                    siftGainDown(gainHeap, heapSize, 0, cachedGain, scores);
                }
                bestIdx = bestCandidatePos = gainHeap[0];
                bestTotalGain = cachedGain[bestIdx]; bestQuality = scores[bestIdx];
                heapSize--;
                if (heapSize > 0) {
                    gainHeap[0] = gainHeap[heapSize];
                    siftGainDown(gainHeap, heapSize, 0, cachedGain, scores);
                }
            }
            if (!finiteGains || VERIFY_LAZY) {
                int heapBest = bestIdx;
                double heapGain = bestTotalGain;
                bestIdx = -1; bestCandidatePos = -1; bestTotalGain = 0.0; bestQuality = 0.0;
            for (int c = 0; c < candidateCount; c++) {
                int idx = phase2.pointOf(c);
                if (usedInPool[idx]) {
                    continue;
                }
                double gain = 0.0 + (1.0 - alpha) * marginalPart(
                    coverage.row(c), currentMax, utilityWeightArray, coverageDen,
                    activeDirections, activeCount);
                if (QUALITY_MAX) gain += alpha * marginalPart(
                    phase2.row(c), qualityCurrent, utilityWeightArray, qualityDen,
                    qualityActive, qualityCount);
                double quality = scores[idx];

                boolean better = bestIdx == -1
                    || gain > bestTotalGain
                    || (gain == bestTotalGain && quality > bestQuality)
                    || (gain == bestTotalGain && quality == bestQuality && idx < bestIdx);
                if (better) {
                    bestIdx = idx;
                    bestCandidatePos = c;
                    bestTotalGain = gain;
                    bestQuality = quality;
                }
            }

                if (VERIFY_LAZY && finiteGains) {
                    scanEquivalentEvaluations += candidateCount - new_Q.size();
                    if (bestIdx != heapBest || Double.doubleToLongBits(bestTotalGain) != Double.doubleToLongBits(heapGain))
                        throw new AssertionError("Lazy choice differs from full scan");
                }
            }

            if (bestIdx == -1) {
                break;
            }

            double[] bestRow = coverage.row(bestCandidatePos);
            for (int u = 0; u < U; u++) {
                if (bestRow[u] > currentMax[u]) {
                    currentMax[u] = bestRow[u];
                }
            }
            int kept = 0;
            for (int a = 0; a < activeCount; a++) {
                int u = activeDirections[a];
                if (currentMax[u] < maxEffPerDirection[u]) {
                    activeDirections[kept++] = u;
                }
            }
            activeCount = kept;
            if (QUALITY_MAX) {
                double[] qualityRow = phase2.row(bestCandidatePos);
                for (int u = 0; u < U; u++) qualityCurrent[u] = Math.max(qualityCurrent[u], qualityRow[u]);
                int qKept = 0;
                for (int a = 0; a < qualityCount; a++) {
                    int u = qualityActive[a];
                    if (qualityCurrent[u] < qualityDen[u]) qualityActive[qKept++] = u;
                }
                qualityCount = qKept;
            }
            usedInPool[bestIdx] = true;

            int t_join = t;
            int orig_idx = bestIdx;
            if (bestIdx < Q_size) {
                t_join = state.Q.get(bestIdx).t_join;
                orig_idx = state.Q.get(bestIdx).original_idx;
            }

            double repScore = bestIdx >= 0 && bestIdx < scores.length
                ? scores[bestIdx]
                : bestTotalGain / Math.max(1, U);

                selectedPointIndices[new_Q.size()] = bestIdx;
                new_Q.add(new StreamingState.Representative(
                dualTree.data[bestIdx].clone(),
                repScore,
                    t_join,
                    orig_idx
                ));
            }

        return new_Q;
    }

    /** Ranks by absolute scores without marginal-gain selection. */
    private List<StreamingState.Representative> selectAbsoluteTopR(
            double[] scores, int Q_size, int t) {
        List<Integer> ranked = new ArrayList<>(scores.length);
        for (int idx = 0; idx < scores.length; idx++) {
            ranked.add(idx);
        }
        ranked.sort((a, b) -> Double.compare(scores[b], scores[a]));

        int limit = Math.min(state.r, ranked.size());
        selectedPointIndices = new int[limit];
        List<StreamingState.Representative> newQ = new ArrayList<>(limit);
        for (int i = 0; i < limit; i++) {
            int idx = ranked.get(i);
            selectedPointIndices[i] = idx;
            int tJoin = t;
            int origIdx = idx;
            if (idx < Q_size) {
                tJoin = state.Q.get(idx).t_join;
                origIdx = state.Q.get(idx).original_idx;
            }
            double repScore = scores[idx];
            newQ.add(new StreamingState.Representative(
                dualTree.data[idx].clone(),
                repScore,
                tJoin,
                origIdx
            ));
        }
        return newQ;
    }

    private double computeSoftPenalty(int idx, List<StreamingState.Representative> currentQ) {
        if (currentQ.isEmpty()) {
            return 1.0;
        }

        double[] x = dualTree.data[idx];
        double maxCos = 0.0;

        for (StreamingState.Representative rep : currentQ) {
            double cos = VectorUtil.cosine(x, rep.vector);
            if (cos > maxCos) {
                maxCos = cos;
            }
        }

        if (maxCos <= state.min_cos) {
            return 1.0;
        } else {

            double t = (maxCos - state.min_cos) / (1.0 - state.min_cos);
            return 1.0 - 0.7 * t;
        }
    }

    private double computeLogSumUtility(
            List<StreamingState.Representative> Q,
            List<double[]> utilityVectors,
            List<Double> utilityWeights) {

        int U = utilityVectors.size();

        double[] utilityWeightArray = new double[U];
        for (int u = 0; u < U; u++) {
            utilityWeightArray[u] = utilityWeights.get(u);
        }
        double utility = 0.0;

        for (int u = 0; u < U; u++) {
            double maxSim = 0.0;
            for (StreamingState.Representative rep : Q) {
                double sim = VectorUtil.cosine_unit(rep.vector, utilityVectors.get(u));
                if (sim > maxSim) {
                    maxSim = sim;
                }
            }
            utility += utilityWeights.get(u) * Math.log(1.0 + maxSim);
        }

        return utility;
    }

    /** Updates bounded direction history and preserves coordinate-axis provenance. */
    private void updateUtilityHistory(List<StreamingState.Representative> new_Q) {
        int sampleCount = dualTree.sample_size;
        List<StreamingState.WeightedUtility> new_U_hist = new ArrayList<>(sampleCount);

        Set<DirectionKey> histSeen = new HashSet<>(sampleCount * 2);

        for (int u_idx = 0; u_idx < dualTree.sample_size; u_idx++) {
            if (!histSeen.add(new DirectionKey(dualTree.samples[u_idx]))) {
                continue;
            }

            double max_cos = 0.0;
            for (int selected = 0; selected < new_Q.size(); selected++) {
                double cos = dualTree.sampleDot(selectedPointIndices[selected], u_idx);
                if (cos > max_cos) {
                    max_cos = cos;
                }
            }

            boolean isAxis = currentPool.get(u_idx).source == CurrentUtilityPool.Source.AXIS;
            double finalWeight = state.utilityFeedbackEnabled ? max_cos : 1.0;
            if (state.utilityFeedbackEnabled && isAxis) {
                finalWeight = StreamingState.AXIS_RETENTION_PRIORITY;
            }

            new_U_hist.add(new StreamingState.WeightedUtility(
                dualTree.samples[u_idx].clone(),
                finalWeight,
                state.current_batch,
                isAxis
            ));
        }

        int maxSize = Math.min(state.max_U_hist, new_U_hist.size());
        List<StreamingState.WeightedUtility> finalHist = new ArrayList<>();

        Set<StreamingState.WeightedUtility> finalHistSeen =
            Collections.newSetFromMap(new IdentityHashMap<>());

        List<StreamingState.WeightedUtility> axisSpies = new ArrayList<>();
        List<StreamingState.WeightedUtility> nonAxisVectors = new ArrayList<>();

        for (StreamingState.WeightedUtility wu : new_U_hist) {
            if (wu.axisProvenance) {
                axisSpies.add(wu);
            } else {
                nonAxisVectors.add(wu);
            }
        }

        finalHist.addAll(axisSpies);
        finalHistSeen.addAll(axisSpies);

        int remainingSlots = Math.max(0, maxSize - axisSpies.size());

        nonAxisVectors.sort((a, b) -> Double.compare(a.weight, b.weight));

        double minCoverageThreshold = 0.05;
        int lowCount = (int)(remainingSlots * 0.1);
        int addedLow = 0;
        for (int i = 0; i < nonAxisVectors.size() && addedLow < lowCount; i++) {
            if (nonAxisVectors.get(i).weight >= minCoverageThreshold) {
                finalHist.add(nonAxisVectors.get(i));
                finalHistSeen.add(nonAxisVectors.get(i));
                addedLow++;
            }
        }

        int highCount = (int)(remainingSlots * 0.5);
        int startHigh = Math.max(0, nonAxisVectors.size() - highCount);
        for (int i = startHigh; i < nonAxisVectors.size(); i++) {
            if (finalHistSeen.add(nonAxisVectors.get(i))) {
                finalHist.add(nonAxisVectors.get(i));
            }
        }

        if (!state.U_hist.isEmpty()) {

            Map<String, Double> oldWeights = new HashMap<>(state.U_hist.size() * 2);
            for (StreamingState.WeightedUtility old : state.U_hist) {

                String key = vectorKey(old.vector);
                oldWeights.put(key, old.weight);
            }

            List<StreamingState.WeightedUtility> declined = new ArrayList<>();
            Map<StreamingState.WeightedUtility, Double> dropOf = new IdentityHashMap<>();
            for (StreamingState.WeightedUtility wu : nonAxisVectors) {
                String key = vectorKey(wu.vector);
                Double oldWeight = oldWeights.get(key);
                if (oldWeight != null) {
                    double drop = oldWeight - wu.weight;
                    if (drop > 0.1) {
                        if (!finalHistSeen.contains(wu)) {
                            declined.add(wu);
                            dropOf.put(wu, drop);
                        }
                    }
                }
            }

            declined.sort((a, b) -> Double.compare(dropOf.get(b), dropOf.get(a)));

            int declineCount = (int)(remainingSlots * 0.2);
            for (int i = 0; i < Math.min(declineCount, declined.size()); i++) {
                finalHist.add(declined.get(i));
                finalHistSeen.add(declined.get(i));
            }
        }

        Random rand = new Random(state.retentionSeed());
        while (finalHist.size() < maxSize && nonAxisVectors.size() > 0) {
            StreamingState.WeightedUtility wu = nonAxisVectors.get(rand.nextInt(nonAxisVectors.size()));
            if (finalHistSeen.add(wu)) {
                finalHist.add(wu);
            }
        }

        state.U_hist = new ArrayList<>(finalHist);
    }

    private String vectorKey(double[] vec) {
        return state.historyVectorKey(vec);
    }

    /** Quantizes directions for stable deduplication within the sample pool. */
    private static final class DirectionKey {
        private static final double SCALE = 1e9;
        private final long[] fixed;
        private final int hash;

        DirectionKey(double[] vec) {
            fixed = new long[vec.length];
            for (int i = 0; i < vec.length; i++) {
                fixed[i] = Math.round(vec[i] * SCALE);
            }
            hash = Arrays.hashCode(fixed);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof DirectionKey
                && Arrays.equals(fixed, ((DirectionKey) other).fixed);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public Set<Integer> getRepresentatives() {
        Set<Integer> result = new HashSet<>();
        for (StreamingState.Representative rep : state.Q) {
            result.add(rep.original_idx);
        }
        return result;
    }

    public DualTree getDualTree() {
        dualTree.materializeSearchResults();
        return dualTree;
    }

    public boolean isIncrementalUsed() {
        return incrementalUsed;
    }

    public double[][] getCurrentSamples() {
        return dualTree.samples;
    }

    public CurrentUtilityPool getCurrentPool() {
        return currentPool;
    }
}
