package structures;

import utils.VectorUtil;
import java.util.*;

/**
 * Core streaming RMS: sampled utilities over merged Q and current window, bounded |Q|.
 */
public class StreamingRMS {
    
    private final int dim;
    @SuppressWarnings("unused")
    private final int k;
    @SuppressWarnings("unused")
    private final double eps;
    /** Mixing coefficient between absolute score and marginal gain. */
    private final double alpha;
    
    public StreamingState state;
    private DualTree dualTree;
    
    /** Maps row index in merged data to label (Q_* or B_*). */
    private Map<Integer, String> indexMap;
    
    /** Euclidean norm of each row using first dim coordinates (before sphere embed). */
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

    private static double usedMemMB() {
        Runtime rt = Runtime.getRuntime();
        long used = rt.totalMemory() - rt.freeMemory();
        return used / (1024.0 * 1024.0);
    }
    
    /**
     * @param batch_data tuples arriving in the current window (already sphere-embedded rows).
     */
    public StreamingRMS(int dim, int k, double eps, double alpha, StreamingState state, double[][] batch_data) {
        this.dim = dim;
        this.k = k;
        this.eps = eps;
        this.alpha = StreamingState.normalizeAlpha(alpha);
        this.state = state;
        this.indexMap = new HashMap<>();
        
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
    
    private void buildOrUpdateIndex(double[][] merged_data, double[][] samples, int total_size) {
        Set<Integer> current_Q_indices = new HashSet<>();
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
            
            Set<Integer> to_delete = new HashSet<>(state.prev_Q_indices);
            to_delete.removeAll(current_Q_indices);
            
            int Q_size = state.Q.size();
            for (int i = Q_size; i < total_size; i++) {
                this.dualTree.insert(i);
            }
            
            this.dualTree.uIdx = new ConeTree(dim + 1, k, eps, 0.99, this.dualTree);
            
        } else {
            this.dualTree = new DualTree(dim, k, eps, total_size, total_size, 
                                         samples.length, merged_data, samples);
        }
    }
    
    private double[][] mergeQAndBatch(double[][] batch_data) {
        int Q_size = state.Q.size();
        int B_size = batch_data.length;
        int total_size = Q_size + B_size;
        
        double[][] merged = new double[total_size][dim + 1];
        dataMagnitudes = new double[total_size];
        
        for (int i = 0; i < Q_size; i++) {
            dataMagnitudes[i] = computeOriginalNorm(state.Q.get(i).vector);
            System.arraycopy(state.Q.get(i).vector, 0, merged[i], 0, dim + 1);
            indexMap.put(i, "Q_" + i);
        }
        
        for (int i = 0; i < B_size; i++) {
            dataMagnitudes[Q_size + i] = computeOriginalNorm(batch_data[i]);
            System.arraycopy(batch_data[i], 0, merged[Q_size + i], 0, dim + 1);
            indexMap.put(Q_size + i, "B_" + i);
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
    
    /** adaptive | fixed | uniform utility sampling. */
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

    private double[][] generateAdaptiveSamples(double[][] data, int batch_size) {
        int s = state.sample_size;
        List<double[]> samples = new ArrayList<>();
        Random rand = new Random(42);

        int n_hist = (int)(s * state.ratio_hist);
        if (!state.U_hist.isEmpty()) {
            List<StreamingState.WeightedUtility> sorted_hist = new ArrayList<>(state.U_hist);
            sorted_hist.sort((a, b) -> Double.compare(b.weight, a.weight));

            for (int i = 0; i < Math.min(n_hist, sorted_hist.size()); i++) {
                samples.add(sorted_hist.get(i).vector.clone());
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
                samples.add(perturbed);
            }

            for (int i = 0; i < n_rand && sorted_Q.size() > 0; i++) {
                int idx = rand.nextInt(sorted_Q.size());
                double[] perturbed = perturbVector(sorted_Q.get(idx).vector, 0.15, rand);
                samples.add(perturbed);
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
                for (double[] u : samples) {
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
            samples.add(u_new);
        }

        appendAxisSamples(samples);
        return finalizeSamples(samples);
    }

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

        List<double[]> samples = new ArrayList<>();
        for (double[] sample : state.fixedSamples) {
            samples.add(sample.clone());
        }
        appendAxisSamples(samples);
        return finalizeSamples(samples);
    }

    private double[][] generateUniformSamples(double[][] data) {
        int targetSize = Math.min(state.sample_size, data.length);
        List<double[]> samples = sampleUniformCore(data, targetSize);
        appendAxisSamples(samples);
        return finalizeSamples(samples);
    }

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

    private void appendAxisSamples(List<double[]> samples) {
        for (int d = 0; d < dim; d++) {
            double[] axis = new double[dim + 1];
            axis[d] = 1.0;
            axis[dim] = 0.0;
            samples.add(axis);
        }
    }

    private double[][] finalizeSamples(List<double[]> samples) {
        double[][] result = new double[samples.size()][dim + 1];
        for (int i = 0; i < samples.size(); i++) {
            result[i] = samples.get(i);
            VectorUtil.to_unit(result[i]);
        }
        return result;
    }
    
    private double[] perturbVector(double[] v, double noise, Random rand) {
        double[] result = new double[v.length];
        for (int i = 0; i < v.length; i++) {
            result[i] = v[i] + (rand.nextDouble() - 0.5) * 2 * noise;
        }
        VectorUtil.to_unit(result);
        return result;
    }

    private double[] resolveCurrentSampleWeights() {
        double[] weights = new double[dualTree.sample_size];
        Arrays.fill(weights, 1.0);
        if (!state.utilityFeedbackEnabled) {
            return weights;
        }
        if (state.U_hist.isEmpty()) {
            return weights;
        }

        String samplingMode = StreamingState.normalizeSamplingMode(state.samplingMode);
        if (StreamingState.SAMPLING_ADAPTIVE.equals(samplingMode)) {
            int limit = Math.min(state.U_hist.size(), dualTree.sample_size);
            for (int i = 0; i < limit; i++) {
                weights[i] = state.U_hist.get(i).weight;
            }
            return weights;
        }

        Map<String, Double> histWeights = new HashMap<>();
        for (StreamingState.WeightedUtility wu : state.U_hist) {
            histWeights.put(vectorKey(wu.vector), wu.weight);
        }
        for (int i = 0; i < dualTree.sample_size; i++) {
            weights[i] = histWeights.getOrDefault(vectorKey(dualTree.samples[i]), 1.0);
        }
        return weights;
    }
    
    /** Recompute Q from current dual tree and advance window counter. */
    public void updateRepresentatives() {
        long t0 = System.nanoTime();
        
        int t = state.current_batch;
        int Q_size = state.Q.size();
        int total_size = dualTree.data_size;
        
        Map<Integer, Double> scores = computeWeightedScores(Q_size, t);
        
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
    
    private Map<Integer, Double> computeWeightedScores(int Q_size, int t) {
        Map<Integer, Double> scores = new HashMap<>();
        double[] sampleWeights = resolveCurrentSampleWeights();
        
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
        
        for (int i = 0; i < dualTree.data_size; i++) {
            double score = 0.0;
            
            double magnitude_factor = 1.0;
            if (maxMagnitude > minMagnitude) {
                magnitude_factor = 0.5 + (dataMagnitudes[i] - minMagnitude) / (maxMagnitude - minMagnitude);
            }
            
            boolean isWorldRecord = false;
            for (int d = 0; d < dim; d++) {
                double val = dualTree.data[i][d];
                if (val > state.globalMaxVals[d]) {
                    state.globalMaxVals[d] = val;
                    isWorldRecord = true;
                }
            }
            
            for (int u_idx = 0; u_idx < dualTree.sample_size; u_idx++) {
                double cos = VectorUtil.inner_product(
                    dualTree.data[i], 
                    dualTree.samples[u_idx]
                );
                
                double w_u = sampleWeights[u_idx];
                
                double time_factor = 1.0;
                if (i < Q_size && t > 0) {
                    int t_join = state.Q.get(i).t_join;
                    time_factor = 1.0 + state.lambda * (t - t_join) / (double)t;
                }
                
                score += w_u * cos * magnitude_factor * time_factor;
            }
            
            if (isWorldRecord) {
                score += state.worldRecordBonus;
            }
            
            scores.put(i, score / dualTree.sample_size);
        }
        
        return scores;
    }
    
    /**
     * Two-phase greedy: pool by absolute score (top 5r), then marginal-gain style selection in pool.
     */
    private List<StreamingState.Representative> selectDiverseTopK(
            Map<Integer, Double> scores, int Q_size, int t) {
        String phase3Selection = StreamingState.normalizePhase3SelectionStrategy(
            state.phase3SelectionStrategy
        );
        if (StreamingState.PHASE3_SELECTION_ABSOLUTE_TOPR.equals(phase3Selection)) {
            return selectAbsoluteTopR(scores, Q_size, t);
        }
        
        int total_size = dualTree.data_size;
        
        List<Integer> candidatePool = new ArrayList<>();
        if (!scores.isEmpty()) {
            List<Integer> allCandidates = new ArrayList<>(scores.keySet());
            allCandidates.sort((a, b) -> Double.compare(scores.get(b), scores.get(a)));
            
            int poolSize = Math.min(state.r * 5, allCandidates.size());
            for (int i = 0; i < poolSize; i++) {
                candidatePool.add(allCandidates.get(i));
            }
        } else {
            for (int i = 0; i < total_size; i++) {
                candidatePool.add(i);
            }
        }
        
        List<double[]> utilityVectors = new ArrayList<>();
        List<Double> utilityWeights = new ArrayList<>();
        String samplingMode = StreamingState.normalizeSamplingMode(state.samplingMode);
        if (StreamingState.SAMPLING_ADAPTIVE.equals(samplingMode) && !state.U_hist.isEmpty()) {
            for (StreamingState.WeightedUtility wu : state.U_hist) {
                utilityVectors.add(wu.vector);
                utilityWeights.add(wu.weight);
            }
        } else {
            double[] sampleWeights = resolveCurrentSampleWeights();
            for (int u_idx = 0; u_idx < dualTree.sample_size; u_idx++) {
                utilityVectors.add(dualTree.samples[u_idx]);
                utilityWeights.add(sampleWeights[u_idx]);
            }
        }
        
        int U = utilityVectors.size();
        double[] currentMax = new double[U];
        Arrays.fill(currentMax, 0.0);
        
        double[][] pointUtilityScores = new double[total_size][U];
        
        double maxMagnitude = 0.0;
        double minMagnitude = Double.MAX_VALUE;
        for (int i = 0; i < total_size; i++) {
            maxMagnitude = Math.max(maxMagnitude, dataMagnitudes[i]);
            minMagnitude = Math.min(minMagnitude, dataMagnitudes[i]);
        }
        
        for (int i = 0; i < total_size; i++) {
            double magnitude_factor = 1.0;
            if (maxMagnitude > minMagnitude) {
                magnitude_factor = 0.5 + (dataMagnitudes[i] - minMagnitude) / 
                                  (maxMagnitude - minMagnitude);
            }
            
            double[] x = dualTree.data[i];
            for (int u = 0; u < U; u++) {
                double base = VectorUtil.cosine_unit(x, utilityVectors.get(u));
                pointUtilityScores[i][u] = Math.max(0.0, base * magnitude_factor);
            }
        }
        
        List<StreamingState.Representative> new_Q = new ArrayList<>();
        Set<Integer> usedInPool = new HashSet<>();
        
        while (new_Q.size() < state.r) {
            int bestIdx = -1;
            double bestScore = -Double.MAX_VALUE;
            double bestTotalGain = 0.0;
            
            for (int idx : candidatePool) {
                if (usedInPool.contains(idx)) continue;
                
                double absoluteQuality = scores.getOrDefault(idx, 0.0);
                if (absoluteQuality < 0.3) {
                    continue;
                }
                
                double totalGain = 0.0;
                double alpha = this.alpha;
                
                for (int u = 0; u < U; u++) {
                    double absoluteScore = pointUtilityScores[idx][u];
                    double marginalGain = Math.max(0.0, absoluteScore - currentMax[u]);
                    
                    double mixedGain = (1.0 - alpha) * marginalGain + alpha * absoluteScore;
                    totalGain += utilityWeights.get(u) * mixedGain;
                }
                
                double finalScore = totalGain;
                
                if (finalScore > bestScore) {
                    bestScore = finalScore;
                    bestIdx = idx;
                    bestTotalGain = totalGain;
                }
            }
            
            if (bestIdx == -1 && new_Q.size() < state.r) {
                double fallbackBest = -Double.MAX_VALUE;
                
                for (int idx : candidatePool) {
                    if (usedInPool.contains(idx)) continue;
                    
                    double rawScore = scores.getOrDefault(idx, 0.0);
                    
                    boolean isTooSimilar = false;
                for (StreamingState.Representative rep : new_Q) {
                        double cos = VectorUtil.cosine_unit(dualTree.data[idx], rep.vector);
                        if (cos > 0.99) {
                            isTooSimilar = true;
                        break;
                        }
                    }
                    
                    double finalFallbackScore = isTooSimilar ? rawScore * 0.8 : rawScore;
                    
                    if (finalFallbackScore > fallbackBest) {
                        fallbackBest = finalFallbackScore;
                        bestIdx = idx;
                        bestScore = finalFallbackScore;
                    }
                }
            }
            
            if (bestIdx == -1) {
                break;
            }
            
            for (int u = 0; u < U; u++) {
                double val = pointUtilityScores[bestIdx][u];
                if (val > currentMax[u]) {
                    currentMax[u] = val;
                }
            }
            usedInPool.add(bestIdx);
            
            int t_join = t;
            int orig_idx = bestIdx;
            if (bestIdx < Q_size) {
                t_join = state.Q.get(bestIdx).t_join;
                orig_idx = state.Q.get(bestIdx).original_idx;
            }
            
            double repScore = scores.containsKey(bestIdx)
                ? scores.get(bestIdx)
                : bestTotalGain / Math.max(1, U);
                
                new_Q.add(new StreamingState.Representative(
                dualTree.data[bestIdx].clone(), 
                repScore, 
                    t_join,
                    orig_idx
                ));
            }
        
        return new_Q;
    }

    /** Phase-3 ablation: take top-r by precomputed absolute score only. */
    private List<StreamingState.Representative> selectAbsoluteTopR(
            Map<Integer, Double> scores, int Q_size, int t) {
        List<Integer> ranked = new ArrayList<>(scores.keySet());
        ranked.sort((a, b) -> Double.compare(scores.getOrDefault(b, 0.0), scores.getOrDefault(a, 0.0)));

        int limit = Math.min(state.r, ranked.size());
        List<StreamingState.Representative> newQ = new ArrayList<>(limit);
        for (int i = 0; i < limit; i++) {
            int idx = ranked.get(i);
            int tJoin = t;
            int origIdx = idx;
            if (idx < Q_size) {
                tJoin = state.Q.get(idx).t_join;
                origIdx = state.Q.get(idx).original_idx;
            }
            double repScore = scores.getOrDefault(idx, 0.0);
            newQ.add(new StreamingState.Representative(
                dualTree.data[idx].clone(),
                repScore,
                tJoin,
                origIdx
            ));
        }
        return newQ;
    }
    
    /** Similarity-based penalty vs current Q (optional; currently unused in main pick). */
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
    
    /** Log-sum welfare over utilities (optional fairness-style objective). */
    private double computeLogSumUtility(
            List<StreamingState.Representative> Q,
            List<double[]> utilityVectors,
            List<Double> utilityWeights) {
        
        int U = utilityVectors.size();
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
    
    /** True if v is (approximately) axis-aligned in the first dim coordinates. */
    private boolean isAxisAligned(double[] v) {
        int countNearOne = 0;
        int countNearZero = 0;
        double threshold = 0.95;
        
        for (int d = 0; d < dim; d++) {
            if (Math.abs(v[d]) > threshold) {
                countNearOne++;
            } else if (Math.abs(v[d]) < 0.05) {
                countNearZero++;
            }
        }
        
        return (countNearOne == 1 && countNearZero >= dim - 1);
    }
    
    /** Refresh U_hist with mixed retention (low/high coverage, decline, axis-aligned always kept). */
    private void updateUtilityHistory(List<StreamingState.Representative> new_Q) {
        List<StreamingState.WeightedUtility> new_U_hist = new ArrayList<>();
        
        for (int u_idx = 0; u_idx < dualTree.sample_size; u_idx++) {
            double max_cos = 0.0;
            for (StreamingState.Representative rep : new_Q) {
                double cos = VectorUtil.cosine_unit(rep.vector, dualTree.samples[u_idx]);
                if (cos > max_cos) {
                    max_cos = cos;
                }
            }
            
            double finalWeight = state.utilityFeedbackEnabled ? max_cos : 1.0;
            if (state.utilityFeedbackEnabled && isAxisAligned(dualTree.samples[u_idx])) {
                finalWeight = state.axisSpyWeight;
            }
            
            new_U_hist.add(new StreamingState.WeightedUtility(
                dualTree.samples[u_idx].clone(), 
                finalWeight,
                state.current_batch
            ));
        }
        
        int maxSize = Math.min(state.max_U_hist, new_U_hist.size());
        List<StreamingState.WeightedUtility> finalHist = new ArrayList<>();
        
        List<StreamingState.WeightedUtility> axisSpies = new ArrayList<>();
        List<StreamingState.WeightedUtility> nonAxisVectors = new ArrayList<>();
        
        for (StreamingState.WeightedUtility wu : new_U_hist) {
            if (isAxisAligned(wu.vector)) {
                axisSpies.add(wu);
            } else {
                nonAxisVectors.add(wu);
            }
        }
        
        finalHist.addAll(axisSpies);
        
        int remainingSlots = Math.max(0, maxSize - axisSpies.size());
        
        nonAxisVectors.sort((a, b) -> Double.compare(a.weight, b.weight));
        
        double minCoverageThreshold = 0.05;
        int lowCount = (int)(remainingSlots * 0.1);
        int addedLow = 0;
        for (int i = 0; i < nonAxisVectors.size() && addedLow < lowCount; i++) {
            if (nonAxisVectors.get(i).weight >= minCoverageThreshold) {
                finalHist.add(nonAxisVectors.get(i));
                addedLow++;
            }
        }
        
        int highCount = (int)(remainingSlots * 0.5);
        int startHigh = Math.max(0, nonAxisVectors.size() - highCount);
        for (int i = startHigh; i < nonAxisVectors.size(); i++) {
            if (!finalHist.contains(nonAxisVectors.get(i))) {
                finalHist.add(nonAxisVectors.get(i));
            }
        }
        
        if (!state.U_hist.isEmpty()) {
            Map<String, Double> oldWeights = new HashMap<>();
            for (StreamingState.WeightedUtility old : state.U_hist) {
                String key = vectorKey(old.vector);
                oldWeights.put(key, old.weight);
            }
            
            List<StreamingState.WeightedUtility> declined = new ArrayList<>();
            for (StreamingState.WeightedUtility wu : nonAxisVectors) {
                String key = vectorKey(wu.vector);
                if (oldWeights.containsKey(key)) {
                    double drop = oldWeights.get(key) - wu.weight;
                    if (drop > 0.1) {
                        if (!finalHist.contains(wu)) {
                            declined.add(wu);
                        }
                    }
                }
            }
            
            declined.sort((a, b) -> {
                double dropA = oldWeights.getOrDefault(vectorKey(a.vector), 0.0) - a.weight;
                double dropB = oldWeights.getOrDefault(vectorKey(b.vector), 0.0) - b.weight;
                return Double.compare(dropB, dropA);
            });
            
            int declineCount = (int)(remainingSlots * 0.2);
            for (int i = 0; i < Math.min(declineCount, declined.size()); i++) {
                finalHist.add(declined.get(i));
            }
        }
        
        Random rand = new Random(state.current_batch);
        while (finalHist.size() < maxSize && nonAxisVectors.size() > 0) {
            StreamingState.WeightedUtility wu = nonAxisVectors.get(rand.nextInt(nonAxisVectors.size()));
            if (!finalHist.contains(wu)) {
                finalHist.add(wu);
            }
        }
        
        state.U_hist = new ArrayList<>(finalHist);
    }
    
    private String vectorKey(double[] vec) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.min(3, vec.length); i++) {
            sb.append(String.format("%.3f,", vec[i]));
        }
        return sb.toString();
    }
    
    public Set<Integer> getRepresentatives() {
        Set<Integer> result = new HashSet<>();
        for (StreamingState.Representative rep : state.Q) {
            result.add(rep.original_idx);
        }
        return result;
    }
    
    public DualTree getDualTree() {
        return dualTree;
    }
    
    public boolean isIncrementalUsed() {
        return incrementalUsed;
    }
    
    public double[][] getCurrentSamples() {
        return dualTree.samples;
    }
}

