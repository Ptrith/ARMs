package structures;

import java.io.*;
import java.util.*;

/** Serializable state, configuration and bounded history shared across input batches. */
public class StreamingState implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final double DEFAULT_ALPHA = 0.4;

    public static final double DEFAULT_WORLD_RECORD_BONUS = 0.0;

    public static final double DEFAULT_AXIS_OBJECTIVE_WEIGHT = 100.0;

    /** Priority for retaining axis directions in history, independent of their objective weight. */
    public static final double AXIS_RETENTION_PRIORITY = 100.0;
    public static final String SAMPLING_ADAPTIVE = "adaptive";
    public static final String SAMPLING_FIXED = "fixed";
    public static final String SAMPLING_UNIFORM = "uniform";
    public static final String PHASE3_SELECTION_MIXED_GAIN = "mixed_gain";
    public static final String PHASE3_SELECTION_ABSOLUTE_TOPR = "absolute_topr";
    public static final String INDEX_MODE_TREE = "tree";
    public static final String INDEX_MODE_NONE = "none";
    public static final int DEFAULT_RMS_K = 1;

    public static final int DEFAULT_SAMPLING_SEED = 42;
    public static final String K_TRUNCATION_HARD = "hard";
    public static final String K_TRUNCATION_SOFT = "soft";
    public static final double DEFAULT_K_SOFT_BETA = 1.0;
    public static final double DEFAULT_K_SOFT_GAMMA = 1.0;
    public static final double DEFAULT_K_SOFT_ETA = 1.0;

    /** A representative vector with its score, insertion batch and candidate index. */
    public static class Representative implements Serializable {
        private static final long serialVersionUID = 1L;

        public double[] vector;
        public double score;
        public int t_join;
        public int original_idx;

        public Representative(double[] vec, double score, int t, int idx) {
            this.vector = vec;
            this.score = score;
            this.t_join = t;
            this.original_idx = idx;
        }
    }

    /** A retained utility direction with feedback weight and persistent axis provenance. */
    public static class WeightedUtility implements Serializable {
        private static final long serialVersionUID = 1L;

        public double[] vector;
        public double weight;
        public int last_update;

        public boolean axisProvenance;

        public WeightedUtility(double[] vec, double w, int t) {
            this(vec, w, t, false);
        }

        public WeightedUtility(double[] vec, double w, int t, boolean axisProvenance) {
            this.vector = vec;
            this.weight = w;
            this.last_update = t;
            this.axisProvenance = axisProvenance;
        }
    }

    public List<Representative> Q;
    public List<WeightedUtility> U_hist;

    private transient HistoryKeyCache historyKeyCache;

    String historyVectorKey(double[] vector) {
        if (historyKeyCache == null) historyKeyCache = new HistoryKeyCache();
        return historyKeyCache.key(vector);
    }

    public int current_batch;

    public Set<Integer> prev_Q_indices;
    public boolean enable_incremental;

    public double[] globalMaxVals;

    public int r;
    public int max_U_hist;
    /** Strength of historical retention; larger nonnegative values favor older representatives. */
    public double lambda;
    public double tau;
    public double beta;
    public double delta;
    public double min_cos;
    public int sample_size;
    public double ratio_hist;
    public double ratio_seed;
    public double ratio_new;
    public double alpha;
    /** Optional coordinate-maximum score bonus; disabled by the supplied launcher. */
    public double worldRecordBonus;
    public double axisObjectiveWeight;
    private boolean axisWeightsConfigured;
    public boolean utilityFeedbackEnabled;
    private boolean utilityFeedbackConfigured;
    /** Controls current objective weights without disabling utility-history retention. */
    public boolean objectiveWeightingEnabled;
    private boolean objectiveWeightingConfigured;
    private boolean alphaConfigured;
    public transient boolean alphaRecoveredFromLegacyState;
    public int rmsK;
    private boolean rmsKConfigured;
    public transient boolean rmsKRecoveredFromLegacyState;
    public String kTruncationStrategy;
    private boolean kTruncationStrategyConfigured;
    public transient boolean kTruncationRecoveredFromLegacyState;
    public double kSoftBeta;
    private boolean kSoftBetaConfigured;
    public double kSoftGamma;
    private boolean kSoftGammaConfigured;
    public double kSoftEta;
    private boolean kSoftEtaConfigured;

    public int samplingSeed;
    private boolean samplingSeedConfigured;
    public String samplingMode;
    private boolean samplingModeConfigured;
    public transient boolean samplingModeRecoveredFromLegacyState;

    public boolean axisSamplesEnabled;
    private boolean axisSamplesConfigured;
    public String phase3SelectionStrategy;
    private boolean phase3SelectionStrategyConfigured;
    public transient boolean phase3SelectionRecoveredFromLegacyState;
    public String indexMode;
    private boolean indexModeConfigured;
    public transient boolean indexModeRecoveredFromLegacyState;
    public List<double[]> fixedSamples;

    public long total_points_seen;
    public long total_processing_time;

    public StreamingState(int r) {
        this.Q = new ArrayList<>();
        this.U_hist = new ArrayList<>();
        this.current_batch = 0;
        this.r = r;

        this.prev_Q_indices = new HashSet<>();
        this.enable_incremental = true;

        this.max_U_hist = 100;
        this.lambda = 0.7;
        this.tau = 0.985;
        this.beta = 0.8;
        this.delta = 0.1;
        this.min_cos = 0.99;
        this.sample_size = 50;
        this.ratio_hist = 0.6;
        this.ratio_seed = 0.2;
        this.ratio_new = 0.2;
        this.alpha = DEFAULT_ALPHA;
        this.worldRecordBonus = DEFAULT_WORLD_RECORD_BONUS;
        this.axisObjectiveWeight = DEFAULT_AXIS_OBJECTIVE_WEIGHT;
        this.axisWeightsConfigured = true;
        this.utilityFeedbackEnabled = true;
        this.utilityFeedbackConfigured = true;
        this.objectiveWeightingEnabled = true;
        this.objectiveWeightingConfigured = true;
        this.alphaConfigured = true;
        this.alphaRecoveredFromLegacyState = false;
        this.rmsK = DEFAULT_RMS_K;
        this.rmsKConfigured = true;
        this.rmsKRecoveredFromLegacyState = false;
        this.kTruncationStrategy = K_TRUNCATION_HARD;
        this.kTruncationStrategyConfigured = true;
        this.kTruncationRecoveredFromLegacyState = false;
        this.kSoftBeta = DEFAULT_K_SOFT_BETA;
        this.kSoftBetaConfigured = true;
        this.kSoftGamma = DEFAULT_K_SOFT_GAMMA;
        this.kSoftGammaConfigured = true;
        this.kSoftEta = DEFAULT_K_SOFT_ETA;
        this.kSoftEtaConfigured = true;
        this.samplingSeed = DEFAULT_SAMPLING_SEED;
        this.samplingSeedConfigured = true;
        this.samplingMode = SAMPLING_ADAPTIVE;
        this.samplingModeConfigured = true;
        this.samplingModeRecoveredFromLegacyState = false;
        this.axisSamplesEnabled = true;
        this.axisSamplesConfigured = true;
        this.phase3SelectionStrategy = PHASE3_SELECTION_MIXED_GAIN;
        this.phase3SelectionStrategyConfigured = true;
        this.phase3SelectionRecoveredFromLegacyState = false;
        this.indexMode = INDEX_MODE_TREE;
        this.indexModeConfigured = true;
        this.indexModeRecoveredFromLegacyState = false;
        this.fixedSamples = new ArrayList<>();

        this.total_points_seen = 0;
        this.total_processing_time = 0;
    }

    public void setHyperparameters(double lambda, double tau, double beta,
                                   double delta, double min_cos, int sample_size) {
        this.lambda = lambda;
        this.tau = tau;
        this.beta = beta;
        this.delta = delta;
        this.min_cos = min_cos;
        this.sample_size = sample_size;
    }

    public void setSamplingRatios(double ratio_hist, double ratio_seed, double ratio_new) {
        this.ratio_hist = ratio_hist;
        this.ratio_seed = ratio_seed;
        this.ratio_new = ratio_new;
    }

    public void setAlpha(double alpha) {
        this.alpha = normalizeAlpha(alpha);
        this.alphaConfigured = true;
    }

    public void setRmsK(int rmsK) {
        this.rmsK = normalizeRmsK(rmsK);
        this.rmsKConfigured = true;
    }

    public void setKTruncationStrategy(String strategy) {
        this.kTruncationStrategy = normalizeKTruncationStrategy(strategy);
        this.kTruncationStrategyConfigured = true;
    }

    public void setKSoftBeta(double beta) {
        this.kSoftBeta = normalizeKSoftBeta(beta);
        this.kSoftBetaConfigured = true;
    }

    public void setKSoftGamma(double gamma) {
        this.kSoftGamma = normalizeKSoftGamma(gamma);
        this.kSoftGammaConfigured = true;
    }

    public void setKSoftEta(double eta) {
        this.kSoftEta = normalizeKSoftEta(eta);
        this.kSoftEtaConfigured = true;
    }

    public void setSamplingMode(String samplingMode) {
        this.samplingMode = normalizeSamplingMode(samplingMode);
        this.samplingModeConfigured = true;
    }

    public void setAxisSamplesEnabled(boolean enabled) {
        this.axisSamplesEnabled = enabled;
        this.axisSamplesConfigured = true;
    }

    /** Sets the reproducible seed used for randomized direction generation. */
    public void setSamplingSeed(int seed) {
        this.samplingSeed = seed;
        this.samplingSeedConfigured = true;
    }

    /** Derives the random seed used by utility-history retention. */
    public long retentionSeed() {
        return current_batch + (samplingSeed - DEFAULT_SAMPLING_SEED) * 1000003L;
    }

    public void setPhase3SelectionStrategy(String strategy) {
        this.phase3SelectionStrategy = normalizePhase3SelectionStrategy(strategy);
        this.phase3SelectionStrategyConfigured = true;
    }

    public void setIndexMode(String mode) {
        this.indexMode = normalizeIndexMode(mode);
        this.indexModeConfigured = true;
    }

    public void setHeuristicWeights(double worldRecordBonus, double axisSpyWeight) {
        this.worldRecordBonus = normalizeWorldRecordBonus(worldRecordBonus);

        this.axisObjectiveWeight = normalizeAxisWeight(axisSpyWeight);
        this.axisWeightsConfigured = true;
    }

    public void setUtilityFeedbackEnabled(boolean enabled) {
        this.utilityFeedbackEnabled = enabled;
        this.utilityFeedbackConfigured = true;
    }

    public void setObjectiveWeightingEnabled(boolean enabled) {
        this.objectiveWeightingEnabled = enabled;
        this.objectiveWeightingConfigured = true;
    }

    /** Historical-retention factor: 1 + lambda * max(0, t - tJoin) / t.
     * Returns 1 at t <= 0; for nonnegative lambda it increases with representative age. */
    public static double timeFactor(int currentBatch, int tJoin, double lambda) {
        if (currentBatch <= 0) {
            return 1.0;
        }
        double relativeAge = (currentBatch - tJoin) / (double) currentBatch;
        if (relativeAge < 0.0) {
            relativeAge = 0.0;
        }
        return 1.0 + lambda * relativeAge;
    }

    public static double normalizeAlpha(double alpha) {
        if (Double.isNaN(alpha) || Double.isInfinite(alpha) || alpha < 0.0 || alpha > 1.0) {
            return DEFAULT_ALPHA;
        }
        return alpha;
    }

    public static int normalizeRmsK(int rmsK) {
        return Math.max(DEFAULT_RMS_K, rmsK);
    }

    public static String normalizeKTruncationStrategy(String strategy) {
        if (strategy == null) {
            return K_TRUNCATION_HARD;
        }
        String normalized = strategy.trim().toLowerCase(Locale.ROOT);
        if (K_TRUNCATION_SOFT.equals(normalized)) {
            return K_TRUNCATION_SOFT;
        }
        return K_TRUNCATION_HARD;
    }

    public static double normalizeKSoftBeta(double beta) {
        if (Double.isNaN(beta) || Double.isInfinite(beta) || beta <= 0.0) {
            return DEFAULT_K_SOFT_BETA;
        }
        return beta;
    }

    public static double normalizeKSoftGamma(double gamma) {
        if (Double.isNaN(gamma) || Double.isInfinite(gamma) || gamma <= 0.0) {
            return DEFAULT_K_SOFT_GAMMA;
        }
        return gamma;
    }

    public static double normalizeKSoftEta(double eta) {
        if (Double.isNaN(eta) || Double.isInfinite(eta) || eta < 0.0) {
            return DEFAULT_K_SOFT_ETA;
        }
        return eta;
    }

    public static String normalizeSamplingMode(String samplingMode) {
        if (samplingMode == null) {
            return SAMPLING_ADAPTIVE;
        }
        String normalized = samplingMode.trim().toLowerCase(Locale.ROOT);
        if (SAMPLING_FIXED.equals(normalized) || SAMPLING_UNIFORM.equals(normalized)) {
            return normalized;
        }
        return SAMPLING_ADAPTIVE;
    }

    public static double normalizeWorldRecordBonus(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value) || value < 0.0) {
            return DEFAULT_WORLD_RECORD_BONUS;
        }
        return value;
    }

    public static double normalizeAxisWeight(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value) || value < 0.0) {
            return DEFAULT_AXIS_OBJECTIVE_WEIGHT;
        }
        return value;
    }

    public static String normalizePhase3SelectionStrategy(String strategy) {
        if (strategy == null) {
            return PHASE3_SELECTION_MIXED_GAIN;
        }
        String normalized = strategy.trim().toLowerCase(Locale.ROOT);
        if (PHASE3_SELECTION_ABSOLUTE_TOPR.equals(normalized)) {
            return normalized;
        }
        return PHASE3_SELECTION_MIXED_GAIN;
    }

    public static String normalizeIndexMode(String mode) {
        if (mode == null) {
            return INDEX_MODE_TREE;
        }
        String normalized = mode.trim().toLowerCase(Locale.ROOT);
        if (INDEX_MODE_NONE.equals(normalized)) {
            return INDEX_MODE_NONE;
        }
        return INDEX_MODE_TREE;
    }

    public void save(String path) throws IOException {
        File file = new File(path);
        file.getParentFile().mkdirs();

        try (ObjectOutputStream oos = new ObjectOutputStream(
                new BufferedOutputStream(new FileOutputStream(file)))) {
            oos.writeObject(this);
        }
    }

    public static StreamingState load(String path) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(
                new BufferedInputStream(new FileInputStream(path)))) {
            return (StreamingState) ois.readObject();
        }
    }

    /** Restores serialized state and initializes fields absent from older checkpoints. */
    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        ois.defaultReadObject();
        if (!alphaConfigured) {
            alpha = DEFAULT_ALPHA;
            alphaConfigured = true;
            alphaRecoveredFromLegacyState = true;
        } else {
            alpha = normalizeAlpha(alpha);
            alphaRecoveredFromLegacyState = false;
        }
        if (!samplingSeedConfigured) {
            samplingSeed = DEFAULT_SAMPLING_SEED;
            samplingSeedConfigured = true;
        }
        if (!samplingModeConfigured) {
            samplingMode = SAMPLING_ADAPTIVE;
            samplingModeConfigured = true;
            samplingModeRecoveredFromLegacyState = true;
        } else {
            samplingMode = normalizeSamplingMode(samplingMode);
            samplingModeRecoveredFromLegacyState = false;
        }
        if (!axisSamplesConfigured) {
            axisSamplesEnabled = true;
            axisSamplesConfigured = true;
        }
        if (!phase3SelectionStrategyConfigured) {
            phase3SelectionStrategy = PHASE3_SELECTION_MIXED_GAIN;
            phase3SelectionStrategyConfigured = true;
            phase3SelectionRecoveredFromLegacyState = true;
        } else {
            phase3SelectionStrategy = normalizePhase3SelectionStrategy(phase3SelectionStrategy);
            phase3SelectionRecoveredFromLegacyState = false;
        }
        if (!indexModeConfigured) {
            indexMode = INDEX_MODE_TREE;
            indexModeConfigured = true;
            indexModeRecoveredFromLegacyState = true;
        } else {
            indexMode = normalizeIndexMode(indexMode);
            indexModeRecoveredFromLegacyState = false;
        }
        if (!rmsKConfigured) {
            rmsK = DEFAULT_RMS_K;
            rmsKConfigured = true;
            rmsKRecoveredFromLegacyState = true;
        } else {
            rmsK = normalizeRmsK(rmsK);
            rmsKRecoveredFromLegacyState = false;
        }
        if (!kTruncationStrategyConfigured) {
            kTruncationStrategy = K_TRUNCATION_HARD;
            kTruncationStrategyConfigured = true;
            kTruncationRecoveredFromLegacyState = true;
        } else {
            kTruncationStrategy = normalizeKTruncationStrategy(kTruncationStrategy);
            kTruncationRecoveredFromLegacyState = false;
        }
        if (!kSoftBetaConfigured) {
            kSoftBeta = DEFAULT_K_SOFT_BETA;
            kSoftBetaConfigured = true;
        } else {
            kSoftBeta = normalizeKSoftBeta(kSoftBeta);
        }
        if (!kSoftGammaConfigured) {
            kSoftGamma = DEFAULT_K_SOFT_GAMMA;
            kSoftGammaConfigured = true;
        } else {
            kSoftGamma = normalizeKSoftGamma(kSoftGamma);
        }
        if (!kSoftEtaConfigured) {
            kSoftEta = DEFAULT_K_SOFT_ETA;
            kSoftEtaConfigured = true;
        } else {
            kSoftEta = normalizeKSoftEta(kSoftEta);
        }
        worldRecordBonus = normalizeWorldRecordBonus(worldRecordBonus);
        if (!axisWeightsConfigured) {

            axisObjectiveWeight = DEFAULT_AXIS_OBJECTIVE_WEIGHT;
            axisWeightsConfigured = true;
        } else {
            axisObjectiveWeight = normalizeAxisWeight(axisObjectiveWeight);
        }
        if (!utilityFeedbackConfigured) {
            utilityFeedbackEnabled = true;
            utilityFeedbackConfigured = true;
        }

        if (U_hist != null) {
            for (WeightedUtility utility : U_hist) {
                if (utility.axisProvenance) {
                    utility.weight = utilityFeedbackEnabled ? AXIS_RETENTION_PRIORITY : 1.0;
                }
            }
        }
        if (!objectiveWeightingConfigured) {
            objectiveWeightingEnabled = true;
            objectiveWeightingConfigured = true;
        }
        if (fixedSamples == null) {
            fixedSamples = new ArrayList<>();
        }
    }

    public String getSummary() {
        return String.format(
            "StreamingState[batch=%d, |Q|=%d, |U_hist|=%d, mode=%s, index_mode=%s, rms_k=%d, k_truncation=%s, total_points=%d, avg_time=%.2fms]",
            current_batch, Q.size(), U_hist.size(), samplingMode, indexMode, rmsK, kTruncationStrategy, total_points_seen,
            current_batch > 0 ? total_processing_time / (double)current_batch : 0.0
        );
    }

    public void pruneStaleUtilities(int max_age) {
        U_hist.removeIf(u -> (current_batch - u.last_update) > max_age);
    }
}
