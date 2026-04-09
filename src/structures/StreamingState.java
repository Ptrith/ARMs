package structures;

import java.io.*;
import java.util.*;

/**
 * Serializable state for streaming RMS: representative set Q, utility history U_hist, hyperparameters.
 */
public class StreamingState implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final double DEFAULT_ALPHA = 0.4;
    public static final double DEFAULT_WORLD_RECORD_BONUS = 1000.0;
    public static final double DEFAULT_AXIS_SPY_WEIGHT = 100.0;
    public static final String SAMPLING_ADAPTIVE = "adaptive";
    public static final String SAMPLING_FIXED = "fixed";
    public static final String SAMPLING_UNIFORM = "uniform";
    public static final String PHASE3_SELECTION_MIXED_GAIN = "mixed_gain";
    public static final String PHASE3_SELECTION_ABSOLUTE_TOPR = "absolute_topr";
    
    /**
     * One point in Q: embedding, score, join window index, original data index.
     */
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
    
    /**
     * Historical utility direction with weight.
     */
    public static class WeightedUtility implements Serializable {
        private static final long serialVersionUID = 1L;
        
        public double[] vector;
        public double weight;
        public int last_update;
        
        public WeightedUtility(double[] vec, double w, int t) {
            this.vector = vec;
            this.weight = w;
            this.last_update = t;
        }
    }
    
    public List<Representative> Q;
    public List<WeightedUtility> U_hist;
    /** Current window index (field name kept for serialized state compatibility). */
    public int current_batch;
    
    public Set<Integer> prev_Q_indices;
    public boolean enable_incremental;
    
    public double[] globalMaxVals;
    
    public int r;
    public int max_U_hist;
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
    public double worldRecordBonus;
    public double axisSpyWeight;
    public boolean utilityFeedbackEnabled;
    private boolean utilityFeedbackConfigured;
    private boolean alphaConfigured;
    public transient boolean alphaRecoveredFromLegacyState;
    public String samplingMode;
    private boolean samplingModeConfigured;
    public transient boolean samplingModeRecoveredFromLegacyState;
    public String phase3SelectionStrategy;
    private boolean phase3SelectionStrategyConfigured;
    public transient boolean phase3SelectionRecoveredFromLegacyState;
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
        this.axisSpyWeight = DEFAULT_AXIS_SPY_WEIGHT;
        this.utilityFeedbackEnabled = true;
        this.utilityFeedbackConfigured = true;
        this.alphaConfigured = true;
        this.alphaRecoveredFromLegacyState = false;
        this.samplingMode = SAMPLING_ADAPTIVE;
        this.samplingModeConfigured = true;
        this.samplingModeRecoveredFromLegacyState = false;
        this.phase3SelectionStrategy = PHASE3_SELECTION_MIXED_GAIN;
        this.phase3SelectionStrategyConfigured = true;
        this.phase3SelectionRecoveredFromLegacyState = false;
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

    public void setSamplingMode(String samplingMode) {
        this.samplingMode = normalizeSamplingMode(samplingMode);
        this.samplingModeConfigured = true;
    }

    public void setPhase3SelectionStrategy(String strategy) {
        this.phase3SelectionStrategy = normalizePhase3SelectionStrategy(strategy);
        this.phase3SelectionStrategyConfigured = true;
    }

    public void setHeuristicWeights(double worldRecordBonus, double axisSpyWeight) {
        this.worldRecordBonus = normalizeWorldRecordBonus(worldRecordBonus);
        this.axisSpyWeight = normalizeAxisSpyWeight(axisSpyWeight);
    }

    public void setUtilityFeedbackEnabled(boolean enabled) {
        this.utilityFeedbackEnabled = enabled;
        this.utilityFeedbackConfigured = true;
    }
    
    public static double normalizeAlpha(double alpha) {
        if (Double.isNaN(alpha) || Double.isInfinite(alpha) || alpha < 0.0 || alpha > 1.0) {
            return DEFAULT_ALPHA;
        }
        return alpha;
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

    public static double normalizeAxisSpyWeight(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value) || value < 0.0) {
            return DEFAULT_AXIS_SPY_WEIGHT;
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
        if (!samplingModeConfigured) {
            samplingMode = SAMPLING_ADAPTIVE;
            samplingModeConfigured = true;
            samplingModeRecoveredFromLegacyState = true;
        } else {
            samplingMode = normalizeSamplingMode(samplingMode);
            samplingModeRecoveredFromLegacyState = false;
        }
        if (!phase3SelectionStrategyConfigured) {
            phase3SelectionStrategy = PHASE3_SELECTION_MIXED_GAIN;
            phase3SelectionStrategyConfigured = true;
            phase3SelectionRecoveredFromLegacyState = true;
        } else {
            phase3SelectionStrategy = normalizePhase3SelectionStrategy(phase3SelectionStrategy);
            phase3SelectionRecoveredFromLegacyState = false;
        }
        worldRecordBonus = normalizeWorldRecordBonus(worldRecordBonus);
        axisSpyWeight = normalizeAxisSpyWeight(axisSpyWeight);
        if (!utilityFeedbackConfigured) {
            utilityFeedbackEnabled = true;
            utilityFeedbackConfigured = true;
        }
        if (fixedSamples == null) {
            fixedSamples = new ArrayList<>();
        }
    }
    
    public String getSummary() {
        return String.format(
            "StreamingState[window=%d, |Q|=%d, |U_hist|=%d, mode=%s, total_points=%d, avg_time=%.2fms]",
            current_batch, Q.size(), U_hist.size(), samplingMode, total_points_seen,
            current_batch > 0 ? total_processing_time / (double)current_batch : 0.0
        );
    }
    
    /** Drop utilities not updated for more than max_age windows. */
    public void pruneStaleUtilities(int max_age) {
        U_hist.removeIf(u -> (current_batch - u.last_update) > max_age);
    }
}
