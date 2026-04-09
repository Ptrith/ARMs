package structures;

import utils.VectorUtil;
import java.util.*;

/**
 * Quality metrics for streaming RMS: coverage, diversity, average scores.
 */
public class QualityMetrics {
    
    /**
     * Fraction of sample utilities that are within threshold cosine similarity of some point in Q.
     */
    public static double computeCoverage(List<StreamingState.Representative> Q, 
                                        double[][] samples, 
                                        double threshold) {
        if (Q.isEmpty() || samples.length == 0) return 0.0;
        
        int covered = 0;
        for (double[] u : samples) {
            boolean is_covered = false;
            for (StreamingState.Representative rep : Q) {
                double cos = VectorUtil.cosine_unit(rep.vector, u);
                if (cos >= threshold) {
                    is_covered = true;
                    break;
                }
            }
            if (is_covered) covered++;
        }
        
        return (double) covered / samples.length;
    }
    
    /**
     * Average over Q of (1 - min_{j!=i} cos(q_i, q_j)); higher means more spread out.
     */
    public static double computeDiversity(List<StreamingState.Representative> Q) {
        if (Q.size() <= 1) return 0.0;
        
        double sum_min_dist = 0.0;
        for (int i = 0; i < Q.size(); i++) {
            double min_cos = 1.0;
            for (int j = 0; j < Q.size(); j++) {
                if (i == j) continue;
                double cos = VectorUtil.cosine_unit(Q.get(i).vector, Q.get(j).vector);
                min_cos = Math.min(min_cos, cos);
            }
            sum_min_dist += (1.0 - min_cos);
        }
        
        return sum_min_dist / Q.size();
    }
    
    public static double computeAverageScore(List<StreamingState.Representative> Q) {
        if (Q.isEmpty()) return 0.0;
        
        double sum = 0.0;
        for (StreamingState.Representative rep : Q) {
            sum += rep.score;
        }
        return sum / Q.size();
    }
    
    public static double computeAverageUtilityWeight(List<StreamingState.WeightedUtility> U_hist) {
        if (U_hist.isEmpty()) return 0.0;
        
        double sum = 0.0;
        for (StreamingState.WeightedUtility u : U_hist) {
            sum += u.weight;
        }
        return sum / U_hist.size();
    }
    
    public static class QualityReport {
        public double coverage;
        public double diversity;
        public double avg_score;
        public double avg_utility_weight;
        public int Q_size;
        public int U_hist_size;
        
        public QualityReport(double coverage, double diversity, double avg_score, 
                            double avg_utility_weight, int Q_size, int U_hist_size) {
            this.coverage = coverage;
            this.diversity = diversity;
            this.avg_score = avg_score;
            this.avg_utility_weight = avg_utility_weight;
            this.Q_size = Q_size;
            this.U_hist_size = U_hist_size;
        }
        
        @Override
        public String toString() {
            return String.format(
                "QualityReport[Coverage=%.3f, Diversity=%.3f, AvgScore=%.3f, " +
                "AvgUtilityWeight=%.3f, |Q|=%d, |U_hist|=%d]",
                coverage, diversity, avg_score, avg_utility_weight, Q_size, U_hist_size
            );
        }
        
        public String toCsvRow() {
            return String.format("%.6f,%.6f,%.6f,%.6f,%d,%d",
                coverage, diversity, avg_score, avg_utility_weight, Q_size, U_hist_size);
        }
    }
    
    public static QualityReport generateReport(StreamingState state, 
                                               double[][] samples,
                                               double coverage_threshold) {
        double coverage = computeCoverage(state.Q, samples, coverage_threshold);
        double diversity = computeDiversity(state.Q);
        double avg_score = computeAverageScore(state.Q);
        double avg_utility_weight = computeAverageUtilityWeight(state.U_hist);
        
        return new QualityReport(coverage, diversity, avg_score, avg_utility_weight,
                                state.Q.size(), state.U_hist.size());
    }
}
