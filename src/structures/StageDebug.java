package structures;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;
import java.util.List;

/** Optional bounded diagnostic logging for utility binding and greedy selection.
 * Disabled unless explicitly enabled; diagnostic write failures do not interrupt selection. */
public final class StageDebug {

    private static boolean enabled = false;
    private static int maxBatches = 0;
    private static BufferedWriter utilityWriter;
    private static BufferedWriter phase3Writer;
    private static BufferedWriter celfWriter;
    private static BufferedWriter bindingWriter;
    private static BufferedWriter shadowWriter;

    private StageDebug() {
    }

    /** Enables diagnostic files for the first requested number of batches. */
    public static void enable(String dir, int batches) throws IOException {
        new File(dir).mkdirs();
        enabled = true;
        maxBatches = batches;

        utilityWriter = new BufferedWriter(new FileWriter(new File(dir, "debug_utility_pool.csv")));
        utilityWriter.write("batch,utility_id,source,vector_hash,raw_weight,normalized_weight\n");

        phase3Writer = new BufferedWriter(new FileWriter(new File(dir, "debug_phase3_utilities.csv")));
        phase3Writer.write("batch,phase3_source,current_pool_size,phase3_utility_count,"
            + "hash_overlap,weight_mismatch_count,weight_max_abs_diff,axis_weight_share\n");

        celfWriter = new BufferedWriter(new FileWriter(new File(dir, "debug_celf_shadow.csv")));
        celfWriter.write("batch,round,celf_point,greedy_point,celf_gain,greedy_gain,agree\n");

        bindingWriter = new BufferedWriter(new FileWriter(new File(dir, "debug_weight_binding.csv")));
        bindingWriter.write("batch,pool_size,u_hist_size,covered_by_u_hist,"
            + "u_hist_order_mismatch,beyond_u_hist,axis_count,axis_weight_share\n");

        shadowWriter = new BufferedWriter(new FileWriter(new File(dir, "debug_shadow_weights.csv")));
        shadowWriter.write("batch,source,count,differing,max_abs_diff,"
            + "old_share,shadow_share\n");
    }

    public static boolean active(int batch) {
        return enabled && batch < maxBatches;
    }

    public static void logUtilityPool(int batch, CurrentUtilityPool pool) {
        if (!active(batch)) {
            return;
        }
        try {
            for (CurrentUtilityPool.Entry e : pool.entries()) {
                utilityWriter.write(String.format("%d,%d,%s,%s,%.6f,%.8f%n",
                    batch, e.id, e.source, e.vectorHash(), e.rawWeight, e.normalizedWeight));
            }
            utilityWriter.flush();
        } catch (IOException ignore) {

        }
    }

    /** Records direction overlap and weight differences between selection and the current pool. */
    public static void logPhase3Utilities(int batch, String source,
                                          CurrentUtilityPool pool,
                                          List<double[]> phase3Vectors,
                                          List<Double> phase3Weights) {
        if (!active(batch)) {
            return;
        }
        try {
            java.util.Set<String> poolHashes = new java.util.HashSet<>();
            for (CurrentUtilityPool.Entry e : pool.entries()) {
                poolHashes.add(e.vectorHash());
            }
            int overlap = 0;
            for (double[] v : phase3Vectors) {
                CurrentUtilityPool probe = new CurrentUtilityPool();
                probe.add(v, CurrentUtilityPool.Source.HIST);
                if (poolHashes.contains(probe.get(0).vectorHash())) {
                    overlap++;
                }
            }

            int mismatch = 0;
            double maxDiff = 0.0;
            int limit = Math.min(pool.size(), phase3Weights.size());
            for (int i = 0; i < limit; i++) {
                double diff = Math.abs(pool.get(i).rawWeight - phase3Weights.get(i));
                if (diff > 1e-12) {
                    mismatch++;
                }
                maxDiff = Math.max(maxDiff, diff);
            }

            phase3Writer.write(String.format("%d,%s,%d,%d,%d,%d,%.6f,%.6f%n",
                batch, source, pool.size(), phase3Vectors.size(), overlap,
                mismatch, maxDiff, pool.weightShare(CurrentUtilityPool.Source.AXIS)));
            phase3Writer.flush();
        } catch (IOException ignore) {

        }
    }

    public static void logWeightBinding(int batch, CurrentUtilityPool pool,
                                        List<StreamingState.WeightedUtility> uHist) {
        if (!active(batch)) {
            return;
        }
        try {
            int covered = Math.min(pool.size(), uHist.size());
            int mismatch = 0;
            for (int i = 0; i < covered; i++) {
                CurrentUtilityPool probe = new CurrentUtilityPool();
                probe.add(uHist.get(i).vector, CurrentUtilityPool.Source.HIST);
                if (!pool.get(i).vectorHash().equals(probe.get(0).vectorHash())) {
                    mismatch++;
                }
            }
            bindingWriter.write(String.format("%d,%d,%d,%d,%d,%d,%d,%.6f%n",
                batch, pool.size(), uHist.size(), covered, mismatch,
                pool.size() - covered,
                pool.countBySource(CurrentUtilityPool.Source.AXIS),
                pool.weightShare(CurrentUtilityPool.Source.AXIS)));
            bindingWriter.flush();
        } catch (IOException ignore) {

        }
    }

    public static void logShadowWeights(int batch, CurrentUtilityPool pool, double[] shadow) {
        if (!active(batch) || shadow.length != pool.size()) {
            return;
        }
        try {
            double oldSum = 0.0;
            double shadowSum = 0.0;
            for (int i = 0; i < shadow.length; i++) {
                oldSum += pool.get(i).rawWeight;
                shadowSum += shadow[i];
            }

            for (CurrentUtilityPool.Source source : CurrentUtilityPool.Source.values()) {
                int count = 0;
                int differing = 0;
                double maxDiff = 0.0;
                double oldShare = 0.0;
                double shadowShare = 0.0;
                for (int i = 0; i < shadow.length; i++) {
                    if (pool.get(i).source != source) {
                        continue;
                    }
                    count++;
                    double oldW = pool.get(i).rawWeight;
                    if (Math.abs(oldW - shadow[i]) > 1e-9) {
                        differing++;
                    }
                    maxDiff = Math.max(maxDiff, Math.abs(oldW - shadow[i]));
                    oldShare += oldSum > 0 ? oldW / oldSum : 0.0;
                    shadowShare += shadowSum > 0 ? shadow[i] / shadowSum : 0.0;
                }
                if (count == 0) {
                    continue;
                }
                shadowWriter.write(String.format("%d,%s,%d,%d,%.6f,%.6f,%.6f%n",
                    batch, source, count, differing, maxDiff, oldShare, shadowShare));
            }
            shadowWriter.flush();
        } catch (IOException ignore) {

        }
    }

    public static void logCelfShadow(int batch, int round, int celfPoint, int greedyPoint,
                                     double celfGain, double greedyGain) {
        if (!active(batch)) {
            return;
        }
        try {
            celfWriter.write(String.format("%d,%d,%d,%d,%.10f,%.10f,%s%n",
                batch, round, celfPoint, greedyPoint, celfGain, greedyGain,
                celfPoint == greedyPoint ? "true" : "false"));
            celfWriter.flush();
        } catch (IOException ignore) {

        }
    }

    public static void close() {
        for (BufferedWriter w : new BufferedWriter[]{
                utilityWriter, phase3Writer, celfWriter, bindingWriter, shadowWriter}) {
            if (w != null) {
                try {
                    w.close();
                } catch (IOException ignore) {

                }
            }
        }
        enabled = false;
    }
}
