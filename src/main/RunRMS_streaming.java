package main;

import org.apache.commons.cli.*;
import structures.*;
import utils.VectorUtil;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Entry point for streaming RMS.
 * Processes the stream in windows (one file per window); maintains a bounded representative set Q.
 */
public class RunRMS_streaming {
    
    public static void main(String[] args) {
        try {
            Options options = new Options();
            options.addOption(Option.builder("i")
                    .longOpt("input")
                    .hasArg(true)
                    .required(true)
                    .desc("Input path prefix; window t is read from prefix_sliding_window_t.txt (legacy prefix_batch_t.txt is also supported)")
                    .build());
            options.addOption(Option.builder("o")
                    .longOpt("output")
                    .hasArg(true)
                    .required(true)
                    .desc("Output directory")
                    .build());
            options.addOption(Option.builder("d")
                    .longOpt("dim")
                    .hasArg(true)
                    .required(true)
                    .desc("Data dimension")
                    .build());
            options.addOption(Option.builder("k")
                    .longOpt("k")
                    .hasArg(true)
                    .required(true)
                    .desc("Top-k parameter")
                    .build());
            options.addOption(Option.builder("r")
                    .longOpt("capacity")
                    .hasArg(true)
                    .required(true)
                    .desc("Maximum representative set size")
                    .build());
            options.addOption(Option.builder("e")
                    .longOpt("eps")
                    .hasArg(true)
                    .required(true)
                    .desc("Approximation parameter epsilon")
                    .build());
            options.addOption(Option.builder("n")
                    .longOpt("num_windows")
                    .hasArg(true)
                    .required(true)
                    .desc("Number of windows to process (files prefix_sliding_window_0 .. prefix_sliding_window_{n-1}.txt)")
                    .build());
            options.addOption(Option.builder("s")
                    .longOpt("state")
                    .hasArg(true)
                    .required(false)
                    .desc("State file path (optional, for resume)")
                    .build());
            options.addOption(Option.builder("l")
                    .longOpt("lambda")
                    .hasArg(true)
                    .required(false)
                    .desc("Time-decay factor (default 0.7)")
                    .build());
            options.addOption(Option.builder("m")
                    .longOpt("sample_size")
                    .hasArg(true)
                    .required(false)
                    .desc("Number of sampled utility vectors (default 50)")
                    .build());
            options.addOption(Option.builder("t")
                    .longOpt("tau")
                    .hasArg(true)
                    .required(false)
                    .desc("Similarity threshold tau (default 0.985)")
                    .build());
            options.addOption(Option.builder("c")
                    .longOpt("mincos")
                    .hasArg(true)
                    .required(false)
                    .desc("Diversity cosine threshold (default 0.99)")
                    .build());
            options.addOption(Option.builder("a")
                    .longOpt("alpha")
                    .hasArg(true)
                    .required(false)
                    .desc("Score mixing coefficient alpha in [0,1] (default 0.4; higher favors absolute score)")
                    .build());
            options.addOption(Option.builder("rh")
                    .longOpt("ratio_hist")
                    .hasArg(true)
                    .required(false)
                    .desc("Fraction from history in utility sampling (default 0.6)")
                    .build());
            options.addOption(Option.builder("rs")
                    .longOpt("ratio_seed")
                    .hasArg(true)
                    .required(false)
                    .desc("Fraction from Q seeds (default 0.2)")
                    .build());
            options.addOption(Option.builder("rn")
                    .longOpt("ratio_new")
                    .hasArg(true)
                    .required(false)
                    .desc("Fraction of new directions (default 0.2)")
                    .build());
            options.addOption(Option.builder("sm")
                    .longOpt("sampling_mode")
                    .hasArg(true)
                    .required(false)
                    .desc("Sampling mode: adaptive / fixed / uniform (default adaptive)")
                    .build());
            options.addOption(Option.builder("wb")
                    .longOpt("world_bonus")
                    .hasArg(true)
                    .required(false)
                    .desc("Global extreme-value bonus (default 1000)")
                    .build());
            options.addOption(Option.builder("aw")
                    .longOpt("axis_weight")
                    .hasArg(true)
                    .required(false)
                    .desc("Axis-aligned utility weight (default 100)")
                    .build());
            options.addOption(Option.builder("uf")
                    .longOpt("utility_feedback")
                    .hasArg(true)
                    .required(false)
                    .desc("Enable utility-weight feedback loop (true/false, default true)")
                    .build());
            options.addOption(Option.builder("p3s")
                    .longOpt("phase3_selection")
                    .hasArg(true)
                    .required(false)
                    .desc("Phase-3 selection: mixed_gain / absolute_topr (default mixed_gain)")
                    .build());
            
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd;
            
            try {
                cmd = parser.parse(options, args);
                
                int dim = Integer.parseInt(cmd.getOptionValue("d"));
                int k = Integer.parseInt(cmd.getOptionValue("k"));
                int r = Integer.parseInt(cmd.getOptionValue("r"));
                double eps = Double.parseDouble(cmd.getOptionValue("e"));
                int numWindows = Integer.parseInt(cmd.getOptionValue("n"));
                String input_prefix = cmd.getOptionValue("i");
                String output_dir = cmd.getOptionValue("o");
                
                double lambda = cmd.hasOption("l") ? 
                    Double.parseDouble(cmd.getOptionValue("l")) : 0.7;
                int sample_size = cmd.hasOption("m") ? 
                    Integer.parseInt(cmd.getOptionValue("m")) : 50;
                double tau = cmd.hasOption("t") ?
                    Double.parseDouble(cmd.getOptionValue("t")) : 0.985;
                double mincos = cmd.hasOption("c") ?
                    Double.parseDouble(cmd.getOptionValue("c")) : 0.99;
                double alpha = cmd.hasOption("a") ?
                    Double.parseDouble(cmd.getOptionValue("a")) : StreamingState.DEFAULT_ALPHA;
                if (Double.isNaN(alpha) || Double.isInfinite(alpha) || alpha < 0.0 || alpha > 1.0) {
                    System.err.println("Invalid alpha; falling back to default 0.4");
                    alpha = StreamingState.DEFAULT_ALPHA;
                }
                
                double ratio_hist = cmd.hasOption("rh") ?
                    Double.parseDouble(cmd.getOptionValue("rh")) : 0.6;
                double ratio_seed = cmd.hasOption("rs") ?
                    Double.parseDouble(cmd.getOptionValue("rs")) : 0.2;
                double ratio_new = cmd.hasOption("rn") ?
                    Double.parseDouble(cmd.getOptionValue("rn")) : 0.2;
                String samplingMode = cmd.hasOption("sm") ?
                    cmd.getOptionValue("sm") : StreamingState.SAMPLING_ADAPTIVE;
                samplingMode = StreamingState.normalizeSamplingMode(samplingMode);
                double worldBonus = cmd.hasOption("wb") ?
                    Double.parseDouble(cmd.getOptionValue("wb")) : StreamingState.DEFAULT_WORLD_RECORD_BONUS;
                worldBonus = StreamingState.normalizeWorldRecordBonus(worldBonus);
                double axisWeight = cmd.hasOption("aw") ?
                    Double.parseDouble(cmd.getOptionValue("aw")) : StreamingState.DEFAULT_AXIS_SPY_WEIGHT;
                axisWeight = StreamingState.normalizeAxisSpyWeight(axisWeight);
                boolean utilityFeedback = cmd.hasOption("uf") ?
                    Boolean.parseBoolean(cmd.getOptionValue("uf")) : true;
                String phase3Selection = cmd.hasOption("p3s") ?
                    cmd.getOptionValue("p3s") : StreamingState.PHASE3_SELECTION_MIXED_GAIN;
                phase3Selection = StreamingState.normalizePhase3SelectionStrategy(phase3Selection);
                
                String state_path = cmd.hasOption("s") ? 
                    cmd.getOptionValue("s") : output_dir + "/state.ser";
                
                runStreamingRMS(dim, k, r, eps, numWindows, input_prefix, 
                               output_dir, state_path, lambda, sample_size, tau, mincos,
                               ratio_hist, ratio_seed, ratio_new, alpha, samplingMode,
                               worldBonus, axisWeight, utilityFeedback, phase3Selection);
                
            } catch (ParseException pe) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("runRMS-streaming.jar", options);
                System.exit(1);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Main streaming RMS loop.
     */
    private static void runStreamingRMS(int dim, int k, int r, double eps, 
                                       int numWindows, String input_prefix, 
                                       String output_dir, String state_path,
                                       double lambda, int sample_size,
                                       double tau, double mincos,
                                       double ratio_hist, double ratio_seed, double ratio_new,
                                       double alpha, String samplingMode,
                                       double worldBonus, double axisWeight,
                                       boolean utilityFeedback,
                                       String phase3Selection) throws Exception {
        
        // Load or initialize state
        StreamingState state;
        int start_batch = 0;
        
            if (new File(state_path).exists()) {
            try {
                state = StreamingState.load(state_path);
                start_batch = state.current_batch;
                if (state.alphaRecoveredFromLegacyState) {
                    System.out.println("Legacy state or invalid alpha; using default 0.4");
                }
                if (state.samplingModeRecoveredFromLegacyState) {
                    System.out.println("Legacy state missing sampling_mode; using adaptive");
                }
                if (state.phase3SelectionRecoveredFromLegacyState) {
                    System.out.println("Legacy state missing phase3_selection; using mixed_gain");
                }
                state.setAlpha(alpha);
                state.setSamplingMode(samplingMode);
                state.setHeuristicWeights(worldBonus, axisWeight);
                state.setUtilityFeedbackEnabled(utilityFeedback);
                state.setPhase3SelectionStrategy(phase3Selection);
                System.out.println("Resumed: " + state.getSummary());
                System.out.println("Using alpha=" + state.alpha + ", sampling_mode=" + state.samplingMode +
                    ", world_bonus=" + state.worldRecordBonus + ", axis_weight=" + state.axisSpyWeight +
                    ", utility_feedback=" + state.utilityFeedbackEnabled +
                    ", phase3_selection=" + state.phase3SelectionStrategy);
            } catch (Exception e) {
                System.err.println("Failed to read state; starting fresh: " + e.getMessage());
                try { new File(state_path).delete(); } catch (Exception ignore) {}
                    state = new StreamingState(r);
                    state.setHyperparameters(lambda, tau, 0.8, 0.1, mincos, sample_size);
                    state.setSamplingRatios(ratio_hist, ratio_seed, ratio_new);
                    state.setAlpha(alpha);
                    state.setSamplingMode(samplingMode);
                    state.setHeuristicWeights(worldBonus, axisWeight);
                    state.setUtilityFeedbackEnabled(utilityFeedback);
                    state.setPhase3SelectionStrategy(phase3Selection);
                System.out.println("Initialized: r=" + r + ", lambda=" + lambda + 
                                 ", sample_size=" + sample_size + 
                                 ", ratios=[" + ratio_hist + "," + ratio_seed + "," + ratio_new + "]" +
                                 ", alpha=" + state.alpha +
                                 ", sampling_mode=" + state.samplingMode +
                                 ", world_bonus=" + state.worldRecordBonus +
                                 ", axis_weight=" + state.axisSpyWeight +
                                 ", utility_feedback=" + state.utilityFeedbackEnabled +
                                 ", phase3_selection=" + state.phase3SelectionStrategy);
            }
        } else {
            state = new StreamingState(r);
                state.setHyperparameters(lambda, tau, 0.8, 0.1, mincos, sample_size);
                state.setSamplingRatios(ratio_hist, ratio_seed, ratio_new);
                state.setAlpha(alpha);
                state.setSamplingMode(samplingMode);
                state.setHeuristicWeights(worldBonus, axisWeight);
                state.setUtilityFeedbackEnabled(utilityFeedback);
                state.setPhase3SelectionStrategy(phase3Selection);
            System.out.println("Initialized: r=" + r + ", lambda=" + lambda + 
                             ", sample_size=" + sample_size + 
                             ", ratios=[" + ratio_hist + "," + ratio_seed + "," + ratio_new + "]" +
                             ", alpha=" + state.alpha +
                             ", sampling_mode=" + state.samplingMode +
                             ", world_bonus=" + state.worldRecordBonus +
                             ", axis_weight=" + state.axisSpyWeight +
                             ", utility_feedback=" + state.utilityFeedbackEnabled +
                             ", phase3_selection=" + state.phase3SelectionStrategy);
        }
        
        new File(output_dir).mkdirs();
        
        BufferedWriter wr_result = new BufferedWriter(
            new FileWriter(output_dir + "/representatives.txt", true));
        BufferedWriter wr_time = new BufferedWriter(
            new FileWriter(output_dir + "/timing.txt", true));
        BufferedWriter wr_summary = new BufferedWriter(
            new FileWriter(output_dir + "/summary.csv", true));
        BufferedWriter wr_final = new BufferedWriter(
            new FileWriter(output_dir + "/final_representatives.txt", start_batch == 0 ? false : true));
        
        if (start_batch == 0) {
            wr_summary.write("window,sampling_mode,Q_size,U_hist_size,total_points,sampling_ms," +
                           "index_ms,scoring_ms,selection_ms,total_ms,avg_score," +
                           "sampling_mem_mb,index_mem_mb,scoring_mem_mb,selection_mem_mb," +
                           "coverage,diversity,avg_utility_weight,incremental_used\n");
        }
        
        DecimalFormat df = new DecimalFormat("0.000000");
        
        for (int t = start_batch; t < numWindows; t++) {
            long batch_start = System.nanoTime();
            
            System.out.println("\n=== Window " + t + " ===");
            
            String windowFile = resolveWindowFile(input_prefix, t);
            if (windowFile == null) {
                System.err.println("Window data file missing: " +
                    input_prefix + "_sliding_window_" + t + ".txt" +
                    " (or legacy " + input_prefix + "_batch_" + t + ".txt)");
                break;
            }
            
            double[][] batch_data = readWindowFile(windowFile, dim);
            System.out.println("Loaded tuples in window: " + batch_data.length);
            
            StreamingRMS rms = new StreamingRMS(dim, k, eps, state.alpha, state, batch_data);
            
            rms.updateRepresentatives();
            
            double[][] currentSamples = rms.getCurrentSamples();
            QualityMetrics.QualityReport quality = QualityMetrics.generateReport(
                state, currentSamples, 0.7);
            
            long batch_end = System.nanoTime();
            double total_time = (batch_end - batch_start) / 1e6;
            state.total_processing_time += (long)total_time;
            
            wr_result.write("=== Window " + t + " (|W_t|=" + batch_data.length + 
                          ", |Q|=" + state.Q.size() + ") ===\n");
            wr_result.write("Quality: " + quality.toString() + "\n");
            
            double avg_score = 0.0;
            for (StreamingState.Representative rep : state.Q) {
                wr_result.write(String.format("idx=%d t_join=%d score=%.6f vec=[", 
                    rep.original_idx, rep.t_join, rep.score));
                for (int d = 0; d < dim; d++) {
                    wr_result.write(df.format(rep.vector[d]) + " ");
                }
                wr_result.write("]\n");
                avg_score += rep.score;
            }
            avg_score /= state.Q.size();
            wr_result.flush();
            
            // final_representatives.txt: index <window> <Q size> <avg score>
            wr_final.write(String.format("index %d %d %.6f\n", 
                t, state.Q.size(), avg_score));
            for (StreamingState.Representative rep : state.Q) {
                for (int d = 0; d < dim; d++) {
                    wr_final.write(df.format(rep.vector[d]) + " ");
                }
                wr_final.write("\n");
            }
            wr_final.write("\n");
            wr_final.flush();
            
            wr_time.write(String.format(
                "window=%d sampling=%.2f index=%.2f scoring=%.2f selection=%.2f total=%.2f\n",
                t, rms.samplingTime, rms.indexTime, rms.scoringTime, 
                rms.selectionTime, total_time
            ));
            wr_time.flush();
            
            boolean incremental_used = rms.isIncrementalUsed();
            wr_summary.write(String.format("%d,%s,%d,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.6f,%.2f,%.2f,%.2f,%.2f,%.6f,%.6f,%.6f,%s\n",
                t, state.samplingMode, state.Q.size(), state.U_hist.size(), state.total_points_seen,
                rms.samplingTime, rms.indexTime, rms.scoringTime, 
                rms.selectionTime, total_time, avg_score,
                rms.samplingMemMB, rms.indexMemMB, rms.scoringMemMB, rms.selectionMemMB,
                quality.coverage, quality.diversity, quality.avg_utility_weight,
                incremental_used ? "true" : "false"
            ));
            wr_summary.flush();
            
            if (t % 10 == 9 || t == numWindows - 1) {
                state.save(state_path);
                System.out.println("State saved: " + state_path);
            }
            
            if (t % 20 == 19) {
                state.pruneStaleUtilities(50);
            }
            
            System.out.println(state.getSummary());
            System.out.println(String.format(
                "Timing: sampling=%.2fms index=%.2fms%s scoring=%.2fms selection=%.2fms total=%.2fms",
                rms.samplingTime, rms.indexTime, incremental_used ? " (incr.)" : " (full)",
                rms.scoringTime, rms.selectionTime, total_time
            ));
            System.out.println("Quality: " + quality.toString());
        }
        
        wr_result.close();
        wr_time.close();
        wr_summary.close();
        wr_final.close();
        
        System.out.println("\n=== Streaming run finished ===");
        System.out.println("Final: " + state.getSummary());
        System.out.println("Outputs written to: " + output_dir);
        System.out.println("Representatives: " + output_dir + "/final_representatives.txt");
    }
    
    /**
     * Read one window file: no header, one d-dimensional tuple per line; embed on sphere.
     */
    private static double[][] readWindowFile(String path, int dim) throws IOException {
        List<double[]> data = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader(path));
        
        String line;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) continue;
            
            String[] tokens = line.split("\\s+");
            if (tokens.length < dim) {
                System.err.println("Warning: short line skipped: " + line);
                continue;
            }
            
            double[] point = new double[dim + 1];
            for (int d = 0; d < dim; d++) {
                point[d] = Double.parseDouble(tokens[d]);
            }
            point[dim] = Math.sqrt(dim - VectorUtil.norm2(point));
            data.add(point);
        }
        br.close();
        
        return data.toArray(new double[0][]);
    }

    private static String resolveWindowFile(String inputPrefix, int windowIdx) {
        String slidingWindowFile = inputPrefix + "_sliding_window_" + windowIdx + ".txt";
        if (new File(slidingWindowFile).exists()) {
            return slidingWindowFile;
        }
        String legacyBatchFile = inputPrefix + "_batch_" + windowIdx + ".txt";
        if (new File(legacyBatchFile).exists()) {
            return legacyBatchFile;
        }
        return null;
    }
}
