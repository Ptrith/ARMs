package main;

import org.apache.commons.cli.*;
import structures.*;
import utils.VectorUtil;

import java.io.*;
import java.nio.file.*;
import java.text.DecimalFormat;
import java.util.*;

/** Command-line entry point for batchwise processing and checkpointed representative selection. */
public class RunRMS_streaming {

    /** Buffer flush interval used by the reduced-output runtime mode. */
    public static final int DEFAULT_FAST_FLUSH_EVERY = 1000;

    public static void main(String[] args) {
        try {
            Options options = new Options();
            options.addOption(Option.builder("i")
                    .longOpt("input")
                    .hasArg(true)
                    .required(true)
                    .desc("分批输入文件前缀（自动补 _batch_N.txt）；使用 -sf 时填同一输入路径")
                    .build());
            options.addOption(Option.builder("o")
                    .longOpt("output")
                    .hasArg(true)
                    .required(true)
                    .desc("输出目录")
                    .build());
            options.addOption(Option.builder("d")
                    .longOpt("dim")
                    .hasArg(true)
                    .required(true)
                    .desc("数据维度")
                    .build());
            options.addOption(Option.builder("k")
                    .longOpt("k")
                    .hasArg(true)
                    .required(true)
                    .desc("top-k参数")
                    .build());
            options.addOption(Option.builder("r")
                    .longOpt("capacity")
                    .hasArg(true)
                    .required(true)
                    .desc("代表集大小上限")
                    .build());
            options.addOption(Option.builder("e")
                    .longOpt("eps")
                    .hasArg(true)
                    .required(true)
                    .desc("近似参数ε")
                    .build());
            options.addOption(Option.builder("n")
                    .longOpt("num_batches")
                    .hasArg(true)
                    .required(true)
                    .desc("批次总数")
                    .build());
            options.addOption(Option.builder("s")
                    .longOpt("state")
                    .hasArg(true)
                    .required(false)
                    .desc("状态文件路径（可选，用于恢复）")
                    .build());
            options.addOption(Option.builder("l")
                    .longOpt("lambda")
                    .hasArg(true)
                    .required(false)
                    .desc("历史保留因子强度（默认0.7）")
                    .build());
            options.addOption(Option.builder("m")
                    .longOpt("sample_size")
                    .hasArg(true)
                    .required(false)
                    .desc("采样效用向量数（默认50）")
                    .build());
            options.addOption(Option.builder("t")
                    .longOpt("tau")
                    .hasArg(true)
                    .required(false)
                    .desc("相似阈值τ，默认0.985")
                    .build());
            options.addOption(Option.builder("c")
                    .longOpt("mincos")
                    .hasArg(true)
                    .required(false)
                    .desc("多样性余弦阈值，默认0.99")
                    .build());
            options.addOption(Option.builder("a")
                    .longOpt("alpha")
                    .hasArg(true)
                    .required(false)
                    .desc("混合目标中的质量项系数alpha（0~1，默认0.4）")
                    .build());
            options.addOption(Option.builder("rh")
                    .longOpt("ratio_hist")
                    .hasArg(true)
                    .required(false)
                    .desc("历史继承比例（默认0.6）")
                    .build());
            options.addOption(Option.builder("rs")
                    .longOpt("ratio_seed")
                    .hasArg(true)
                    .required(false)
                    .desc("种子扩展比例（默认0.2）")
                    .build());
            options.addOption(Option.builder("rn")
                    .longOpt("ratio_new")
                    .hasArg(true)
                    .required(false)
                    .desc("新方向比例（默认0.2）")
                    .build());
            options.addOption(Option.builder("sm")
                    .longOpt("sampling_mode")
                    .hasArg(true)
                    .required(false)
                    .desc("采样模式：adaptive / fixed / uniform（默认adaptive）")
                    .build());
            options.addOption(Option.builder("sd")
                    .longOpt("seed")
                    .hasArg(true)
                    .required(false)
                    .desc("采样随机种子（默认42）")
                    .build());
            options.addOption(Option.builder("ase")
                    .longOpt("axis_samples_enabled")
                    .hasArg(true)
                    .required(false)
                    .desc("是否添加轴向探针样本（true/false，默认true）")
                    .build());
            options.addOption(Option.builder("dbg")
                    .longOpt("debug_dir")
                    .hasArg(true)
                    .required(false)
                    .desc("诊断输出目录：效用池、Phase3效用来源、CELF影子比较（默认关闭）")
                    .build());
            options.addOption(Option.builder("dbgn")
                    .longOpt("debug_batches")
                    .hasArg(true)
                    .required(false)
                    .desc("诊断输出覆盖的批次数（默认5；仅在指定 -dbg 时生效）")
                    .build());
            options.addOption(Option.builder("wb")
                    .longOpt("world_bonus")
                    .hasArg(true)
                    .required(false)
                    .desc("全局极值奖励（默认0，即关闭）")
                    .build());
            options.addOption(Option.builder("aw")
                    .longOpt("axis_weight")
                    .hasArg(true)
                    .required(false)
                    .desc("轴向方向在本轮目标函数中的权重（默认100）")
                    .build());
            options.addOption(Option.builder("uf")
                    .longOpt("utility_feedback")
                    .hasArg(true)
                    .required(false)
                    .desc("是否启用效用权重反馈闭环（true/false，默认true）")
                    .build());
            options.addOption(Option.builder("ow")
                    .longOpt("objective_weighting")
                    .hasArg(true)
                    .required(false)
                    .desc("本轮 H_t 的方向自适应权重开关（true/false，默认true；false 时全等权但保留历史 retention）")
                    .build());
            options.addOption(Option.builder("p3s")
                    .longOpt("phase3_selection")
                    .hasArg(true)
                    .required(false)
                    .desc("Phase3 选点策略：mixed_gain / absolute_topr（默认mixed_gain）")
                    .build());
            options.addOption(Option.builder("idx")
                    .longOpt("index_mode")
                    .hasArg(true)
                    .required(false)
                    .desc("索引模式：tree / none（默认tree；none跳过KdTree/ConeTree构建）")
                    .build());
            options.addOption(Option.builder("q")
                    .longOpt("quiet")
                    .hasArg(true)
                    .required(false)
                    .desc("是否减少控制台日志输出（true/false，默认与 fast_experiment 相同）")
                    .build());
            options.addOption(Option.builder("sfo")
                    .longOpt("save_final_only")
                    .hasArg(true)
                    .required(false)
                    .desc("是否只保存最终代表集（快速模式固定true；-fe false 时默认false）")
                    .build());
            options.addOption(Option.builder("se")
                    .longOpt("save_every")
                    .hasArg(true)
                    .required(false)
                    .desc("每隔多少批保存一次代表集明细（默认1；save_final_only=true时忽略）")
                    .build());
            options.addOption(Option.builder("rk")
                    .longOpt("rms_k")
                    .hasArg(true)
                    .required(false)
                    .desc("RMS 目标中的 k（当前 mixed_gain 目标仅支持 k=1）")
                    .build());
            options.addOption(Option.builder("kst")
                    .longOpt("k_truncation")
                    .hasArg(true)
                    .required(false)
                    .desc("K-RMS 截断方式：hard / soft（默认hard）")
                    .build());
            options.addOption(Option.builder("ksb")
                    .longOpt("k_soft_beta")
                    .hasArg(true)
                    .required(false)
                    .desc("soft truncation 对数衰减强度 beta（默认1.0）")
                    .build());
            options.addOption(Option.builder("ksg")
                    .longOpt("k_soft_gamma")
                    .hasArg(true)
                    .required(false)
                    .desc("soft truncation 超额放大系数 gamma（默认1.0）")
                    .build());
            options.addOption(Option.builder("kse")
                    .longOpt("k_soft_eta")
                    .hasArg(true)
                    .required(false)
                    .desc("soft truncation 额外收益权重 eta（默认1.0）")
                    .build());
            options.addOption(Option.builder("fe")
                    .longOpt("fast_experiment")
                    .hasArg(true)
                    .required(false)
                    .desc("快速模式：减少 flush，只保存最终状态/明细，质量指标只在末批计算（默认true；false恢复逐批输出和定期检查点）")
                    .build());
            options.addOption(Option.builder("ffe")
                    .longOpt("flush_every")
                    .hasArg(true)
                    .required(false)
                    .desc("fast_experiment 下 summary.csv/timing.txt 的 flush 间隔批次数（默认1000）")
                    .build());
            options.addOption(Option.builder("sf")
                    .longOpt("stream_file")
                    .hasArg(true)
                    .required(false)
                    .desc("打包后的连续数据流文件；给定时不再逐批打开 <prefix>_batch_N.txt，需配合 -ws")
                    .build());
            options.addOption(Option.builder("ws")
                    .longOpt("window_size")
                    .hasArg(true)
                    .required(false)
                    .desc("packed stream 模式下每批的有效数据条数 w（最后一批不足 w 时保留）")
                    .build());

            CommandLineParser parser = new DefaultParser();
            CommandLine cmd;

            try {

                for (String arg : args) {
                    if (arg.equals("-arp") || arg.startsWith("-arp=") ||
                            arg.equals("--axis_retention_priority") ||
                            arg.startsWith("--axis_retention_priority=")) {
                        throw new ParseException("axis_retention_priority 已固定为 100，请删除 -arp/--axis_retention_priority 参数");
                    }
                }
                cmd = parser.parse(options, args);

                int dim = Integer.parseInt(cmd.getOptionValue("d"));
                int k = Integer.parseInt(cmd.getOptionValue("k"));
                int r = Integer.parseInt(cmd.getOptionValue("r"));
                double eps = Double.parseDouble(cmd.getOptionValue("e"));
                int num_batches = Integer.parseInt(cmd.getOptionValue("n"));
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
                    System.err.println("alpha 参数不合法，自动回退到默认值 0.4");
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
                int samplingSeed = cmd.hasOption("sd") ?
                    Integer.parseInt(cmd.getOptionValue("sd")) : StreamingState.DEFAULT_SAMPLING_SEED;
                boolean axisSamplesEnabled = cmd.hasOption("ase") ?
                    Boolean.parseBoolean(cmd.getOptionValue("ase")) : true;
                String debugDir = cmd.hasOption("dbg") ? cmd.getOptionValue("dbg") : null;
                int debugBatches = cmd.hasOption("dbgn") ?
                    Integer.parseInt(cmd.getOptionValue("dbgn")) : 5;
                double worldBonus = cmd.hasOption("wb") ?
                    Double.parseDouble(cmd.getOptionValue("wb")) : StreamingState.DEFAULT_WORLD_RECORD_BONUS;
                worldBonus = StreamingState.normalizeWorldRecordBonus(worldBonus);
                double axisWeight = cmd.hasOption("aw") ?
                    Double.parseDouble(cmd.getOptionValue("aw")) : StreamingState.DEFAULT_AXIS_OBJECTIVE_WEIGHT;
                axisWeight = StreamingState.normalizeAxisWeight(axisWeight);
                boolean utilityFeedback = cmd.hasOption("uf") ?
                    Boolean.parseBoolean(cmd.getOptionValue("uf")) : true;
                boolean objectiveWeighting = cmd.hasOption("ow") ?
                    Boolean.parseBoolean(cmd.getOptionValue("ow")) : true;
                String phase3Selection = cmd.hasOption("p3s") ?
                    cmd.getOptionValue("p3s") : StreamingState.PHASE3_SELECTION_MIXED_GAIN;
                phase3Selection = StreamingState.normalizePhase3SelectionStrategy(phase3Selection);
                String indexMode = cmd.hasOption("idx") ?
                    cmd.getOptionValue("idx") : StreamingState.INDEX_MODE_TREE;
                indexMode = StreamingState.normalizeIndexMode(indexMode);
                boolean fastExperiment = cmd.hasOption("fe") ?
                    Boolean.parseBoolean(cmd.getOptionValue("fe")) : true;
                boolean quiet = cmd.hasOption("q") ?
                    Boolean.parseBoolean(cmd.getOptionValue("q")) : fastExperiment;
                boolean saveFinalOnly = cmd.hasOption("sfo") ?
                    Boolean.parseBoolean(cmd.getOptionValue("sfo")) : false;
                int saveEvery = cmd.hasOption("se") ?
                    Math.max(1, Integer.parseInt(cmd.getOptionValue("se"))) : 1;
                int rmsK = cmd.hasOption("rk") ?
                    Integer.parseInt(cmd.getOptionValue("rk")) : StreamingState.DEFAULT_RMS_K;
                rmsK = StreamingState.normalizeRmsK(rmsK);
                String kTruncation = cmd.hasOption("kst") ?
                    cmd.getOptionValue("kst") : StreamingState.K_TRUNCATION_HARD;
                kTruncation = StreamingState.normalizeKTruncationStrategy(kTruncation);
                double kSoftBeta = cmd.hasOption("ksb") ?
                    Double.parseDouble(cmd.getOptionValue("ksb")) : StreamingState.DEFAULT_K_SOFT_BETA;
                kSoftBeta = StreamingState.normalizeKSoftBeta(kSoftBeta);
                double kSoftGamma = cmd.hasOption("ksg") ?
                    Double.parseDouble(cmd.getOptionValue("ksg")) : StreamingState.DEFAULT_K_SOFT_GAMMA;
                kSoftGamma = StreamingState.normalizeKSoftGamma(kSoftGamma);
                double kSoftEta = cmd.hasOption("kse") ?
                    Double.parseDouble(cmd.getOptionValue("kse")) : StreamingState.DEFAULT_K_SOFT_ETA;
                kSoftEta = StreamingState.normalizeKSoftEta(kSoftEta);
                int flushEvery = cmd.hasOption("ffe") ?
                    Math.max(1, Integer.parseInt(cmd.getOptionValue("ffe"))) : DEFAULT_FAST_FLUSH_EVERY;
                String streamFile = cmd.hasOption("sf") ? cmd.getOptionValue("sf") : null;
                int packedWindow = cmd.hasOption("ws") ?
                    Integer.parseInt(cmd.getOptionValue("ws")) : -1;
                if (streamFile != null && packedWindow <= 0) {
                    throw new ParseException("--stream_file 需要同时给出正整数的 --window_size");
                }

                if (fastExperiment) {
                    saveFinalOnly = true;
                }

                String state_path = cmd.hasOption("s") ?
                    cmd.getOptionValue("s") : output_dir + "/state.ser";

                runStreamingRMS(dim, k, r, eps, num_batches, input_prefix,
                               output_dir, state_path, lambda, sample_size, tau, mincos,
                               ratio_hist, ratio_seed, ratio_new, alpha, samplingMode,
                               axisSamplesEnabled,
                               worldBonus, axisWeight, utilityFeedback, objectiveWeighting, phase3Selection,
                               indexMode, quiet, saveFinalOnly, saveEvery,
                               rmsK, kTruncation, kSoftBeta, kSoftGamma, kSoftEta,
                               samplingSeed, debugDir, debugBatches,
                               fastExperiment, flushEvery, streamFile, packedWindow);

            } catch (ParseException pe) {
                System.err.println("命令行解析失败: " + pe.getMessage());
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("runRMS-streaming.jar", options);
                System.exit(1);
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void runStreamingRMS(int dim, int k, int r, double eps,
                                       int num_batches, String input_prefix,
                                       String output_dir, String state_path,
                                       double lambda, int sample_size,
                                       double tau, double mincos,
                                       double ratio_hist, double ratio_seed, double ratio_new,
                                       double alpha, String samplingMode,
                                       boolean axisSamplesEnabled,
                                       double worldBonus, double axisWeight,
                                       boolean utilityFeedback,
                                       boolean objectiveWeighting,
                                       String phase3Selection,
                                       String indexMode,
                                       boolean quiet, boolean saveFinalOnly, int saveEvery,
                                       int rmsK, String kTruncation, double kSoftBeta,
                                       double kSoftGamma, double kSoftEta,
                                       int samplingSeed, String debugDir,
                                       int debugBatches,
                                       boolean fastExperiment, int flushEvery,
                                       String streamFile, int packedWindow) throws Exception {

        if (debugDir != null) {
            StageDebug.enable(debugDir, debugBatches);
        }

        StreamingState state;
        int start_batch = 0;

            if (new File(state_path).exists()) {
            try {
                state = StreamingState.load(state_path);
                start_batch = state.current_batch;
                if (state.alphaRecoveredFromLegacyState) {
                    System.out.println("检测到旧状态或无效 alpha，已回退到默认值 0.4");
                }
                if (state.samplingModeRecoveredFromLegacyState) {
                    System.out.println("检测到旧状态缺失 sampling_mode，已回退到 adaptive");
                }
                if (state.phase3SelectionRecoveredFromLegacyState) {
                    System.out.println("检测到旧状态缺失 phase3_selection，已回退到 mixed_gain");
                }
                state.setAlpha(alpha);
                state.setSamplingMode(samplingMode);
                state.setAxisSamplesEnabled(axisSamplesEnabled);
                state.setHeuristicWeights(worldBonus, axisWeight);
                state.setUtilityFeedbackEnabled(utilityFeedback);
                state.setObjectiveWeightingEnabled(objectiveWeighting);
                state.setPhase3SelectionStrategy(phase3Selection);
                state.setIndexMode(indexMode);
                state.setRmsK(rmsK);
                state.setKTruncationStrategy(kTruncation);
                state.setKSoftBeta(kSoftBeta);
                state.setKSoftGamma(kSoftGamma);
                state.setKSoftEta(kSoftEta);
                System.out.println("恢复状态：" + state.getSummary());
                System.out.println("恢复后使用 alpha=" + state.alpha + ", sampling_mode=" + state.samplingMode +
                    ", world_bonus=" + state.worldRecordBonus + ", axis_weight=" + state.axisObjectiveWeight +
                    ", axis_retention_fixed=" + StreamingState.AXIS_RETENTION_PRIORITY +
                    ", utility_feedback=" + state.utilityFeedbackEnabled + ", objective_weighting=" + state.objectiveWeightingEnabled +
                    ", phase3_selection=" + state.phase3SelectionStrategy +
                    ", index_mode=" + state.indexMode +
                    ", rms_k=" + state.rmsK +
                    ", k_truncation=" + state.kTruncationStrategy +
                    ", k_soft_beta=" + state.kSoftBeta +
                    ", k_soft_gamma=" + state.kSoftGamma +
                    ", k_soft_eta=" + state.kSoftEta);
            } catch (Exception e) {
                System.err.println("状态文件读取失败，将从新状态启动: " + e.getMessage());

                try { new File(state_path).delete(); } catch (Exception ignore) {}
                    state = new StreamingState(r);
                    state.setHyperparameters(lambda, tau, 0.8, 0.1, mincos, sample_size);
                    state.setSamplingRatios(ratio_hist, ratio_seed, ratio_new);
                    state.setAlpha(alpha);
                    state.setSamplingMode(samplingMode);
                    state.setAxisSamplesEnabled(axisSamplesEnabled);
                    state.setHeuristicWeights(worldBonus, axisWeight);
                    state.setUtilityFeedbackEnabled(utilityFeedback);
                    state.setObjectiveWeightingEnabled(objectiveWeighting);
                    state.setPhase3SelectionStrategy(phase3Selection);
                    state.setIndexMode(indexMode);
                    state.setRmsK(rmsK);
                    state.setKTruncationStrategy(kTruncation);
                    state.setKSoftBeta(kSoftBeta);
                    state.setKSoftGamma(kSoftGamma);
                    state.setKSoftEta(kSoftEta);
                System.out.println("初始化新状态：r=" + r + ", λ=" + lambda +
                                 ", sample_size=" + sample_size +
                                 ", ratios=[" + ratio_hist + "," + ratio_seed + "," + ratio_new + "]" +
                                 ", alpha=" + state.alpha +
                                 ", sampling_mode=" + state.samplingMode +
                                 ", world_bonus=" + state.worldRecordBonus +
                                 ", axis_weight=" + state.axisObjectiveWeight +
                                 ", axis_retention_fixed=" + StreamingState.AXIS_RETENTION_PRIORITY +
                                 ", utility_feedback=" + state.utilityFeedbackEnabled + ", objective_weighting=" + state.objectiveWeightingEnabled +
                                 ", phase3_selection=" + state.phase3SelectionStrategy +
                                 ", index_mode=" + state.indexMode +
                                 ", rms_k=" + state.rmsK +
                                 ", k_truncation=" + state.kTruncationStrategy +
                                 ", k_soft_beta=" + state.kSoftBeta +
                                 ", k_soft_gamma=" + state.kSoftGamma +
                                 ", k_soft_eta=" + state.kSoftEta);
            }
        } else {
            state = new StreamingState(r);
                state.setHyperparameters(lambda, tau, 0.8, 0.1, mincos, sample_size);
                state.setSamplingRatios(ratio_hist, ratio_seed, ratio_new);
                state.setAlpha(alpha);
                state.setSamplingMode(samplingMode);
                state.setAxisSamplesEnabled(axisSamplesEnabled);
                state.setHeuristicWeights(worldBonus, axisWeight);
                state.setUtilityFeedbackEnabled(utilityFeedback);
                state.setObjectiveWeightingEnabled(objectiveWeighting);
                state.setPhase3SelectionStrategy(phase3Selection);
                state.setIndexMode(indexMode);
                state.setRmsK(rmsK);
                state.setKTruncationStrategy(kTruncation);
                state.setKSoftBeta(kSoftBeta);
                state.setKSoftGamma(kSoftGamma);
                state.setKSoftEta(kSoftEta);
            System.out.println("初始化新状态：r=" + r + ", λ=" + lambda +
                             ", sample_size=" + sample_size +
                             ", ratios=[" + ratio_hist + "," + ratio_seed + "," + ratio_new + "]" +
                             ", alpha=" + state.alpha +
                             ", sampling_mode=" + state.samplingMode +
                             ", world_bonus=" + state.worldRecordBonus +
                             ", axis_weight=" + state.axisObjectiveWeight +
                             ", axis_retention_fixed=" + StreamingState.AXIS_RETENTION_PRIORITY +
                             ", utility_feedback=" + state.utilityFeedbackEnabled + ", objective_weighting=" + state.objectiveWeightingEnabled +
                             ", phase3_selection=" + state.phase3SelectionStrategy +
                             ", index_mode=" + state.indexMode +
                             ", rms_k=" + state.rmsK +
                             ", k_truncation=" + state.kTruncationStrategy +
                             ", k_soft_beta=" + state.kSoftBeta +
                             ", k_soft_gamma=" + state.kSoftGamma +
                             ", k_soft_eta=" + state.kSoftEta);
        }

        state.setSamplingSeed(samplingSeed);
        System.out.println("采样种子 seed=" + state.samplingSeed
            + (debugDir != null ? "，诊断输出=" + debugDir + "（前 " + debugBatches + " 批）" : ""));

        new File(output_dir).mkdirs();

        BufferedWriter wr_result = fastExperiment ? null : new BufferedWriter(
            new FileWriter(output_dir + "/representatives.txt", true));
        BufferedWriter wr_time = new BufferedWriter(
            new FileWriter(output_dir + "/timing.txt", true));
        BufferedWriter wr_summary = new BufferedWriter(
            new FileWriter(output_dir + "/summary.csv", true));
        BufferedWriter wr_final = new BufferedWriter(
            new FileWriter(output_dir + "/final_representatives.txt", start_batch == 0 ? false : true));

        if (start_batch == 0) {
            wr_summary.write("batch,sampling_mode,Q_size,U_hist_size,total_points,sampling_ms," +
                           "index_ms,scoring_ms,selection_ms,total_ms,avg_score," +
                           "sampling_mem_mb,index_mem_mb,scoring_mem_mb,selection_mem_mb," +
                           "coverage,diversity,avg_utility_weight,incremental_used\n");
        }

        DecimalFormat df = new DecimalFormat("0.000000");

        int lastProcessedBatch = -1;
        double lastAvgScore = 0.0;
        int finalBlockWrittenFor = -1;
        int stateSavedFor = -1;

        try (StreamSource source = streamFile != null
                ? new PackedStreamSource(streamFile, packedWindow, dim)
                : new BatchFileSource(input_prefix, dim)) {

        if (!quiet) {
            System.out.println("输入模式: " + source.describe()
                + (fastExperiment ? "，runtime_mode=fast_experiment_v1（flush 间隔 "
                                    + flushEvery + " 批）"
                                  : "，runtime_mode=legacy"));
        }

        for (int t = start_batch; t < num_batches; t++) {
            long batch_start = System.nanoTime();

            if (!quiet) {
                System.out.println("\n=== 处理批次 " + t + " ===");
            }

            double[][] batch_data = source.nextBatch(t);
            if (batch_data == null) {
                System.err.println("批次数据不存在: " + source.missingMessage(t));
                break;
            }
            if (!quiet) {
                System.out.println("读取批次数据: " + batch_data.length + " 条");
            }

            StreamingRMS rms = new StreamingRMS(dim, k, eps, state.alpha, state, batch_data);

            rms.updateRepresentatives();

            boolean lastBatch = (t == num_batches - 1);

            QualityMetrics.QualityReport quality = null;
            if (!fastExperiment || lastBatch) {
                double[][] currentSamples = rms.getCurrentSamples();
                quality = QualityMetrics.generateReport(state, currentSamples, 0.7);
            }

            long batch_end = System.nanoTime();
            double total_time = (batch_end - batch_start) / 1e6;
            state.total_processing_time += (long)total_time;

            double avg_score = 0.0;
            for (StreamingState.Representative rep : state.Q) {
                avg_score += rep.score;
            }
            avg_score /= state.Q.size();
            lastProcessedBatch = t;
            lastAvgScore = avg_score;

            boolean shouldSaveDetail = !saveFinalOnly && (t % saveEvery == 0);
            if (shouldSaveDetail) {
                wr_result.write("=== Batch " + t + " (|B_t|=" + batch_data.length +
                              ", |Q|=" + state.Q.size() + ") ===\n");
                wr_result.write("Quality: " + quality.toString() + "\n");
                for (StreamingState.Representative rep : state.Q) {
                    wr_result.write(String.format("idx=%d t_join=%d score=%.6f vec=[",
                        rep.original_idx, rep.t_join, rep.score));
                    for (int d = 0; d < dim; d++) {
                        wr_result.write(df.format(rep.vector[d]) + " ");
                    }
                    wr_result.write("]\n");
                }
                wr_result.flush();
            }

            if (shouldSaveDetail) {
                wr_final.write(buildFinalBlock(t, state.Q, avg_score, dim, df));
                wr_final.flush();
                finalBlockWrittenFor = t;
            }

            wr_time.write(String.format(
                "batch=%d sampling=%.2f index=%.2f scoring=%.2f selection=%.2f total=%.2f\n",
                t, rms.samplingTime, rms.indexTime, rms.scoringTime,
                rms.selectionTime, total_time
            ));

            boolean incremental_used = rms.isIncrementalUsed();
            wr_summary.write(String.format("%d,%s,%d,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.6f,%.2f,%.2f,%.2f,%.2f,%s,%s\n",
                t, state.samplingMode, state.Q.size(), state.U_hist.size(), state.total_points_seen,
                rms.samplingTime, rms.indexTime, rms.scoringTime,
                rms.selectionTime, total_time, avg_score,
                rms.samplingMemMB, rms.indexMemMB, rms.scoringMemMB, rms.selectionMemMB,
                quality == null ? ",," : String.format("%.6f,%.6f,%.6f",
                    quality.coverage, quality.diversity, quality.avg_utility_weight),
                incremental_used ? "true" : "false"
            ));

            if (!fastExperiment || lastBatch || (t + 1) % flushEvery == 0) {
                wr_time.flush();
                wr_summary.flush();
            }

            if (fastExperiment ? lastBatch : (t % 10 == 9 || lastBatch)) {
                state.save(state_path);
                stateSavedFor = t;
                if (!quiet) {
                    System.out.println("状态已保存: " + state_path);
                }
            }

            if (t % 20 == 19) {
                state.pruneStaleUtilities(50);
            }

            if (!quiet) {
                System.out.println(state.getSummary());
                System.out.println(String.format(
                    "计时: 采样=%.2fms 索引=%.2fms%s 评分=%.2fms 选择=%.2fms 总计=%.2fms",
                    rms.samplingTime, rms.indexTime, incremental_used ? "(增量)" : "(全量)",
                    rms.scoringTime, rms.selectionTime, total_time
                ));
                System.out.println("质量: "
                    + (quality == null ? "（fast_experiment：仅最后一批计算）" : quality.toString()));
            }
        }

        }

        if (lastProcessedBatch >= 0 && finalBlockWrittenFor != lastProcessedBatch) {
            wr_final.write(buildFinalBlock(lastProcessedBatch, state.Q, lastAvgScore, dim, df));
            wr_final.flush();
        }

        if (lastProcessedBatch >= 0 && stateSavedFor != lastProcessedBatch) {
            state.save(state_path);
        }

        if (wr_result != null) {
            wr_result.close();
        }
        wr_time.close();
        wr_summary.close();
        wr_final.close();
        StageDebug.close();

        writeRuntimeMeta(output_dir, fastExperiment, flushEvery, streamFile, packedWindow,
                         input_prefix, num_batches, lastProcessedBatch);

        if (!quiet) {
            System.out.println("\n=== 流式处理完成 ===");
            System.out.println("最终状态: " + state.getSummary());
            System.out.println("结果已保存至: " + output_dir);
            System.out.println("流式代表集导出: " + output_dir + "/final_representatives.txt");
        }
    }

    /** Formats the final representative set using the same numeric format as batch output. */
    private static String buildFinalBlock(int batchIdx,
                                          List<StreamingState.Representative> Q,
                                          double avgScore, int dim, DecimalFormat df) {
        StringBuilder sb = new StringBuilder(64 + Q.size() * (dim + 1) * 10);
        sb.append(String.format("index %d %d %.6f\n", batchIdx, Q.size(), avgScore));
        for (StreamingState.Representative rep : Q) {
            for (int d = 0; d < dim; d++) {
                sb.append(df.format(rep.vector[d])).append(" ");
            }
            sb.append("\n");
        }
        sb.append("\n");
        return sb.toString();
    }

    private static void writeRuntimeMeta(String outputDir, boolean fastExperiment, int flushEvery,
                                         String streamFile, int packedWindow,
                                         String inputPrefix, int numBatches,
                                         int lastProcessedBatch) throws IOException {
        try (BufferedWriter w = new BufferedWriter(
                new FileWriter(outputDir + "/runtime_meta.txt", false))) {
            w.write("runtime_mode=" + (fastExperiment ? "fast_experiment_v1" : "legacy") + "\n");
            w.write("input_mode=" + (streamFile != null ? "packed_stream" : "batch_files") + "\n");
            w.write("input_prefix=" + inputPrefix + "\n");
            w.write("stream_file=" + (streamFile == null ? "" : streamFile) + "\n");
            w.write("packed_window_size=" + (streamFile == null ? "" : String.valueOf(packedWindow)) + "\n");
            w.write("flush_every=" + (fastExperiment ? String.valueOf(flushEvery) : "1") + "\n");
            w.write("num_batches=" + numBatches + "\n");
            w.write("last_processed_batch=" + lastProcessedBatch + "\n");
        }
    }

    /** Supplies consecutive batches from individual files or a single packed stream. */
    private interface StreamSource extends Closeable {

        double[][] nextBatch(int batchIdx) throws IOException;

        String describe();

        String missingMessage(int batchIdx);
    }

    private static final class BatchFileSource implements StreamSource {
        private final String prefix;
        private final int dim;

        BatchFileSource(String prefix, int dim) {
            this.prefix = prefix;
            this.dim = dim;
        }

        @Override
        public double[][] nextBatch(int batchIdx) throws IOException {
            String path = resolveBatchFile(prefix, batchIdx);
            if (path == null) {
                return null;
            }
            List<double[]> data = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new FileReader(path), 1 << 16)) {
                String line;
                while ((line = br.readLine()) != null) {
                    double[] point = parsePoint(line, dim);
                    if (point != null) {
                        data.add(point);
                    }
                }
            }
            return data.toArray(new double[0][]);
        }

        @Override
        public String describe() {
            return "batch_files(" + prefix + "_batch_N.txt)";
        }

        @Override
        public String missingMessage(int batchIdx) {
            return prefix + "_batch_" + batchIdx + ".txt 或 "
                + prefix + "_sliding_window_" + batchIdx + ".txt";
        }

        @Override
        public void close() {
        }
    }

    /** Reads up to the configured batch size, retaining the final partial batch. */
    private static final class PackedStreamSource implements StreamSource {
        private final String path;
        private final int window;
        private final int dim;
        private final BufferedReader reader;
        private boolean exhausted = false;
        private int nextBatchIndex = 0;

        PackedStreamSource(String path, int window, int dim) throws IOException {
            this.path = path;
            this.window = window;
            this.dim = dim;
            this.reader = new BufferedReader(new FileReader(path), 1 << 20);
        }

        @Override
        public double[][] nextBatch(int batchIdx) throws IOException {
            if (batchIdx < nextBatchIndex) {
                throw new IOException("packed stream 不支持向前回读批次: " + batchIdx);
            }

            while (nextBatchIndex < batchIdx) {
                if (readBatch() == null) {
                    return null;
                }
                nextBatchIndex++;
            }
            double[][] batch = readBatch();
            if (batch != null) {
                nextBatchIndex++;
            }
            return batch;
        }

        private double[][] readBatch() throws IOException {
            if (exhausted) {
                return null;
            }
            List<double[]> data = new ArrayList<>(window);
            String line;
            while (data.size() < window && (line = reader.readLine()) != null) {
                double[] point = parsePoint(line, dim);
                if (point != null) {
                    data.add(point);
                }
            }
            if (data.size() < window) {
                exhausted = true;
            }
            if (data.isEmpty()) {
                return null;
            }
            return data.toArray(new double[0][]);
        }

        @Override
        public String describe() {
            return "packed_stream(" + path + ", w=" + window + ")";
        }

        @Override
        public String missingMessage(int batchIdx) {
            return "packed stream " + path + " 在第 " + batchIdx + " 批处已读完";
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    private static String resolveBatchFile(String inputPrefix, int batchIdx) {
        String batchFile = inputPrefix + "_batch_" + batchIdx + ".txt";
        if (new File(batchFile).exists()) {
            return batchFile;
        }
        String slidingWindowFile = inputPrefix + "_sliding_window_" + batchIdx + ".txt";
        if (new File(slidingWindowFile).exists()) {
            return slidingWindowFile;
        }
        return null;
    }

    /** Parses one whitespace-separated point and constructs its lifted representation. */
    private static double[] parsePoint(String line, int dim) {

        int en = line.length();
        int st = 0;
        while (st < en && line.charAt(st) <= ' ') {
            st++;
        }
        while (en > st && line.charAt(en - 1) <= ' ') {
            en--;
        }
        if (st >= en) {
            return null;
        }
        double[] point = new double[dim + 1];
        int pos = st;
        for (int d = 0; d < dim; d++) {
            while (pos < en && isSplitSpace(line.charAt(pos))) {
                pos++;
            }
            if (pos >= en) {
                System.err.println("警告: 数据维度不足，跳过行: " + line.substring(st, en));
                return null;
            }
            int start = pos;
            while (pos < en && !isSplitSpace(line.charAt(pos))) {
                pos++;
            }
            point[d] = parseCoordinate(line, start, pos);
        }

        point[dim] = Math.sqrt(dim - VectorUtil.norm2(point));
        return point;
    }

    /** Parses common fixed-point coordinates without temporary substrings; falls back to Double parsing. */
    private static double parseCoordinate(String line, int start, int end) {
        int pos = start;
        boolean negative = false;
        if (pos < end && (line.charAt(pos) == '-' || line.charAt(pos) == '+')) {
            negative = line.charAt(pos++) == '-';
        }
        long value = 0;
        int digits = 0;
        int scale = 0;
        boolean dot = false;
        for (; pos < end; pos++) {
            char ch = line.charAt(pos);
            if (ch >= '0' && ch <= '9' && digits < 15) {
                value = value * 10 + (ch - '0');
                digits++;
                if (dot) scale++;
            } else if (ch == '.' && !dot) {
                dot = true;
            } else {
                return Double.parseDouble(line.substring(start, end));
            }
        }
        if (digits == 0) {
            return Double.parseDouble(line.substring(start, end));
        }
        double result = (double) value / DECIMAL_SCALES[scale];
        return negative ? -result : result;
    }

    private static final double[] DECIMAL_SCALES = {
        1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0,
        10000000.0, 100000000.0, 1000000000.0, 10000000000.0,
        100000000000.0, 1000000000000.0, 10000000000000.0,
        100000000000000.0, 1000000000000000.0
    };

    private static boolean isSplitSpace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == 0x0B || c == '\f' || c == '\r';
    }
}
