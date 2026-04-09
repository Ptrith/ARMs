package main;

import structures.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Simple baseline: run streaming RMS on NBA split windows and estimate regret ratio.
 */
public class TestNBABaseline {
    
    public static void main(String[] args) throws Exception {
        System.out.println("======================================================================");
        System.out.println("Java StreamingRMS baseline - NBA");
        System.out.println("======================================================================");
        
        String dataPrefix = "dataset/NBA_streaming/NBA";
        int dim = 5;
        int K = 20;
        int r = 20;
        double eps = 0.01;
        int sampleSize = 50;
        double lambda = 0.7;
        double mincos = 0.707;
        
        System.out.println("\n[Config]");
        System.out.println("  dataset prefix: " + dataPrefix);
        System.out.println("  dim: " + dim);
        System.out.println("  K: " + K);
        System.out.println("  r: " + r);
        System.out.println("  sample_size: " + sampleSize);
        System.out.println("  lambda: " + lambda);
        System.out.println("  min_cos: " + mincos);
        
        StreamingState state = new StreamingState(r);
        state.setHyperparameters(lambda, 0.5, 0.8, 0.1, mincos, sampleSize);
        
        System.out.println("\n[Load windows]");
        List<double[][]> batches = new ArrayList<>();
        int batchIdx = 0;
        while (true) {
            String windowFile = resolveWindowFile(dataPrefix, batchIdx);
            if (windowFile == null) {
                break;
            }
            
            double[][] batchData = loadBatch(windowFile, dim);
            batches.add(batchData);
            batchIdx++;
            
            if (batchIdx % 50 == 0) {
                System.out.println("  loaded " + batchIdx + " windows...");
            }
        }
        
        System.out.println("  total windows: " + batches.size());
        System.out.println("  tuples (first window size * count): " + (batches.size() * batches.get(0).length));
        
        System.out.println("\n[Streaming]");
        long startTime = System.currentTimeMillis();
        
        List<Double> regretRatios = new ArrayList<>();
        for (int i = 0; i < batches.size(); i++) {
            StreamingRMS rms = new StreamingRMS(dim, K, eps, state, batches.get(i));
            rms.updateRepresentatives();
            
            if ((i + 1) % 20 == 0 || i == batches.size() - 1) {
                double regret = computeRegretRatio(state.Q, 2000, dim);
                regretRatios.add(regret);
                System.out.printf("  window %3d: regret_ratio=%.4f, |Q|=%d%n", 
                    i + 1, regret, state.Q.size());
            }
        }
        
        long endTime = System.currentTimeMillis();
        
        System.out.println("\n[Final]");
        double finalRegret = computeRegretRatio(state.Q, 10000, dim);
        System.out.printf("  final regret_ratio: %.4f (10000 samples)%n", finalRegret);
        System.out.printf("  |Q|: %d%n", state.Q.size());
        System.out.printf("  wall time: %.2f s%n", (endTime - startTime) / 1000.0);
        
        if (!regretRatios.isEmpty()) {
            double avgRegret = regretRatios.stream().mapToDouble(d -> d).average().orElse(0);
            double minRegret = regretRatios.stream().mapToDouble(d -> d).min().orElse(0);
            System.out.printf("  mean regret_ratio: %.4f%n", avgRegret);
            System.out.printf("  min regret_ratio: %.4f%n", minRegret);
        }
        
        System.out.println("\n======================================================================");
    }
    
    private static double[][] loadBatch(String filepath, int dim) throws IOException {
        List<double[]> data = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                
                String[] parts = line.split("\\s+");
                if (parts.length != dim) continue;
                
                double[] point = new double[dim];
                for (int i = 0; i < dim; i++) {
                    point[i] = Double.parseDouble(parts[i]);
                }
                data.add(point);
            }
        }
        return data.toArray(new double[0][]);
    }

    private static String resolveWindowFile(String prefix, int windowIdx) {
        String slidingWindowFile = prefix + "_sliding_window_" + windowIdx + ".txt";
        if (Files.exists(Paths.get(slidingWindowFile))) {
            return slidingWindowFile;
        }
        String legacyBatchFile = prefix + "_batch_" + windowIdx + ".txt";
        if (Files.exists(Paths.get(legacyBatchFile))) {
            return legacyBatchFile;
        }
        return null;
    }
    
    private static double computeRegretRatio(List<StreamingState.Representative> Q, int numSamples, int dim) {
        if (Q.isEmpty()) return 1.0;
        
        Random rand = new Random(42);
        double sumRegret = 0.0;
        
        for (int i = 0; i < numSamples; i++) {
            double[] u = new double[dim];
            double norm = 0;
            for (int j = 0; j < dim; j++) {
                u[j] = rand.nextGaussian();
                norm += u[j] * u[j];
            }
            norm = Math.sqrt(norm);
            for (int j = 0; j < dim; j++) {
                u[j] /= norm;
            }
            
            double maxUtility = Double.NEGATIVE_INFINITY;
            for (StreamingState.Representative rep : Q) {
                double utility = dot(rep.vector, u);
                maxUtility = Math.max(maxUtility, utility);
            }
            
            sumRegret += (1.0 - maxUtility);
        }
        
        return sumRegret / numSamples;
    }
    
    private static double dot(double[] a, double[] b) {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        return sum;
    }
}
