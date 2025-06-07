import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

public class G23HW3 {

    private static final int RANDOM_SEED = 123;
    private static final String HOSTNAME = "algo.dei.unipd.it";
    private static final int P = 8191; // Prime number for hash function
    private static final int BATCH_MILLISECONDS = 100; // Batch duration in milliseconds

    private static void updateCountMinSketch(int[][] sketch, int item, int[][] a, int[][] b, int W) {
        int D = sketch.length;

        // Increment all counters in the hash positions
        for (int i = 0; i < D; i++) {
            int hash = ((a[i][0] * item + b[i][0]) % P) % W;
            // Ensure positive index
            if (hash < 0)
                hash += W;
            sketch[i][hash]++;
        }
    }

    private static int estimateCountMin(int[][] sketch, int item, int[][] a, int[][] b, int W) {
        int D = sketch.length;
        int minValue = Integer.MAX_VALUE;

        for (int i = 0; i < D; i++) {
            int hash = ((a[i][0] * item + b[i][0]) % P) % W;
            // Ensure positive index
            if (hash < 0)
                hash += W;
            minValue = Math.min(minValue, sketch[i][hash]);
        }

        return minValue;
    }

    private static void updateCountSketch(int[][] sketch, int item, int[][] a, int[][] b,
            int[][] a_sign, int[][] b_sign, int W) {
        int D = sketch.length;

        for (int i = 0; i < D; i++) {
            int hash = ((a[i][0] * item + b[i][0]) % P) % W;
            // Ensure positive index
            if (hash < 0)
                hash += W;

            int signHash = ((a_sign[i][0] * item + b_sign[i][0]) % P) % 2;
            int sign = (signHash == 0) ? 1 : -1;

            sketch[i][hash] += sign;
        }
    }

    // Estimate frequency using count sketch
    private static int estimateCountSketch(int[][] sketch, int item, int[][] a, int[][] b,
            int[][] a_sign, int[][] b_sign, int W) {
        int D = sketch.length;
        int[] estimates = new int[D];

        for (int i = 0; i < D; i++) {
            int hash = ((a[i][0] * item + b[i][0]) % P) % W;
            // Ensure positive index
            if (hash < 0)
                hash += W;

            int signHash = ((a_sign[i][0] * item + b_sign[i][0]) % P) % 2;
            int sign = (signHash == 0) ? 1 : -1;

            estimates[i] = sketch[i][hash] * sign;
        }

        // Sort to find median
        Arrays.sort(estimates);
        return (D % 2 == 1) ? estimates[D / 2] : (estimates[D / 2 - 1] + estimates[D / 2]) / 2;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException("Usage: G23HW3 <portExp> <T> <D> <W> <K>");
        }

        int portExp = Integer.parseInt(args[0]); // Port number
        int T = Integer.parseInt(args[1]); // Target number of items to process
        int D = Integer.parseInt(args[2]); // Number of sketch rows
        int W = Integer.parseInt(args[3]); // Number of sketch columns
        int K = Integer.parseInt(args[4]); // Number of top frequent items

        // Set up Spark configuration
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("G23HW3");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(BATCH_MILLISECONDS));
        sc.sparkContext().setLogLevel("ERROR");

        // Stopping the streaming context when T items are processed
        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        // For tracking values between RDDs
        long[] streamLength = new long[1];
        streamLength[0] = 0L;
        Map<Integer, Integer> exactFrequencies = new HashMap<>();

        int[][] countMinSketch = new int[D][W];
        int[][] a_cm = new int[D][1]; // 'a' values for hash functions
        int[][] b_cm = new int[D][1]; // 'b' values for hash functions

        int[][] countSketch = new int[D][W];
        int[][] a_cs = new int[D][1]; // 'a' values for hash functions
        int[][] b_cs = new int[D][1]; // 'b' values for hash functions
        int[][] a_sign = new int[D][1]; // 'a' values for sign hash functions
        int[][] b_sign = new int[D][1]; // 'b' values for sign hash functions

        // Generate hash function parameters

        // Fixed random seed for reproducibility
        // Random rand = new Random(RANDOM_SEED);

        // New random instance for each run
        Random rand = new Random();

        for (int i = 0; i < D; i++) {
            // For count-min sketch
            a_cm[i][0] = rand.nextInt(1, (P - 1) + 1); // [1, p-1]
            b_cm[i][0] = rand.nextInt(0, P); // [0, p-1]

            // For count sketch
            a_cs[i][0] = rand.nextInt(1, (P - 1) + 1); // [1, p-1]
            b_cs[i][0] = rand.nextInt(0, P); // [0, p-1]
            a_sign[i][0] = rand.nextInt(1, (P - 1) + 1); // [1, p-1]
            b_sign[i][0] = rand.nextInt(0, P); // [0, p-1]
        }

        // Process the stream
        JavaDStream<String> stream = sc.socketTextStream(HOSTNAME, portExp, StorageLevels.MEMORY_AND_DISK);

        stream.foreachRDD((rdd, time) -> {
            if (streamLength[0] < T) {
                long availableItems = Math.min(rdd.count(), T - streamLength[0]);

                if (availableItems > 0) {
                    // Process only items to reach T
                    List<Integer> items = rdd.map(s -> Integer.parseInt(s)).take((int) availableItems);

                    for (Integer item : items) {
                        // Update exact frequencies
                        exactFrequencies.put(item, exactFrequencies.getOrDefault(item, 0) + 1);

                        // Update count-min sketch
                        updateCountMinSketch(countMinSketch, item, a_cm, b_cm, W);

                        // Update count sketch
                        updateCountSketch(countSketch, item, a_cs, b_cs, a_sign, b_sign, W);

                        streamLength[0]++;
                        if (streamLength[0] >= T) {
                            break;
                        }
                    }

                    if (streamLength[0] >= T) {
                        stoppingSemaphore.release();
                    }
                }
            }
        });

        // Start the computation
        sc.start();
        stoppingSemaphore.acquire();
        sc.stop(false, false);

        // Filter top-K heavy hitters
        List<Map.Entry<Integer, Integer>> sortedFrequencies = new ArrayList<>(exactFrequencies.entrySet());
        sortedFrequencies.sort((e1, e2) -> e2.getValue().compareTo(e1.getValue()));

        // Find the K-th frequency (phi(K))
        final int kthFrequency = (!sortedFrequencies.isEmpty() && K <= sortedFrequencies.size())
                ? sortedFrequencies.get(K - 1).getValue()
                : 0;

        // Get top-K heavy hitters
        List<Map.Entry<Integer, Integer>> heavyHitters = sortedFrequencies.stream()
                .filter(entry -> entry.getValue() >= kthFrequency)
                .collect(Collectors.toList());

        // Calculate average relative error for count-min sketch
        double cmTotalError = 0.0;
        for (Map.Entry<Integer, Integer> entry : heavyHitters) {
            int item = entry.getKey();
            int trueFreq = entry.getValue();
            int estimatedFreq = estimateCountMin(countMinSketch, item, a_cm, b_cm, W);
            double relativeError = (double) Math.abs(trueFreq - estimatedFreq) / trueFreq;
            cmTotalError += relativeError;
        }
        double cmAvgError = cmTotalError / heavyHitters.size();

        // Calculate average relative error for count sketch
        double csTotalError = 0.0;
        for (Map.Entry<Integer, Integer> entry : heavyHitters) {
            int item = entry.getKey();
            int trueFreq = entry.getValue();
            int estimatedFreq = estimateCountSketch(countSketch, item, a_cs, b_cs, a_sign, b_sign, W);
            double relativeError = (double) Math.abs(trueFreq - estimatedFreq) / trueFreq;
            csTotalError += relativeError;
        }
        double csAvgError = csTotalError / heavyHitters.size();

        // Print results
        System.out.println("Port = " + portExp + " T = " + T + " D = " + D + " W = " + W + " K = " + K);
        System.out.println("Number of processed items = " + streamLength[0]);
        System.out.println("Number of distinct items  = " + exactFrequencies.size());
        System.out.println("Number of Top-K Heavy Hitters = " + heavyHitters.size());
        System.out.println("Avg Relative Error for Top-K Heavy Hitters with CM = " + cmAvgError);
        System.out.println("Avg Relative Error for Top-K Heavy Hitters with CS = " + csAvgError);

        // Print heavy hitter information only if K <= 10
        if (K <= 10) {
            System.out.println("Top-K Heavy Hitters:");
            // Sort by in ascending order of item ID
            heavyHitters.sort((e1, e2) -> e1.getKey().compareTo(e2.getKey()));

            for (int i = 0; i < Math.min(K, heavyHitters.size()); i++) {
                Map.Entry<Integer, Integer> entry = heavyHitters.get(i);
                int item = entry.getKey();
                int trueFreq = entry.getValue();
                int cmEstFreq = estimateCountMin(countMinSketch, item, a_cm, b_cm, W);

                System.out.println("Item " + item + " True Frequency = " + trueFreq + " Estimated Frequency with CM = " + cmEstFreq);
            }
        }
    }
}
