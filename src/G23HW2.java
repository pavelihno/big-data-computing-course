import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;

public class G23HW2 {

    private static final int RANDOM_SEED = 123;
    private static final boolean LOCAL = true;
    private static final boolean SAVE_OUTPUT = true;
    private static final String CSV_FAIR_OUTPUT_PATH = "./data/fair_points.csv";
    private static final String CSV_STANDARD_OUTPUT_PATH = "./data/standard_points.csv";

    private static Tuple2<Double, Integer> getMinSquaredDistance(Vector point, Vector[] centers) {
        /*
         * Computes the squared distance from a point to its nearest center and returns both
         * the distance and the index of the nearest center
         */
        double minDistance = Double.MAX_VALUE;
        int minIndex = 0;

        for (int i = 0; i < centers.length; i++) {
            double distance = Vectors.sqdist(point, centers[i]);
            if (distance < minDistance) {
                minDistance = distance;
                minIndex = i;
            }
        }
        return new Tuple2<>(minDistance, minIndex);
    }

    private static double MRComputeFairObjective(JavaPairRDD<Vector, String> points, Vector[] centers) {
        /*
         * Computes the average squared distance from all points to their nearest
         * centers
         * for both sets of points A and B, and returns the maximum of the two averages
         */
        JavaPairRDD<Vector, String> pointsA = points.filter(tuple -> tuple._2().equals("A"));
        JavaPairRDD<Vector, String> pointsB = points.filter(tuple -> tuple._2().equals("B"));

        double totalSquaredDistanceA = pointsA.map(tuple -> {
            return getMinSquaredDistance(tuple._1(), centers)._1();
        }).reduce((a, b) -> a + b);
        double totalSquaredDistanceB = pointsB.map(tuple -> {
            return getMinSquaredDistance(tuple._1(), centers)._1();
        }).reduce((a, b) -> a + b);

        double averageSquaredDistanceA = totalSquaredDistanceA / pointsA.count();
        double averageSquaredDistanceB = totalSquaredDistanceB / pointsB.count();

        return Math.max(averageSquaredDistanceA, averageSquaredDistanceB);
    }

    private static Vector[] MRLloyds(JavaPairRDD<Vector, String> points, int K, int M) {
        /*
         * Computes the K-means clustering using Lloyd's algorithm
         * and returns the cluster centers
         */
        KMeansModel kmeansModel = KMeans.train(
                points.keys().rdd(),
                K, // Number of clusters
                M, // Number of iterations
                "k-means||",
                RANDOM_SEED);

        return kmeansModel.clusterCenters();
    }

    private static Vector[] MRFairLloyds(JavaPairRDD<Vector, String> points, int K, int M) {
        /*
         * Computes the K-means clustering using Fair Lloyd's algorithm
         * and returns the cluster centers
         * 
         * TODO: Implement Fair Lloyd's algorithm
         */
        return new Vector[0];
    }

    private static <T> Tuple2<T, Double> timeOperation(Supplier<T> operation) {
        /*
         * Computes the time (in seconds) taken to execute an operation and returns the result along
         * with the time taken
         */

        long startTime = System.currentTimeMillis();
        T result = operation.get();
        long endTime = System.currentTimeMillis();
        
        double durationSeconds = (endTime - startTime) / 1000.0; 
        return new Tuple2<>(result, durationSeconds);
    }

    private static void savePoints(JavaPairRDD<Vector, String> points, Vector[] centers, String outputPath) {
        /*
        * Saves the points to a CSV file
        */

        try {
            // Map each point to its nearest center
            JavaRDD<String> pointsAsStrings = points.map(tuple -> {
                Vector point = tuple._1();
                String group = tuple._2();
            
                // Find the nearest center
                int nearestCluster = getMinSquaredDistance(point, centers)._2();
            
                // Format directly as a string
                return String.format(Locale.US, 
                        "%.3f,%.3f,%c,%d", 
                        point.apply(0), point.apply(1), 
                        group.charAt(0), nearestCluster);
            });

            // Collect all points (safe only in LOCAL mode)
            List<String> lines = pointsAsStrings.collect();

            try (FileWriter writer = new FileWriter(outputPath)) {
                for (String line : lines) {
                    writer.write(line + "\n");
                }
            } catch (IOException e) {
                System.out.println("Error writing to file: " + e.getMessage());
            }

            System.out.println("Points saved to " + outputPath);

        } catch (Exception e) {
            System.err.println("Error saving points: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        if (args.length != 4) {
            throw new IllegalArgumentException("USAGE: <input_file_path> <L> <K> <M>");
        }

        String inputFilePath = args[0];
        int L = Integer.parseInt(args[1]); // Number of partitions
        int K = Integer.parseInt(args[2]); // Number of clusters
        int M = Integer.parseInt(args[3]); // Number of iterations

        String masterAddress = LOCAL ? "local[*]" : "yarn";
        SparkConf conf = new SparkConf(true).setAppName("G23HW2").setMaster(masterAddress);
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        try {
            // Read input file into RDD with L partitions
            JavaPairRDD<Vector, String> inputPoints = sc.textFile(inputFilePath, L)
                    .map(line -> line.split(","))
                    .filter(tokens -> tokens.length >= 2)
                    .mapToPair(tokens -> {
                        String group = tokens[tokens.length - 1];
                        double[] coords = new double[tokens.length - 1];
                        for (int i = 0; i < coords.length; i++)
                            coords[i] = Double.parseDouble(tokens[i]);
                        Vector point = Vectors.dense(coords);
                        return new Tuple2<>(point, group);
                    });

            inputPoints.cache();

            // Count total points and points in each demographic group
            long N = inputPoints.count();
            long NA = inputPoints.filter(tuple -> tuple._2().equals("A")).count();
            long NB = inputPoints.filter(tuple -> tuple._2().equals("B")).count();

            System.out.printf("Input file = %s, L = %d, K = %d, M = %d\n", inputFilePath, L, K, M);
            System.out.printf("N = %d, NA = %d, NB = %d\n", N, NA, NB);

            // Run clustering algorithms to get centers (with timers)
            Tuple2<Vector[], Double> standardCentersResult = timeOperation(() -> MRLloyds(inputPoints, K, M));
            Vector[] standardCenters = standardCentersResult._1();
            
            Tuple2<Vector[], Double> fairCentersResult = timeOperation(() -> MRFairLloyds(inputPoints, K, M));
            Vector[] fairCenters = fairCentersResult._1();

            // Compute fair objectives (with timers)
            Tuple2<Double, Double> standardCentersObjResult = timeOperation(() -> MRComputeFairObjective(inputPoints, standardCenters));
            double standardCentersObj = standardCentersObjResult._1();

            Tuple2<Double, Double> fairCentersObjResult = timeOperation(() -> MRComputeFairObjective(inputPoints, fairCenters));
            double fairCentersObj = fairCentersObjResult._1();

            // Print statistics
            System.out.printf(Locale.US, "Phi(A,B,C_stand) = %.6f\n", standardCentersObj);
            System.out.printf(Locale.US, "Phi(A,B,C_fair) = %.6f\n", fairCentersObj);
            System.out.printf(Locale.US, "Time for standard Lloyd's algorithm = %.3fs\n", standardCentersResult._2());
            System.out.printf(Locale.US, "Time for fair Lloyd's algorithm = %.3fs\n", fairCentersResult._2());

            System.out.printf(Locale.US, "Time for objective function with standard centers = %.3fs\n", standardCentersObjResult._2());
            System.out.printf(Locale.US, "Time for objective function with fair centers = %.3fs\n", fairCentersObjResult._2());

            // Save points to CSV files
            if (SAVE_OUTPUT && LOCAL) {
                savePoints(inputPoints, standardCenters, CSV_STANDARD_OUTPUT_PATH);
                savePoints(inputPoints, fairCenters, CSV_FAIR_OUTPUT_PATH);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sc.close();
        }
    }
}
