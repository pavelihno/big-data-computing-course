import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
import scala.Tuple4;

public class G23HW2 {

    private static final int RANDOM_SEED = 123;
    private static final boolean LOCAL = true;
    private static final boolean SAVE_OUTPUT = LOCAL && true;
    private static final String CSV_FAIR_OUTPUT_PATH = "./data/fair_points.csv";
    private static final String CSV_STANDARD_OUTPUT_PATH = "./data/standard_points.csv";

    private static Tuple2<Double, Integer> getMinSquaredDistance(Vector point, Vector[] centers) {
        /*
         * Computes the squared distance from a point to its nearest center and returns
         * both the distance and the index of the nearest center
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

    private static double MRComputeFairObjective(JavaPairRDD<Vector, String> pointsA, JavaPairRDD<Vector, String> pointsB, Vector[] centers) {
        /*
         * Computes the average squared distance from all points to their nearest
         * centers
         * for both sets of points A and B, and returns the maximum of the two averages
         */

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

    private static double[] computeVectorX(double fixedA, double fixedB, double[] alpha, double[] beta, double[] ell, int K) {
        double gamma = 0.5;
        double[] xDist = new double[K];
        double fA, fB;
        double power = 0.5;
        int T = 10;
        for (int t = 1; t <= T; t++) {
            fA = fixedA;
            fB = fixedB;
            power = power / 2;
            for (int i = 0; i < K; i++) {
                double temp = (1 - gamma) * beta[i] * ell[i] / (gamma * alpha[i] + (1 - gamma) * beta[i]);
                xDist[i] = temp;
                fA += alpha[i] * temp * temp;
                temp = (ell[i] - temp);
                fB += beta[i] * temp * temp;
            }
            if (fA == fB) {
                break;
            }
            gamma = (fA > fB) ? gamma + power : gamma - power;
        }
        return xDist;
    }

    private static Vector[] CentroidsSelection(JavaPairRDD<Vector, String> points, Vector[] centers, int K, long NA, long NB) {
        /*
         * Computes the new centers for the clusters
         */

        JavaPairRDD<Integer, Tuple2<Vector, String>> clusteredPoints = points.mapToPair(tuple -> {
            // Map each point to its nearest center
            Vector point = tuple._1();
            String group = tuple._2();
            int clusterId = getMinSquaredDistance(point, centers)._2();
            return new Tuple2<>(clusterId, new Tuple2<>(point, group));
        });

        Map<Integer, Tuple4<Long, Vector, Long, Vector>> clusterStats = clusteredPoints.mapToPair(tuple -> {
            // Calculate stats for each cluster inside one partition
            int clusterId = tuple._1();
            Vector point = tuple._2()._1();
            String group = tuple._2()._2();

            long countA = 0, countB = 0;
            Vector sumA = null, sumB = null;
            if (group.equals("A")) {
                countA = 1;
                sumA = point;
            } else {
                countB = 1;
                sumB = point;
            }
            return new Tuple2<>(clusterId, new Tuple4<>(countA, sumA, countB, sumB));
        }).reduceByKey((stats1, stats2) -> {
            // Calculate stats for each cluster across all partitions
            long countA = stats1._1() + stats2._1();
            long countB = stats1._3() + stats2._3();

            Vector sumA1 = stats1._2();
            Vector sumB1 = stats1._4();
            Vector sumA2 = stats2._2();
            Vector sumB2 = stats2._4();

            Vector sumA, sumB;

            // Merge sum vectors (sumA and sumB)
            if (sumA1 == null)
                sumA = sumA2;
            else if (sumA2 == null)
                sumA = sumA1;
            else {
                double[] values = new double[sumA1.size()];
                for (int i = 0; i < values.length; i++) {
                    values[i] = sumA1.apply(i) + sumA2.apply(i);
                }
                sumA = Vectors.dense(values);
            }

            if (sumB1 == null)
                sumB = sumB2;
            else if (sumB2 == null)
                sumB = sumB1;
            else {
                double[] values = new double[sumB1.size()];
                for (int i = 0; i < values.length; i++) {
                    values[i] = sumB1.apply(i) + sumB2.apply(i);
                }
                sumB = Vectors.dense(values);
            }

            return new Tuple4<>(countA, sumA, countB, sumB);
        }).collectAsMap();

        // Compute alpha, beta, mu_a, mu_b, ell for each cluster
        double[] alpha = new double[K];
        double[] beta = new double[K];
        double[] ell = new double[K];
        Vector[] mu_a = new Vector[K];
        Vector[] mu_b = new Vector[K];

        // Initialize mu_a and mu_b arrays
        int dimensions = centers[0].size();
        Vector zeroVector = Vectors.zeros(dimensions);

        for (int i = 0; i < K; i++) {
            mu_a[i] = zeroVector;
            mu_b[i] = zeroVector;
        }

        for (int i = 0; i < K; i++) {
            Tuple4<Long, Vector, Long, Vector> stats = clusterStats.get(i);
            long countA = stats._1();
            long countB = stats._3();
            Vector sumA = stats._2();
            Vector sumB = stats._4();

            alpha[i] = (double) countA / NA;
            beta[i] = (double) countB / NB;

            if (countA > 0) {
                // Calculate mu_a[i] as the centroid of points from group A in cluster
                double[] muAValues = new double[dimensions];
                for (int j = 0; j < dimensions; j++) {
                    muAValues[j] = sumA.apply(j) / countA;
                }
                mu_a[i] = Vectors.dense(muAValues);
            }
            else {
                mu_a[i] = countB > 0 ? mu_b[i] : centers[i];
            }

            if (countB > 0) {
                // Calculate mu_b[i] as the centroid of points from group B in cluster
                double[] muBValues = new double[dimensions];
                for (int j = 0; j < dimensions; j++) {
                    muBValues[j] = sumB.apply(j) / countB;
                }
                mu_b[i] = Vectors.dense(muBValues);
            }
            else {
                mu_b[i] = countA > 0 ? mu_a[i] : centers[i];
            }

            // Calculate ell[i] as Euclidean distance between mu_a[i] and mu_b[i]
            ell[i] = Math.sqrt(Vectors.sqdist(mu_a[i], mu_b[i]));
        }

        // Compute fixedA and fixedB
        Map<String, Double> fixedValues = clusteredPoints.mapToPair(tuple -> {
                int clusterId = tuple._1();
                Vector point = tuple._2()._1();
                String group = tuple._2()._2();
                double sqDist = 0.0;

                if (group.equals("A")) {
                    sqDist = Vectors.sqdist(point, mu_a[clusterId]);
                    return new Tuple2<>("A", sqDist);
                } else {
                    sqDist = Vectors.sqdist(point, mu_b[clusterId]);
                    return new Tuple2<>("B", sqDist);
                }
            })
            .reduceByKey((a, b) -> a + b)
            .collectAsMap();

        double fixedA = fixedValues.getOrDefault("A", 0.0) / NA;
        double fixedB = fixedValues.getOrDefault("B", 0.0) / NB;

        double[] x = computeVectorX(fixedA, fixedB, alpha, beta, ell, K);

        // For each cluster compute new center
        Vector[] newCenters = new Vector[K];
        for (int i = 0; i < K; i++) {
            double[] centerValues = new double[dimensions];
            double factor1 = (ell[i] - x[i]) / ell[i];
            double factor2 = x[i] / ell[i];

            for (int j = 0; j < dimensions; j++) {
                centerValues[j] = factor1 * mu_a[i].apply(j) + factor2 * mu_b[i].apply(j);
            }

            newCenters[i] = Vectors.dense(centerValues);
        }
        return newCenters;
    }

    private static Vector[] MRFairLloyds(JavaPairRDD<Vector, String> points, long NA, long NB, int K, int M) {
        /*
         * Computes the K-means clustering using Fair Lloyd's algorithm
         * and returns the cluster centers
         */

        // Initialization of centers (using kmeans|| with 0 iterations)
        Vector[] centers = MRLloyds(points, K, 0);

        for (int it = 0; it < M; it++) {
            // Computation of the new centers
            centers = CentroidsSelection(points, centers, K, NA, NB);
        }
        return centers;
    }

    private static <T> Tuple2<T, Double> timeOperation(Supplier<T> operation) {
        /*
         * Computes the time (in milliseconds) taken to execute an operation and
         * returns the result along with the time taken
         */

        long startTime = System.currentTimeMillis();
        T result = operation.get();
        long endTime = System.currentTimeMillis();

        double duration = (endTime - startTime);
        return new Tuple2<>(result, duration);
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

                // Format output string
                return String.format(Locale.US,
                        "%.3f,%.3f,%c,%d",
                        point.apply(0), point.apply(1),
                        group.charAt(0), nearestCluster);
            });

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
                        int groupIdx = tokens.length - 1;
                        try {
                            // clusterId given as the last column -> ignore it 
                            Integer.parseInt(tokens[groupIdx]);
                            groupIdx--;
                        } catch (NumberFormatException e) {}
                        String group = tokens[groupIdx];
                        double[] coords = new double[groupIdx];
                        for (int i = 0; i < coords.length; i++)
                            coords[i] = Double.parseDouble(tokens[i]);
                        Vector point = Vectors.dense(coords);
                        return new Tuple2<>(point, group);
                    });

            inputPoints.cache();

            // Run clustering using standard Lloyd's algorithm (with timer)
            Tuple2<Vector[], Double> standardCentersResult = timeOperation(() -> MRLloyds(inputPoints, K, M));
            Vector[] standardCenters = standardCentersResult._1();

            // Filter points by demographic group
            JavaPairRDD<Vector, String> inputPointsA = inputPoints.filter(tuple -> tuple._2().equals("A"));
            JavaPairRDD<Vector, String> inputPointsB = inputPoints.filter(tuple -> tuple._2().equals("B"));

            inputPointsA.cache();
            inputPointsB.cache();

            // Count total points and points in each demographic group
            long N = inputPoints.count();
            long NA = inputPointsA.count();
            long NB = N - NA;

            // Run clustering using Fair Lloyd's algorithm (with timer)
            Tuple2<Vector[], Double> fairCentersResult = timeOperation(() -> MRFairLloyds(inputPoints, NA, NB, K, M));
            Vector[] fairCenters = fairCentersResult._1();

            // Compute fair objectives (with timers)
            Tuple2<Double, Double> standardCentersObjResult = timeOperation(
                    () -> MRComputeFairObjective(inputPointsA, inputPointsB, standardCenters));
            double standardCentersObj = standardCentersObjResult._1();

            Tuple2<Double, Double> fairCentersObjResult = timeOperation(
                    () -> MRComputeFairObjective(inputPointsA, inputPointsB, fairCenters));
            double fairCentersObj = fairCentersObjResult._1();

            // Print statistics
            System.out.printf("Input file = %s, L = %d, K = %d, M = %d\n", inputFilePath, L, K, M);
            System.out.printf("N = %d, NA = %d, NB = %d\n", N, NA, NB);

            System.out.printf(Locale.US, "Fair Objective with Standard Centers = %.4f\n", standardCentersObj);
            System.out.printf(Locale.US, "Fair Objective with Fair Centers = %.4f\n", fairCentersObj);
            System.out.printf("Time to compute standard centers = %.0f ms\n", standardCentersResult._2());
            System.out.printf("Time to compute fair centers = %.0f ms\n", fairCentersResult._2());
            System.out.printf("Time to compute objective with standard centers = %.0f ms\n", standardCentersObjResult._2());
            System.out.printf("Time to compute objective with fair centers = %.0f ms\n", fairCentersObjResult._2());

            // Save points to CSV files
            if (SAVE_OUTPUT) {
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
