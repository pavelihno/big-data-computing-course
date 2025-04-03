import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;

public class G23HW1 {

    public static double getMinSquaredDistance(Vector point, ArrayList<Vector> centers) {
        /*
         * Computes the squared distance from a point to its nearest center
         */
        double minDistance = Double.MAX_VALUE;
        for (Vector center : centers) {
            double distance = Vectors.sqdist(point, center);
            minDistance = Math.min(minDistance, distance);
        }
        return minDistance;
    }

    public static double MRComputeStandardObjective(JavaPairRDD<Vector, String> points, ArrayList<Vector> centers) {
        /*
         * Computes the total squared distance from all points to their nearest centers
         * for both sets of points A and B, and returns the sum
         */
        double totalSquaredDistance = points.map(tuple -> getMinSquaredDistance(tuple._1(), centers))
                .reduce((a, b) -> a + b);

        return totalSquaredDistance / points.count();
    }

    public static double MRComputeFairObjective(JavaPairRDD<Vector, String> points, ArrayList<Vector> centers) {
        /*
         * Computes the average squared distance from all points to their nearest
         * centers
         * for both sets of points A and B, and returns the maximum of the two averages
         */
        JavaPairRDD<Vector, String> pointsA = points.filter(tuple -> tuple._2().equals("A"));
        JavaPairRDD<Vector, String> pointsB = points.filter(tuple -> tuple._2().equals("B"));

        double totalSquaredDistanceA = pointsA.map(tuple -> getMinSquaredDistance(tuple._1(), centers))
                .reduce((a, b) -> a + b);
        double totalSquaredDistanceB = pointsB.map(tuple -> getMinSquaredDistance(tuple._1(), centers))
                .reduce((a, b) -> a + b);

        double averageSqauredDistanceA = totalSquaredDistanceA / pointsA.count();
        double averageSquaredDistanceB = totalSquaredDistanceB / pointsB.count();

        return Math.max(averageSqauredDistanceA, averageSquaredDistanceB);
    }

    public static void MRPrintStatistics(JavaPairRDD<Vector, String> points, ArrayList<Vector> centers) {
        /*
         * Computes and prints statistics for each cluster
         */

        // Count group A and B for each cluster
        HashMap<Integer, Tuple2<Integer, Integer>> counts = new HashMap<>();

        counts.putAll(points.mapToPair(tuple -> {
        /* Local space  = O(1) */
            Vector point = tuple._1();
            String group = tuple._2();
            int cluster = -1;
            double bestDist = Double.MAX_VALUE;
            for (int i = 0; i < centers.size(); i++) {
                double dist = Vectors.sqdist(point, centers.get(i));
                if (dist < bestDist) {
                    bestDist = dist;
                    cluster = i;
                }
            }

            int countA = group.equals("A") ? 1 : 0;
            int countB = group.equals("B") ? 1 : 0;

            return new Tuple2<>(cluster, new Tuple2<>(countA, countB));
        }).reduceByKey((t1, t2) -> {
        /* Local space (worst case: all points belong to a single cluster) = O(n) */
            return new Tuple2<>(t1._1() + t2._1(), t1._2() + t2._2());
        }).collectAsMap());

        // Print statistics for each cluster
        for (int i = 0; i < centers.size(); i++) {
            Vector center = centers.get(i);
            Tuple2<Integer, Integer> tuple = counts.get(i);
            int na = (tuple != null) ? tuple._1() : 0;
            int nb = (tuple != null) ? tuple._2() : 0;
            System.out.printf(
                "i = %d, center = (%.6f,%.6f), NA%d = %d, NB%d = %d\n",
                i, center.apply(0), center.apply(1), i, na, i, nb
            );
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

        SparkConf conf = new SparkConf(true).setAppName("G23HW1").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        try {
            // Read input file into RDD
            JavaPairRDD<Vector, String> inputPoints = sc.textFile(inputFilePath, L)
                    .map(line -> line.split(","))
                    .filter(tokens -> tokens.length == 3)
                    .mapToPair(tokens -> {
                        Vector point = Vectors.dense(
                                Double.parseDouble(tokens[0]),
                                Double.parseDouble(tokens[1]));
                        String group = tokens[2].trim();
                        return new Tuple2<>(point, group);
                    });

            inputPoints.cache();

            // Count total points and points in each demographic group
            long N = inputPoints.count();
            long NA = inputPoints.filter(tuple -> tuple._2().equals("A")).count();
            long NB = inputPoints.filter(tuple -> tuple._2().equals("B")).count();

            System.out.printf("Input file: %s, L = %d, K = %d, M = %d\n", inputFilePath, L, K, M);
            System.out.printf("N = %d, NA = %d, NB = %d\n", N, NA, NB);

            // Run K-means clustering
            KMeansModel kmeansModel = KMeans.train(
                    inputPoints.keys().rdd(),
                    K, // Number of clusters
                    M, // Number of iterations
                    "k-means||",
                    123);

            // Extract cluster centers
            ArrayList<Vector> centers = new ArrayList<>(Arrays.asList(kmeansModel.clusterCenters()));

            double standardObjective = MRComputeStandardObjective(inputPoints, centers);
            double fairObjective = MRComputeFairObjective(inputPoints, centers);

            System.out.printf("Delta(U,C) = %.6f\n", standardObjective);
            System.out.printf("Phi(A,B,C) = %.6f\n", fairObjective);

            MRPrintStatistics(inputPoints, centers);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sc.close();
        }
    }
}
