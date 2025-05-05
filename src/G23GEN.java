import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;

public class G23GEN {

    private static final boolean DEBUG = false;
    private static final int RANDOM_SEED = 123;
    private static final double GROUP_A_PROPORTION = 0.3;       // percentage of points in group A
    private static final double GROUP_A_STD = 5.0;              // SD for group A
    private static final double GROUP_B_STD = 15.0;             // SD for group B
    private static final double OFFSET_B = 15.0;                // offset for group B (creates unfairness)
    private static final double CLUSTER_SPACING = 40.0;         // Distance between cluster centers
    private static final String CSV_OUTPUT_PATH = "./data/generated_points.csv";

    /**
     * Generates cluster centers in a grid-like pattern
     */
    private static double[][] generateClusterCenters(int K, Random rand) {
        double[][] centers = new double[K][2];
        int gridSize = (int) Math.ceil(Math.sqrt(K));

        // Place centers in a grid-like pattern
        for (int i = 0; i < K; i++) {
            centers[i][0] = CLUSTER_SPACING * (i % gridSize) + 10 * rand.nextDouble();
            centers[i][1] = CLUSTER_SPACING * (i / gridSize) + 10 * rand.nextDouble();
        }

        return centers;
    }

    /**
     * Distributes points across clusters evenly
     */
    private static int[] distributePoints(int totalPoints, int numClusters) {
        int[] pointsPerCluster = new int[numClusters];
        int basePoints = totalPoints / numClusters;
        int remainder = totalPoints % numClusters;

        Arrays.fill(pointsPerCluster, basePoints);

        // Distribute remainder
        for (int i = 0; i < remainder; i++) {
            pointsPerCluster[i]++;
        }

        return pointsPerCluster;
    }

    /**
     * Generates and outputs points for both groups
     */
    private static void generatePoints(
            double[][] centers, int[] pointsPerClusterA, int[] pointsPerClusterB,
            Random rand) {
        class Point {
            double x;
            double y;
            char group;
            int clusterId;

            Point(double x, double y, char group, int clusterId) {
                this.x = x;
                this.y = y;
                this.group = group;
                this.clusterId = clusterId;
            }

            @Override
            public String toString() {
                if (DEBUG) {
                    return String.format(Locale.US, "%.3f,%.3f,%c,%d", x, y, group, clusterId);
                }
                else {
                    return String.format(Locale.US, "%.3f,%.3f,%c", x, y, group);
                }
            }
        }

        // Create a list to store all points
        List<Point> points = new ArrayList<>();

        for (int i = 0; i < centers.length; i++) {
            // Group A - tightly clustered
            for (int j = 0; j < pointsPerClusterA[i]; j++) {
                double x = centers[i][0] + rand.nextGaussian() * GROUP_A_STD;
                double y = centers[i][1] + rand.nextGaussian() * GROUP_A_STD;
                points.add(new Point(x, y, 'A', i));
            }

            // Group B - more spread out and offset
            for (int j = 0; j < pointsPerClusterB[i]; j++) {
                double x = centers[i][0] + OFFSET_B + rand.nextGaussian() * GROUP_B_STD;
                double y = centers[i][1] + OFFSET_B + rand.nextGaussian() * GROUP_B_STD;
                points.add(new Point(x, y, 'B', i));
            }
        }

        // Shuffle the points randomly
        Collections.shuffle(points, rand);

        // Output points to CSV file
        if (DEBUG) {
            try (FileWriter writer = new FileWriter(new File(CSV_OUTPUT_PATH))) {
                for (Point point : points) {
                    writer.write(point.toString() + "\n");
                }
            } catch (IOException e) {
                System.out.println("Error writing to file: " + e.getMessage());
            }
        }
        // Print points to console
        else {
            for (Point point : points) {
                System.out.println(point.toString());
            }
        }
    }

    public static void main(String[] args) {

        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: <N> <K>");
        }

        int N = Integer.parseInt(args[0]); // Total number of points
        int K = Integer.parseInt(args[1]); // Number of clusters

        // Ensure at least K points and K ≥ 2
        if (N < K || K < 2) {
            throw new IllegalArgumentException("N must be >= K and K must be >= 2");
        }

        try {
            Random rand = new Random(RANDOM_SEED);

            // Calculate points per group
            int pointsA = (int) (N * GROUP_A_PROPORTION);
            int pointsB = N - pointsA;

            // Generate K centers
            double[][] centers = generateClusterCenters(K, rand);

            // System.out.println("Cluster centers: " + Arrays.deepToString(centers));

            // Calculate number of points per cluster for each group
            int[] pointsPerClusterA = distributePoints(pointsA, K);
            int[] pointsPerClusterB = distributePoints(pointsB, K);

            // Generate and output points
            generatePoints(centers, pointsPerClusterA, pointsPerClusterB, rand);
        } catch (Exception e) {
            throw new RuntimeException("Error: " + e.getMessage(), e);
        }
    }
}
