import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;


public class G23HW1 {

    public static double computeTotalDistance(ArrayList<Vector> points, ArrayList<Vector> centers) {
        /*
            Computes the total distance from all points to their nearest centers
        */
        double totalDistance = 0d;
        for (Vector point : points) {
            double minDistance = Double.MAX_VALUE;
            for (Vector center : centers) {
            double distance = Vectors.sqdist(point, center);
            if (distance < minDistance) {
                minDistance = distance;
            }
            }
            totalDistance += minDistance;
        }
        return totalDistance;
    }

    public static double MRComputeStandardObjective(ArrayList<Vector> pointsA, ArrayList<Vector> pointsB, ArrayList<Vector> centers) {
        /*
            Computes the total distance from all points to their nearest centers
            for both sets of points A and B, and returns the sum
        */
        ArrayList<Vector> points = new ArrayList<>(pointsA);
        points.addAll(pointsB);
        
        double totalDistance = computeTotalDistance(points, centers);

        return totalDistance;
    }

    public static double MRComputeFairObjective(ArrayList<Vector> pointsA, ArrayList<Vector> pointsB, ArrayList<Vector> centers) {
        /*
            Computes the average distance from all points to their nearest centers
            for both sets of points A and B, and returns the maximum of the two averages
        */
        double totalDistanceA = computeTotalDistance(pointsA, centers);
        double totalDistanceB = computeTotalDistance(pointsB, centers);

        double averageDistanceA = totalDistanceA / pointsA.size();
        double averageDistanceB = totalDistanceB / pointsB.size();

        return Math.max(averageDistanceA, averageDistanceB);
    }

    public static void MRPrintStatistics(ArrayList<Vector> points, ArrayList<Vector> centers) {
        /*
            Computes the triplets (ci,NAi,NBi), for 1≤i≤K=|C|, 
            where ci is the i-th centroid in C,
            NAi, NBi are the numbers of points of A and B, respectively, in the cluster Ui centered in ci
        */
    }

    public static void main(String[] args) {
        
        if (args.length != 4) {
            throw new IllegalArgumentException("USAGE: <input_file_path> <L> <K> <M>");
        }

        String inputFilePath = args[0];
        int L = Integer.parseInt(args[1]);
        int K = Integer.parseInt(args[2]);
        int M = Integer.parseInt(args[3]);

        // Read input file and separate points into two lists based on labels
        ArrayList<Vector> pointsA = new ArrayList<>();
        ArrayList<Vector> pointsB = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(",");
                if (tokens.length != 3) continue;
                Vector point = Vectors.dense(
                    Double.parseDouble(tokens[0]), 
                    Double.parseDouble(tokens[1])
                );
                String label = tokens[2].trim();
                if (label.equals("A")) {
                    pointsA.add(point);
                } else if (label.equals("B")) {
                    pointsB.add(point);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Shuffle points and select K random centers
        ArrayList<Vector> points = new ArrayList<>(pointsA);
        points.addAll(pointsB);
        Collections.shuffle(points);
        ArrayList<Vector> centers = new ArrayList<>(points.subList(0, K));

        System.out.println(MRComputeStandardObjective(pointsA, pointsB, centers));
        System.out.println(MRComputeFairObjective(pointsA, pointsB, centers));
    }
}
