import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.chart.util.ShapeUtils;

import javax.swing.JFrame;
import java.awt.Color;
import java.awt.Shape;
import java.awt.geom.Ellipse2D;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class G23Visualize {

    private static final Color[] CLUSTER_COLORS = {
            Color.BLUE,
            Color.ORANGE,
            Color.GREEN,
            Color.RED,
            Color.MAGENTA,
            Color.PINK,
            Color.GRAY,
            Color.YELLOW,
            Color.CYAN,
            Color.DARK_GRAY,
            Color.LIGHT_GRAY
    };

    public static void main(String[] args) {
        try {
            
            if (args.length != 1) {
                throw new IllegalArgumentException("USAGE: <input_file_path>");
            }

            String inputFilePath = args[0];
            
            // Read data from CSV file and create dataset
            XYDataset dataset = createDataset(inputFilePath);

            // Create chart
            JFreeChart chart = ChartFactory.createScatterPlot(
                    "Points by Cluster and Demographic Group",
                    "x1",
                    "x2",
                    dataset,
                    PlotOrientation.VERTICAL,
                    true,
                    true,
                    false);

            // Customize chart appearance
            XYPlot plot = (XYPlot) chart.getPlot();
            plot.setBackgroundPaint(Color.WHITE);
            plot.setRangeGridlinePaint(Color.LIGHT_GRAY);
            plot.setDomainGridlinePaint(Color.LIGHT_GRAY);

            // Customize renderer for different shapes and colors
            XYItemRenderer renderer = plot.getRenderer();
            int seriesCount = dataset.getSeriesCount();

            // Set colors and shapes for each series
            for (int i = 0; i < seriesCount; i++) {
                String seriesKey = (String) dataset.getSeriesKey(i);
                int clusterId = Integer.parseInt(seriesKey.split("-")[0].trim());
                String group = seriesKey.split("-")[1].trim();

                // Use cluster color with different alpha for different groups
                Color baseColor = CLUSTER_COLORS[clusterId % CLUSTER_COLORS.length];
                Color color;
                Shape shape;

                if (group.equals("A")) {
                    color = baseColor;
                    shape = new Ellipse2D.Double(-4.0, -4.0, 8.0, 8.0);
                } else {
                    color = new Color(
                            baseColor.getRed(),
                            baseColor.getGreen(),
                            baseColor.getBlue(),
                            75); // More transparent
                    shape = ShapeUtils.createDiamond(6.0f);
                }

                renderer.setSeriesPaint(i, color);
                renderer.setSeriesShape(i, shape);
            }

            // Display the chart in a frame
            JFrame frame = new JFrame("Point Visualization by Cluster and Group");
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.add(new ChartPanel(chart));
            frame.setSize(900, 700);
            frame.setLocationRelativeTo(null);
            frame.setVisible(true);

        } catch (IOException e) {
            System.err.println("Error reading CSV file: " + e.getMessage());
        }
    }

    private static XYDataset createDataset(String inputFilePath) throws IOException {
        // Map to store series by cluster and group
        Map<String, XYSeries> seriesMap = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                try {
                    double x = Double.parseDouble(values[0]);
                    double y = Double.parseDouble(values[1]);

                    // Extract group and cluster
                    char group = values[values.length - 2].charAt(0); // Assuming format x,y,group,clusterId
                    int clusterId = Integer.parseInt(values[values.length - 1]);

                    // Create key for series map
                    String key = clusterId + "-" + group;

                    // Get or create series
                    XYSeries series = seriesMap.get(key);
                    if (series == null) {
                        series = new XYSeries(key);
                        seriesMap.put(key, series);
                    }

                    // Add point
                    series.add(x, y);
                } catch (Exception e) {
                    System.err.println("Error parsing line: " + line + " - " + e.getMessage());
                }
            }
        }

        // Create collection and add all series
        XYSeriesCollection dataset = new XYSeriesCollection();
        seriesMap.forEach((key, series) -> dataset.addSeries(series));

        return dataset;
    }
}