package nexmark.sources.LoadPattern.plotting;

import java.awt.*;

import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.renderer.category.LineAndShapeRenderer;
import org.jfree.chart.ui.ApplicationFrame;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;


import java.util.List;

/***
 * LoadPatternChart_AWT is used by the LoadPatternGenerator to display loadpatterns.
 * As input it requires the necessary information for generating such a graph.
 * It only shows the graph in a new window
 */
public class LoadPatternChart_AWT extends ApplicationFrame {

    /**
     * Constructor to plot a chart
     * @param applicationTitle Title of the application (name of process)
     * @param chartTitle Title of the chart
     * @param xData List of Integers being the xData
     * @param yData List of Integers being the values
     * @param xUnit The name of the XName unit ("minute" for example)
     * @param xAxisLabel Label to be displayed on the xAxis
     * @param yAxisLabel Label to be displayed on the yAxis
     */
    public LoadPatternChart_AWT(
            String applicationTitle,
            String chartTitle,
            List<Integer> xData,
            List<Integer> yData,
            String xUnit,
            String xAxisLabel,
            String yAxisLabel
    )

    {
        super(applicationTitle);

        // Create chart
        JFreeChart lineChart = ChartFactory.createLineChart(
                chartTitle,
                yAxisLabel,xAxisLabel,
                createDataset(xData, xUnit, yData),
                PlotOrientation.VERTICAL,
                true, false, false
        );

        // Disable tick labels on x-axis. This stops the x-axis values to be displayed in the graph.
        CategoryPlot plot = lineChart.getCategoryPlot();
        CategoryAxis axis = plot.getDomainAxis();
        axis.setTickLabelsVisible(false);

        // Set background paint
        lineChart.getPlot().setBackgroundPaint(Color.white);

        // Configure line layout
        LineAndShapeRenderer renderer = (LineAndShapeRenderer) plot.getRenderer();
        // Set line width
        renderer.setAutoPopulateSeriesStroke(false);
        renderer.setDefaultStroke(new BasicStroke(2.0f));
        // Set line color
        renderer.setAutoPopulateSeriesPaint(false);
        renderer.setDefaultPaint(Color.BLUE);

        // Display chart
        ChartPanel chartPanel = new ChartPanel( lineChart );
        chartPanel.setPreferredSize( new java.awt.Dimension( 1000 , 500 ) );

        setContentPane( chartPanel );
    }

    /***
     * Create a dataset that can be read by JFreeChart
     * @param xData List of integers as Xdata
     * @param xUnit Unit of the x-values ("Minute" for example)
     * @param yData List of integers as yData
     * @return a DefaultCatagoryDataset that can be read by JFreeChart
     * The xData and yData corresponding to each other should have the same index in both lists.
     */
    private DefaultCategoryDataset createDataset(List<Integer> xData, String xUnit, List<Integer> yData) {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset( );
        for (int i = 0; i < Math.min(xData.size(), yData.size()); i++) {
            dataset.addValue(yData.get(i), xUnit, xData.get(i));
        }
        return dataset;
    }
}