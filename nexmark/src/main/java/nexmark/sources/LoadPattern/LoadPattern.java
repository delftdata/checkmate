package nexmark.sources.LoadPattern;
import nexmark.sources.LoadPattern.plotting.LoadPatternChart_AWT;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.Random;

/**
 * Super class for all LoadPatterns
 */
public abstract class LoadPattern {

    // Seed to generate load patterns from (default value = 1066)
    private int seed;
    // Period to run the experiment for in terms of minutes (default value = 140 minutes)
    private int loadPatternPeriod;
    // Query to generate the load pattern for.
    private int query;

    public LoadPattern(int query, int loadPatternPeriod) {
        // Default seed is 1066
        this.setSeed(1066);

        this.setLoadPatternPeriod(loadPatternPeriod);
        this.setQuery(query);
    }

    /**
     * Getter for Seed used for random number generation
     * @return seed.
     */
    public int getSeed() {
        return this.seed;
    }

    /**
     * Set the current seed to provided parameter
     * @param seed seed to set current seed to.
     */
    public void setSeed(int seed){
        this.seed = seed;
    }

    /**
     * Get the epriod for which the loadpattern should be generated
     * The provided LoadPatternPeriod is the amount of minutes to run the experiments from
     * It is by default set to 140 minutes
     * @return amount of minutes to run the experiments for
     */
    public int getLoadPatternPeriod() {
        return this.loadPatternPeriod;
    }

    /**
     * Set a new value as loadpatternperiod.
     * @param loadPatternPeriod Loadpattern period in minutes to run the experiments for.
     */
    public void setLoadPatternPeriod(int loadPatternPeriod) {
        this.loadPatternPeriod = loadPatternPeriod;
    }

    /**
     * Get the query to generate the load pattern for
     * @return Integer indicating the query {1, 3, 11}
     */
    public int getQuery(){
        return this.query;
    }

    /**
     * Set the query to generate a load pattern for. This value is used by initialization to set default values.
     * @param query Integer indicating the query {1, 3, 11}
     */
    public void setQuery(int query) {
        this.query = query;
    }

    /**
     * Get a fresh Random class to generate random numbers from using the current value of this.seed.
     * @return A Random class to generate random numbers from.
     */
    Random getRandomClass() {
        return new Random(this.getSeed());
    }

    /**
     * Generate a loadpattern given the provided values.
     * FUnction should be implemented by all types of loadpatterns.
     * @return A tuple with a list if indices and a list of values. Values with the same index in both list correspond
     * to each other.
     */
    public abstract Tuple2<List<Integer>, List<Integer>> getLoadPattern();

    /**
     * Function to be implemented in implementing classes.
     * The default values should be set using the current value of this.query.
     */
    public abstract void setDefaultValues();



    public abstract String getLoadPatternTitle();

    /**
     * Get the loadpattern of the current configuration and plot it.
     */
    public void plotLoadPattern() {
        Tuple2<List<Integer>, List<Integer>> loadPattern = this.getLoadPattern();
        List<Integer> indices = loadPattern.f0;
        List<Integer> values = loadPattern.f1;
        String title = this.getLoadPatternTitle();

        LoadPatternChart_AWT loadPatternChart = new LoadPatternChart_AWT(
                title, title,
                indices,
                values,
                "Input rate",
                "Input rate",
                "Time (0 - " + this.getLoadPatternPeriod() + " min)"
        );
        loadPatternChart.pack();
        loadPatternChart.setVisible(true);
    }



    public static void main( String[ ] args ) throws Exception {
//        RandomLoadPattern random_pattern = new RandomLoadPattern(11, 140, 1250000, -500000, 500000, 2500000);
//        RandomLoadPattern random_pattern = new RandomLoadPattern(11, 140, 1250000, -30000, 50000, 2500000);
//        IncreaseLoadPattern increase_pattern = new IncreaseLoadPattern(11, 140);
//        DecreaseLoadPattern decrease_pattern = new DecreaseLoadPattern(11, 140);
//
//        random_pattern.plotLoadPattern();
//        increase_pattern.plotLoadPattern();
//        decrease_pattern.plotLoadPattern();
        int start_seed = 1066;
        for (int i = 0; i < 10; i++) {
            int seed = start_seed + i;
            RandomLoadPattern pattern = new RandomLoadPattern(11, 140, 1250000, -500000, 500000, 2500000);
            pattern.setSeed(seed);
            pattern.plotLoadPattern();
        }
    }

}
