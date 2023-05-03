package nexmark.sources.LoadPattern;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.Random;
import java.util.ArrayList;



public class StaticLoadPattern extends SpikingLoadPattern{
    
    int rate;
    int maximumNoise;

    /**
     * Default constructor for load pattern. This uses all default variables.
     * @param query Query the load pattern is generated for.
     * @param loadPatternPeriod Total length of the load pattern.
     */
    public StaticLoadPattern(int query, int loadPatternPeriod){
        super(query, loadPatternPeriod);
        this.setDefaultValues();
    }


    /**
     * Constructor of ConvergenceLoadPattern
     * @param query Query to generate the load for.
     * @param initialRoundLength Length of the initial round.
     * @param regularRoundLength Length of the remaining rounds
     * @param roundRates Rates to produce for every round.
     * @param maximumNoise Maximum amount of noise to introduce into pattern.
     */
    public StaticLoadPattern(int query, int loadPatternPeriod, int rate, int maximumNoise){
        
        //set super class variables
        super(query, loadPatternPeriod);

        // Set variables
        this.rate = rate;
        this.maximumNoise = maximumNoise;
        this.setDefaultSpikeSettings(this.rate * 2);
    }


        /**
     * Set default values for the static load pattern.
     * This creates a rates of [0, 1000000, 2000000, 1000000].
     * Experiment time is divided in the following way: [1/7, 2/7, 2/7, 2/7]
     */
    @Override
    public void setDefaultValues() {
        // Set rate
        this.rate = 100_000;
        // Set maximum noise
        this.maximumNoise = 0;
        // Set default spike settings.
        this.setDefaultSpikeSettings(this.rate * 2);
    }


    /**
     * Generate the convergence load pattern
     * @return LoadPattern containing the convergence load pattern.
     */
    @Override
    public Tuple2<List<Integer>, List<Integer>> getLoadPattern() {
        Random random = this.getRandomClass();
        List<Integer> values = new ArrayList<>();
        List<Integer> indices = new ArrayList<>();

        for (int i = 0; i < this.getLoadPatternPeriod(); i++) {
            
            int value = this.rate;

            // Add random noise
            value += random.nextDouble() * (2 * this.getMaximumNoise()) - this.getMaximumNoise();


            // Add spike load (function returns 0 when disabled)
            int nextSpikeLoad = this.getNextSpikeLoad(random);
            value += nextSpikeLoad;

            // Ensure value cannot drop lower than 0.
            value = Math.max(0, value);

            // Save values in values list and indice in indices list.
            values.add(value);
            indices.add(i);
        }
        return new Tuple2<>(indices, values);
    }


    private int getMaximumNoise() {
        return this.maximumNoise;
    }

}
