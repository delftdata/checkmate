package nexmark.sources.LoadPattern;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * LoadPattern following a cosine distribution.
 */
public class CosineLoadPattern extends SpikingLoadPattern {

    int meanInputRate;
    int maximumDivergence;
    int cosinePeriod;
    int maxNoise;

    /**
     * Generate Cosinus Load pattern using default values dependign on query.
     * @param query query to generate default values from.
     */
    public CosineLoadPattern(int query, int loadPatternPeriod) {
        super(query, loadPatternPeriod);
        this.setDefaultValues();
    }


    /**
     * Generate cosine load pattern using custom values.
     * @param query Query to generate load pattern from. Only used for graph description.
     * @param loadPatternPeriod Experiment length (minutes) to generate load for
     * @param cosinePeriod Time a full cosinus period takes (minutes)
     * @param maximumDivergence Maximum input rate to diverge from mean
     * @param meanInputRate Average input rate
     */
    public CosineLoadPattern(int query, int loadPatternPeriod, int cosinePeriod, int maximumDivergence, int meanInputRate, int maxNoise) {
        super(query, loadPatternPeriod);
        this.cosinePeriod = cosinePeriod;
        this.meanInputRate = meanInputRate;
        this.maximumDivergence = maximumDivergence;
        this.setMaxNoise(maxNoise);
        this.setDefaultSpikeSettings(this.maximumDivergence);
    }

    /**
     * Setting for maxNoise
     * @param maxNoise Maximum noise that can be introduced in query
     */
    public void setMaxNoise(int maxNoise){
        this.maxNoise = maxNoise;
    }

    /**
     * Set the default values based on this.query.
     */
    @Override
    public void setDefaultValues() {
        // Spike settings are set in value constructors
        this.cosinePeriod = 60;
        this.maxNoise = 10000;
        switch (this.getQuery()) {
            case 1:
                this.meanInputRate = 150000;
                this.maximumDivergence = 100000;
                break;
            case 3:
                this.meanInputRate = 50000;
                this.maximumDivergence = 25000;
                break;
            case 11:
                this.meanInputRate = 30000;
                this.maximumDivergence = 15000;
                break;
            default:
                System.out.println("Error: query " + this.getQuery() + " not recognized.");
        }
        this.setDefaultSpikeSettings(this.maximumDivergence);
    }

    @Override
    public String getLoadPatternTitle() {
        return "Cosine pattern ("+ this.getSeed() + ")\n" +
                "Query " + this.getQuery() +
                " - Period " + this.cosinePeriod +
                " - Mean " + this.meanInputRate +
                " - Div " + this.maximumDivergence +
                " - Noise " + this.maxNoise +  "\n" +
                super.getLoadPatternTitle();
    }

    /**
     * Generate Cosinus pattern
     * @return Tuple with a list of indices and a list of indexes.
     */
    @Override
    public Tuple2<List<Integer>, List<Integer>> getLoadPattern() {
        Random random = this.getRandomClass();
        List<Integer> values = new ArrayList<>();
        List<Integer> indices = new ArrayList<>();

        for (int i = 0; i < this.getLoadPatternPeriod(); i++) {
            double period = (2 * Math.PI / this.cosinePeriod);
            double value = this.meanInputRate + this.maximumDivergence * Math.cos(period * i);

            value += random.nextDouble() * (2 * this.maxNoise) - this.maxNoise;

            // Add spike load (function returns 0 when disabled)
            int nextSpikeLoad = this.getNextSpikeLoad(random);
            value += nextSpikeLoad;

            // Ensure value cannot drop lower than 0.
            value = Math.max(0, value);

            // Save values in values list and indicce in indices list.
            values.add((int) value);
            indices.add(i);
        }
        return new Tuple2<>(indices, values);
    }
}