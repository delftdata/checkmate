package nexmark.sources.LoadPattern;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

public class ConvergenceLoadPattern extends SpikingLoadPattern {
    List<Integer> roundRates;
    int initialRoundLength;
    int regularRoundLength;
    int maximumNoise;

    /**
     * Default constructor for load pattern. This uses all default variables.
     * @param query Query the load pattern is generated for.
     * @param loadPatternPeriod Total length of the load pattern.
     */
    public ConvergenceLoadPattern(int query, int loadPatternPeriod) {
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
    public ConvergenceLoadPattern(int query, int loadPatternPeriod, int initialRoundLength, int regularRoundLength, List<Integer> roundRates,
                                  int maximumNoise) {
        // set super class variables
        super(query, loadPatternPeriod);

        // Print warning if the expected time provided by initialRoundLength and regularRoundLength is not equal to
        // loadPatternPeriod.
        int roundsExpectedExperimentDuration = initialRoundLength + regularRoundLength * (roundRates.size() - 1);
        if (roundsExpectedExperimentDuration != loadPatternPeriod) {
            System.out.println("Warning: load-pattern-period " + loadPatternPeriod + " is not the same as the " +
                    "expected period determined by initial-round-length: " + initialRoundLength +
                    " and regular-round-length: " + regularRoundLength + " summing up to a total length of: "
                    + roundsExpectedExperimentDuration + ".");
        }

        // Set variables
        this.setInitialRoundLength(initialRoundLength);
        this.setRegularRoundLength(regularRoundLength);
        this.setRoundRates(roundRates);
        this.setMaximumNoise(maximumNoise);
        // Set spike settings with maximum value the maximum of the roundRates list.
        this.setDefaultSpikeSettings(Collections.max(roundRates));
    }

    /**
     * Getter for round rates.
     * @return List of round rates.
     */
    public List<Integer> getRoundRates() {
        return roundRates;
    }

    /**
     * Setter for roundRates.
     * @param roundRates RoundRates to set variable to..
     */
    public void setRoundRates(List<Integer> roundRates) {
        this.roundRates = roundRates;
    }

    /**
     * Getter for initial record length.
     * @return The initial record length.
     */
    public int getInitialRoundLength() {
        return initialRoundLength;
    }

    /**
     * Set initial round length.
     * @param initialRoundLength Initial round length to set variable to.
     */
    public void setInitialRoundLength(int initialRoundLength) {
        this.initialRoundLength = initialRoundLength;
    }

    /**
     * Getter for regularRoundLength.
     * @return The value of regularRoundLength.
     */
    public int getRegularRoundLength() {
        return regularRoundLength;
    }

    /**
     * Setter for regularRoundLength.
     * @param regularRoundLength To set the value of regularRoundLength to.
     */
    public void setRegularRoundLength(int regularRoundLength) {
        this.regularRoundLength = regularRoundLength;
    }

    /**
     * Get the maximum amount of noise to be introduced in the pattern.
     * @return The maximum amount of noise to be introduced.
     */
    public int getMaximumNoise() {
        return maximumNoise;
    }

    /**
     * Set the maximum amount of noise to be introduced in the pattern.
     * @param maximumNoise Maximum amount of noise to be introduced in the pattern.
     */
    public void setMaximumNoise(int maximumNoise) {
        this.maximumNoise = maximumNoise;
    }


    /**
     * Set default values for the convergence load pattern.
     * This creates a rates of [0, 1000000, 2000000, 1000000].
     * Experiment time is divided in the following way: [1/7, 2/7, 2/7, 2/7]
     */
    @Override
    public void setDefaultValues() {
        int stepSize = 1000000;
        // Set initial round to 1/7 and the three regular round lengths to 2/7
        int timeUnit = this.getLoadPatternPeriod() / 7;
        this.setInitialRoundLength(timeUnit);
        this.setRegularRoundLength(timeUnit * 2);
        // Set round rates using parameter stepsize
        this.roundRates = Arrays.asList(0, stepSize, stepSize * 2, stepSize);
        // Set maximum noise
        this.maximumNoise = stepSize/10;
        // Set default spike settings.
        this.setDefaultSpikeSettings(stepSize * 2);
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
            int value = 0;

            // Get the index of the current round. Default is the initial round.
            int roundIndex = 0;
            // If we are not at the initial round
            if (i > this.getInitialRoundLength()) {
                // Get index of regular round
                roundIndex = 1 + (i - this.getInitialRoundLength()) / this.getRegularRoundLength();
            }
            // If there exists a corresponding load to the roundIndex, set the value to that load. Else we keep it 0.
            if (this.getRoundRates().size() > roundIndex) {
                value = this.getRoundRates().get(roundIndex);
            }

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

    /**
     * Get title of load pattern providing all settings of current load-pattern.
     * @return String representing current load-pattern configuration.
     */
    @Override
    public String getLoadPatternTitle() {
        return "Convergence pattern ("+ this.getSeed() + ")\n" +
                "Query " + this.getQuery() +
                " - length: " + this.getLoadPatternPeriod () +
                " (" + this.getInitialRoundLength() + ", " + this.getRegularRoundLength() + ") " +
                "- max-noise: " + this.getMaximumNoise() + "\n" +
                " Round-rates: " + this.getRoundRates().toString() + "\n" +
                super.getLoadPatternTitle();
    }
}
