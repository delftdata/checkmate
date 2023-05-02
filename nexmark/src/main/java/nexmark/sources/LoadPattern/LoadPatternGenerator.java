package nexmark.sources.LoadPattern;

import org.apache.flink.api.java.utils.ParameterTool;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class LoadPatternGenerator {

    /**
     * This method uses the following parameters to be provided by the ParameterTool.
     * upspike-chance: DOUBLE
     *      Chance to have an upsike-chance event. Disabled (0.0 chance) by default.
     *          (Default = 0.0)
     * upspike-maximum-period: INT
     *      Maximum period in which an upspike event can happen. Default is set to 3.
     *          (Default = 3)
     * upspike-maximum-input-rate: INT
     *      Maximum increase in input rate during upspike event. Default is set by loadpattern type.
     *          (Default = Defined by implementing load pattern)
     * upspike-minimum-input-rate: INT
     *      Minimum increase in input rate during upspike event. Default is set by loadpattern type.
     *          (Default = upspike-maximum-input-rate / 2)
     * downspike-chance: DOUBLE
     *      Chance to have an downspike-chance event. Disabled (0.0 chance) by default.
     *          (Default = 0.0)
     * downspike-maximum-period: INT
     *      Maximum period that a downspike event takes. Default is set to 3.
     *          (Default = 3)
     * downspike-minimum-input-rate: INT
     *      Minimum decrease  in input rate during a downspike event. Default is set by loadpattern type.
     *          (Default = Defined by implementing load pattern)
     * downspike-maximum-input-rate: INT
     *      Maximum decrease in input rate during a downspike event. Default is set by loadpattern type.
     *          (Default = downspike-minimum-input-rate / 2
     * @param params ParameterTool to fetch spike configurations from.
     * @param loadPattern Load pattern to set the spike configurations in.
     */
    private static void parseSpikeConfigurations(ParameterTool params, SpikingLoadPattern loadPattern) {
        // get spikeUpChance
        double spikeUpChance = params.getDouble("upspike-chance", -1d);
        if (spikeUpChance != -1) { loadPattern.setSpikeUpChance(spikeUpChance); }

        // Set maximum scale-up period
        int spikeUpMaximumPeriod = params.getInt("upspike-maximum-period", -1);
        if (spikeUpMaximumPeriod != -1) { loadPattern.setSpikeUpMaximumPeriod(spikeUpMaximumPeriod); }

        // Set maximum scale-up spike. Maximum scale-up spike is required.
        // Minimum scale-up spike is half the maximum spike by default.
        int spikeUpMaximumInputRate = params.getInt("upspike-maximum-input-rate", -1);
        if (spikeUpMaximumInputRate != -1 ){
            // if minimuminputrate is not provided, set it it half the maximum input rate
            int spikeUpMinimumInputRate = params.getInt("upspike-minimum-input-rate", -1);
            spikeUpMinimumInputRate = spikeUpMinimumInputRate == -1 ? spikeUpMaximumInputRate / 2 : spikeUpMinimumInputRate;
            loadPattern.setSpikeUpInputRateRange(spikeUpMinimumInputRate, spikeUpMaximumInputRate);
        }

        // Get downspike-chance
        double spikeDownChance = params.getDouble("downspike-chance", -1d);
        if (spikeDownChance != -1) { loadPattern.setSpikeDownChance(spikeDownChance); }

        // Set maximum scale-down period
        int spikeDownMaximumPeriod = params.getInt("downspike-maximum-period", -1);
        if (spikeDownMaximumPeriod != -1) { loadPattern.setSpikeDownMaximumPeriod(spikeDownMaximumPeriod); }

        // Set maximum scale-down spike. Maximum scale-down spike is required.
        // Minimum is half the maximum if not provided.
        int spikeDownMaximumInputRate = params.getInt("downspike-maximum-input-rate", -1);
        int spikeDownMinimumInputRate = params.getInt("downspike-minimum-input-rate", -1);
        if (spikeDownMaximumInputRate != -1) {
            spikeDownMinimumInputRate = spikeDownMinimumInputRate == -1 ? spikeDownMaximumInputRate / 2 : spikeDownMinimumInputRate;
            loadPattern.setSpikeDownInputRateRange(spikeDownMinimumInputRate, spikeDownMaximumInputRate);
        }
    }

    /**
     * Custom parameters:
     *   cosine-period: INT
     *      Time in minutes in which the input rate performs one cosine pattern
     *   input-rate-mean: INT
     *      Mean input-rate
     *   input-rate-maximum-divergence: INT
     *      Amount of events the pattern diverges from the mean value
     *   max-noise: INT
     *      Amount of noise introduce
     * @param query Query to generate the pattern for
     * @param experimentLength length of the experiment
     * @param useDefaultConfigurations Whether to use the default configurations.
     * @param params ParameterTool to fetch remaining parameters from.
     * @return A loadPattern following the provided cosine pattern.
     */
    static LoadPattern getCosineLoadPattern(int query, int experimentLength, boolean useDefaultConfigurations,
                                            ParameterTool params) {

        CosineLoadPattern loadPattern;
        if (useDefaultConfigurations) {
            loadPattern =  new CosineLoadPattern(query, experimentLength);
            int maxNoise = params.getInt("max-noise", -1);
            if (maxNoise != -1){ loadPattern.setMaxNoise(maxNoise); }
        } else {
            int cosinePeriod = params.getInt("cosine-period");
            int inputRateMaximumDivergence = params.getInt("input-rate-maximum-divergence");
            int inputRateMean = params.getInt("input-rate-mean");
            int maxNoise = params.getInt("max-noise");
            loadPattern = new CosineLoadPattern(query, experimentLength, cosinePeriod, inputRateMaximumDivergence, inputRateMean, maxNoise);
        }

        // Cosinus is a supports spikes (is SpikingLoadPattern
        LoadPatternGenerator.parseSpikeConfigurations(params, loadPattern);

        return loadPattern;
    }

    /**
     * Custom parameters:
     *      initial-round-length: INT
     *          Length of the initial round
     *      regular-round-length: INT
     *          Length of the regular round
     *      round-rates: LIST[INT], as comma seperated line: 1, 2, 3, 4
     *          List of rates that should be generated after the provided periods of time.
     *      max-noise: INT
     *          Maximum amount of noise to be added to the input rate.
     * @param query Query to generate the pattern for
     * @param experimentLength length of the experiment
     * @param useDefaultConfigurations Whether to use the default configurations.
     * @param params ParameterTool to fetch remaining parameters from.
     * @return A loadPattern following the provided cosine pattern.
     */
    static LoadPattern getConvergenceLoadPattern(int query, int experimentLength, boolean useDefaultConfigurations,
                                                 ParameterTool params) {
        ConvergenceLoadPattern loadPattern;
        if (useDefaultConfigurations) {
            loadPattern = new ConvergenceLoadPattern(query, experimentLength);
        } else {
            int initialRoundLength = params.getInt("initial-round-length");
            int regularRoundLength = params.getInt("regular-round-length");
            List<Integer> roundRates = new ArrayList<Integer>();
            String roundRatesString = params.get("round-rates");
            for (String roundRateString: roundRatesString.split(",")) {
                roundRates.add(Integer.parseInt(roundRateString));
            }
            int maxNoise = params.getInt("max-noise");
            loadPattern = new ConvergenceLoadPattern(query, experimentLength, initialRoundLength, regularRoundLength,
                    roundRates, maxNoise);
        }
        LoadPatternGenerator.parseSpikeConfigurations(params, loadPattern);
        return loadPattern;
    }

    /**
     * Custom parameters:
     *   initial-input-rate: INT
     *      Input rate to start with
     *   min-divergence: INT
     *      Minimum increase (decrease if negative) per minute
     *   max-divergence: INT
     *      Maximum increase (decrease if negative) per minute
     * @param query Query to generate the pattern for
     * @param experimentLength length of the experiment
     * @param useDefaultConfigurations Whether to use the default configurations.
     * @param params ParameterTool to fetch remaining parameters from.
     * @return A loadPattern following the provided random pattern.
     */
    static LoadPattern getRandomLoadPattern(int query, int experimentLength, boolean useDefaultConfigurations,
                                            ParameterTool params) {

        if (useDefaultConfigurations) {
            return new RandomLoadPattern(query, experimentLength);
        } else {
            int initialInputRate = params.getInt("initial-input-rate");
            int minDivergence = params.getInt("min-divergence");
            int maxDivergence = params.getInt("max-divergence");
            int maxInputRate = params.getInt("max-input-rate", Integer.MAX_VALUE);
            return new RandomLoadPattern(query, experimentLength, initialInputRate, minDivergence, maxDivergence, maxInputRate);
        }
    }

    /**

     * Custom paramters:
     *   initial-input-rate: INT
     *      Input rate to start with
     *   total-rate-increase: INT
     *      Total increase in rate over a period of 140 minutes.
     * @param query Query to generate the pattern for
     * @param experimentLength length of the experiment
     * @param useDefaultConfigurations Whether to use the default configurations.
     * @param params ParameterTool to fetch remaining parameters from.
     * @return A loadPattern following the provided increase pattern.
     */
    static LoadPattern getIncreaseLoadPattern(int query, int experimentLength, boolean useDefaultConfigurations,
                                              ParameterTool params) {

        if (useDefaultConfigurations) {
            return new IncreaseLoadPattern(query, experimentLength);
        } else {
            int initialInputRate = params.getInt("initial-input-rate");
            int RateIncreaseOver140Minutes = params.getInt("total-rate-increase");
            return new IncreaseLoadPattern(query,experimentLength, initialInputRate, RateIncreaseOver140Minutes);
        }
    }

    /**
     * Custom paramters:
     *  initial-input-rate: INT
     *      Input rate to start with. From this input rate, it decreases over a period of 140 minutes to 0.
     * @param query Query to generate the pattern for
     * @param experimentLength length of the experiment
     * @param useDefaultConfigurations Whether to use the default configurations.
     * @param params ParameterTool to fetch remaining parameters from.
     * @return A loadPattern following the provided increase pattern.
     */
    static LoadPattern getDecreaseLoadPattern(int query, int experimentLength, boolean useDefaultConfigurations,
                                              ParameterTool params)  {
        if (useDefaultConfigurations) {
            return new DecreaseLoadPattern(query, experimentLength);
        } else {
            int initialInputRate = params.getInt("initial-input-rate");
            return new DecreaseLoadPattern(query, experimentLength, initialInputRate);
        }
    }

    /**
     * Get a testrun scale-up event
     * @return Load pattern including a single scale-up event.
     */
    static LoadPattern getTestRun_ScaleUp() {
        return new TestrunLoadPattern(true);
    }

    /**
     * Get a testrun scale-down event
     * @return Load pattern including a single scale-up event.
     */
    static LoadPattern getTestRun_ScaleDown() {
        return new TestrunLoadPattern(false);
    }

    /**
     * Get a testrun  event
     * @return Load pattern including two scaling events.
     */
    static LoadPattern getTestRun(int query, int experimentLength, boolean useDefaultConfigurations,
                                  ParameterTool params)  {
        /**
         * Custom paramters:
         * - inputrate0: int
         * - inputrate1: int
         * - inputrate2: int
         */
        if (useDefaultConfigurations) {
            // TestRun has already a default experimentLength defined in its class.
            // Changing its expeirmentLength can only be done via non-default configuration
            return new TestrunLoadPattern(query, experimentLength);
        } else {
            int inputrate0 = params.getInt("inputrate0");
            int inputrate1 = params.getInt("inputrate1");
            int inputrate2 = params.getInt("inputrate2");
            return new TestrunLoadPattern(query, experimentLength, inputrate0, inputrate1, inputrate2);
        }
    }

    /**
     * Get a loadpattern based on paramters provided by params
     * @param params parameters passed through args.
     * @return LoadPattern class based on parameters
     * @throws Exception Throw exception when invalid parameters are provided.
     */
    public static LoadPattern getLoadPatternFromParameterTool(ParameterTool params) throws Exception{

        // REQUIRED
        String loadPatternName = params.getRequired("load-pattern");
        System.out.println("Found load-pattern name: " + loadPatternName);
        int query = params.getInt("query", 1);
        System.out.println("Found query: " + query);

        // OPTIONAL
        boolean useDefaultConfiguration = params.getBoolean("use-default-configuration", true);
        int experimentLength = params.getInt("experiment-length", 140);
        int seed = params.getInt("use-seed", 1066);
        LoadPattern loadPattern = null;
        switch (loadPatternName) {
            case "cosine": {
                loadPattern = LoadPatternGenerator.getCosineLoadPattern(query, experimentLength, useDefaultConfiguration, params);
                break;
            }
            case "random": {
                loadPattern = LoadPatternGenerator.getRandomLoadPattern(query, experimentLength, useDefaultConfiguration, params);
                break;
            }
            case "increase": {
                loadPattern = LoadPatternGenerator.getIncreaseLoadPattern(query, experimentLength, useDefaultConfiguration, params);
                break;
            }
            case "decrease": {
                loadPattern = LoadPatternGenerator.getDecreaseLoadPattern(query, experimentLength, useDefaultConfiguration, params);
                break;
            }
            case "testrun-scaleup": {
                loadPattern = LoadPatternGenerator.getTestRun_ScaleUp();
                break;
            }
            case "testrun-scaledown": {
                loadPattern = LoadPatternGenerator.getTestRun_ScaleDown();
                break;
            }
            case "testrun": {
                loadPattern = LoadPatternGenerator.getTestRun(query, experimentLength, useDefaultConfiguration, params);
                break;
            }
            case "convergence": {
                loadPattern = LoadPatternGenerator.getConvergenceLoadPattern(query, experimentLength, useDefaultConfiguration, params);
                break;
            }
            default: {
                throw new ParseException("Loadpattern " + loadPatternName + " is not recognized.", 0);
            }
        }
        loadPattern.setSeed(seed);
        return loadPattern;
    }
}
