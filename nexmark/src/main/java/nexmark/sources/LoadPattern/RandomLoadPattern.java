package nexmark.sources.LoadPattern;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/***
 * RandomLoadPattern generates a RandomLoadPattern based on its provided configuration
 * It is based on the following Python code:
 *   if query == "query-1":
 *       val = 150000
 *   elif query == "query-3":
 *       val = 50000
 *   elif query == "query-11":
 *       val = 30000
 *   values = []
 *   indices = []
 *   for i in range(0, time):
 *       val += random.randrange(-10000, 10000)
 *       values.append(val)
 *       indices.append(i)
 *   values = [int(val) for val in values]
 *   values = [-1*val if val < 0 else val for val in values]
 *   return indices, values
 */
public class RandomLoadPattern extends LoadPattern {
    int startValue;
    int minDivergence;
    int maxDivergence;
    int maxInputRate;

    /**
     * Constructor using default values based on query input
     * @param query Query to generate loadpattern for
     */
    public RandomLoadPattern(int query, int loadPatternPeriod){
        super(query, loadPatternPeriod);
        this.setDefaultValues();
    }

    /**
     * Cosntructor using custom values based on query input
     * @param query Query to generate load pattern for. QUery is only used for graph description.
     * @param loadPatternPeriod Experiment length (minutes) to generate load for
     * @param startValue Value to start from for Random Load Pattern.
     */
    public RandomLoadPattern(
            int query,
            int loadPatternPeriod,
            int startValue,
            int minDivergence,
            int maxDivergence,
            int maxInputRate
    )
    {
        super(query, loadPatternPeriod);
        this.startValue = startValue;
        this.minDivergence = minDivergence;
        this.maxDivergence = maxDivergence;
        this.maxInputRate = maxInputRate;
    }

    /**
     * Set the configuration of the class to the default values based on this.query.
     */
    @Override
    public void setDefaultValues() {
        this.maxInputRate = Integer.MAX_VALUE;
        this.minDivergence = -10000;
        this.maxDivergence = 10000;
        switch (this.getQuery()) {
            case 1:
                this.startValue = 150000;
                break;
            case 3:
                this.startValue = 50000;
                break;
            case 11:
                this.startValue = 30000;
                break;
            default:
                System.out.println("Error: query " + this.getQuery() + " not recognized.");
        }
    }

    @Override
    public String getLoadPatternTitle() {
        return "Random pattern ("+ this.getSeed() + ")\n" +
                "Query " + this.getQuery() +
                " - Start Value " + this.startValue +
                " - Step Range ( " + this.minDivergence + ", " + this.maxDivergence + ")\n" +
                "Maximum Input-rate " + this.maxInputRate;
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

        int value = startValue;
        for (int i = 0; i < this.getLoadPatternPeriod(); i++) {
            // Ensure first value is the startValue
            if (i > 0) {
                value += random.nextDouble() * (this.maxDivergence - this.minDivergence) + minDivergence;
            }
            value = Math.min(this.maxInputRate, value);
            value = Math.max(0, value);
            values.add(value);
            indices.add(i);
        }
        return new Tuple2<>(indices, values);
    }


}
