package nexmark.sources.LoadPattern;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Decreasing load pattern class.
 * Implementation is based on the following Python code:
 *   if query == "query-1":
 *       val = 240000
 *   elif query == "query-3":
 *       val = 80000
 *   elif query == "query-11":
 *       val = 150000
 *   initial_val = val
 *   values = []
 *   indices = []
 *   val += 2000
 *   for i in range(0, time):
 *       val += random.randrange(int(-initial_val * (1 / 21)), int(initial_val * (1/28)))
 *       values.append(val)
 *       indices.append(i)
 *   values = [int(val) for val in values]
 *   values = [-1*val if val < 0 else val for val in values]
 *   return indices, values
 */
public class DecreaseLoadPattern extends LoadPattern {
    int query;
    int initialInputRate;

    /**
     * Constructor for DecreaseLoadPattern setting values of class based on provided query.
     * @param query Query to produce load pattern for.
     */
    public DecreaseLoadPattern(int query, int loadPatternPeriod) {
        super(query, loadPatternPeriod);
        this.setDefaultValues();
    }

    public DecreaseLoadPattern(int query, int loadPatternPeriod, int initialInputRate) {
        super(query, loadPatternPeriod);
        this.initialInputRate = initialInputRate;
    }

    /**
     * Set the default values based on this.query.
     */
    @Override
    public void setDefaultValues() {
        switch (this.getQuery()) {
            case 1:
                this.initialInputRate = 240000;
                break;
            case 3:
                this.initialInputRate = 80000;
                break;
            case 11:
                this.initialInputRate = 150000;
                break;
            default:
                System.out.println("Error: query " + this.query + " not recognized.");
        }
    }

    @Override
    public String getLoadPatternTitle() {
        return "Decrease pattern ("+ this.getSeed() + ")\n" +
                "Query " + this.getQuery() +
                " - Start Value " + this.initialInputRate;
    }

    /**
     * Generate Decrease pattern
     * @return Tuple with a list of indices and a list of indexes.
     */
    @Override
    public Tuple2<List<Integer>, List<Integer>> getLoadPattern() {
        Random random = this.getRandomClass();

        List<Integer> values = new ArrayList<>();
        List<Integer> indices = new ArrayList<>();

        int value = this.initialInputRate + 2000;
        for (int i = 0; i < this.getLoadPatternPeriod(); i++) {
            int minRange = -1 * this.initialInputRate / 21;
            int maxRange = this.initialInputRate / 28;
            value += random.nextDouble() * (maxRange - minRange) + minRange;
            value = Math.max(0, value);
            values.add(value);
            indices.add(i);
        }
        return new Tuple2<>(indices, values);
    }
}
