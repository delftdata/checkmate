package nexmark.sources.LoadPattern;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Testrun Load pattern
 * This class has three constructors:
 * 1 creating a simple scale-up or scale-down load pattern
 * 1 creating a combination of the scale-up and scale-down pattern
 * 1 for creating a custom loadpattern
 */
public class TestrunLoadPattern extends LoadPattern {

    public int scenario = -1;
    public int inputrate0;
    public int inputrate1;
    public int inputrate2;

    /**
     * Fast testrun with scaleUp or scaleDown functionality.
     * This is used for fast testing of scalign operations
     * @param scaleUp Boolean indicating whether the pattern sh scale up or down
     */
    public TestrunLoadPattern(boolean scaleUp) {
        super(0, 10);
        if (scaleUp){
            // default scale up pattern
            this.inputrate0 = 100000;
            this.inputrate1 = 200000;
            this.inputrate2 = -1;
        } else {
            // default scale down pattern
            this.inputrate0 = 200000;
            this.inputrate1 = 100000;
            this.inputrate2 = -1;
        }
    }

    /**
     * Constructor using default values (scale-up and scale-down combined)
     * @param query Query corresponing to the test-run. Can be ignored for this loadpattern.
     * @param loadPatternPeriod Length of the experiments in minutes
     */
    public TestrunLoadPattern(int query, int loadPatternPeriod) {
        super (query, loadPatternPeriod);
        this.setDefaultValues();
    }

    /**
     * Custom constructur of load pattern
     * @param query Query corresponing to the test-run. Can be ignored for this loadpattern.
     * @param loadPatternPeriod Length of the experiments in minutes
     * @param inputRate0 First input rate. Is ignored when set < 0.
     * @param inputRate1 Second input rate. is ignored when set < 0.
     * @param inputRate2 Third input rate. Is ignored when set <0.
     * Setting all three parameters < 0 will result in Illegal Argument exception
     */
    public TestrunLoadPattern(int query, int loadPatternPeriod, int inputRate0, int inputRate1, int inputRate2) {
        super(query, loadPatternPeriod);
        this.inputrate0 = inputRate0;
        this.inputrate1 = inputRate1;
        this.inputrate2 = inputRate2;
    }

    /**
     * Get a Tuple2 with a list in indices and a list of values.
     * Load pattern is based on currently set this.inputrate0, this.inputrate1, this.inputrate2
     * The total time of the experiments is divided equally between the three input rates, chainging value to the corresponding one.
     * If a value is < 0, it is ignored and its time is divided equally between the other input rates.
     * Setting all three inputrates to -1 results in an IlligalArgumentException.
     * @return Tuple2 with a list of indices and a list of values. Both of size this.getLoadPatternPeriod()
     */
    @Override
    public Tuple2<List<Integer>, List<Integer>> getLoadPattern() {

        int periods = (this.inputrate0 >= 0 ? 1: 0) + (this.inputrate1 >= 0 ? 1: 0) + (this.inputrate2 >= 0 ? 1: 0);

        if (periods <= 0) {
            throw new IllegalArgumentException("Error: all provided periods are not valid.");
        }

        List<Integer> indices = new ArrayList<>();
        List<Integer> values = new ArrayList<>();
        int periodSize = Math.floorDiv(this.getLoadPatternPeriod(), periods);
        int periodRemainer = this.getLoadPatternPeriod() % periods;

        int indice = 0;
        if (this.inputrate0 >= 0) {
            for (int i = 0; i < periodSize; i++) {
                values.add(this.inputrate0);
                indices.add(indice);
                indice++;
            }
        }
        if (this.inputrate1 >= 0) {
            for (int i = 0; i < periodSize; i++) {
                values.add(this.inputrate1);
                indices.add(indice);
                indice++;
            }
        }
        if (this.inputrate2 >= 0) {
            for (int i = 0; i < periodSize + periodRemainer; i++) {
                values.add(this.inputrate2);
                indices.add(indice);
                indice++;
            }
        }
        return new Tuple2<>(indices, values);
    }

    /**
     * Set default values of load pattern
     */
    @Override
    public void setDefaultValues() {
        this.inputrate0 = 100000;
        this.inputrate1 = 200000;
        this.inputrate2 = 120000;
    }

    /**
     * Get the title of the load pattern.
     * @return String indicating the load patterns configuration.
     */
    @Override
    public String getLoadPatternTitle() {
        return "Testrun pattern \n" +
                "Inputrates: " + this.inputrate0 + ", " + this.inputrate1 + ", " + this.inputrate2;
    }
}
