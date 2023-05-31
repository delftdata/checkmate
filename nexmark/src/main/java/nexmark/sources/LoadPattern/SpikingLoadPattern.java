package nexmark.sources.LoadPattern;

import java.util.Random;

public abstract class SpikingLoadPattern extends LoadPattern{

    /**
     * Spike up configuration
     *  spikeUpChance = the chance for an upspike to occur.
     *  spikeUpMaximumPeriod = the maximum amount of time an upspike can take place (between 1 and spikeUpPeriod)
     *  spikeUpMaximumInputRate = the minimum value to be added during an upspike event
     *  spikeUpMinimumInputRate = the maximum value to be added during an upspike event
     */
    double spikeUpChance;               // default = 0
    int spikeUpMaximumPeriod;           // default = 3
    int spikeUpMaximumInputRate;     // default = 2 * this.maximumDivergence
    int spikeUpMinimumInputRate;     // default = this.maximumDivergence

    /**
     * Spike down configuration
     *  spikeDownChance = the chance for a downspike to occur.
     *  spikeDownMaximumPeriod = the maximum amount of time a downspike can take place (between 1 and spikeDownPeriod)
     *  spikeDownMaximumInputRate = the minimum value to be subtracted during a downspike event
     *  spikeDownMinimumInputRate = the maximum value to be subtracted during a downspike event
     *  An additional check mekes sure Minimum < Maximum.
     */
    // Spike down configurations
    double spikeDownChance;                   // default = 0
    int spikeDownMaximumPeriod;               // default = 3
    int spikeDownMaximumInputRate;         // default = 2 * this.maximumDivergence
    int spikeDownMinimumInputRate;         // default = this.maximumDivergence

    /**
     * Constructor of spiking load pattern
     * @param query Query to be passed down to Load Pattern.
     * @param loadPatternPeriod LoadPatternPeriod to be passed down to LoadPattern
     */
    public SpikingLoadPattern(int query, int loadPatternPeriod) {
        super(query, loadPatternPeriod);
    }

    /**
     * Set default settings for spike-load, given the maximum allowed spike. The range for both up and down spikes will
     * be [maximumSpikeSize / 2, maximumSpikeSize]. Spiking is disabled by default and has a maximum period of 3.
     * @param maximumSpikeSize Indicator for how big the spikes may be. Spike range will be between [spikeDivergence and
     *                        2 * spikeDivergence]
     */
    public void setDefaultSpikeSettings(int maximumSpikeSize){
        this.setSpikeUpInputRateRange(maximumSpikeSize / 2, maximumSpikeSize);
        this.setSpikeUpChance(0);
        this.setSpikeUpMaximumPeriod(3);

        this.setSpikeDownInputRateRange(maximumSpikeSize / 2, maximumSpikeSize);
        this.setSpikeDownChance(0);
        this.setSpikeDownMaximumPeriod(3);
    }

    /**
     * Set the chance for an upspike event to occur in a range of (0, 1)
     * @param spikeUpChance Chance for a upspike event to occur
     */
    public void setSpikeUpChance(double spikeUpChance) {
        this.spikeUpChance = spikeUpChance;
    }

    /**
     * Set the maximum period a spike up event can occur for randomly selected from range (1, spikeUpMaximumPeriod)
     * @param spikeUpMaximumPeriod Maximum period a spike event can take occur for
     */
    public void setSpikeUpMaximumPeriod(int spikeUpMaximumPeriod) {
        this.spikeUpMaximumPeriod = spikeUpMaximumPeriod;
    }

    /**
     * Set SpikeInputRate Range with the decrease being between (spikeUpMinimumRate and spikeUpMaximumRate)
     * @param spikeUpMinimumRate Minimum value the input rate can decrease with
     * @param spikeUpMaximumRate Maximum value the input rate can decrease with
     * An additional check makes place to ensure spikeUpMinimum < spikeUpMaximum
     */
    public void setSpikeUpInputRateRange(int spikeUpMinimumRate, int spikeUpMaximumRate) {
        this.spikeUpMinimumInputRate = Math.min(spikeUpMinimumRate, spikeUpMaximumRate);
        this.spikeUpMaximumInputRate = Math.max(spikeUpMinimumRate, spikeUpMaximumRate);
    }

    /**
     * Set the chance for a downspike event to occur in a range of (0, 1)
     * @param spikeDownChance Chance for a downspike event to occur
     */
    public void setSpikeDownChance(double spikeDownChance) {
        this.spikeDownChance = spikeDownChance;
    }

    /**
     * Set the maximum period a spike event can occur for randomly selected from range (1, spikeDownMaximumPeriod)
     * @param spikeDownMaximumPeriod Maximum period a spike event can take occur for
     */
    public void setSpikeDownMaximumPeriod(int spikeDownMaximumPeriod) {
        this.spikeDownMaximumPeriod = spikeDownMaximumPeriod;
    }

    /**
     * Set SpikeInputRate Range with the decrease being between (spikeDownMinimumRate and spikeDownMaximumRate)
     * @param spikeDownMinimumRate Minimum value the input rate can decrease with
     * @param spikeDownMaximumRate Maximum value the input rate can decrease with
     * An additional check makes place to ensure spikeDownMinimum < spikeDownMaximum
     */
    public void setSpikeDownInputRateRange(int spikeDownMinimumRate, int spikeDownMaximumRate) {
        this.spikeDownMinimumInputRate = Math.min(spikeDownMinimumRate, spikeDownMaximumRate);
        this.spikeDownMaximumInputRate = Math.max(spikeDownMinimumRate, spikeDownMaximumRate);
    }

    ////////////////////////////


    // Counter to keep track of spiking rounds to occur
    int remainingSpikePeriods = 0;

    /**
     * Get the next load spike value. This is an iterator that determines the loadspike for the next round.
     * The load spike is calculated for both up-spikes and down-spikes. If no spike is occuring, it checks whether both
     * a up-spike and a down-spike can occur (both random chance). If both can occur, we pick one of them randomly.
     * Then, it is determined for how many rounds a spike takes place, which is a random value between zero and
     * setSpike(Up/Down)MaximumPeriod. Then for this iteration and the next remaining n-1 iterations, a value between
     * spike(Up/Down)MinimumInputRate and spike(Up/Down)MaximumInputRate is returned. If scaling down this value is
     * multiplied by -1.
     * @return Additional value of spike that should be added to load pattern in current round.
     */
    public int getNextSpikeLoad(Random random) {
        int nextSpikeLoad = 0;
        // If not spiking
        if (this.remainingSpikePeriods == 0) {
            // Check whether a spike-up event can occur
            boolean triggerUpSpike = random.nextDouble() < this.spikeUpChance;
            // Check whether a spike-down event can occur
            boolean triggerDownSpike = random.nextDouble() < this.spikeDownChance;
            // If both can occur, have a 50% chance for both of them
            if (triggerUpSpike && triggerDownSpike){
                triggerUpSpike = 0.5 > random.nextDouble();
                triggerDownSpike = !triggerUpSpike;
            }
            // if spike-up is triggered
            if (triggerUpSpike) {
                // Determine spike periods and save as positive value
                this.remainingSpikePeriods = 1 + (int) (random.nextDouble() * this.spikeUpMaximumPeriod);
            // if spike-down is triggered
            } else if (triggerDownSpike) {
                // Determine spike periods and save as positive value
                this.remainingSpikePeriods = -1 + -1 * (int) (random.nextDouble() * this.spikeDownMaximumPeriod);
            }
        // If currently a spike-up event is happening
        } else if (remainingSpikePeriods > 0) {
            // subtract one spike period
            this.remainingSpikePeriods -= 1;
            // set spike-load for this
            nextSpikeLoad = (int) (this.spikeUpMinimumInputRate + random.nextDouble() *
                    (this.spikeUpMaximumInputRate - this.spikeUpMinimumInputRate));

        // If currently a spike-down event is happening
         } else {
            // Increase spikeEventPeriod by one
            this.remainingSpikePeriods += 1;
            // Set next spike-load to random value between maximumInput and MinimumInput
            nextSpikeLoad = (int) (this.spikeDownMinimumInputRate + random.nextDouble() *
                    (this.spikeDownMaximumInputRate - this.spikeDownMinimumInputRate));
            nextSpikeLoad = nextSpikeLoad * -1;
        }
        return nextSpikeLoad;
    }

    /**
     * Get title of load pattern. This returns an overview of the spike configurations.
     * If spiking is disabled (no chance of one occurring) it returns "spiking disabled"
     * @return String containing the current spike configurations.
     */
    @Override
    public String getLoadPatternTitle() {
        if (this.spikeDownChance + this.spikeUpChance > 0) {
            return "Spike Up: " + this.spikeUpChance * 100 + "%, " + this.spikeUpMaximumPeriod +
                    ", " + this.spikeUpMinimumInputRate + ", " + this.spikeUpMaximumInputRate + "\n" +
                    "Spike Down: " + this.spikeDownChance * 100 + "%, " + this.spikeDownMaximumPeriod +
                    ", " + this.spikeDownMinimumInputRate + ", " + this.spikeDownMaximumInputRate;
        } else {
            return "Spiking Disabled";
        }
    }

}
