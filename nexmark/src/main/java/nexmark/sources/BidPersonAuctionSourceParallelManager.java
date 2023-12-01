package nexmark.sources;

import java.util.ArrayList;
import java.util.List;

/**
 * BidPersonAuctionSourceParallelManager manages the execution of multiple BidPersonAuctionSourceParallelFunctions
 * running in parallel.
 */
public class BidPersonAuctionSourceParallelManager {

    int parallelism;
    long epochDurationMs;
    List<BidPersonAuctionSourceParallelFunction> sourceFunctions;

    /**
     * Constructor of the BidPersonAuctionSourceParallelManager
     * @param kafkaServer String providing the location (url) of the kafka server.
     * @param epochDurationMs Amount of ms an epoch takes.
     * @param enablePersonTopic Whether to generate person records.
     * @param enableAuctionTopic Whether to generate auction records.
     * @param enableBidTopic Whether to generate bid records.
     * @param parallelism The total amount of BidPersonAuctionSource functions.
     * @param uniBidsPartitions The partitions of the universalis Bids source operator
     * @param uniAuctionsPartitions The partitions of the universalis Auctions source operator
     * @param uniPersonsPartitions The partitions of the universalis Persons source operator
     */
    public BidPersonAuctionSourceParallelManager(String kafkaServer,
                                                 long epochDurationMs,
                                                 boolean enablePersonTopic,
                                                 boolean enableAuctionTopic,
                                                 boolean enableBidTopic,
                                                 int parallelism,
                                                 int uniBidsPartitions,
                                                 int uniAuctionsPartitions,
                                                 int uniPersonsPartitions,
                                                 double skew) {
        this.parallelism = parallelism;
        this.epochDurationMs = epochDurationMs;
        this.sourceFunctions = new ArrayList<>();
        for (int i = 0; i < this.parallelism; i++) {
            this.sourceFunctions.add(new BidPersonAuctionSourceParallelFunction(kafkaServer, epochDurationMs,
                    enablePersonTopic, enableAuctionTopic, enableBidTopic, parallelism, i,
                    uniBidsPartitions, uniAuctionsPartitions, uniPersonsPartitions, skew));
        }
    }

    /**
     * Get the amount of records that should be generated per epoch.
     * @param totalRatePerSecond The amount of records generated per second.
     * @return Amount of records generated per epoch.
     */
    public int getRatePerEpoch(int totalRatePerSecond) {
        double epochsPerSecond = 1000d / this.epochDurationMs;
        return (int) Math.ceil(totalRatePerSecond / epochsPerSecond);
    }

    /**
     * Get the total amount of epochs that are run in the provided period.
     * @param periodDurationMs Period in which to run a certain amount of epochs.
     * @return Amount of epochs that can be run in the provided period.
     */
    public int getAmountOfEpochs(int periodDurationMs) {
        return (int) Math.ceil(periodDurationMs / (double) this.epochDurationMs);
    }

    /**
     * Run the generators for a specific period of time, provided the time to run by and the rate per second to generate
     * records by.
     * @param totalRatePerSecond Rate of records to produce per second.
     * @param periodDurationMs Amount of ms a period takes.
     * @throws InterruptedException Exception thrown when thread is interrupted.
     */
    public void runGeneratorsForPeriod(int totalRatePerSecond, int periodDurationMs) throws InterruptedException {
        int ratePerEpoch = this.getRatePerEpoch(totalRatePerSecond);
        int amountOfEpochs = this.getAmountOfEpochs(periodDurationMs);
        for (BidPersonAuctionSourceParallelFunction sourceFunction: this.sourceFunctions) {
            sourceFunction.startNewThread(ratePerEpoch, amountOfEpochs);
        }
        Thread.sleep(periodDurationMs);
        for (BidPersonAuctionSourceParallelFunction sourceFunction: this.sourceFunctions) {
            sourceFunction.stopThread();
        }
    }
}
