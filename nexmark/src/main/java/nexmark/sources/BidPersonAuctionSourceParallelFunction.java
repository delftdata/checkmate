package nexmark.sources;

/**
 * BidPersonAuctionSourceParallelFunction is the parallel instantiation of BidPersonAuctionSourceFunction.
 * It creates the records that are expected of the instantiation of this.parallelismIndex.
 * All BidPersonAuctionSourceParallelFunction instantiations are managed by the BidPersonAuctionSourceParallelManager.
 */
public class BidPersonAuctionSourceParallelFunction extends BidPersonAuctionSourceFunction implements Runnable {
    // Parallelism status
    int parallelism;
    int parallelismIndex;

    // Thread status
    Thread thread;
    // private int stoppedIterationNumber is defined in BidPersonAuctionSourceFunction
    private int currentIterationNumber;

    // Epoch status
    private int amountOfEpochs;
    private int totalEventsPerEpoch;

    /**
     * Constructor of BidPersonAuctionSourceParallelFunction
     * @param kafkaServer String providing the location (url) of the kafka server.
     * @param epochDurationMs Amount of ms an epoch takes.
     * @param enablePersonTopic Whether to generate person records.
     * @param enableAuctionTopic Whether to generate auction records.
     * @param enableBidTopic Whether to generate bid records.
     * @param parallelism The total amount of BidPersonAuctionSource functions.
     * @param parallelismIndex The ID / index of the instantiation of the function. Should be between [0, parallelism-1]
     */
    public BidPersonAuctionSourceParallelFunction(
        String kafkaServer,
        long epochDurationMs, 
        boolean enablePersonTopic, 
        boolean enableAuctionTopic, 
        boolean enableBidTopic, 
        int parallelism, 
        int parallelismIndex,
        int uniBidsPartitions, 
        int uniAuctionsPartitions, 
        int uniPersonsPartitions,
        double skew) {

        super(kafkaServer, epochDurationMs, enablePersonTopic, enableAuctionTopic, enableBidTopic, uniBidsPartitions, uniAuctionsPartitions, uniPersonsPartitions, skew);
        this.parallelism = parallelism;
        this.parallelismIndex = parallelismIndex;

        this.currentIterationNumber = 0;
        this.stoppedIterationNumber = 0;

    }

    /**
     * Set epochStartTimeMs and EventCountSoFar to the provided variables.
     * This is used to synchronise the parallel source functions, so the timestamps and IDs generated remain consistent.
     * @param epochStartTimeMs Epoch start time.
     * @param eventsCountSoFar Events-count so far.
     */
    public void synchroniseSourceFunction(long epochStartTimeMs, long eventsCountSoFar) {
        this.epochStartTimeMs = epochStartTimeMs;
        this.eventsCountSoFar = eventsCountSoFar;
    }

    /**
     * Start a new thread. As preconditions, no thread should be running.
     * @param totalEventsPerEpoch Events that are generated in total by all generators combined per epoch.
     * @param amountOfEpochs Amount of epochs to generate events for
     */
    public void startNewThread(int totalEventsPerEpoch, int amountOfEpochs) {
        if (this.currentIterationNumber == this.stoppedIterationNumber) {
            this.currentIterationNumber += 1;
            int amountOfEpochsOffset = amountOfEpochs % this.parallelism;
            this.totalEventsPerEpoch = totalEventsPerEpoch + amountOfEpochsOffset;
            this.amountOfEpochs = amountOfEpochs;

            this.thread = new Thread(this);
            this.thread.start();
        } else {
            System.out.println("Error: attempting to start generator[ " + this.parallelismIndex + "] while it is already" +
                    " running");
        }
    }

    /**
     * Stop the currently running thread. Stopping is done by interrupting the thread.
     * As precondition, a thread should be running.
     */
    public void stopThread() {
        if (this.currentIterationNumber != this.stoppedIterationNumber) {
            this.stoppedIterationNumber = this.currentIterationNumber;
            this.thread.interrupt();
            System.out.println("Shutting down generator[" + this.parallelismIndex + "]");
        } else {
            System.out.println("Warning: attempting to shut down generator[" + this.parallelismIndex + "] while it " +
                    "was already shutting down.");
        }
    }

    /**
     * Get the amount of events every generator has to generate.
     * @return The amoutn every generator has to generate.
     */
    int getEpochEventChunkSize(int totalEvents) {
        return (int) Math.ceil( totalEvents / (double) this.parallelism);
    }

    /**
     * Get the index of the first event the generator should generate.
     * @return The index of the first event the generator should generate
     */
    int getFirstEventIndex(int totalEvents) {
        int epochChunkSize = this.getEpochEventChunkSize(totalEvents);
        return epochChunkSize * this.parallelismIndex;
    }

    /**
     * Run function of the source generator.
     * It runs for this.amountOfEpochs * this.epochDurationMs seconds.
     * This time might increase when the load per epoch takes longer to generate than the epoch.
     * The function determines the amount of events it has to generate (total / parallelism) and the from which
     * event it should start generating ((total / parallelism) * index).
     */
    public void run() {
        int iteration = this.currentIterationNumber;
        System.out.println("Starting iteration " + iteration + " on generator[" + this.parallelismIndex + "].");
        for (int i = 0; (i < this.amountOfEpochs && iteration > this.stoppedIterationNumber); i++) {
            long epochStartTime = System.currentTimeMillis();
            int eventChunkSize = this.getEpochEventChunkSize(this.totalEventsPerEpoch);
            int firstEventIndex = this.getFirstEventIndex(this.totalEventsPerEpoch);
            try {
                this.generatePortionOfEpochEvents(
                        this.totalEventsPerEpoch,
                        firstEventIndex,
                        eventChunkSize,
                        iteration);
            }  catch (Exception e){
                e.printStackTrace();
            }
            long emitTime = System.currentTimeMillis() - epochStartTime;
            if (emitTime < this.epochDurationMs) {
                try {
                    Thread.sleep(this.epochDurationMs - emitTime);
                } catch (InterruptedException e) {
                    System.out.println("Generator[" + this.parallelismIndex + "] was interrupted. Stopping generator...");
                    return;
                }
            }
        }
        if (iteration > this.stoppedIterationNumber) {
            System.out.println("Generator[" + this.parallelismIndex + "] finished iteration " + iteration + ".");
        } else {
            System.out.println("Generator[" + this.parallelismIndex + "]'s iteration " + iteration + "was stopped.");
        }
    }
}
