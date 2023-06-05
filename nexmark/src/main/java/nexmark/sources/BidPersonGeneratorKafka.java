/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nexmark.sources;

import nexmark.sources.LoadPattern.*;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.*;


/*
 * Required parameters
 *   Load pattern selection
 *     Required parameters:
 *          load-pattern: STRING
 *              Load pattern to use during experiment. Possible configurations:  {"cosine", "random", "decrease",
 *              "increase", "testrun", "testrun-scaleup", "testrun-scaledown", "convergence"}
 *
 *     Optional parameters
 *          query (1) : INT {1, 3, 11}
 *              Query to perform the experiments on (default = 1). Possible configurations: {1, 3, 11}
 *          use-default-configuration (true): BOOLEAN
 *              Use default configurations of load patterns (default = true)
 *          experiment-length (140): INT
 *              Total length of the experiment (default = 140 minutes).
 *              The convergence experiment overwrites this value with initial-round-length and regular-round-length.
 *          use-seed (1066): INT
 *              Seed to use for load pattern generation (default = 1066)
 *     Configuration specific parameters
 *          load-pattern = "cosine" && use-default-configuration = false
 *              cosine-period: INT
 *                  Time in minutes in which the input rate performs one cosine pattern
 *              input-rate-mean: INT
 *                  Mean input-rate
 *              input-rate-maximum-divergence: INT
 *                  Amount of events the pattern diverges from the mean value
 *              max-noise: INT
 *                  Amount of noise introduced
 *              Additional optional parameters
 *
 *          load-pattern = "random" && use-default-configuration = false
 *              initial-input-rate: INT
 *                  Input rate to start with
 *              min-divergence: INT
 *                  Minimum increase (decrease if negative) per minute
 *              max-divergence: INT
 *                  Maximum increase (decrease if negative) per minute

 *          load-pattern = "increase" && use-default-configuration = false
 *              initial-input-rate: INT
 *                  Input rate to start with
 *              total-rate-increase: INT
 *                  Total increase in rate over a period of 140 minutes.
 *
 *          load-pattern = "decrease" && use-default-configuration = false
 *              initial-input-rate: INT
 *                  Input rate to start with
 *
 *          load-pattern = "testrun" && use-default-configuration = false
 *              inputrate0: INT
 *                  Initial input rate
 *              inputrate1: INT
 *                  Input rate after first time-period ends
 *              inputrate2: INT
 *                  Input rate after second time-period ends
 *
 *          load-pattern = "convergence" && use-default-configuration = false

 *              initial-round-length: INT
 *                   Length of the initial round.
 *              regular-round-length: INT
 *                  Length of the regular round
 *              round-rates: LIST[INT], as comma seperated line: 1, 2, 3, 4
 *                  List of rates that should be generated after the provided periods of time.
 *              max-noise: INT
 *                  Maximum amount of noise to be added to the input rate.
 *          ATTENTION: total round time determined with  initial-round-length and regular-round-length might be
 *          different then loadPatternPeriod. In that case, the experiment is cut off or 0 input rate is provided.
 *
 *   Kafka setup
 *     Required paramters:
 *          enable-bids-topic (false): BOOLEAN
 *                Generate bids topic events. Default value: false
 *          enable-person-topic (false): BOOLEAN
 *                Generate person topic events. Default value: false
 *          enable-auction-topic (false): BOOLEAN
 *                Generate auction topic events. Default value: false
 *      Optional parameters
 *          kafka-server: STRING ("kafka-service:9092")
 *                Kafka server location. Default value: "kafka-service:9092"
 *          uni-bids-partitions (6): Int
 *                Number of partitions for the bids source operators in universalis. Default value: 6
 *          uni-persons-partitions (6): Int
 *                Number of partitions for the persons source operators in universalis. Default value: 6
 *          uni-auctions-partitions (6): Int
 *                Number of partitions for the auctions source operators in universalis. Default value: 6
 *
 * 
 *   Generator setup
 *          iteration-duration-ms (60000): Int
 *                Duration of an iteration in ms. An iteration is the period in which a specific input rate from the
 *                load pattern is being generated. Default value: 60_000ms.
 *          epoch-duration-ms (1000): Int
 *                Duration of an epoch in ms. iteration_duration_ms should be dividable by epoch-duration-ms for the
 *                best generator-performance.
 *          generatorParallelism (1): Int
 *                Amount of threads that running simultaneously while generating data. Default value: 1.
 *
 *    Other
 *      Optional parameters
 *          debugging: BOOLEAN (false)
 *                Enable debugging mode. Default value: false
 */




/**
 * A ParallelSourceFunction that generates Nexmark Bid data
 */
@SuppressWarnings("ALL")
public class BidPersonGeneratorKafka {


    private volatile boolean running = true;

    private List<Integer> loadPattern;

    private boolean debuggingEnabled = false;

    public void run(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        this.debuggingEnabled = params.getBoolean("debugging", false);
        if (this.debuggingEnabled) {
            System.out.println("Debuggin-mode is enabled.");
        }

        // Get load pattern
        LoadPattern loadPatternConfiguration = LoadPatternGenerator.getLoadPatternFromParameterTool(params);
        List<Integer> loadPattern = loadPatternConfiguration.getLoadPattern().f1;
        System.out.println("Running workbench with the following loadpattern:\n\"\"\"\n" +
                loadPatternConfiguration.getLoadPatternTitle() + "\n\"\"\"");
        if (this.debuggingEnabled) { loadPatternConfiguration.plotLoadPattern(); }


        // Get kafka service
        String kafkaServer = params.get("kafka-server", "kafka-service:9092");

        // Get topics to enable
        boolean bidsTopicEnabled = params.getBoolean("enable-bids-topic", false);
        boolean personTopicEnabled = params.getBoolean("enable-persons-topic", false);
        boolean auctionTopicEnabled = params.getBoolean("enable-auctions-topic", false);

        if (!bidsTopicEnabled && !personTopicEnabled && !auctionTopicEnabled) {
            bidsTopicEnabled = true;
            System.out.println("Warning: No topics are enabled. Bids topic is enabled by default.");
        }
        System.out.println("The following topics are enabled:" + (bidsTopicEnabled ? " bids-topic": "")
                + (personTopicEnabled ? " person-topic": "")  + (auctionTopicEnabled ? " auction-topic": "")
        );

        int epochDurationMs = params.getInt("epoch-duration-ms", 100);
        int iterationDurationMs = params.getInt("iteration-duration-ms", 60_000);
        if (iterationDurationMs % epochDurationMs != 0) {
            System.out.println("Warning: for most accurate performance, iterationDurationMs (" + iterationDurationMs +
                    "ms) should be dividable by epoch-duration-ms (" + epochDurationMs + "ms)");
        }

        int generatorParallelism = params.getInt("generator-parallelism", 1);

        // Universalis partitioning
        int uniBidsPartitions = params.getInt("uni-bids-partitions", 6);
        int uniPersonsPartitions = params.getInt("uni-persons-partitions", 6);
        int uniAuctionsPartitions = params.getInt("uni-auctions-partitions", 6);

        Set<String> remainingParameters = params.getUnrequestedParameters();
        if (remainingParameters.size() > 0) {
            System.out.println("Warning: did not recognize the following parameters: " + String.join(",", remainingParameters));
        }

        System.out.println("Instantiating generators with parallelism[" + generatorParallelism + "] iterationtime[" +
                iterationDurationMs + "ms] epochduration[" + epochDurationMs + "ms]." );

        /**
         * Beginning event generation
         */
        // Creating producer

        BidPersonAuctionSourceParallelManager sourceManager = new BidPersonAuctionSourceParallelManager(kafkaServer,
                epochDurationMs, personTopicEnabled, auctionTopicEnabled, bidsTopicEnabled, generatorParallelism, 
                uniBidsPartitions, uniAuctionsPartitions, uniPersonsPartitions);

        // Starting iteration
        long start_time = System.currentTimeMillis();
        // While the loadPatternPeriod is not over
        for (int iteration = 0; iteration < loadPatternConfiguration.getLoadPatternPeriod(); iteration++) {
            int iterationInputRatePerSecond = loadPattern.get(iteration);
            System.out.println("Starting iteration " + iteration + " with " + iterationInputRatePerSecond + "r/s after " +
                    (System.currentTimeMillis() - start_time) / 1000 + "s");
            sourceManager.runGeneratorsForPeriod(iterationInputRatePerSecond, iterationDurationMs);
        }
        System.out.println("Finished workbench execution after " +  (System.currentTimeMillis() - start_time) / 1000 +
                " at time "+ System.currentTimeMillis() / 1000 + "s");
    }

    public static void main(String[] args){
        BidPersonGeneratorKafka bidpersonGenerator = new BidPersonGeneratorKafka();
        try{
            bidpersonGenerator.run(args);
            Thread.sleep(1200000);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

}