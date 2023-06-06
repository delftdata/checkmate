Required parameters

    Load pattern selection
    
      Required parameters:
           load-pattern: STRING
               Load pattern to use during experiment. Possible configurations:  {"cosine", "random", "decrease",
               "increase", "testrun", "testrun-scaleup", "testrun-scaledown", "convergence"}
 
      Optional parameters
           query (1) : INT {1, 3, 11}
               Query to perform the experiments on (default = 1). Possible configurations: {1, 3, 11}
           use-default-configuration (true): BOOLEAN
               Use default configurations of load patterns (default = true)
           experiment-length (140): INT
               Total length of the experiment (default = 140 minutes).
               The convergence experiment overwrites this value with initial-round-length and regular-round-length.
           use-seed (1066): INT
               Seed to use for load pattern generation (default = 1066)
      Configuration specific parameters
           load-pattern = "cosine" && use-default-configuration = false
               cosine-period: INT
                   Time in minutes in which the input rate performs one cosine pattern
               input-rate-mean: INT
                   Mean input-rate
               input-rate-maximum-divergence: INT
                   Amount of events the pattern diverges from the mean value
               max-noise: INT
                   Amount of noise introduced
               Additional optional parameters
 
           load-pattern = "random" && use-default-configuration = false
               initial-input-rate: INT
                   Input rate to start with
               min-divergence: INT
                   Minimum increase (decrease if negative) per minute
               max-divergence: INT
                   Maximum increase (decrease if negative) per minute

           load-pattern = "increase" && use-default-configuration = false
               initial-input-rate: INT
                   Input rate to start with
               total-rate-increase: INT
                   Total increase in rate over a period of 140 minutes.
 
           load-pattern = "decrease" && use-default-configuration = false
               initial-input-rate: INT
                   Input rate to start with
 
           load-pattern = "testrun" && use-default-configuration = false
               inputrate0: INT
                   Initial input rate
               inputrate1: INT
                   Input rate after first time-period ends
               inputrate2: INT
                   Input rate after second time-period ends
 
           load-pattern = "convergence" && use-default-configuration = false

               initial-round-length: INT
                    Length of the initial round.
               regular-round-length: INT
                   Length of the regular round
               round-rates: LIST[INT], as comma seperated line: 1, 2, 3, 4
                   List of rates that should be generated after the provided periods of time.
               max-noise: INT
                   Maximum amount of noise to be added to the input rate.
           ATTENTION: total round time determined with  initial-round-length and regular-round-length might be
           different then loadPatternPeriod. In that case, the experiment is cut off or 0 input rate is provided.
 
    Kafka setup
      Required paramters:
           enable-bids-topic (false): BOOLEAN
                 Generate bids topic events. Default value: false
           enable-person-topic (false): BOOLEAN
                 Generate person topic events. Default value: false
           enable-auction-topic (false): BOOLEAN
                 Generate auction topic events. Default value: false
       Optional parameters
           kafka-server: STRING ("kafka-service:9092")
                 Kafka server location. Default value: "kafka-service:9092"
           uni-bids-partitions (6): Int
                 Number of partitions for the bids source operators in universalis. Default value: 6
           uni-persons-partitions (6): Int
                 Number of partitions for the persons source operators in universalis. Default value: 6
           uni-auctions-partitions (6): Int
                 Number of partitions for the auctions source operators in universalis. Default value: 6
 
  
    Generator setup
           iteration-duration-ms (60000): Int
                 Duration of an iteration in ms. An iteration is the period in which a specific input rate from the
                 load pattern is being generated. Default value: 60_000ms.
           epoch-duration-ms (1000): Int
                 Duration of an epoch in ms. iteration_duration_ms should be dividable by epoch-duration-ms for the
                 best generator-performance.
           generatorParallelism (1): Int
                 Amount of threads that running simultaneously while generating data. Default value: 1.
 
     Other
       Optional parameters
           debugging: BOOLEAN (false)
                 Enable debugging mode. Default value: false
