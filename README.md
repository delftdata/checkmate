# CheckMate
CheckMate: Evaluating Checkpointing Protocols for Streaming Dataflows

## Preliminaries

This project requires an environment with *python 3.11* installed. 
Please install the universalis-package and all the requirements of the coordinator
and the worker modules as well as pandas, numpy and matplotlib. 

You can use the following commands:

```
pip install universalis-package/.  
pip install -r coordinator/requirements.txt
pip install -r worker/requirements.txt
pip install pandas numpy matplotlib
```

## Running experiments

In the scripts directory, we provide a number of different scripts that can be used to run the experiments of CheckMate.
The easiest way is to create a csv file formatted as follows:

```
# experiment_name,query,protocol,checkpoint_interval,num_of_workers,input_rate,failure,hot_item_ratio
example-q1-unc,q1,UNC,5,4,4000,true,0.0
example-q1-cor,q1,COR,5,4,4000,true,0.0

```
The csv file _**should not**_ include the header and a newline ***is required*** after the last line of configuration.

Each parameter can take the following values:

|    **Parameter**    |                                                                               **Values**                                                                                |
|:-------------------:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
|   experiment_name   | Any name allowed by your OS. It will be used to create a folder<br> where all the results of the experiment will be stored, as well as<br> prefixing the created files. |
|        query        |                                                                 q1, q3, q8-running, q12-running, cyclic                                                                 |
|      protocol       |                                                                           NOC, UNC, COR, CIC                                                                            |
| checkpoint interval |                                                                              Any value > 0                                                                              |
|   num_of_workers    |                                                             Any integer > 0. Every worker requires 2 cpus.                                                              |
|     input_rate*     |                                                                            Any integer > 0.                                                                             |
|       failure       |                                                                              true / false                                                                               |
|   hot_item_ratio    |                                                      0 $\leq$ x $\leq$ 1<br> (Applicable only in NexMark queries.)                                                      |

\* In the case of the cyclic query, the generator uses 3 threads, so the value should be the 1/3 of the desired total 
input rate.


We provide a csv file containing a sample of exemplary configurations. A csv file containing all the used configurations
in our experiments will follow. 

Using either the provided or your own csv files, you can run the experiments using the following script from the ***root*** 
of the repository:  
`./scripts/run_batch_experiments.sh location_of_the_csv_file directory_to_save_results`


## Alternative way of execution

**Alternatively**, you can also handle the individual components of the pipeline as follows. First, you need to deploy 
the Kafka cluster and the MinIO storage.

### Kafka

To run kafka: `docker compose -f docker-compose-kafka.yml up`

To clear kafka: `docker compose -f docker-compose-kafka.yml down --volumes`

---

### MinIO

To run MinIO: `docker-compose up -f docker-compose-simple-minio.yml up`

To clear MinIO: `docker-compose -f docker-compose-simple-minio.yml down --volumes`

---
  
Then, you can start the stream processing engine and specify the desired scale.

### Stateflow Engine

To run the SPE: `docker-compose up --build --scale worker=4`

To clear the SPE: `docker-compose down --volumes`

