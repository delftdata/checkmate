# msc-checkpointing-benchmark
Benchmarking checkpointing algorithms for stream processing as a master thesis by Gianni Wiemers.

## Instructions

### Kafka

To run kafka: `docker-compose -f docker-compose-kafka.yml up`

To clear kafka: `docker-compose -f docker-compose-kafka.yml down --volumes`

### STYX

To run styx: `docker-compose up --build --scale worker=2`

To clear styx: `docker-compose down --volumes`


### Demo

First run the `kafka_output_comsumer.py` (rerun it if it throws a warning) then run the `pure_kafka_demo.py`

To get performance metrics kill the `kafka_output_comsumer.py` and then run `calculate_metrics.py`

### Pytest

Replace coordinator with worker to run the CIC tests:
`python -m pytest coordinator/ -rP -W ignore::DeprecationWarning`
