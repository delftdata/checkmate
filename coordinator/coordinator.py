import os
import time

from confluent_kafka.admin import AdminClient, NewTopic, KafkaException

from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_ingress import IngressTypes
from universalis.common.logging import logging

from scheduler.round_robin import RoundRobin


class NotAStateflowGraph(Exception):
    pass


class Coordinator(object):

    def __init__(self, server_port: int):
        self.worker_counter: int = 0
        self.workers: dict[int, tuple[str, int]] = {}
        self.server_port = server_port

    def register_worker(self, worker_ip: str):
        self.worker_counter += 1
        self.workers[self.worker_counter] = (worker_ip, self.server_port)
        return self.worker_counter

    async def submit_stateflow_graph(self,
                                     network_manager,
                                     stateflow_graph: StateflowGraph,
                                     ingress_type: IngressTypes = IngressTypes.KAFKA,
                                     scheduler_type=None):
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        scheduler = RoundRobin()
        # Create kafka topic per worker
        if ingress_type == IngressTypes.KAFKA:
            self.create_kafka_ingress_topics(stateflow_graph, self.workers.keys())
        # Return the following await, should contain operators/partitions per workerid
        return await scheduler.schedule(self.workers, stateflow_graph, network_manager)

    @staticmethod
    def create_kafka_ingress_topics(stateflow_graph: StateflowGraph, workers):
        kafka_url: str = os.getenv('KAFKA_URL', None)
        if kafka_url is None:
            logging.error('Kafka URL not given')
        while True:
            try:
                client = AdminClient({'bootstrap.servers': kafka_url})
                break
            except KafkaException:
                logging.warning(f'Kafka at {kafka_url} not ready yet, sleeping for 1 second')
                time.sleep(1)
        partitions_per_operator = {}
        topics = []
        for operator in stateflow_graph.nodes.values():
            partitions_per_operator[operator.name] = operator.n_partitions
            topics.append(NewTopic(topic=operator.name, num_partitions=operator.n_partitions, replication_factor=1))
        topics.append(NewTopic(topic='universalis-egress', num_partitions=len(workers), replication_factor=1))

        futures = client.create_topics(topics)
        for topic, future in futures.items():
            try:
                future.result()
                logging.warning(f"Topic {topic} created")
            except KafkaException as e:
                logging.warning(f"Failed to create topic {topic}: {e}")
