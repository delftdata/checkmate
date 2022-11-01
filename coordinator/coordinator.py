import os
import time

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, NodeNotReadyError, TopicAlreadyExistsError

from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_ingress import IngressTypes
from universalis.common.logging import logging

from scheduler.round_robin import RoundRobin


class NotAStateflowGraph(Exception):
    pass


class Coordinator:

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
        if ingress_type == IngressTypes.KAFKA:
            self.create_kafka_ingress_topics(stateflow_graph)
        await scheduler.schedule(self.workers, stateflow_graph, network_manager)

    @staticmethod
    def create_kafka_ingress_topics(stateflow_graph: StateflowGraph):
        kafka_url: str = os.getenv('KAFKA_URL', None)
        if kafka_url is None:
            logging.error('Kafka URL not given')
        while True:
            try:
                client = KafkaAdminClient(bootstrap_servers=kafka_url)
                break
            except (NoBrokersAvailable, NodeNotReadyError):
                logging.warning(f'Kafka at {kafka_url} not ready yet, sleeping for 1 second')
                time.sleep(1)
        topics = [NewTopic(name=operator.name, num_partitions=operator.n_partitions, replication_factor=1)
                  for operator in stateflow_graph.nodes.values()] + [NewTopic(name='universalis-egress',
                                                                              num_partitions=1,
                                                                              replication_factor=1)]
        try:
            client.create_topics(topics)
        except TopicAlreadyExistsError:
            logging.warning(f'Some of the Kafka topics already exists, job already submitted or rescaling')
