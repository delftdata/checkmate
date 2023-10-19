import asyncio
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaConnectionError

from universalis.common.logging import logging


class KafkaConsumerPool(object):

    def __init__(self, worker_id: int, kafka_url: str, topic_partitions: list[tuple[TopicPartition, int]]):
        self.worker_id: int = worker_id
        self.kafka_url: str = kafka_url
        self.topic_partitions: list[tuple[TopicPartition, int]] = topic_partitions
        self.size: int = len(topic_partitions)
        self.consumer_pool: dict[TopicPartition, AIOKafkaConsumer] = {}
        self.index: int = 0

    def __iter__(self):
        return self

    def __next__(self) -> AIOKafkaConsumer:
        conn = list(self.consumer_pool.values())[self.index]
        next_idx = self.index + 1
        self.index = 0 if next_idx == self.size else next_idx
        return conn

    async def start(self):
        for partition, offset in self.topic_partitions:
            self.consumer_pool[partition] = await self.start_kafka_ingress_consumer(partition, offset)

    async def close(self):
        for consumer in self.consumer_pool.values():
            await consumer.stop()

    async def start_kafka_ingress_consumer(self, partition, offset):
        kafka_ingress_consumer = AIOKafkaConsumer(
            bootstrap_servers=[self.kafka_url]
        )
        kafka_ingress_consumer.assign([partition])
        while True:
            try:
                await kafka_ingress_consumer.start()
                kafka_ingress_consumer.seek(partition, offset)
            except KafkaConnectionError:
                await asyncio.sleep(1)
                logging.info("Waiting for Kafka")
                continue
            break
        return kafka_ingress_consumer
