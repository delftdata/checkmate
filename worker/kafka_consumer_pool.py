import asyncio
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaConnectionError

from universalis.common.logging import logging

class KafkaConsumerPool(object):

    def __init__(self, worker_id: int, kafka_url: str, topic_partitions: list[TopicPartition]):
        self.worker_id = worker_id
        self.kafka_url = kafka_url
        self.topic_partitions = topic_partitions
        self.consumer_pool: list[AIOKafkaConsumer] = []
        self.index = 0
    
    def __iter__(self):
        return self

    def __next__(self) -> AIOKafkaConsumer:
        conn = self.consumer_pool[self.index]
        next_idx = self.index + 1
        self.index = 0 if next_idx == self.size else next_idx
        return conn
    
    async def start(self):
        for partition in self.topic_partitions:
            self.consumer_pool.append(await self.start_kafka_ingress_consumer(partition))
    
    async def close(self):
        for consumer in self.consumer_pool:
            await consumer.stop()


    async def start_kafka_ingress_consumer(self, partition):
        kafka_ingress_consumer = AIOKafkaConsumer(
            bootstrap_servers=[self.kafka_url],
            group_id="ingress_consumers"
        )
        kafka_ingress_consumer.assign([partition])
        while True:
            try:
                await kafka_ingress_consumer.start()
            except KafkaConnectionError:
                await asyncio.sleep(1)
                logging.info("Waiting for Kafka")
                continue
            break
        return kafka_ingress_consumer
    

