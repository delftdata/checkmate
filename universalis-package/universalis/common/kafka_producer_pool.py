import asyncio
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from universalis.common.logging import logging

class KafkaProducerPool(object):

    def __init__(self, worker_id: int, kafka_url: str, size: int = 4):
        self.worker_id = worker_id
        self.kafka_url = kafka_url
        self.size = size
        self.producer_pool: list[AIOKafkaProducer] = []
        self.index = 0
    
    def __iter__(self):
        return self

    def __next__(self) -> AIOKafkaProducer:
        conn = self.producer_pool[self.index]
        next_idx = self.index + 1
        self.index = 0 if next_idx == self.size else next_idx
        return conn
    
    def pick_producer(self, partition):
        return self.producer_pool[partition]
    
    async def start(self):
        for _ in range(self.size):
            self.producer_pool.append(await self.start_kafka_egress_producer())
    
    async def close(self):
        for producer in self.producer_pool:
            await producer.stop()


    async def start_kafka_egress_producer(self):
        kafka_egress_producer = AIOKafkaProducer(
            bootstrap_servers=[self.kafka_url],
            client_id=str(self.worker_id),
            enable_idempotence=True,
            max_batch_size=10*16384
        )
        while True:
            try:
                await kafka_egress_producer.start()
            except KafkaConnectionError:
                await asyncio.sleep(1)
                logging.info("Waiting for Kafka")
                continue
            break
        return kafka_egress_producer
