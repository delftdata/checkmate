import asyncio
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from kafka import KafkaProducer

from universalis.common.logging import logging
from universalis.common.kafka_rate_limited_producer import KafkaRateLimitedProducer


class KafkaProducerPool(object):

    def __init__(self, worker_id: int, kafka_url: str, size: int = 4):
        self.worker_id = worker_id
        self.kafka_url = kafka_url
        self.size = size
        self.producer_pool: list[AIOKafkaProducer] = []
        self.limited_producer_pool: list[KafkaRateLimitedProducer] = []
        self.sync_producer_pool: list[KafkaProducer] = []
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

    def pick_sync_producer(self, partition) -> KafkaProducer:
        return self.sync_producer_pool[partition]

    def pick_limited_producer(self, partition) -> KafkaRateLimitedProducer:
        return self.limited_producer_pool[partition]

    async def start(self):
        for _ in range(self.size):
            self.producer_pool.append(await self.start_kafka_egress_producer())

    def start_sync(self):
        for _ in range(self.size):
            self.sync_producer_pool.append(self.start_kafka_sync_producer())
        logging.warning(len(self.sync_producer_pool))

    async def start_limited(self):
        for _ in range(self.size):
            self.limited_producer_pool.append(await self.start_rate_limited_kafka_producer(rate_limit=10))

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

    async def start_rate_limited_kafka_producer(self, rate_limit):
        kafka_producer = KafkaRateLimitedProducer(
            kafka_url=self.kafka_url,
            rate=rate_limit)
        await kafka_producer.start(self.worker_id)
        return kafka_producer

    def start_kafka_sync_producer(self):
        kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_url, acks=1)
        return kafka_producer
