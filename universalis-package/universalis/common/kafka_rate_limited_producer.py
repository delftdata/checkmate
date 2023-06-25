import asyncio
import os

from aiokafka import AIOKafkaProducer
from aiokafka.structs import RecordMetadata
from kafka.errors import KafkaConnectionError
from universalis.common.logging import logging


class KafkaRateLimitedProducer(object):

    def __init__(self, kafka_url: str, rate: int = 150):
        self.kafka_url = kafka_url
        self.producer: AIOKafkaProducer = ...
        self.rate_limiter: asyncio.Semaphore = asyncio.Semaphore(rate)
        self.worker_id: int = -1

    async def start(self, worker_id: int):
        self.worker_id = worker_id
        self.producer = AIOKafkaProducer(
            bootstrap_servers=[self.kafka_url],
            client_id=str(worker_id),
            enable_idempotence=True
        )
        while True:
            try:
                await self.producer.start()
            except KafkaConnectionError:
                await asyncio.sleep(1)
                logging.info("Waiting for Kafka")
                continue
            break

    async def close(self):
        await self.producer.stop()

    async def send_message(self, topic_name, value, partition):
        async with self.rate_limiter:
            res: RecordMetadata = await self.producer.send_and_wait(topic_name,
                                                                    value=value,
                                                                    partition=partition)
        return res.offset
