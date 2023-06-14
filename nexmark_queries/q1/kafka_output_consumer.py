import sys
from aiokafka import AIOKafkaConsumer
import pandas as pd

import asyncio
import uvloop
from universalis.common.serialization import msgpack_deserialization

protocol = sys.argv[1]

async def consume():
    records = []
    consumer = AIOKafkaConsumer(
        'universalis-egress',
        key_deserializer=msgpack_deserialization,
        value_deserializer=msgpack_deserialization,
        bootstrap_servers='localhost:9093',
        auto_offset_reset="earliest")
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            print("consumed: ", msg.key, msg.value, msg.timestamp)
            records.append((msg.key, msg.value, msg.timestamp))
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()
        pd.DataFrame.from_records(records, columns=['request_id', 'response', 'timestamp']).to_csv(f'./results/q1/{protocol}-output.csv',
                                                                                                   index=False)

uvloop.install()
asyncio.run(consume())