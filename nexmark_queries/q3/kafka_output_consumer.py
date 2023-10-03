import sys
from aiokafka import AIOKafkaConsumer
import pandas as pd

import asyncio
import uvloop
from universalis.common.serialization import msgpack_deserialization

saving_dir = sys.argv[1]
experiment_name = sys.argv[2]

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
        while True:
            data = await consumer.getmany(timeout_ms=1000)
            if not data:
                break
            for _, messages in data.items():
                for msg in messages:
                    # print("consumed: ", msg.key, msg.value, msg.timestamp)
                    records.append((msg.key, msg.value, msg.timestamp))
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()
        pd.DataFrame.from_records(records, columns=['request_id', 'response', 'timestamp']).to_csv(f'{saving_dir}/{experiment_name}/{experiment_name}-output.csv',
                                                                                                   index=False)

uvloop.install()
asyncio.run(consume())
