import sys
from aiokafka import AIOKafkaConsumer
import pandas as pd

import asyncio
import uvloop
from universalis.common.serialization import msgpack_deserialization
from universalis.common.networking import NetworkingManager

saving_dir = sys.argv[1]
experiment_name = sys.argv[2]
networking = NetworkingManager()

async def consume():
    records = []
    consumer = AIOKafkaConsumer(
        key_deserializer=msgpack_deserialization,
        bootstrap_servers='localhost:9093',
        auto_offset_reset="earliest")
    await consumer.start()
    consumer.subscribe(['auctionsSource', 'personsSource'])
    try:
        # Consume messages
        while True:
            data = await consumer.getmany(timeout_ms=1000)
            if not data:
                break
            for _, messages in data.items():
                for msg in messages:
                    value = networking.decode_message(msg.value)
                    # print("consumed: ", msg.key, value, msg.timestamp)
                    records.append((msg.key, value, msg.timestamp))
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()
        pd.DataFrame.from_records(records, columns=['request_id', 'request', 'timestamp']).to_csv(f'{saving_dir}/{experiment_name}/{experiment_name}-input.csv',
                                                                                                   index=False)

uvloop.install()

asyncio.run(consume())
