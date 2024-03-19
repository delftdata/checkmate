import asyncio
import random
import time
import pandas as pd
from timeit import default_timer as timer


import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis

from operators import graph
from operators.graph import source_operator
from operators.graph import count_operator

N_VALUES = 20000
messages_per_second = 8000
sleeps_per_second = 100
sleep_time = 0.00085

UNIVERSALIS_HOST: str = 'localhost'
UNIVERSALIS_PORT: int = 8886
KAFKA_URL = 'localhost:9093'


async def main():
    universalis = Universalis(UNIVERSALIS_HOST, UNIVERSALIS_PORT,
                              ingress_type=IngressTypes.KAFKA,
                              kafka_url=KAFKA_URL)
    await universalis.start()

    # List of channels; (fromOperator, toOperator, boolean)
    # Use None as fromOperator for source operators
    # Use None as toOperator for sink operators
    # The boolean does not matter in these two cases
    channel_list = [
        (None, 'source', False),
        ('source', 'count', True),
        ('count', 'sink', True),
        ('sink', None, False)
    ]

    await universalis.send_channel_list(channel_list)

    ####################################################################################################################
    # SUBMIT STATEFLOW GRAPH ###########################################################################################
    ####################################################################################################################
    await universalis.submit(graph.g)

    print('Graph submitted')

    timestamped_request_ids = {}

    time.sleep(2)

    tasks = []
    # SEND REQUESTS
    value = 1
    for _ in range(60):
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                await asyncio.gather(*tasks)
                tasks = []
                time.sleep(sleep_time)
            key = random.randint(0, 3)
            tasks.append(universalis.send_kafka_event(operator=source_operator,
                                                      key=key,
                                                      function='read',
                                                      params=(value, )))
        await asyncio.gather(*tasks)
        tasks = []
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f'Latency per second: {sec_end2 - sec_start}')
    print('Run done, waiting 15 seconds...')
    time.sleep(15)
    tasks = []
    for key in range(4):
        tasks.append(universalis.send_kafka_event(operator=count_operator,
                                                  key=key,
                                                  function="triggerLogging",
                                                  params=(30.0, )
                                                  ))
    print('SENDING triggerLogging')
    responses = await asyncio.gather(*tasks)
    print(responses)

    await universalis.close()
    pd.DataFrame(timestamped_request_ids.items(), columns=['request_id', 'timestamp']).to_csv('client_requests.csv',
                                                                                              index=False)

uvloop.install()
asyncio.run(main())
