import asyncio
import random
import time
import pandas as pd
from timeit import default_timer as timer

import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis
from universalis.common.logging import logging

from operators import window_graph
from operators.window_graph import tumbling_window_operator

N_VALUES = 40000
messages_per_second=10000
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
    ####################################################################################################################
    # SUBMIT STATEFLOW GRAPH ###########################################################################################
    ####################################################################################################################
    await universalis.submit(window_graph.g)

    print('Graph submitted')

    timestamped_request_ids = {}

    time.sleep(1)
    # QALI IDEA
    # START WINDOW TRIGGER
    tasks = []
    for key in range(0, 6):
        tasks.append(universalis.send_kafka_event(operator=tumbling_window_operator,
                                           key=key,
                                           function="trigger",
                                           params=(10, )
                                           ))
    responses = await asyncio.gather(*tasks)
    print(responses)
    tasks = []
    # SEND REQUESTS
    time.sleep(10)

    value = 1.0
    for _ in range(2):
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                responses = await asyncio.gather(*tasks)
                tasks=[]
                time.sleep(sleep_time)
            key = random.randint(0, 5)
            tasks.append(universalis.send_kafka_event(operator=tumbling_window_operator,
                                                  key=key,
                                                  function='window',
                                                  params=(value, )))
        responses = await asyncio.gather(*tasks)
        tasks=[]
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f'Latency per second: {sec_end2 - sec_start}')
        

    await universalis.close()

    pd.DataFrame(timestamped_request_ids.items(), columns=['request_id', 'timestamp']).to_csv('client_requests.csv',
                                                                                              index=False)


uvloop.install()
asyncio.run(main())
