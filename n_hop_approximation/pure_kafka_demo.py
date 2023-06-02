import asyncio
import random
import string
import time
import pandas as pd
from timeit import default_timer as timer

import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis
from universalis.common.logging import logging

from operators import nhop_graph
from operators.nhop_graph import first_map_operator

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
    await universalis.submit(nhop_graph.g)

    print('Graph submitted')

    timestamped_request_ids = {}

    time.sleep(1)

    responses = []
    tasks = []
    # SEND REQUESTS
    time.sleep(10)

    for _ in range(4):
        sec_start = timer()
        for i in range(messages_per_second):
            key = random.randint(0, 5)
            first_node = random.choice(string.ascii_letters)
            second_node = first_node
            while second_node == first_node:
                second_node = random.choice(string.ascii_letters)
            edge = (first_node, second_node, 1)
            print(edge)
            tasks.append(universalis.send_kafka_event(operator=first_map_operator,
                                                  key=key,
                                                  function='find_neighbors',
                                                  params=(edge, key, ))) 
            if i % (messages_per_second // sleeps_per_second) == 0:
                responses = responses + await asyncio.gather(*tasks)
                tasks = []
                time.sleep(sleep_time)
        responses = responses + await asyncio.gather(*tasks)
        tasks = []
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f'Latency per second: {sec_end2 - sec_start}')

    await universalis.close()

    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    pd.DataFrame(timestamped_request_ids.items(), columns=['request_id', 'timestamp']).to_csv('client_requests.csv',
                                                                                              index=False)


uvloop.install()
asyncio.run(main())
