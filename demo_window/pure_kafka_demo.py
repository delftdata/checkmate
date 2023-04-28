import asyncio
import random
import time
import pandas as pd

import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis

from operators import window_graph
from operators.window_graph import tumbling_window_operator

N_VALUES = 120000


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
    for key in range(6):
        await universalis.send_kafka_event(operator=tumbling_window_operator,
                                           key=key,
                                           function="trigger",
                                           params=(20, )
                                           )

    # SEND REQUESTS
    tasks = []
    for index in range(N_VALUES):
        if index % 1000 == 0:
            time.sleep(1)
        # value = random.uniform(0, 10.0)
        value = 1.0
        key = random.randint(0, 6)
        tasks.append(universalis.send_kafka_event(operator=tumbling_window_operator,
                                                  key=key,
                                                  function='window',
                                                  params=(value, )))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    await universalis.close()

    pd.DataFrame(timestamped_request_ids.items(), columns=['request_id', 'timestamp']).to_csv('client_requests.csv',
                                                                                              index=False)

uvloop.install()
asyncio.run(main())
