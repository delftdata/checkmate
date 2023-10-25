import asyncio
import random
import subprocess
import time
import pandas as pd
from timeit import default_timer as timer

import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.nexmark.setup import setup
from universalis.universalis import Universalis
from universalis.common.logging import logging

from operators.count import count_operator
from operators import q12_graph
from operators.tumbling_window import tumbling_window_operator

UNIVERSALIS_HOST: str = 'localhost'
UNIVERSALIS_PORT: int = 8886
KAFKA_URL = 'localhost:9093'


async def main():
    args = setup()

    universalis = Universalis(UNIVERSALIS_HOST, UNIVERSALIS_PORT,
                              ingress_type=IngressTypes.KAFKA,
                              kafka_url=KAFKA_URL)
    await universalis.start()

    channel_list = [
        (None, 'bidsSource', False),
        ('bidsSource', 'count', True),
        ('count', 'sink', False),
        ('sink', None, False)
    ]

    await universalis.send_channel_list(channel_list)

    ####################################################################################################################
    # SUBMIT STATEFLOW GRAPH ###########################################################################################
    ####################################################################################################################
    await universalis.submit(q12_graph.g)

    print('Graph submitted')

    time.sleep(60)
    # input("Press when you want to start producing")

    # START WINDOW TRIGGER
    tasks = []
    for key in range(4):
        tasks.append(universalis.send_kafka_event(operator=count_operator,
                                                  key=key,
                                                  function="trigger",
                                                  params=(10,)
                                                  ))
    responses = await asyncio.gather(*tasks)
    print(responses)
    tasks = []
    # SEND REQUESTS
    # time.sleep(10)

    subprocess.call(["java", "-jar", "nexmark/target/nexmark-generator-1.0-SNAPSHOT-jar-with-dependencies.jar",
                     "--query", "1",
                     "--generator-parallelism", "1",
                     "--enable-bids-topic", "true",
                     "--load-pattern", "static",
                     "--experiment-length", "1",
                     "--use-default-configuration", "false",
                     "--rate", args.rate,
                     "--max-noise", "0",
                     "--iteration-duration-ms", "90000",
                     "--kafka-server", "localhost:9093",
                     "--uni-bids-partitions", args.bids_partitions
                     ])

    await universalis.close()


uvloop.install()
asyncio.run(main())
